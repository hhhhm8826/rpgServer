using System.Text.Json;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace GameServer.WorldServer.Persistence;

public sealed class WriteBehindGrainStorage
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly IConnectionMultiplexer _redis;
    private readonly IAmazonDynamoDB _dynamoDb;
    private readonly ILogger<WriteBehindGrainStorage> _logger;
    private readonly WorldStorageOptions _options;

    public WriteBehindGrainStorage(
        IConnectionMultiplexer redis,
        IAmazonDynamoDB dynamoDb,
        IOptions<WorldStorageOptions> options,
        ILogger<WriteBehindGrainStorage> logger)
    {
        _redis = redis;
        _dynamoDb = dynamoDb;
        _logger = logger;
        _options = options.Value;
    }

    public async Task<T?> ReadAsync<T>(StorageKey key, CancellationToken cancellationToken = default)
    {
        var db = GetRedisDatabase();
        var redisKey = ToRedisKey(key);
        var payload = await db.StringGetAsync(redisKey);

        // 비활성화 TTL 캐시는 재활성화될 경우 다시 Hot State로 전환함
        if (payload.HasValue)
        {
            await db.KeyPersistAsync(redisKey);
            return JsonSerializer.Deserialize<T>(payload.ToString(), JsonOptions);
        }

        GetItemResponse response;
        try
        {
            response = await _dynamoDb.GetItemAsync(new GetItemRequest
            {
                TableName = GetTableName(key),
                ConsistentRead = true,
                Key = ToDynamoKey(key)
            }, cancellationToken).WaitAsync(_options.DynamoDbReadTimeout, cancellationToken);
        }
        catch (Exception ex) when (ex is TimeoutException or ResourceNotFoundException or AmazonDynamoDBException)
        {
            _logger.LogWarning(ex, "DynamoDB read-through failed for grain state {StorageKey}; using empty state.", key.Value);
            return default;
        }

        if (response.Item is null || response.Item.Count == 0 || !response.Item.TryGetValue("Payload", out var dynamoPayload))
        {
            return default;
        }

        await db.StringSetAsync(redisKey, dynamoPayload.S);
        return JsonSerializer.Deserialize<T>(dynamoPayload.S, JsonOptions);
    }

    public async Task WriteAsync<T>(StorageKey key, T state, CancellationToken cancellationToken = default)
        => await WriteAsync(key, state, TimeSpan.Zero, cancellationToken);

    public async Task WriteAsync<T>(
        StorageKey key,
        T state,
        TimeSpan flushAfter,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var payload = JsonSerializer.Serialize(state, JsonOptions);
        var db = GetRedisDatabase();
        await db.StringSetAsync(ToRedisKey(key), payload);
        // Redis에는 즉시 반영하고 DynamoDB flush 시점만 dirty set score로 지연시킴
        var dueAt = DateTimeOffset.UtcNow.Add(flushAfter).ToUnixTimeMilliseconds();
        await db.SortedSetAddAsync(_options.DirtySetKey, key.Value, dueAt);
    }

    public async Task FlushAsync(StorageKey key, CancellationToken cancellationToken = default)
        => await FlushCoreAsync(key, removeDirty: false, cancellationToken);

    public async Task FlushAndClearDirtyAsync(StorageKey key, CancellationToken cancellationToken = default)
        => await FlushCoreAsync(key, removeDirty: true, cancellationToken);

    private async Task FlushCoreAsync(StorageKey key, bool removeDirty, CancellationToken cancellationToken)
    {
        var db = GetRedisDatabase();
        var payload = await db.StringGetAsync(ToRedisKey(key));
        if (!payload.HasValue)
        {
            if (removeDirty)
            {
                await db.SortedSetRemoveAsync(_options.DirtySetKey, key.Value);
            }

            return;
        }

        // 명시적 Flush는 Redis 최신 payload를 DynamoDB에 반영함
        await PutDynamoItemAsync(key, payload!, cancellationToken);
        if (removeDirty)
        {
            await db.SortedSetRemoveAsync(_options.DirtySetKey, key.Value);
        }
    }

    // Flush 완료 후 TTL을 부여해 재활성화 시 Redis에서 빠르게 복구할 수 있도록 함
    public async Task ExpireRedisAsync(StorageKey key, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await GetRedisDatabase().KeyExpireAsync(ToRedisKey(key), _options.DeactivateRedisTTL);
    }

    // 여러 WorldServer가 동시에 dirty set을 훑지 않도록 짧은 Redis lock을 사용
    public async Task<int> FlushDirtyBatchAsync(CancellationToken cancellationToken)
    {
        var db = GetRedisDatabase();
        var lockToken = Guid.NewGuid().ToString("N");
        var lockTaken = await db.StringSetAsync(
            _options.FlushLockKey,
            lockToken,
            TimeSpan.FromSeconds(Math.Max(2, _options.DirtyScanIntervalSec + 1)),
            When.NotExists);

        if (!lockTaken)
        {
            return 0;
        }

        try
        {
            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var entries = await db.SortedSetRangeByScoreWithScoresAsync(
                _options.DirtySetKey,
                stop: now,
                take: Math.Max(1, _options.FlushBatchSize));

            var flushed = 0;
            foreach (var entry in entries)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var value = entry.Element.ToString();
                if (string.IsNullOrWhiteSpace(value))
                {
                    continue;
                }

                var key = ParseStorageKey(value);
                await FlushAsync(key, cancellationToken);

                // Flush 중 새 write가 들어오면 score가 올라가므로 dirty를 제거하지 않음
                var latestScore = await db.SortedSetScoreAsync(_options.DirtySetKey, value);
                if (latestScore is not null && latestScore <= entry.Score)
                {
                    await db.SortedSetRemoveAsync(_options.DirtySetKey, value);
                }

                flushed++;
            }

            if (flushed > 0)
            {
                _logger.LogInformation("Flushed {Count} dirty grain states to DynamoDB.", flushed);
            }

            return flushed;
        }
        finally
        {
            await ReleaseLockAsync(db, _options.FlushLockKey, lockToken);
        }
    }

    private async Task PutDynamoItemAsync(StorageKey key, string payload, CancellationToken cancellationToken)
    {
        await _dynamoDb.PutItemAsync(new PutItemRequest
        {
            TableName = GetTableName(key),
            Item = ToDynamoItem(key, payload)
        }, cancellationToken);
    }

    private string GetTableName(StorageKey key) => key.Kind switch
    {
        StorageKey.UserKind => _options.DynamoDb.UserStateTableName,
        StorageKey.UserInventoryKind => _options.DynamoDb.UserInventoryTableName,
        StorageKey.UserCurrencyKind => _options.DynamoDb.UserCurrencyTableName,
        _ => throw new InvalidOperationException($"Unsupported storage key kind: {key.Kind}")
    };

    private static Dictionary<string, AttributeValue> ToDynamoKey(StorageKey key)
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = key.Id }
        };

        if (RequiresSortKey(key.Kind))
        {
            if (string.IsNullOrWhiteSpace(key.SortId))
            {
                throw new InvalidOperationException($"Storage key kind {key.Kind} requires a sort key.");
            }

            item["SK"] = new AttributeValue { S = key.SortId };
        }

        return item;
    }

    private static Dictionary<string, AttributeValue> ToDynamoItem(StorageKey key, string payload)
    {
        var item = ToDynamoKey(key);
        item["Kind"] = new AttributeValue { S = key.Kind };
        item["Payload"] = new AttributeValue { S = payload };
        item["ETag"] = new AttributeValue { S = Guid.NewGuid().ToString("N") };
        return item;
    }

    private static bool RequiresSortKey(string kind)
        => kind == StorageKey.UserInventoryKind;

    private IDatabase GetRedisDatabase() => _redis.GetDatabase(_options.RedisDatabase);

    // Redis key는 테이블 분리와 무관하게 storage를 prefix에 포함함
    private RedisKey ToRedisKey(StorageKey key) => $"{_options.RedisKeyPrefix}:{key.Value}";

    private static StorageKey ParseStorageKey(string value)
    {
        var separator = value.IndexOf(':', StringComparison.Ordinal);
        if (separator <= 0 || separator == value.Length - 1)
        {
            throw new InvalidOperationException($"Invalid storage key: {value}");
        }

        var kind = value[..separator];
        var id = value[(separator + 1)..];

        if (kind != StorageKey.UserInventoryKind)
        {
            return new StorageKey(kind, id);
        }

        var sortSeparator = id.IndexOf(':', StringComparison.Ordinal);
        if (sortSeparator <= 0 || sortSeparator == id.Length - 1)
        {
            throw new InvalidOperationException($"Invalid inventory storage key: {value}");
        }

        return new StorageKey(kind, id[..sortSeparator], id[(sortSeparator + 1)..]);
    }

    private static async Task ReleaseLockAsync(IDatabase db, RedisKey lockKey, string lockToken)
    {
        const string script = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end";
        await db.ScriptEvaluateAsync(
            script,
            [lockKey],
            [new RedisValue(lockToken)]);
    }
}
