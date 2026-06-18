using GameServer.GatewayServer.Networking;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace GameServer.GatewayServer.Sessions;

public sealed class RedisSessionStore
{
    private readonly IConnectionMultiplexer _redis;
    private readonly GatewayOptions _options;
    private readonly GatewayEventBus _eventBus;

    public RedisSessionStore(
        IConnectionMultiplexer redis,
        IOptions<GatewayOptions> options,
        GatewayEventBus eventBus)
    {
        _redis = redis;
        _options = options.Value;
        _eventBus = eventBus;
    }

    public async Task<bool> TryRegisterAsync(long userDbId, string sessionId)
    {
        var db = _redis.GetDatabase();
        var userSessionKey = UserSessionKey(userDbId);
        var sessionKey = SessionKey(sessionId);

        // userDbId -> sessionId 키가 로그인 중복과 Gateway scale-out 세션 소유권을 결정
        var registered = await db.StringSetAsync(userSessionKey, sessionId, _options.SessionTTL, When.NotExists);
        if (!registered && !await TryTakeOverStaleSessionAsync(db, userSessionKey, sessionKey, sessionId))
        {
            return false;
        }

        await db.HashSetAsync(sessionKey,
        [
            new HashEntry("userDbId", userDbId),
            new HashEntry("gatewayId", _options.GatewayId),
            new HashEntry("lastSeenUnixMs", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())
        ]);
        await db.KeyExpireAsync(sessionKey, _options.SessionTTL);
        return true;
    }

    private async Task<bool> TryTakeOverStaleSessionAsync(IDatabase db, RedisKey userSessionKey, RedisKey newSessionKey, string newSessionId)
    {
        var existingSessionId = await db.StringGetAsync(userSessionKey);
        if (existingSessionId.IsNullOrEmpty)
        {
            return await db.StringSetAsync(userSessionKey, newSessionId, _options.SessionTTL, When.NotExists);
        }

        var existingSessionKey = SessionKey(existingSessionId.ToString());
        var existingGatewayId = await db.HashGetAsync(existingSessionKey, "gatewayId");
        if (!existingGatewayId.IsNullOrEmpty && await _eventBus.IsGatewayAliveAsync(existingGatewayId.ToString()))
        {
            // 기존 Gateway가 살아 있으면 중복 로그인으로 보고 client retry를 유도
            return false;
        }

        // 죽은 Gateway의 세션만 원자적으로 새 sessionId로 교체
        const string script = "if redis.call('GET', KEYS[1]) == ARGV[1] then redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3]); redis.call('DEL', KEYS[2]); redis.call('DEL', KEYS[3]); return 1 else return 0 end";
        var result = (int)await db.ScriptEvaluateAsync(
            script,
            [userSessionKey, existingSessionKey, newSessionKey],
            [existingSessionId, newSessionId, (long)_options.SessionTTL.TotalMilliseconds]);
        return result == 1;
    }

    public async Task RefreshAsync(long userDbId, string sessionId)
    {
        var db = _redis.GetDatabase();
        await db.HashSetAsync(SessionKey(sessionId), "lastSeenUnixMs", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        // 연결 유지 중에는 양쪽 키 TTL을 함께 연장해 죽은 키로 오인하는 상황을 방지
        await db.KeyExpireAsync(SessionKey(sessionId), _options.SessionTTL);
        await db.KeyExpireAsync(UserSessionKey(userDbId), _options.SessionTTL);
    }

    public async Task RemoveAsync(long userDbId, string sessionId)
    {
        var db = _redis.GetDatabase();
        await db.KeyDeleteAsync(SessionKey(sessionId));

        // 다른 Gateway가 이미 같은 userDbId를 선점했다면 새 세션 키는 지우지 않음
        const string script = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end";
        await db.ScriptEvaluateAsync(
            script,
            [UserSessionKey(userDbId)],
            [new RedisValue(sessionId)]);
    }

    private static RedisKey UserSessionKey(long userDbId) => $"gateway:user-session:{userDbId}";

    private static RedisKey SessionKey(string sessionId) => $"gateway:session:{sessionId}";
}
