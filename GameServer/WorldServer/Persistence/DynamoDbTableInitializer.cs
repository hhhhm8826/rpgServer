using System.Net.Http;
using System.Text.Json;
using System.Text.Json.Serialization;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace GameServer.WorldServer.Persistence;

public sealed class DynamoDbTableInitializer : IHostedService
{
    private readonly IAmazonDynamoDB _dynamoDb;
    private readonly WorldStorageOptions _options;
    private readonly IHostEnvironment _environment;
    private readonly ILogger<DynamoDbTableInitializer> _logger;

    public DynamoDbTableInitializer(
        IAmazonDynamoDB dynamoDb,
        IOptions<WorldStorageOptions> options,
        IHostEnvironment environment,
        ILogger<DynamoDbTableInitializer> logger)
    {
        _dynamoDb = dynamoDb;
        _options = options.Value;
        _environment = environment;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (!_options.DynamoDb.CreateLocalTablesIfNotExists)
        {
            // 스테이징/운영 테이블 생성은 X, 현재 코드에서는 DynamoDB Local 개발 편의 기능만 제공
            return;
        }

        // 로컬 개발에서만 AWS CLI와 유사하게 JSON 정의로 DynamoDB Local 테이블 생성
        var tableDefinitions = LoadLocalTableDefinitions()
            .DistinctBy(x => x.TableName, StringComparer.Ordinal)
            .ToDictionary(x => x.TableName, StringComparer.Ordinal);

        foreach (var tableName in GetRequiredTableNames())
        {
            if (!tableDefinitions.TryGetValue(tableName, out var table))
            {
                throw new InvalidOperationException($"DynamoDB local table definition for '{tableName}' was not found in '{_options.DynamoDb.LocalTableDefinitionDirectory}'.");
            }

            for (var attempt = 1; attempt <= 5; attempt++)
            {
                try
                {
                    await EnsureTableAsync(table, cancellationToken);
                    break;
                }
                catch (Exception ex) when (IsTransient(ex) && attempt < 5)
                {
                    _logger.LogWarning(ex, "DynamoDB table initialization for {TableName} failed transiently. Retrying attempt {Attempt}.", table.TableName, attempt + 1);
                    await Task.Delay(TimeSpan.FromMilliseconds(500 * attempt), cancellationToken);
                }
            }
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private async Task EnsureTableAsync(DynamoCreateTableDefinition table, CancellationToken cancellationToken)
    {
        try
        {
            await _dynamoDb.DescribeTableAsync(table.TableName, cancellationToken);
            _logger.LogInformation("DynamoDB local table {TableName} already exists.", table.TableName);
            return;
        }
        catch (ResourceInUseException)
        {
            // AppHost에서 WorldServer가 2개 뜨므로 다른 silo의 생성과 경합할 수 있음, 이미 존재하면 생성 시도 안 함
            _logger.LogInformation("DynamoDB local table {TableName} is being created by another silo.", table.TableName);
            return;
        }
        catch (ResourceNotFoundException)
        {
            _logger.LogInformation("Creating DynamoDB local table {TableName} from JSON definition.", table.TableName);
        }

        try
        {
            await _dynamoDb.CreateTableAsync(table.ToCreateTableRequest(), cancellationToken);
        }
        catch (ResourceInUseException)
        {
            _logger.LogInformation("DynamoDB local table {TableName} was created by another silo.", table.TableName);
        }
    }

    private IEnumerable<string> GetRequiredTableNames()
    {
        yield return _options.DynamoDb.UserStateTableName;
        yield return _options.DynamoDb.UserInventoryTableName;
        yield return _options.DynamoDb.UserCurrencyTableName;
    }

    private IEnumerable<DynamoCreateTableDefinition> LoadLocalTableDefinitions()
    {
        var directory = ResolveDefinitionDirectory();
        if (!Directory.Exists(directory))
        {
            throw new DirectoryNotFoundException($"DynamoDB local table definition directory was not found: {directory}");
        }

        foreach (var file in Directory.EnumerateFiles(directory, "*.json").OrderBy(x => x, StringComparer.Ordinal))
        {
            using var stream = File.OpenRead(file);
            var table = JsonSerializer.Deserialize<DynamoCreateTableDefinition>(stream, JsonOptions)
                ?? throw new InvalidOperationException($"DynamoDB table definition is empty: {file}");

            table.Validate(file);
            yield return table;
        }
    }

    private string ResolveDefinitionDirectory()
    {
        var configured = _options.DynamoDb.LocalTableDefinitionDirectory;
        if (Path.IsPathFullyQualified(configured))
        {
            return configured;
        }

        foreach (var root in new[] { _environment.ContentRootPath, Directory.GetCurrentDirectory(), AppContext.BaseDirectory })
        {
            var current = new DirectoryInfo(root);
            while (current is not null)
            {
                var candidate = Path.Combine(current.FullName, configured);
                if (Directory.Exists(candidate))
                {
                    return candidate;
                }

                current = current.Parent;
            }
        }

        return Path.Combine(_environment.ContentRootPath, configured);
    }

    private static bool IsTransient(Exception ex)
        => ex is HttpRequestException
            or HttpIOException
            or IOException
            or TaskCanceledException;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        Converters = { new JsonStringEnumConverter() }
    };

    private sealed class DynamoCreateTableDefinition
    {
        public string TableName { get; set; } = "";

        public List<DynamoAttributeDefinition> AttributeDefinitions { get; set; } = [];

        public List<DynamoKeySchemaElement> KeySchema { get; set; } = [];

        public string BillingMode { get; set; } = "PAY_PER_REQUEST";

        public DynamoProvisionedThroughput? ProvisionedThroughput { get; set; }

        public void Validate(string source)
        {
            if (string.IsNullOrWhiteSpace(TableName))
            {
                throw new InvalidOperationException($"DynamoDB table definition has no TableName: {source}");
            }

            if (AttributeDefinitions.Count == 0)
            {
                throw new InvalidOperationException($"DynamoDB table definition has no AttributeDefinitions: {source}");
            }

            if (KeySchema.Count == 0)
            {
                throw new InvalidOperationException($"DynamoDB table definition has no KeySchema: {source}");
            }
        }

        public CreateTableRequest ToCreateTableRequest()
        {
            var request = new CreateTableRequest
            {
                TableName = TableName,
                AttributeDefinitions = AttributeDefinitions
                    .Select(x => new AttributeDefinition(x.AttributeName, ScalarAttributeType.FindValue(x.AttributeType)))
                    .ToList(),
                KeySchema = KeySchema
                    .Select(x => new KeySchemaElement(x.AttributeName, KeyType.FindValue(x.KeyType)))
                    .ToList(),
                BillingMode = Amazon.DynamoDBv2.BillingMode.FindValue(BillingMode)
            };

            if (string.Equals(BillingMode, "PROVISIONED", StringComparison.OrdinalIgnoreCase)
                && ProvisionedThroughput is not null)
            {
                request.ProvisionedThroughput = new Amazon.DynamoDBv2.Model.ProvisionedThroughput(
                    ProvisionedThroughput.ReadCapacityUnits,
                    ProvisionedThroughput.WriteCapacityUnits);
            }

            return request;
        }
    }

    private sealed class DynamoAttributeDefinition
    {
        public string AttributeName { get; set; } = "";

        public string AttributeType { get; set; } = "";
    }

    private sealed class DynamoKeySchemaElement
    {
        public string AttributeName { get; set; } = "";

        public string KeyType { get; set; } = "";
    }

    private sealed class DynamoProvisionedThroughput
    {
        public long ReadCapacityUnits { get; set; }

        public long WriteCapacityUnits { get; set; }
    }
}
