namespace GameServer.WorldServer.Persistence;

public sealed class WorldStorageOptions
{
    public const string SectionName = "WorldStorage";

    public int RedisDatabase { get; set; }

    public string RedisKeyPrefix { get; set; } = "rpg:grain";

    public string DirtySetKey { get; set; } = "rpg:grain:dirty";

    public string FlushLockKey { get; set; } = "rpg:grain:dirty:lock";

    public int DirtyScanIntervalSec { get; set; } = 1;

    public int UserDefaultSaveIntervalSec { get; set; } = 5;

    public int UserLowPrioritySaveIntervalSec { get; set; } = 30;

    public int CriticalStateFlushDelaySec { get; set; } = 1;

    public int FlushBatchSize { get; set; } = 250;

    public int DeactivateRedisTTLSec { get; set; } = 300;

    public int DynamoDbReadTimeoutSec { get; set; } = 5;

    public DynamoDbOptions DynamoDb { get; set; } = new();

    public TimeSpan DirtyScanInterval => TimeSpan.FromSeconds(Math.Max(1, DirtyScanIntervalSec));

    public TimeSpan UserDefaultSaveInterval => TimeSpan.FromSeconds(Math.Max(1, UserDefaultSaveIntervalSec));

    public TimeSpan UserLowPrioritySaveInterval => TimeSpan.FromSeconds(Math.Max(1, UserLowPrioritySaveIntervalSec));

    public TimeSpan CriticalStateFlushDelay => TimeSpan.FromSeconds(Math.Max(1, CriticalStateFlushDelaySec));

    public TimeSpan DeactivateRedisTTL => TimeSpan.FromSeconds(Math.Max(1, DeactivateRedisTTLSec));

    public TimeSpan DynamoDbReadTimeout => TimeSpan.FromSeconds(Math.Max(1, DynamoDbReadTimeoutSec));
}

public sealed class DynamoDbOptions
{
    public string UserStateTableName { get; set; } = "RpgUserState";

    public string UserInventoryTableName { get; set; } = "RpgUserInventory";

    public string UserCurrencyTableName { get; set; } = "RpgUserCurrency";

    public string ServiceUrl { get; set; } = "http://localhost:8000";

    public string Region { get; set; } = "ap-northeast-2";

    public string AccessKey { get; set; } = "local";

    public string SecretKey { get; set; } = "local";

    public bool CreateLocalTablesIfNotExists { get; set; }

    public string LocalTableDefinitionDirectory { get; set; } = "Dynamodb/Tables";
}
