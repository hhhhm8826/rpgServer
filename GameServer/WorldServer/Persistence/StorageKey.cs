namespace GameServer.WorldServer.Persistence;

public readonly record struct StorageKey(string Kind, string PartitionId, string SortId = "")
{
    public const string UserKind = "user";

    public const string UserInventoryKind = "user-inventory";

    public const string UserCurrencyKind = "user-currency";

    public string Id => PartitionId;

    public string Value => string.IsNullOrWhiteSpace(SortId)
        ? $"{Kind}:{PartitionId}"
        : $"{Kind}:{PartitionId}:{SortId}";

    public static StorageKey User(long userDbId) => new(UserKind, userDbId.ToString());

    public static StorageKey UserInventoryItem(long userDbId, long itemDbId) => new(UserInventoryKind, userDbId.ToString(), itemDbId.ToString());

    public static StorageKey UserCurrency(long userDbId) => new(UserCurrencyKind, userDbId.ToString());
}
