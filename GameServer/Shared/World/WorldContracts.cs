using Orleans;

namespace GameServer.Shared.World;

[GenerateSerializer]
public enum EntityKind
{
    Unknown = 0,
    User = 1,
    Monster = 2,
    DropItem = 3
}

[GenerateSerializer]
public sealed class WorldPosition
{
    [Id(0)]
    public string MapId { get; set; } = "default";

    [Id(1)]
    public float X { get; set; }

    [Id(2)]
    public float Y { get; set; }

    [Id(3)]
    public float Z { get; set; }

    public double DistanceSquared2D(WorldPosition other)
    {
        var dx = X - other.X;
        var dy = Y - other.Y;
        return dx * dx + dy * dy;
    }

    public WorldPosition Clone() => new()
    {
        MapId = MapId,
        X = X,
        Y = Y,
        Z = Z
    };
}

[GenerateSerializer]
public sealed class ZoneAddress
{
    [Id(0)]
    public string MapId { get; set; } = "default";

    [Id(1)]
    public int CellX { get; set; }

    [Id(2)]
    public int CellY { get; set; }

    public string ToKey() => ZoneMath.ToKey(MapId, CellX, CellY);
}

[GenerateSerializer]
public sealed class EntitySnapshot
{
    [Id(0)]
    public long EntityId { get; set; }

    [Id(1)]
    public EntityKind Kind { get; set; }

    [Id(2)]
    public string DisplayName { get; set; } = "";

    [Id(3)]
    public WorldPosition Position { get; set; } = new();

    [Id(4)]
    public long Version { get; set; }

    public EntitySnapshot Clone() => new()
    {
        EntityId = EntityId,
        Kind = Kind,
        DisplayName = DisplayName,
        Position = Position.Clone(),
        Version = Version
    };
}

public static class ZoneMath
{
    public const float CellSizeMeters = 50f;

    public static ZoneAddress FromPosition(WorldPosition position) => new()
    {
        MapId = position.MapId,
        CellX = FloorToCell(position.X),
        CellY = FloorToCell(position.Y)
    };

    public static IReadOnlyList<ZoneAddress> Neighbors5x5(ZoneAddress center)
        => NeighborsSquare(center, 2);

    public static IReadOnlyList<ZoneAddress> NeighborsSquare(ZoneAddress center, int radius)
    {
        radius = Math.Max(0, radius);
        var size = radius * 2 + 1;
        var zones = new List<ZoneAddress>(size * size);
        for (var y = center.CellY - radius; y <= center.CellY + radius; y++)
        {
            for (var x = center.CellX - radius; x <= center.CellX + radius; x++)
            {
                zones.Add(new ZoneAddress
                {
                    MapId = center.MapId,
                    CellX = x,
                    CellY = y
                });
            }
        }

        return zones;
    }

    public static string ToKey(string mapId, int cellX, int cellY) => $"{mapId}:{cellX}:{cellY}";

    private static int FloorToCell(float value) => (int)MathF.Floor(value / CellSizeMeters);
}
