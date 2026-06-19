using GameServer.Shared.Grains;
using PacketAoiDelta = GameServer.Shared.Protocol.AoiDelta;
using PacketEntityKind = GameServer.Shared.Protocol.EntityKind;
using PacketEntitySnapshot = GameServer.Shared.Protocol.EntitySnapshot;
using PacketErrorRes = GameServer.Shared.Protocol.ErrorRes;
using PacketReliableEvent = GameServer.Shared.Protocol.ReliableEvent;
using PacketWorldPosition = GameServer.Shared.Protocol.WorldPosition;
using WorldEntityKind = GameServer.Shared.World.EntityKind;
using WorldEntitySnapshot = GameServer.Shared.World.EntitySnapshot;
using WorldWorldPosition = GameServer.Shared.World.WorldPosition;

namespace GameServer.Shared.Protocol;

public static class PacketConverters
{
    public static WorldWorldPosition ToWorldPosition(this PacketWorldPosition? position)
    {
        if (position is null)
        {
            return new WorldWorldPosition();
        }

        return new WorldWorldPosition
        {
            MapId = string.IsNullOrWhiteSpace(position.MapId) ? "default" : position.MapId,
            X = position.X,
            Y = position.Y,
            Z = position.Z
        };
    }

    public static PacketWorldPosition ToPacketPosition(this WorldWorldPosition position) => new()
    {
        MapId = position.MapId,
        X = position.X,
        Y = position.Y,
        Z = position.Z
    };

    public static PacketAoiDelta ToPacketAoiDelta(this ZoneDelta delta)
    {
        var packet = new PacketAoiDelta
        {
            MapId = delta.MapId,
            ZoneX = delta.ZoneX,
            ZoneY = delta.ZoneY,
            Sequence = delta.Sequence
        };

        packet.Upserts.AddRange(delta.Upserts.Select(ToPacketEntitySnapshot));
        packet.Removes.AddRange(delta.Removes);
        packet.RemoveReasons.AddRange(delta.Removes.Select(_ => AoiRemoveReason.EntityRemoved));
        return packet;
    }

    public static PacketReliableEvent ToPacketReliableEvent(this ReliableGameEvent reliableEvent) => new()
    {
        EventId = reliableEvent.EventId,
        EventType = reliableEvent.EventType,
        PayloadJson = reliableEvent.PayloadJson
    };

    public static PacketErrorRes ToPacketError(this GatewayError error) => new()
    {
        Code = error.Code,
        Message = error.Message
    };

    private static PacketEntitySnapshot ToPacketEntitySnapshot(WorldEntitySnapshot snapshot) => new()
    {
        EntityId = snapshot.EntityId,
        Kind = ToPacketEntityKind(snapshot.Kind),
        DisplayName = snapshot.DisplayName,
        Position = snapshot.Position.ToPacketPosition(),
        Version = snapshot.Version
    };

    private static PacketEntityKind ToPacketEntityKind(WorldEntityKind kind) => kind switch
    {
        WorldEntityKind.User => PacketEntityKind.User,
        WorldEntityKind.Monster => PacketEntityKind.Monster,
        WorldEntityKind.DropItem => PacketEntityKind.DropItem,
        _ => PacketEntityKind.Unknown
    };
}
