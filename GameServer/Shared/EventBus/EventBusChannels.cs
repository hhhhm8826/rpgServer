using GameServer.Shared.World;

namespace GameServer.Shared.EventBus;

public static class EventBusChannels
{
    public const string GatewaySetKey = "gs:alive:gateways";

    public static string GatewayAliveKey(string gatewayId) => $"gs:alive:gateway:{gatewayId}";

    public static string GatewayBroadcast(string gatewayId) => $"gs:broadcast:gateway:{gatewayId}";

    public static string User(long userDbId) => $"gs:user:{userDbId}";

    public static string Zone(ZoneAddress address) => Zone(address.MapId, address.CellX, address.CellY);

    public static string Zone(string mapId, int cellX, int cellY) => $"gs:{mapId}:zone:{cellX}:{cellY}";
}
