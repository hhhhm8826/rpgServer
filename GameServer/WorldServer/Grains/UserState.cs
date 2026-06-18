using GameServer.Shared.Grains;
using GameServer.Shared.World;
using Orleans;

namespace GameServer.WorldServer.Grains;

[GenerateSerializer]
public sealed class UserState
{
    [Id(0)]
    public long UserDbId { get; set; }

    [Id(1)]
    public bool IsOnline { get; set; }

    [Id(2)]
    public string SessionId { get; set; } = "";

    [Id(3)]
    public string GatewayId { get; set; } = "";

    [Id(4)]
    public WorldPosition Position { get; set; } = new();

    [Id(5)]
    public string CurrentZoneKey { get; set; } = "";

    [Id(7)]
    public long Version { get; set; }

    // 미구현 골격: user channel 기반 재접속/유저 컨텐츠 reliable event 재전송용
    [Id(8)]
    public List<ReliableGameEvent> PendingReconnectReliableEvents { get; set; } = [];
}
