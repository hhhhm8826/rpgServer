using GameServer.Shared.World;
using ProtocolAoiRemoveReason = GameServer.Shared.Protocol.AoiRemoveReason;
using ProtocolErrorCode = GameServer.Shared.Protocol.ErrorCode;
using ProtocolServerDeliveryPolicy = GameServer.Shared.Protocol.ServerDeliveryPolicy;
using Orleans.Concurrency;
using Orleans;

namespace GameServer.Shared.Grains;

public interface IUserGrain : IGrainWithIntegerKey
{
    Task<LoginResult> LoginAsync(LoginCommand command);

    Task<MoveResult> MoveAsync(MoveCommand command);

    Task LogoutAsync(LogoutCommand command);

    Task AckServerEventAsync(string eventId);
}

public interface IZoneGrain : IGrainWithStringKey
{
    [OneWay]
    Task EnterAsync(EntitySnapshot entity);

    // 같은 Zone 이동은 현재 검증 정책이 비어 있어 OneWay로 반영함.
    // 검증이 추가되면 ZoneGrain 직렬 병목을 피할 설계가 먼저 필요함
    [OneWay]
    Task SubmitMoveAsync(ZoneMoveCommand command);

    Task<ZoneMoveResult> MoveAsync(ZoneMoveCommand command);

    Task TransferOutAsync(EntitySnapshot entity);

    Task LeaveAsync(long entityId);

    Task<List<EntitySnapshot>> GetSnapshotAsync();
}

[GenerateSerializer]
public enum GatewayServerEventKind
{
    Unknown = 0,
    AoiDelta = 1,
    ReliableEvent = 2,
    Error = 3
}

[GenerateSerializer]
public sealed class GatewayServerEvent
{
    [Id(0)]
    public string SessionId { get; set; } = "";

    [Id(1)]
    public long UserDbId { get; set; }

    [Id(2)]
    public GatewayServerEventKind Kind { get; set; }

    [Id(3)]
    public ZoneDelta? AoiDelta { get; set; }

    [Id(4)]
    public ReliableGameEvent? ReliableEvent { get; set; }

    [Id(5)]
    public GatewayError? Error { get; set; }
}

[GenerateSerializer]
public sealed class ZoneDelta
{
    [Id(0)]
    public string MapId { get; set; } = "default";

    [Id(1)]
    public int ZoneX { get; set; }

    [Id(2)]
    public int ZoneY { get; set; }

    [Id(3)]
    public long Sequence { get; set; }

    [Id(4)]
    public List<EntitySnapshot> Upserts { get; set; } = [];

    [Id(5)]
    public List<long> Removes { get; set; } = [];

    [Id(6)]
    public ProtocolServerDeliveryPolicy DeliveryPolicy { get; set; } = ProtocolServerDeliveryPolicy.Reliable;

    [Id(7)]
    public List<ProtocolAoiRemoveReason> RemoveReasons { get; set; } = [];
}

[GenerateSerializer]
public sealed class ReliableGameEvent
{
    [Id(0)]
    public string EventId { get; set; } = "";

    [Id(1)]
    public string EventType { get; set; } = "";

    [Id(2)]
    public string PayloadJson { get; set; } = "";
}

[GenerateSerializer]
public sealed class GatewayError
{
    [Id(0)]
    public ProtocolErrorCode Code { get; set; }

    [Id(1)]
    public string Message { get; set; } = "";
}

[GenerateSerializer]
public sealed class LoginCommand
{
    [Id(0)]
    public long UserDbId { get; set; }

    [Id(1)]
    public string SessionId { get; set; } = "";

    [Id(2)]
    public string GatewayId { get; set; } = "";

    [Id(3)]
    public WorldPosition SpawnPosition { get; set; } = new();

}

[GenerateSerializer]
public sealed class LoginResult
{
    [Id(0)]
    public bool Accepted { get; set; }

    [Id(1)]
    public ProtocolErrorCode ErrorCode { get; set; }

    [Id(2)]
    public WorldPosition Position { get; set; } = new();

    [Id(3)]
    public string Message { get; set; } = "";
}

[GenerateSerializer]
public sealed class MoveCommand
{
    [Id(0)]
    public long UserDbId { get; set; }

    [Id(1)]
    public string SessionId { get; set; } = "";

    [Id(2)]
    public WorldPosition Position { get; set; } = new();
}

[GenerateSerializer]
public sealed class MoveResult
{
    [Id(0)]
    public bool Accepted { get; set; }

    [Id(1)]
    public ProtocolErrorCode ErrorCode { get; set; }

    [Id(2)]
    public WorldPosition AuthoritativePosition { get; set; } = new();

    [Id(3)]
    public string Message { get; set; } = "";
}

[GenerateSerializer]
public sealed class ZoneMoveCommand
{
    [Id(0)]
    public EntitySnapshot Entity { get; set; } = new();

    [Id(1)]
    public WorldPosition RequestedPosition { get; set; } = new();
}

[GenerateSerializer]
public sealed class ZoneMoveResult
{
    [Id(0)]
    public bool Accepted { get; set; }

    [Id(1)]
    public ProtocolErrorCode ErrorCode { get; set; }

    [Id(2)]
    public WorldPosition AuthoritativePosition { get; set; } = new();

    [Id(3)]
    public string AuthoritativeZoneKey { get; set; } = "";

    [Id(4)]
    public string Message { get; set; } = "";
}

[GenerateSerializer]
public sealed class LogoutCommand
{
    [Id(0)]
    public long UserDbId { get; set; }

    [Id(1)]
    public string SessionId { get; set; } = "";

    [Id(2)]
    public string Reason { get; set; } = "";
}
