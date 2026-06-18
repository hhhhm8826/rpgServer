using GameServer.Shared.Grains;
using GameServer.Shared.World;
using GameServer.WorldServer.EventBus;
using Orleans;
using Orleans.Runtime;
using ProtocolServerDeliveryPolicy = GameServer.Shared.Protocol.ServerDeliveryPolicy;

namespace GameServer.WorldServer.Grains;

public sealed class ZoneGrain : Grain, IZoneGrain
{
    // 이동 AOI는 200ms마다 최신 위치만 Zone 단위로 발행함
    private static readonly TimeSpan MoveBroadcastInterval = TimeSpan.FromMilliseconds(200);

    private readonly WorldEventBusPublisher _publisher;
    // 추후 Zone 소유 휘발성 객체 추가 필요함. 몬스터, 드랍 아이템 등
    private readonly Dictionary<long, EntitySnapshot> _entities = new();
    // 같은 tick 안의 중복 이동은 entity별 최신 snapshot 하나만 유지함
    private readonly Dictionary<long, EntitySnapshot> _pendingMoveUpserts = new();
    private IGrainTimer? _moveFlushTimer;
    private long _sequence;

    public ZoneGrain(WorldEventBusPublisher publisher)
    {
        _publisher = publisher;
    }

    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        _moveFlushTimer = this.RegisterGrainTimer(
            FlushPendingMovesAsync,
            new GrainTimerCreationOptions
            {
                DueTime = MoveBroadcastInterval,
                Period = MoveBroadcastInterval,
                Interleave = false,
                KeepAlive = false
            });

        return base.OnActivateAsync(cancellationToken);
    }

    public override Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        _moveFlushTimer?.Dispose();
        _moveFlushTimer = null;
        return base.OnDeactivateAsync(reason, cancellationToken);
    }

    public async Task EnterAsync(EntitySnapshot entity)
    {
        _entities[entity.EntityId] = entity.Clone();
        _pendingMoveUpserts.Remove(entity.EntityId);
        // 입장은 누락되면 안 되므로 reliable AOI로 즉시 발행함
        await PublishAsync([entity.Clone()], [], ProtocolServerDeliveryPolicy.Reliable);
    }

    // 이동은 latest-only 이벤트라 중간 경로를 버리고 추후 클라이언트 보간에 맡김
    public Task MoveAsync(EntitySnapshot entity)
    {
        var snapshot = entity.Clone();
        _entities[entity.EntityId] = snapshot;
        _pendingMoveUpserts[entity.EntityId] = snapshot.Clone();
        return Task.CompletedTask;
    }

    public async Task LeaveAsync(long entityId)
    {
        _entities.Remove(entityId);
        _pendingMoveUpserts.Remove(entityId);
        // 퇴장은 화면 잔상 방지를 위해 reliable AOI로 발행함
        await PublishAsync([], [entityId], ProtocolServerDeliveryPolicy.Reliable);
    }

    private Task FlushPendingMovesAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (_pendingMoveUpserts.Count == 0)
        {
            return Task.CompletedTask;
        }

        var upserts = _pendingMoveUpserts.Values
            .Select(entity => entity.Clone())
            .ToList();
        _pendingMoveUpserts.Clear();
        // Gateway가 다시 관찰자마다 AOI 필터링과 latest 병합을 수행함
        return PublishAsync(upserts, [], ProtocolServerDeliveryPolicy.LatestPerEntity);
    }

    private Task PublishAsync(List<EntitySnapshot> upserts, List<long> removes, ProtocolServerDeliveryPolicy deliveryPolicy)
    {
        var address = ParseAddress();
        var delta = new ZoneDelta
        {
            MapId = address.MapId,
            ZoneX = address.CellX,
            ZoneY = address.CellY,
            Sequence = ++_sequence,
            Upserts = upserts,
            Removes = removes,
            DeliveryPolicy = deliveryPolicy
        };

        return _publisher.PublishZoneAsync(delta);
    }

    private ZoneAddress ParseAddress()
    {
        var key = this.GetPrimaryKeyString();
        var parts = key.Split(':', 3);
        if (parts.Length != 3
            || !int.TryParse(parts[1], out var cellX)
            || !int.TryParse(parts[2], out var cellY))
        {
            return new ZoneAddress();
        }

        return new ZoneAddress
        {
            MapId = parts[0],
            CellX = cellX,
            CellY = cellY
        };
    }
}
