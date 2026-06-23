using GameServer.Shared.Grains;
using GameServer.Shared.World;
using GameServer.WorldServer.EventBus;
using Orleans;
using Orleans.Runtime;
using ProtocolAoiRemoveReason = GameServer.Shared.Protocol.AoiRemoveReason;
using ProtocolErrorCode = GameServer.Shared.Protocol.ErrorCode;
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

    public Task SubmitMoveAsync(ZoneMoveCommand command)
    {
        // 같은 존을 이동할때 반영하는 경로. 동기 검증/승인이 필요해지면 이 메서드를 단순히 await 로 바꾸지 말고
        // Zone 내부를 shard/sub-zone 등으로 검증 처리를 분산해야 병목을 피할 수 있음.
        if (command.Entity.EntityId <= 0)
        {
            return Task.CompletedTask;
        }

        var targetZoneKey = ZoneMath.FromPosition(command.RequestedPosition).ToKey();
        if (targetZoneKey != this.GetPrimaryKeyString())
        {
            return Task.CompletedTask;
        }

        var wasTracked = _entities.TryGetValue(command.Entity.EntityId, out var tracked);
        var current = wasTracked
            ? tracked!.Clone()
            : command.Entity.Clone();
        var authorization = AuthorizeMove(current, command.RequestedPosition);
        if (!authorization.Accepted)
        {
            return Task.CompletedTask;
        }

        var snapshot = current.Clone();
        snapshot.Position = authorization.AuthoritativePosition.Clone();
        snapshot.Version = Math.Max(current.Version, command.Entity.Version);
        return MoveWithinCurrentZoneAsync(snapshot, wasTracked);
    }

    public async Task<ZoneMoveResult> MoveAsync(ZoneMoveCommand command)
    {
        if (command.Entity.EntityId <= 0)
        {
            return RejectedMove(command.RequestedPosition.Clone(), this.GetPrimaryKeyString(), "Invalid entity id.");
        }

        var wasTracked = _entities.TryGetValue(command.Entity.EntityId, out var tracked);
        var current = wasTracked
            ? tracked!.Clone()
            : command.Entity.Clone();
        var authorization = AuthorizeMove(current, command.RequestedPosition);
        if (!authorization.Accepted)
        {
            return RejectedMove(authorization.AuthoritativePosition, this.GetPrimaryKeyString(), authorization.Message);
        }

        var snapshot = current.Clone();
        snapshot.Position = authorization.AuthoritativePosition.Clone();
        snapshot.Version = Math.Max(current.Version, command.Entity.Version);

        var targetZoneKey = ZoneMath.FromPosition(snapshot.Position).ToKey();
        if (targetZoneKey == this.GetPrimaryKeyString())
        {
            await MoveWithinCurrentZoneAsync(snapshot, wasTracked);
        }
        else
        {
            await MoveToOtherZoneAsync(snapshot, targetZoneKey, wasTracked);
        }

        return new ZoneMoveResult
        {
            Accepted = true,
            ErrorCode = ProtocolErrorCode.None,
            AuthoritativePosition = snapshot.Position.Clone(),
            AuthoritativeZoneKey = targetZoneKey
        };
    }

    public Task TransferOutAsync(EntitySnapshot entity)
        => TransferOutCoreAsync(entity);

    private Task TransferOutCoreAsync(EntitySnapshot entity)
    {
        var snapshot = entity.Clone();
        _entities.Remove(snapshot.EntityId);
        _pendingMoveUpserts.Remove(snapshot.EntityId);
        // Zone 이동은 제거가 아니라 observer별 AOI membership 재검사용 reliable 이벤트로 발행함
        return PublishAsync(
            [snapshot],
            [snapshot.EntityId],
            ProtocolServerDeliveryPolicy.Reliable,
            [ProtocolAoiRemoveReason.ZoneTransferCheck]);
    }

    private async Task MoveWithinCurrentZoneAsync(EntitySnapshot snapshot, bool wasTracked)
    {
        _entities[snapshot.EntityId] = snapshot.Clone();
        if (wasTracked)
        {
            // 이동은 latest-only 이벤트라 중간 경로를 버리고 추후 클라이언트 보간에 맡김
            _pendingMoveUpserts[snapshot.EntityId] = snapshot.Clone();
            return;
        }

        await PublishAsync([snapshot.Clone()], [], ProtocolServerDeliveryPolicy.Reliable);
    }

    private async Task MoveToOtherZoneAsync(EntitySnapshot snapshot, string targetZoneKey, bool wasTracked)
    {
        await GrainFactory.GetGrain<IZoneGrain>(targetZoneKey).EnterAsync(snapshot.Clone());
        if (wasTracked)
        {
            await TransferOutCoreAsync(snapshot);
            return;
        }

        _entities.Remove(snapshot.EntityId);
        _pendingMoveUpserts.Remove(snapshot.EntityId);
    }

    private static MoveAuthorization AuthorizeMove(EntitySnapshot current, WorldPosition requested)
    {
        var authoritativePosition = requested.Clone();
        if (!PassesTerrainAndMovementRules(current.Position, authoritativePosition))
        {
            return new MoveAuthorization(
                Accepted: false,
                AuthoritativePosition: current.Position.Clone(),
                Message: "Move rejected by world movement rules.");
        }

        return new MoveAuthorization(
            Accepted: true,
            AuthoritativePosition: authoritativePosition,
            Message: "");
    }

    private static bool PassesTerrainAndMovementRules(WorldPosition currentPosition, WorldPosition requestedPosition)
        => true;

    private static ZoneMoveResult RejectedMove(WorldPosition authoritativePosition, string zoneKey, string message)
        => new()
        {
            Accepted = false,
            ErrorCode = ProtocolErrorCode.WorldMoveRejected,
            AuthoritativePosition = authoritativePosition.Clone(),
            AuthoritativeZoneKey = zoneKey,
            Message = message
        };

    public async Task LeaveAsync(long entityId)
    {
        _entities.Remove(entityId);
        _pendingMoveUpserts.Remove(entityId);
        // 퇴장은 화면 잔상 방지를 위해 reliable AOI로 발행함
        await PublishAsync([], [entityId], ProtocolServerDeliveryPolicy.Reliable);
    }

    public Task<List<EntitySnapshot>> GetSnapshotAsync()
    {
        var snapshot = _entities.Values
            .Select(entity => entity.Clone())
            .ToList();
        return Task.FromResult(snapshot);
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

    private Task PublishAsync(
        List<EntitySnapshot> upserts,
        List<long> removes,
        ProtocolServerDeliveryPolicy deliveryPolicy,
        List<ProtocolAoiRemoveReason>? removeReasons = null)
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
            DeliveryPolicy = deliveryPolicy,
            RemoveReasons = removeReasons ?? []
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

    private sealed record MoveAuthorization(
        bool Accepted,
        WorldPosition AuthoritativePosition,
        string Message);
}
