using System.Net.Sockets;
using GameServer.GatewayServer.Sessions;
using GameServer.Shared.Grains;
using GameServer.Shared.Protocol;
using GameServer.Shared.World;
using Orleans;
using PacketWorldPosition = GameServer.Shared.Protocol.WorldPosition;

namespace GameServer.GatewayServer.Networking;

public sealed class GatewayMoveScheduler
{
    // Client MoveReq는 200ms마다 최신값 하나만 UserGrain으로 전달한다.
    private static readonly TimeSpan TickInterval = TimeSpan.FromMilliseconds(200);

    private readonly GatewayEventBus _eventBus;
    private readonly IGrainFactory _grainFactory;
    private readonly GatewaySession _session;
    private readonly ILogger<GatewayMoveScheduler> _logger;
    private readonly CancellationTokenSource _stopCts;
    private readonly Task _runTask;
    // TCP read loop와 move tick 사이에서 최신 이동 요청 하나만 공유한다.
    private readonly object _gate = new();
    private PacketWorldPosition? _latestPosition;
    private long _latestSequence;
    private bool _hasPending;
    private bool _stopping;
    private string _currentZoneKey;

    public GatewayMoveScheduler(
        GatewayEventBus eventBus,
        IGrainFactory grainFactory,
        GatewaySession session,
        PacketWorldPosition initialPosition,
        CancellationToken serverStoppingToken,
        ILogger<GatewayMoveScheduler> logger)
    {
        _eventBus = eventBus;
        _grainFactory = grainFactory;
        _session = session;
        _logger = logger;
        _currentZoneKey = ToZoneKey(initialPosition);
        _stopCts = CancellationTokenSource.CreateLinkedTokenSource(serverStoppingToken);
        _runTask = RunAsync();
    }

    public void Submit(long sequence, PacketWorldPosition position)
    {
        lock (_gate)
        {
            if (_stopping)
            {
                return;
            }

            _latestSequence = sequence;
            _latestPosition = position.Clone();
            _hasPending = true;
        }
    }

    public async Task StopAsync()
    {
        lock (_gate)
        {
            _stopping = true;
            _hasPending = false;
        }

        await _stopCts.CancelAsync();

        try
        {
            await Task.WhenAny(_runTask, Task.Delay(TimeSpan.FromSeconds(1)));
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Move scheduler stop failed for session {SessionId}.", _session.SessionId);
        }
    }

    private async Task RunAsync()
    {
        using var timer = new PeriodicTimer(TickInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(_stopCts.Token))
            {
                var pending = DrainLatest();
                if (pending is null)
                {
                    continue;
                }

                await ProcessMoveAsync(pending.Value.Sequence, pending.Value.Position, _stopCts.Token);
            }
        }
        catch (OperationCanceledException) when (_stopCts.IsCancellationRequested)
        {
        }
    }

    private (long Sequence, PacketWorldPosition Position)? DrainLatest()
    {
        lock (_gate)
        {
            if (!_hasPending || _latestPosition is null)
            {
                return null;
            }

            var pending = (_latestSequence, _latestPosition.Clone());
            _hasPending = false;
            return pending;
        }
    }

    private async Task ProcessMoveAsync(long sequence, PacketWorldPosition position, CancellationToken cancellationToken)
    {
        try
        {
            await UpdateSessionZonesIfChangedAsync(position, cancellationToken);

            var result = await _grainFactory.GetGrain<IUserGrain>(_session.UserDbId)
                .MoveAsync(new MoveCommand
                {
                    UserDbId = _session.UserDbId,
                    SessionId = _session.SessionId,
                    Position = position.ToWorldPosition()
                });

            var envelope = new ServerResEnvelope
            {
                Sequence = sequence,
                Kind = ServerResKind.MoveNty,
                SessionId = _session.SessionId,
                UserDbId = _session.UserDbId,
                Move = new MoveNty
                {
                    Accepted = result.Accepted,
                    ErrorCode = result.ErrorCode,
                    Message = result.Message,
                    AuthoritativePosition = result.AuthoritativePosition.ToPacketPosition()
                }
            };

            _session.UpdatePosition(envelope.Move.AuthoritativePosition);
            await UpdateSessionZonesIfChangedAsync(envelope.Move.AuthoritativePosition, cancellationToken);

            await _session.SendAsync(envelope, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (IOException ex)
        {
            _logger.LogDebug(ex, "Move notification send failed for session {SessionId}, user {UserDbId}.", _session.SessionId, _session.UserDbId);
        }
        catch (SocketException ex)
        {
            _logger.LogDebug(ex, "Move notification socket send failed for session {SessionId}, user {UserDbId}.", _session.SessionId, _session.UserDbId);
        }
        catch (ObjectDisposedException ex)
        {
            _logger.LogDebug(ex, "Move notification skipped for disposed session {SessionId}, user {UserDbId}.", _session.SessionId, _session.UserDbId);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Move scheduler failed for session {SessionId}, user {UserDbId}.", _session.SessionId, _session.UserDbId);
        }
    }

    private async Task UpdateSessionZonesIfChangedAsync(PacketWorldPosition position, CancellationToken cancellationToken)
    {
        var nextZoneKey = ToZoneKey(position);
        // Zone이 바뀐 경우에만 Redis zone 구독 snapshot을 갱신한다.
        if (nextZoneKey == _currentZoneKey)
        {
            return;
        }

        await _eventBus.UpdateSessionZonesAsync(_session, position, cancellationToken);
        _currentZoneKey = nextZoneKey;
    }

    private static string ToZoneKey(PacketWorldPosition position)
        => ZoneMath.FromPosition(position.ToWorldPosition()).ToKey();
}
