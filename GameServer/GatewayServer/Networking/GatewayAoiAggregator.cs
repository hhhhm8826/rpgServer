using System.Net.Sockets;
using System.Threading.Channels;
using GameServer.GatewayServer.Sessions;
using GameServer.Shared.Protocol;
using Microsoft.Extensions.Options;
using PacketEntitySnapshot = GameServer.Shared.Protocol.EntitySnapshot;

namespace GameServer.GatewayServer.Networking;

public sealed class GatewayAoiAggregator : BackgroundService
{
    private readonly GatewayTcpTraceLog _traceLog;
    private readonly GatewayOptions _options;
    private readonly ILogger<GatewayAoiAggregator> _logger;
    private readonly SessionAoiPartition[] _partitions;

    public GatewayAoiAggregator(
        GatewayTcpTraceLog traceLog,
        IOptions<GatewayOptions> options,
        ILogger<GatewayAoiAggregator> logger)
    {
        _traceLog = traceLog;
        _options = options.Value;
        _logger = logger;
        _partitions = Enumerable.Range(0, _options.NormalizedSessionAoiPartitionCount)
            .Select(index => new SessionAoiPartition(index, _traceLog, _options, _logger))
            .ToArray();
    }

    public Task EnqueueLatestZoneAsync(
        ServerResEnvelope envelope,
        IReadOnlyList<GatewaySession> sessions,
        CancellationToken cancellationToken = default)
        => EnqueueZoneBatchAsync(SessionAoiCommandKind.LatestBatch, envelope, sessions, cancellationToken);

    public Task EnqueueReliableZoneAsync(
        ServerResEnvelope envelope,
        IReadOnlyList<GatewaySession> sessions,
        CancellationToken cancellationToken = default)
        => EnqueueZoneBatchAsync(SessionAoiCommandKind.ReliableBatch, envelope, sessions, cancellationToken);

    public async Task FlushSessionAsync(string sessionId, long userDbId, CancellationToken cancellationToken = default)
    {
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var command = SessionAoiCommand.FlushSession(sessionId, completion);
        var partition = _partitions[GatewayPartitionHash.ForUser(userDbId, _partitions.Length)];
        await partition.EnqueueAsync(command, cancellationToken);
        await completion.Task.WaitAsync(cancellationToken);
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
        => Task.WhenAll(_partitions.Select(partition => partition.RunAsync(stoppingToken)));

    private async Task EnqueueZoneBatchAsync(
        SessionAoiCommandKind kind,
        ServerResEnvelope envelope,
        IReadOnlyList<GatewaySession> sessions,
        CancellationToken cancellationToken)
    {
        if (envelope.AoiDelta is null || sessions.Count == 0)
        {
            return;
        }

        var grouped = new List<GatewaySession>?[_partitions.Length];
        foreach (var session in sessions)
        {
            var partitionIndex = GatewayPartitionHash.ForUser(session.UserDbId, _partitions.Length);
            (grouped[partitionIndex] ??= []).Add(session);
        }

        for (var i = 0; i < grouped.Length; i++)
        {
            if (grouped[i] is not { Count: > 0 } partitionSessions)
            {
                continue;
            }

            var command = new SessionAoiCommand(kind, envelope, partitionSessions.ToArray(), "", null);
            await _partitions[i].EnqueueAsync(command, cancellationToken);
        }
    }

    private sealed class SessionAoiPartition
    {
        private const int MaxQueuedCommandsPerLoop = 256;

        private readonly int _index;
        private readonly GatewayTcpTraceLog _traceLog;
        private readonly GatewayOptions _options;
        private readonly ILogger _logger;
        private readonly Channel<SessionAoiCommand> _commands;
        // 관찰자 세션별로 1 tick 1 AOI packet이 되도록 최신 delta를 병합한다.
        private readonly Dictionary<string, PendingAoiDelta> _pendingBySession = new(StringComparer.Ordinal);
        // enter/exit 반경 hysteresis를 위해 관찰자가 이미 보고 있는 entity를 기억한다.
        private readonly Dictionary<string, SessionAoiVisibility> _visibilityBySession = new(StringComparer.Ordinal);
        private readonly SemaphoreSlim _latestSendSlots;
        private DateTimeOffset _lastLatestSendDropLog = DateTimeOffset.MinValue;

        public SessionAoiPartition(
            int index,
            GatewayTcpTraceLog traceLog,
            GatewayOptions options,
            ILogger logger)
        {
            _index = index;
            _traceLog = traceLog;
            _options = options;
            _logger = logger;
            _commands = Channel.CreateBounded<SessionAoiCommand>(new BoundedChannelOptions(_options.NormalizedSessionAoiQueueCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = false
            });
            _latestSendSlots = new SemaphoreSlim(
                _options.NormalizedSessionAoiSendConcurrency,
                _options.NormalizedSessionAoiSendConcurrency);
        }

        public void Enqueue(SessionAoiCommand command, CancellationToken cancellationToken)
        {
            if (_commands.Writer.TryWrite(command))
            {
                return;
            }

            _ = EnqueueAsync(command, cancellationToken);
        }

        public async Task EnqueueAsync(SessionAoiCommand command, CancellationToken cancellationToken)
        {
            try
            {
                await _commands.Writer.WriteAsync(command, cancellationToken);
            }
            catch (ChannelClosedException)
            {
                command.Completion?.TrySetCanceled(cancellationToken);
            }
        }

        public async Task RunAsync(CancellationToken stoppingToken)
        {
            try
            {
                using var timer = new PeriodicTimer(_options.SessionAoiTick);
                var waitRead = _commands.Reader.WaitToReadAsync(stoppingToken).AsTask();
                var waitTick = timer.WaitForNextTickAsync(stoppingToken).AsTask();

                while (!stoppingToken.IsCancellationRequested)
                {
                    await Task.WhenAny(waitRead, waitTick);

                    if (waitTick.IsCompleted)
                    {
                        if (!await waitTick)
                        {
                            break;
                        }

                        await FlushAllAsync(stoppingToken);
                        waitTick = timer.WaitForNextTickAsync(stoppingToken).AsTask();
                    }

                    if (waitRead.IsCompleted)
                    {
                        if (!await waitRead)
                        {
                            break;
                        }

                        var processed = 0;
                        while (processed < MaxQueuedCommandsPerLoop && _commands.Reader.TryRead(out var command))
                        {
                            await ProcessCommandAsync(command, stoppingToken);
                            processed++;

                            if (waitTick.IsCompleted)
                            {
                                break;
                            }
                        }

                        waitRead = _commands.Reader.WaitToReadAsync(stoppingToken).AsTask();
                    }
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Gateway AOI session partition {PartitionIndex} failed.", _index);
            }
        }

        private async Task ProcessCommandAsync(SessionAoiCommand command, CancellationToken cancellationToken)
        {
            try
            {
                switch (command.Kind)
                {
                    case SessionAoiCommandKind.LatestBatch:
                        MergeLatestBatch(command);
                        break;
                    case SessionAoiCommandKind.ReliableBatch:
                        await SendReliableBatchAsync(command, cancellationToken);
                        break;
                    case SessionAoiCommandKind.FlushSession:
                        await FlushSessionCommandAsync(command, cancellationToken);
                        break;
                }
            }
            catch (Exception ex)
            {
                command.Completion?.TrySetException(ex);
                _logger.LogDebug(ex, "Gateway AOI session partition {PartitionIndex} command {CommandKind} failed.", _index, command.Kind);
            }
        }

        private void MergeLatestBatch(SessionAoiCommand command)
        {
            if (command.Envelope is null)
            {
                return;
            }

            foreach (var session in command.Sessions)
            {
                var filtered = CreateFilteredAoiForSession(session, command.Envelope);
                if (filtered is null)
                {
                    continue;
                }

                if (!_pendingBySession.TryGetValue(session.SessionId, out var pending))
                {
                    pending = new PendingAoiDelta();
                    _pendingBySession[session.SessionId] = pending;
                }

                pending.Merge(session, filtered);
            }
        }

        private async Task SendReliableBatchAsync(SessionAoiCommand command, CancellationToken cancellationToken)
        {
            if (command.Envelope is null)
            {
                return;
            }

            foreach (var session in command.Sessions)
            {
                var filtered = CreateFilteredAoiForSession(session, command.Envelope);
                if (filtered is null)
                {
                    continue;
                }

                RemovePendingSupersededUpserts(session.SessionId, filtered.AoiDelta);
                await FlushPendingAsync(session.SessionId, cancellationToken);
                await SendEnvelopeAsync(session, filtered, "reliable-aoi-send", cancellationToken);
            }
        }

        private void RemovePendingSupersededUpserts(string sessionId, AoiDelta? reliableDelta)
        {
            if (reliableDelta is not { Upserts.Count: > 0 }
                || !_pendingBySession.TryGetValue(sessionId, out var pending))
            {
                return;
            }

            pending.RemoveUpsertsSupersededBy(reliableDelta.Upserts);
        }

        private async Task FlushSessionCommandAsync(SessionAoiCommand command, CancellationToken cancellationToken)
        {
            // 연결 종료 시 pending delta와 visibility를 함께 제거해 GatewaySession 참조를 끊는다.
            await FlushPendingAsync(command.SessionId, cancellationToken);
            _pendingBySession.Remove(command.SessionId);
            _visibilityBySession.Remove(command.SessionId);
            command.Completion?.TrySetResult();
        }

        private Task FlushAllAsync(CancellationToken cancellationToken)
        {
            var sends = new List<PendingSessionSend>();
            foreach (var sessionId in _pendingBySession.Keys.ToArray())
            {
                var send = DrainPendingSend(sessionId);
                if (send is not null)
                {
                    sends.Add(send);
                }
            }

            var dropped = 0;
            foreach (var send in sends.OrderBy(x => x.Session.UserDbId))
            {
                if (!_latestSendSlots.Wait(0))
                {
                    dropped++;
                    continue;
                }

                _ = SendLatestBatchAndReleaseAsync(send, cancellationToken);
            }

            LogLatestSendDrops(dropped);
            return Task.CompletedTask;
        }

        private async Task FlushPendingAsync(string sessionId, CancellationToken cancellationToken)
        {
            var send = DrainPendingSend(sessionId);
            if (send is null)
            {
                return;
            }

            await SendPendingBatchAsync(send, "aoi-aggregate-send", cancellationToken);
        }

        private PendingSessionSend? DrainPendingSend(string sessionId)
        {
            if (!_pendingBySession.TryGetValue(sessionId, out var pending))
            {
                return null;
            }

            var envelope = pending.Drain();
            if (envelope is null)
            {
                return null;
            }

            var session = pending.Session;
            if (session is null)
            {
                _pendingBySession.Remove(sessionId);
                _visibilityBySession.Remove(sessionId);
                return null;
            }

            return new PendingSessionSend(session, ToPackets(envelope));
        }

        private ServerResEnvelope? CreateFilteredAoiForSession(GatewaySession session, ServerResEnvelope envelope)
        {
            var source = envelope.AoiDelta;
            if (source is null)
            {
                return null;
            }

            var observerPosition = session.GetPosition();
            var visibility = GetVisibility(session.SessionId);
            var delta = new AoiDelta
            {
                MapId = source.MapId,
                ZoneX = source.ZoneX,
                ZoneY = source.ZoneY,
                Sequence = source.Sequence
            };

            var zoneTransferChecks = GetZoneTransferCheckUpserts(source);
            for (var i = 0; i < source.Removes.Count; i++)
            {
                var remove = source.Removes[i];
                if (remove == session.UserDbId)
                {
                    continue;
                }

                var reason = GatewayAoiAggregator.RemoveReasonAt(source, i);
                if (reason == AoiRemoveReason.ZoneTransferCheck)
                {
                    if (!zoneTransferChecks.TryGetValue(remove, out var transfer)
                        || transfer.Position is null
                        || visibility.IsOlderThanKnown(remove, transfer.Version))
                    {
                        continue;
                    }

                    visibility.RecordVersion(remove, transfer.Version);
                    if (visibility.VisibleEntityIds.Contains(remove)
                        && !IsInView(observerPosition, transfer.Position, _options.AoiExitRadiusSquared))
                    {
                        visibility.VisibleEntityIds.Remove(remove);
                        delta.Removes.Add(remove);
                        delta.RemoveReasons.Add(reason);
                    }

                    continue;
                }

                if (visibility.VisibleEntityIds.Remove(remove))
                {
                    delta.Removes.Add(remove);
                    delta.RemoveReasons.Add(reason);
                }
            }

            foreach (var upsert in source.Upserts)
            {
                if (upsert.EntityId == session.UserDbId || upsert.Position is null)
                {
                    continue;
                }

                if (zoneTransferChecks.ContainsKey(upsert.EntityId))
                {
                    continue;
                }

                if (visibility.IsOlderThanKnown(upsert.EntityId, upsert.Version))
                {
                    continue;
                }

                visibility.RecordVersion(upsert.EntityId, upsert.Version);
                var wasVisible = visibility.VisibleEntityIds.Contains(upsert.EntityId);
                var radiusSquared = wasVisible ? _options.AoiExitRadiusSquared : _options.AoiEnterRadiusSquared;
                // 이미 보이는 entity는 더 큰 exit 반경을 적용해 경계 흔들림을 줄인다.
                var isVisible = IsInView(observerPosition, upsert.Position, radiusSquared);

                if (isVisible)
                {
                    visibility.VisibleEntityIds.Add(upsert.EntityId);
                    delta.Upserts.Add(upsert.Clone());
                    continue;
                }

                if (wasVisible)
                {
                    visibility.VisibleEntityIds.Remove(upsert.EntityId);
                    delta.Removes.Add(upsert.EntityId);
                    delta.RemoveReasons.Add(AoiRemoveReason.OutOfView);
                }
            }

            if (delta.Upserts.Count == 0 && delta.Removes.Count == 0)
            {
                return null;
            }

            return new ServerResEnvelope
            {
                Sequence = envelope.Sequence,
                SessionId = session.SessionId,
                UserDbId = session.UserDbId,
                Kind = ServerResKind.AoiDelta,
                DeliveryPolicy = envelope.DeliveryPolicy,
                AoiDelta = delta
            };
        }

        private static Dictionary<long, PacketEntitySnapshot> GetZoneTransferCheckUpserts(AoiDelta source)
        {
            var transferIds = new HashSet<long>();
            for (var i = 0; i < source.Removes.Count; i++)
            {
                if (GatewayAoiAggregator.RemoveReasonAt(source, i) == AoiRemoveReason.ZoneTransferCheck)
                {
                    transferIds.Add(source.Removes[i]);
                }
            }

            if (transferIds.Count == 0)
            {
                return [];
            }

            var result = new Dictionary<long, PacketEntitySnapshot>();
            foreach (var upsert in source.Upserts)
            {
                if (!transferIds.Contains(upsert.EntityId)
                    || upsert.Position is null)
                {
                    continue;
                }

                if (!result.TryGetValue(upsert.EntityId, out var current)
                    || upsert.Version > current.Version)
                {
                    result[upsert.EntityId] = upsert;
                }
            }

            return result;
        }

        private SessionAoiVisibility GetVisibility(string sessionId)
        {
            if (_visibilityBySession.TryGetValue(sessionId, out var visibility))
            {
                return visibility;
            }

            visibility = new SessionAoiVisibility();
            _visibilityBySession[sessionId] = visibility;
            return visibility;
        }

        private static bool IsInView(WorldPosition observer, WorldPosition target, double radiusSquared)
        {
            if (!string.Equals(observer.MapId, target.MapId, StringComparison.Ordinal))
            {
                return false;
            }

            var dx = (double)observer.X - target.X;
            var dy = (double)observer.Y - target.Y;
            return dx * dx + dy * dy <= radiusSquared;
        }

        private async Task SendPendingBatchAsync(
            PendingSessionSend send,
            string traceKind,
            CancellationToken cancellationToken)
        {
            foreach (var packet in send.Packets)
            {
                await SendEnvelopeAsync(send.Session, packet, traceKind, cancellationToken);
            }
        }

        private async Task SendLatestBatchAndReleaseAsync(PendingSessionSend send, CancellationToken cancellationToken)
        {
            try
            {
                await SendPendingBatchAsync(send, "aoi-aggregate-send", cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                _logger.LogDebug(
                    ex,
                    "Gateway AOI latest send failed for session {SessionId}, user {UserDbId}.",
                    send.Session.SessionId,
                    send.Session.UserDbId);
            }
            finally
            {
                _latestSendSlots.Release();
            }
        }

        private void LogLatestSendDrops(int dropped)
        {
            if (dropped <= 0)
            {
                return;
            }

            var now = DateTimeOffset.UtcNow;
            if (now - _lastLatestSendDropLog < TimeSpan.FromSeconds(10))
            {
                return;
            }

            _lastLatestSendDropLog = now;
            _logger.LogInformation(
                "Dropped latest-only AOI sends for {DroppedCount} sessions in AOI partition {PartitionIndex}.",
                dropped,
                _index);
        }

        private async Task SendEnvelopeAsync(
            GatewaySession session,
            ServerResEnvelope envelope,
            string traceKind,
            CancellationToken cancellationToken)
        {
            try
            {
                await session.SendAsync(envelope, cancellationToken);
            }
            catch (IOException ex)
            {
                _logger.LogDebug(
                    ex,
                    "AOI send failed for session {SessionId}, user {UserDbId}, remote {Remote}: {Error}",
                    session.SessionId,
                    session.UserDbId,
                    session.RemoteEndPoint,
                    ex.Message);
            }
            catch (SocketException ex)
            {
                _logger.LogDebug(
                    ex,
                    "AOI socket send failed for session {SessionId}, user {UserDbId}, remote {Remote}: {SocketError}",
                    session.SessionId,
                    session.UserDbId,
                    session.RemoteEndPoint,
                    ex.SocketErrorCode);
            }
            catch (ObjectDisposedException ex)
            {
                _logger.LogDebug(
                    ex,
                    "AOI send skipped for disposed session {SessionId}, user {UserDbId}, remote {Remote}.",
                    session.SessionId,
                    session.UserDbId,
                    session.RemoteEndPoint);
            }
            catch (Exception ex)
            {
                await _traceLog.WriteAsync(
                    traceKind,
                    "gateway-aoi-partition",
                    $"{ex.GetType().Name}:{ex.Message}",
                    session.SessionId,
                    session.UserDbId,
                    session.RemoteEndPoint,
                    cancellationToken: CancellationToken.None);
                _logger.LogWarning(
                    ex,
                    "AOI send failed unexpectedly for session {SessionId}, user {UserDbId}, remote {Remote}.",
                    session.SessionId,
                    session.UserDbId,
                    session.RemoteEndPoint);
            }
        }
    }

    private sealed class SessionAoiVisibility
    {
        public HashSet<long> VisibleEntityIds { get; } = new();

        private readonly Dictionary<long, long> _knownVersions = new();

        public bool IsOlderThanKnown(long entityId, long version)
            => _knownVersions.TryGetValue(entityId, out var knownVersion)
                && version < knownVersion;

        public void RecordVersion(long entityId, long version)
        {
            if (!_knownVersions.TryGetValue(entityId, out var knownVersion)
                || version > knownVersion)
            {
                _knownVersions[entityId] = version;
            }
        }
    }

    private static AoiRemoveReason RemoveReasonAt(AoiDelta delta, int index)
        => index >= 0 && index < delta.RemoveReasons.Count
            ? delta.RemoveReasons[index]
            : AoiRemoveReason.EntityRemoved;

    private static IReadOnlyList<ServerResEnvelope> ToPackets(ServerResEnvelope envelope)
    {
        if (envelope.AoiDelta is not { } delta)
        {
            return [envelope.Clone()];
        }

        var packets = new List<ServerResEnvelope>();
        var current = CreateDeltaShell(delta);

        for (var i = 0; i < delta.Removes.Count; i++)
        {
            var remove = delta.Removes[i];
            var reason = RemoveReasonAt(delta, i);
            current.Removes.Add(remove);
            current.RemoveReasons.Add(reason);
            if (ToPacket(envelope, current).CalculateSize() <= ProtobufPacketCodec.MaxPacketSize)
            {
                continue;
            }

            current.Removes.RemoveAt(current.Removes.Count - 1);
            current.RemoveReasons.RemoveAt(current.RemoveReasons.Count - 1);
            FlushIfNotEmpty(envelope, current, packets);
            current = CreateDeltaShell(delta);
            current.Removes.Add(remove);
            current.RemoveReasons.Add(reason);
        }

        foreach (var upsert in delta.Upserts)
        {
            current.Upserts.Add(upsert.Clone());
            if (ToPacket(envelope, current).CalculateSize() <= ProtobufPacketCodec.MaxPacketSize)
            {
                continue;
            }

            current.Upserts.RemoveAt(current.Upserts.Count - 1);
            FlushIfNotEmpty(envelope, current, packets);
            current = CreateDeltaShell(delta);
            current.Upserts.Add(upsert.Clone());
        }

        FlushIfNotEmpty(envelope, current, packets);
        return packets;
    }

    private static void FlushIfNotEmpty(ServerResEnvelope envelope, AoiDelta delta, List<ServerResEnvelope> packets)
    {
        if (delta.Upserts.Count == 0 && delta.Removes.Count == 0)
        {
            return;
        }

        packets.Add(ToPacket(envelope, delta));
    }

    private static AoiDelta CreateDeltaShell(AoiDelta source) => new()
    {
        MapId = source.MapId,
        ZoneX = source.ZoneX,
        ZoneY = source.ZoneY,
        Sequence = source.Sequence
    };

    private static ServerResEnvelope ToPacket(ServerResEnvelope envelope, AoiDelta? delta) => new()
    {
        Sequence = envelope.Sequence,
        SessionId = envelope.SessionId,
        UserDbId = envelope.UserDbId,
        Kind = ServerResKind.AoiDelta,
        DeliveryPolicy = envelope.DeliveryPolicy,
        AoiDelta = delta?.Clone()
    };

    private sealed class PendingAoiDelta
    {
        private readonly Dictionary<long, PacketEntitySnapshot> _upserts = new();
        private readonly Dictionary<long, AoiRemoveReason> _removes = new();
        private GatewaySession? _session;
        private string _sessionId = "";
        private long _userDbId;
        private string _mapId = "default";
        private int _zoneX;
        private int _zoneY;
        private long _sequence;
        private ServerDeliveryPolicy _deliveryPolicy = ServerDeliveryPolicy.LatestPerEntity;

        public void Merge(GatewaySession session, ServerResEnvelope envelope)
        {
            var delta = envelope.AoiDelta;
            if (delta is null)
            {
                return;
            }

            _session = session;
            _sessionId = session.SessionId;
            _userDbId = session.UserDbId;
            _mapId = delta.MapId;
            _zoneX = delta.ZoneX;
            _zoneY = delta.ZoneY;
            _sequence = Math.Max(_sequence, delta.Sequence);
            _deliveryPolicy = envelope.DeliveryPolicy;

            for (var i = 0; i < delta.Removes.Count; i++)
            {
                var remove = delta.Removes[i];
                _upserts.Remove(remove);
                _removes[remove] = GatewayAoiAggregator.RemoveReasonAt(delta, i);
            }

            foreach (var upsert in delta.Upserts)
            {
                if (_upserts.TryGetValue(upsert.EntityId, out var current)
                    && upsert.Version < current.Version)
                {
                    continue;
                }

                _removes.Remove(upsert.EntityId);
                _upserts[upsert.EntityId] = upsert.Clone();
            }
        }

        public void RemoveUpsertsSupersededBy(IEnumerable<PacketEntitySnapshot> upserts)
        {
            foreach (var upsert in upserts)
            {
                if (_upserts.TryGetValue(upsert.EntityId, out var current)
                    && current.Version <= upsert.Version)
                {
                    _upserts.Remove(upsert.EntityId);
                }
            }
        }

        public ServerResEnvelope? Drain()
        {
            if (_upserts.Count == 0 && _removes.Count == 0)
            {
                return null;
            }

            var delta = new AoiDelta
            {
                MapId = _mapId,
                ZoneX = _zoneX,
                ZoneY = _zoneY,
                Sequence = _sequence
            };
            delta.Upserts.AddRange(_upserts.Values.Select(x => x.Clone()));
            foreach (var remove in _removes)
            {
                delta.Removes.Add(remove.Key);
                delta.RemoveReasons.Add(remove.Value);
            }

            _upserts.Clear();
            _removes.Clear();

            return new ServerResEnvelope
            {
                SessionId = _sessionId,
                UserDbId = _userDbId,
                Kind = ServerResKind.AoiDelta,
                DeliveryPolicy = _deliveryPolicy,
                AoiDelta = delta
            };
        }

        public GatewaySession? Session => _session;
    }

    private enum SessionAoiCommandKind
    {
        LatestBatch,
        ReliableBatch,
        FlushSession
    }

    private sealed record SessionAoiCommand(
        SessionAoiCommandKind Kind,
        ServerResEnvelope? Envelope,
        IReadOnlyList<GatewaySession> Sessions,
        string SessionId,
        TaskCompletionSource? Completion)
    {
        public static SessionAoiCommand FlushSession(string sessionId, TaskCompletionSource completion)
            => new(SessionAoiCommandKind.FlushSession, null, [], sessionId, completion);
    }

    private sealed record PendingSessionSend(GatewaySession Session, IReadOnlyList<ServerResEnvelope> Packets);
}
