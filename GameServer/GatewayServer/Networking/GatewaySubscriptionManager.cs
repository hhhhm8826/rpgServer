using System.Collections.Concurrent;
using System.Threading.Channels;
using GameServer.GatewayServer.Sessions;
using GameServer.Shared.EventBus;
using GameServer.Shared.Grains;
using GameServer.Shared.Protocol;
using GameServer.Shared.World;
using Orleans;
using StackExchange.Redis;
using PacketWorldPosition = GameServer.Shared.Protocol.WorldPosition;
using ProtocolServerDeliveryPolicy = GameServer.Shared.Protocol.ServerDeliveryPolicy;

namespace GameServer.GatewayServer.Networking;

public sealed class GatewaySubscriptionManager : BackgroundService
{
    private readonly EventBusRedisConnection _redis;
    private readonly IGrainFactory _grainFactory;
    private readonly GatewayAoiAggregator _aoiAggregator;
    private readonly ILogger<GatewaySubscriptionManager> _logger;
    private readonly Channel<SubscriptionCommand> _commands;
    private readonly Dictionary<string, int> _userSubscriptions = new(StringComparer.Ordinal);
    private readonly Dictionary<string, int> _zoneSubscriptions = new(StringComparer.Ordinal);
    private readonly Dictionary<string, SessionAoiState> _sessionAoiStates = new(StringComparer.Ordinal);
    private readonly Dictionary<string, Dictionary<string, GatewaySession>> _sessionsByZone = new(StringComparer.Ordinal);
    // AOI fan-out hot path는 lock 없이 읽도록 single-writer가 snapshot을 교체한다.
    private readonly ConcurrentDictionary<string, GatewaySession[]> _sessionsByZoneSnapshot = new(StringComparer.Ordinal);
    private Action<GatewayEventChannelKind, RedisChannel, RedisValue>? _messageHandler;

    public GatewaySubscriptionManager(
        EventBusRedisConnection redis,
        IGrainFactory grainFactory,
        GatewayAoiAggregator aoiAggregator,
        ILogger<GatewaySubscriptionManager> logger)
    {
        _redis = redis;
        _grainFactory = grainFactory;
        _aoiAggregator = aoiAggregator;
        _logger = logger;
        _commands = Channel.CreateUnbounded<SubscriptionCommand>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });
    }

    internal void SetMessageHandler(Action<GatewayEventChannelKind, RedisChannel, RedisValue> handler)
        => _messageHandler = handler;

    public Task RegisterSessionAsync(GatewaySession session, PacketWorldPosition position, CancellationToken cancellationToken = default)
        => EnqueueAsync(SubscriptionCommand.Register(session, position.Clone()), cancellationToken);

    public Task UnregisterSessionAsync(GatewaySession session, CancellationToken cancellationToken = default)
        => EnqueueAsync(SubscriptionCommand.Unregister(session), cancellationToken);

    public Task UpdateSessionZonesAsync(GatewaySession session, PacketWorldPosition position, CancellationToken cancellationToken = default)
        => EnqueueAsync(SubscriptionCommand.UpdateZones(session, position.Clone()), cancellationToken);

    public IReadOnlyList<GatewaySession> GetSessionsForZone(string channel)
    {
        if (!_sessionsByZoneSnapshot.TryGetValue(channel, out var sessions) || sessions.Length == 0)
        {
            return [];
        }

        return sessions;
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _commands.Writer.TryComplete();
        await base.StopAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            await foreach (var command in _commands.Reader.ReadAllAsync(stoppingToken))
            {
                await ProcessCommandAsync(command, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        finally
        {
            while (_commands.Reader.TryRead(out var command))
            {
                command.Completion.TrySetCanceled(stoppingToken);
            }
        }
    }

    private async Task EnqueueAsync(SubscriptionCommand command, CancellationToken cancellationToken)
    {
        if (!_commands.Writer.TryWrite(command))
        {
            await _commands.Writer.WriteAsync(command, cancellationToken);
        }

        await command.Completion.Task.WaitAsync(cancellationToken);
    }

    private async Task ProcessCommandAsync(SubscriptionCommand command, CancellationToken cancellationToken)
    {
        try
        {
            switch (command.Kind)
            {
                case SubscriptionCommandKind.Register:
                    await SubscribeUserAsync(command.UserDbId);
                    await UpdateSessionZonesCoreAsync(command.Session, command.Position, cancellationToken);
                    break;
                case SubscriptionCommandKind.Unregister:
                    await UnregisterSessionCoreAsync(command.SessionId, command.UserDbId);
                    break;
                case SubscriptionCommandKind.UpdateZones:
                    await UpdateSessionZonesCoreAsync(command.Session, command.Position, cancellationToken);
                    break;
            }

            command.Completion.TrySetResult(true);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Gateway subscription command {CommandKind} failed for session {SessionId}, user {UserDbId}.",
                command.Kind,
                command.SessionId,
                command.UserDbId);
            command.Completion.TrySetException(ex);
        }
    }

    private async Task UnregisterSessionCoreAsync(string sessionId, long userDbId)
    {
        var zones = Array.Empty<string>();
        if (!_sessionAoiStates.Remove(sessionId, out var current))
        {
            await TryUnsubscribeUserAsync(userDbId);
        }
        else
        {
            zones = current.Zones.ToArray();
            foreach (var zone in zones)
            {
                RemoveSessionFromZoneIndex(zone, sessionId);
            }

            await TryUnsubscribeUserAsync(userDbId);
        }

        foreach (var zone in zones)
        {
            await TryDecrementZoneSubscriptionAsync(zone, sessionId);
        }
    }

    private async Task TryUnsubscribeUserAsync(long userDbId)
    {
        try
        {
            await UnsubscribeUserAsync(userDbId);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to unsubscribe user channel for {UserDbId}.", userDbId);
        }
    }

    private async Task TryDecrementZoneSubscriptionAsync(string channel, string sessionId)
    {
        try
        {
            await DecrementZoneSubscriptionAsync(channel);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to decrement zone subscription {Channel} while unregistering session {SessionId}.", channel, sessionId);
        }
    }

    private async Task UpdateSessionZonesCoreAsync(GatewaySession session, PacketWorldPosition position, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var sessionId = session.SessionId;
        var currentZone = ZoneMath.FromPosition(position.ToWorldPosition());
        var centerZoneKey = currentZone.ToKey();
        var desired = ZoneMath.Neighbors5x5(currentZone)
            .Select(EventBusChannels.Zone)
            .ToHashSet(StringComparer.Ordinal);

        if (!_sessionAoiStates.TryGetValue(sessionId, out var state))
        {
            state = new SessionAoiState();
            _sessionAoiStates[sessionId] = state;
        }

        if (state.CenterZoneKey == centerZoneKey && state.Zones.Count > 0)
        {
            // 같은 Zone 안의 이동은 구독 갱신 없이 세션 위치만 AOI 필터링에 사용
            return;
        }

        var add = desired.Except(state.Zones, StringComparer.Ordinal).ToArray();
        var remove = state.Zones.Except(desired, StringComparer.Ordinal).ToArray();

        foreach (var zone in add)
        {
            await IncrementZoneSubscriptionAsync(zone);
        }

        foreach (var zone in add)
        {
            state.Zones.Add(zone);
            AddSessionToZoneIndex(zone, session);
        }

        foreach (var zone in add)
        {
            QueueZoneSnapshot(zone, session);
        }

        foreach (var zone in remove)
        {
            state.Zones.Remove(zone);
            RemoveSessionFromZoneIndex(zone, sessionId);
        }

        foreach (var zone in remove)
        {
            await DecrementZoneSubscriptionAsync(zone);
        }

        state.CenterZoneKey = centerZoneKey;
    }

    private void QueueZoneSnapshot(string channel, GatewaySession session)
    {
        _ = Task.Run(
            () => SendZoneSnapshotAsync(channel, session, CancellationToken.None),
            CancellationToken.None);
    }

    private async Task SendZoneSnapshotAsync(string channel, GatewaySession session, CancellationToken cancellationToken)
    {
        if (!TryParseZoneChannel(channel, out var zone))
        {
            _logger.LogDebug("Skipped AOI snapshot for unrecognized zone channel {Channel}.", channel);
            return;
        }

        try
        {
            var snapshots = await _grainFactory.GetGrain<IZoneGrain>(zone.ToKey())
                .GetSnapshotAsync();
            if (snapshots.Count == 0)
            {
                return;
            }

            var delta = new ZoneDelta
            {
                MapId = zone.MapId,
                ZoneX = zone.CellX,
                ZoneY = zone.CellY,
                DeliveryPolicy = ProtocolServerDeliveryPolicy.Reliable
            };
            delta.Upserts.AddRange(snapshots.Select(entity => entity.Clone()));

            var envelope = new ServerResEnvelope
            {
                Kind = ServerResKind.AoiDelta,
                DeliveryPolicy = ServerDeliveryPolicy.Reliable,
                AoiDelta = delta.ToPacketAoiDelta()
            };

            await _aoiAggregator.EnqueueReliableZoneAsync(envelope, [session], cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to send AOI snapshot for zone {Channel}, session {SessionId}, user {UserDbId}.",
                channel,
                session.SessionId,
                session.UserDbId);
        }
    }

    private static bool TryParseZoneChannel(string channel, out ZoneAddress zone)
    {
        zone = new ZoneAddress();
        var parts = channel.Split(':', 5);
        if (parts.Length != 5
            || !string.Equals(parts[0], "gs", StringComparison.Ordinal)
            || !string.Equals(parts[2], "zone", StringComparison.Ordinal)
            || !int.TryParse(parts[3], out var cellX)
            || !int.TryParse(parts[4], out var cellY))
        {
            return false;
        }

        zone = new ZoneAddress
        {
            MapId = parts[1],
            CellX = cellX,
            CellY = cellY
        };
        return true;
    }

    private async Task SubscribeUserAsync(long userDbId)
    {
        var channel = EventBusChannels.User(userDbId);
        _userSubscriptions.TryGetValue(channel, out var count);
        if (count == 0)
        {
            var queue = await _redis.Subscriber.SubscribeAsync(RedisChannel.Sharded(channel));
            queue.OnMessage(message => DispatchServerMessage(GatewayEventChannelKind.User, message.Channel, message.Message));
        }

        _userSubscriptions[channel] = count + 1;
    }

    private async Task UnsubscribeUserAsync(long userDbId)
    {
        var channel = EventBusChannels.User(userDbId);
        if (!_userSubscriptions.TryGetValue(channel, out var count))
        {
            return;
        }

        if (count <= 1)
        {
            _userSubscriptions.Remove(channel);
            await _redis.Subscriber.UnsubscribeAsync(RedisChannel.Sharded(channel));
            return;
        }

        _userSubscriptions[channel] = count - 1;
    }

    private async Task IncrementZoneSubscriptionAsync(string channel)
    {
        _zoneSubscriptions.TryGetValue(channel, out var count);
        if (count == 0)
        {
            var queue = await _redis.Subscriber.SubscribeAsync(RedisChannel.Sharded(channel));
            queue.OnMessage(message => DispatchServerMessage(GatewayEventChannelKind.Zone, message.Channel, message.Message));
        }

        _zoneSubscriptions[channel] = count + 1;
    }

    private async Task DecrementZoneSubscriptionAsync(string channel)
    {
        if (!_zoneSubscriptions.TryGetValue(channel, out var count))
        {
            return;
        }

        if (count <= 1)
        {
            _zoneSubscriptions.Remove(channel);
            await _redis.Subscriber.UnsubscribeAsync(RedisChannel.Sharded(channel));
            return;
        }

        _zoneSubscriptions[channel] = count - 1;
    }

    private void AddSessionToZoneIndex(string channel, GatewaySession session)
    {
        if (!_sessionsByZone.TryGetValue(channel, out var sessions))
        {
            sessions = new Dictionary<string, GatewaySession>(StringComparer.Ordinal);
            _sessionsByZone[channel] = sessions;
        }

        sessions[session.SessionId] = session;
        PublishZoneSnapshot(channel, sessions);
    }

    private void RemoveSessionFromZoneIndex(string channel, string sessionId)
    {
        if (!_sessionsByZone.TryGetValue(channel, out var sessions))
        {
            return;
        }

        sessions.Remove(sessionId);
        if (sessions.Count == 0)
        {
            _sessionsByZone.Remove(channel);
            _sessionsByZoneSnapshot.TryRemove(channel, out _);
            return;
        }

        PublishZoneSnapshot(channel, sessions);
    }

    private void PublishZoneSnapshot(string channel, Dictionary<string, GatewaySession> sessions)
        => _sessionsByZoneSnapshot[channel] = sessions.Values.ToArray();

    private void DispatchServerMessage(GatewayEventChannelKind kind, RedisChannel channel, RedisValue value)
    {
        var handler = _messageHandler;
        if (handler is null)
        {
            _logger.LogDebug("Gateway subscription message received before handler was ready for {Channel}.", channel);
            return;
        }

        handler(kind, channel, value);
    }

    private sealed class SessionAoiState
    {
        public string CenterZoneKey { get; set; } = "";

        public HashSet<string> Zones { get; } = new(StringComparer.Ordinal);
    }

    private enum SubscriptionCommandKind
    {
        Register,
        Unregister,
        UpdateZones
    }

    private sealed record SubscriptionCommand(
        SubscriptionCommandKind Kind,
        GatewaySession Session,
        PacketWorldPosition Position,
        TaskCompletionSource<bool> Completion)
    {
        public string SessionId => Session.SessionId;

        public long UserDbId => Session.UserDbId;

        public static SubscriptionCommand Register(GatewaySession session, PacketWorldPosition position)
            => new(SubscriptionCommandKind.Register, session, position, CreateCompletion());

        public static SubscriptionCommand Unregister(GatewaySession session)
            => new(SubscriptionCommandKind.Unregister, session, new PacketWorldPosition(), CreateCompletion());

        public static SubscriptionCommand UpdateZones(GatewaySession session, PacketWorldPosition position)
            => new(SubscriptionCommandKind.UpdateZones, session, position, CreateCompletion());

        private static TaskCompletionSource<bool> CreateCompletion()
            => new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
