using GameServer.GatewayServer.Sessions;
using GameServer.Shared.EventBus;
using GameServer.Shared.Protocol;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using System.Threading.Channels;
using PacketWorldPosition = GameServer.Shared.Protocol.WorldPosition;
using ProtocolErrorCode = GameServer.Shared.Protocol.ErrorCode;

namespace GameServer.GatewayServer.Networking;

public sealed class GatewayEventBus : BackgroundService
{
    private readonly EventBusRedisConnection _redis;
    private readonly GatewaySessionRegistry _sessions;
    private readonly GatewaySubscriptionManager _subscriptions;
    private readonly GatewayZoneEventRouter _zoneRouter;
    private readonly GatewayOptions _options;
    private readonly ILogger<GatewayEventBus> _logger;
    private readonly Channel<QueuedGatewayEvent> _eventQueue;

    public GatewayEventBus(
        EventBusRedisConnection redis,
        GatewaySessionRegistry sessions,
        GatewaySubscriptionManager subscriptions,
        GatewayZoneEventRouter zoneRouter,
        IOptions<GatewayOptions> options,
        ILogger<GatewayEventBus> logger)
    {
        _redis = redis;
        _sessions = sessions;
        _subscriptions = subscriptions;
        _zoneRouter = zoneRouter;
        _options = options.Value;
        _logger = logger;
        // Redis Pub/Sub 수신 콜백은 여기서 시작해 이벤트 종류별 Gateway 내부 파이프라인으로 나뉨
        _subscriptions.SetMessageHandler(EnqueueServerMessage);
        _eventQueue = Channel.CreateBounded<QueuedGatewayEvent>(new BoundedChannelOptions(_options.NormalizedEventQueueCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = false
        });
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await SubscribeBroadcastChannelAsync();
        await WriteGatewayHeartbeatAsync();

        var workers = Enumerable.Range(0, _options.NormalizedEventQueueWorkerCount)
            .Select(index => ProcessEventQueueAsync(index, stoppingToken))
            .ToArray();

        using var timer = new PeriodicTimer(_options.GatewayHeartbeatInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                await WriteGatewayHeartbeatAsync();
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        finally
        {
            _eventQueue.Writer.TryComplete();
            try
            {
                await Task.WhenAll(workers).WaitAsync(TimeSpan.FromSeconds(5));
            }
            catch (Exception ex) when (ex is OperationCanceledException or TimeoutException)
            {
            }
        }
    }

    public async Task RegisterSessionAsync(GatewaySession session, PacketWorldPosition position, CancellationToken cancellationToken = default)
    {
        await _subscriptions.RegisterSessionAsync(session, position, cancellationToken);
    }

    public async Task UnregisterSessionAsync(GatewaySession session, CancellationToken cancellationToken = default)
    {
        await _subscriptions.UnregisterSessionAsync(session, cancellationToken);
    }

    public async Task UpdateSessionZonesAsync(GatewaySession session, PacketWorldPosition position, CancellationToken cancellationToken = default)
    {
        await _subscriptions.UpdateSessionZonesAsync(session, position, cancellationToken);
    }

    public Task<bool> IsGatewayAliveAsync(string gatewayId)
        => _redis.Database.KeyExistsAsync(EventBusChannels.GatewayAliveKey(gatewayId));

    private async Task SubscribeBroadcastChannelAsync()
    {
        var queue = await _redis.Subscriber.SubscribeAsync(
            RedisChannel.Sharded(EventBusChannels.GatewayBroadcast(_options.GatewayId)));
        queue.OnMessage(message => EnqueueServerMessage(GatewayEventChannelKind.Broadcast, message.Channel, message.Message));
    }

    private void EnqueueServerMessage(GatewayEventChannelKind kind, RedisChannel channel, RedisValue value)
    {
        try
        {
            var envelope = ParseServerEnvelope(value);
            if (envelope is null)
            {
                return;
            }

            if (kind == GatewayEventChannelKind.Zone)
            {
                // Zone AOI는 일반 세션 이벤트 큐를 거치지 않고 latest 병합 라우터로 보냄
                _zoneRouter.Enqueue(channel.ToString(), envelope);
                return;
            }

            var item = new QueuedGatewayEvent(kind, channel.ToString(), envelope);
            if (_eventQueue.Writer.TryWrite(item))
            {
                return;
            }

            _ = EnqueueServerMessageAsync(item);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Gateway event enqueue failed for {Channel}.", channel);
        }
    }

    private async Task EnqueueServerMessageAsync(QueuedGatewayEvent item)
    {
        try
        {
            await _eventQueue.Writer.WriteAsync(item);
        }
        catch (ChannelClosedException)
        {
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Gateway event queued write failed for {Channel}.", item.Channel);
        }
    }

    private async Task ProcessEventQueueAsync(int workerIndex, CancellationToken stoppingToken)
    {
        try
        {
            await foreach (var item in _eventQueue.Reader.ReadAllAsync(stoppingToken))
            {
                try
                {
                    switch (item.Kind)
                    {
                        case GatewayEventChannelKind.Broadcast:
                        case GatewayEventChannelKind.User:
                            await DeliverServerResponseAsync(item.Envelope);
                            break;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "Gateway queued event processing failed for {Kind} on {Channel}.", item.Kind, item.Channel);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Gateway event queue worker {WorkerIndex} failed.", workerIndex);
        }
    }

    private static ServerResEnvelope? ParseServerEnvelope(RedisValue value)
    {
        var bytes = (byte[]?)value;
        return bytes is null || bytes.Length == 0
            ? null
            : ServerResEnvelope.Parser.ParseFrom(bytes);
    }

    private async Task DeliverServerResponseAsync(ServerResEnvelope? envelope)
    {
        if (envelope is null)
        {
            return;
        }

        GatewaySession? session = null;
        if (!string.IsNullOrWhiteSpace(envelope.SessionId))
        {
            session = _sessions.Get(envelope.SessionId);
        }

        if (session is null && envelope.UserDbId > 0)
        {
            session = _sessions.GetByUserDbId(envelope.UserDbId);
        }

        if (session is null)
        {
            return;
        }

        if (envelope.Kind == ServerResKind.MoveNty
            && envelope.Move?.AuthoritativePosition is not null)
        {
            // 개인 이동 알림 경로에서도 세션 위치를 갱신해 이후 AOI 필터링 기준을 맞춤
            session.UpdatePosition(envelope.Move.AuthoritativePosition);
            await UpdateSessionZonesAsync(session, envelope.Move.AuthoritativePosition);
        }

        await session.SendAsync(envelope, CancellationToken.None);

        if (ShouldForceCloseSession(envelope))
        {
            _logger.LogWarning(
                "Forcing session close for user {UserDbId}, session {SessionId}: {ErrorCode}.",
                session.UserDbId,
                session.SessionId,
                envelope.Error?.Code);
            await session.DisposeAsync();
        }
    }

    private static bool ShouldForceCloseSession(ServerResEnvelope envelope)
        => envelope.Kind == ServerResKind.Error
            && envelope.Error?.Code == ProtocolErrorCode.LoginPresenceInitFailed;

    private async Task WriteGatewayHeartbeatAsync()
    {
        var db = _redis.Database;
        await db.SetAddAsync(EventBusChannels.GatewaySetKey, _options.GatewayId);
        await db.StringSetAsync(
            EventBusChannels.GatewayAliveKey(_options.GatewayId),
            Environment.MachineName,
            _options.GatewayHeartbeatTTL);
    }

    private sealed record QueuedGatewayEvent(
        GatewayEventChannelKind Kind,
        string Channel,
        ServerResEnvelope Envelope);
}

internal enum GatewayEventChannelKind
{
    Broadcast,
    User,
    Zone
}
