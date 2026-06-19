using System.Diagnostics;
using System.Threading.Channels;
using GameServer.GatewayServer.Sessions;
using GameServer.Shared.Protocol;
using Microsoft.Extensions.Options;
using PacketEntitySnapshot = GameServer.Shared.Protocol.EntitySnapshot;

namespace GameServer.GatewayServer.Networking;

public sealed class GatewayZoneEventRouter : BackgroundService
{
    private readonly ZonePartition[] _partitions;

    public GatewayZoneEventRouter(
        GatewaySubscriptionManager subscriptions,
        GatewayAoiAggregator aoiAggregator,
        IOptions<GatewayOptions> options,
        ILogger<GatewayZoneEventRouter> logger)
    {
        var gatewayOptions = options.Value;
        _partitions = Enumerable.Range(0, gatewayOptions.NormalizedLatestEventPartitionCount)
            .Select(index => new ZonePartition(index, subscriptions, aoiAggregator, gatewayOptions, logger))
            .ToArray();
    }

    public void Enqueue(string channel, ServerResEnvelope envelope)
    {
        if (envelope.AoiDelta is null)
        {
            return;
        }

        var partition = _partitions[GatewayPartitionHash.ForZone(channel, _partitions.Length)];
        partition.Enqueue(new QueuedZoneEvent(channel, envelope));
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
        => Task.WhenAll(_partitions.Select(partition => partition.RunAsync(stoppingToken)));

    private sealed class ZonePartition
    {
        private const int MaxQueuedEventsPerLoop = 256;

        private readonly int _index;
        private readonly GatewaySubscriptionManager _subscriptions;
        private readonly GatewayAoiAggregator _aoiAggregator;
        private readonly GatewayOptions _options;
        private readonly ILogger _logger;
        private readonly Channel<QueuedZoneEvent> _events;
        // Zone별 latest-only AOI를 tick 전까지 최신 entity 상태 하나로 병합한다.
        private readonly Dictionary<string, PendingLatestZoneDelta> _latestByZone = new(StringComparer.Ordinal);
        private DateTimeOffset _lastLatestDropLog = DateTimeOffset.MinValue;

        public ZonePartition(
            int index,
            GatewaySubscriptionManager subscriptions,
            GatewayAoiAggregator aoiAggregator,
            GatewayOptions options,
            ILogger logger)
        {
            _index = index;
            _subscriptions = subscriptions;
            _aoiAggregator = aoiAggregator;
            _options = options;
            _logger = logger;
            _events = Channel.CreateBounded<QueuedZoneEvent>(new BoundedChannelOptions(_options.NormalizedLatestEventQueueCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = false
            });
        }

        public void Enqueue(QueuedZoneEvent item)
        {
            if (_events.Writer.TryWrite(item))
            {
                return;
            }

            if (item.Envelope.DeliveryPolicy == ServerDeliveryPolicy.LatestPerEntity)
            {
                LogLatestDrops(item.Envelope.AoiDelta?.Upserts.Count + item.Envelope.AoiDelta?.Removes.Count ?? 1);
                return;
            }

            _ = EnqueueAsync(item);
        }

        public async Task RunAsync(CancellationToken stoppingToken)
        {
            try
            {
                using var timer = new PeriodicTimer(_options.LatestEventTick);
                var waitRead = _events.Reader.WaitToReadAsync(stoppingToken).AsTask();
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

                        await ProcessLatestTickAsync(stoppingToken);
                        waitTick = timer.WaitForNextTickAsync(stoppingToken).AsTask();
                    }

                    if (waitRead.IsCompleted)
                    {
                        if (!await waitRead)
                        {
                            break;
                        }

                        var processed = 0;
                        while (processed < MaxQueuedEventsPerLoop && _events.Reader.TryRead(out var item))
                        {
                            await ProcessEventAsync(item, stoppingToken);
                            processed++;

                            if (waitTick.IsCompleted)
                            {
                                break;
                            }
                        }

                        waitRead = _events.Reader.WaitToReadAsync(stoppingToken).AsTask();
                    }
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Gateway zone partition {PartitionIndex} failed.", _index);
            }
        }

        private async Task EnqueueAsync(QueuedZoneEvent item)
        {
            try
            {
                await _events.Writer.WriteAsync(item);
            }
            catch (ChannelClosedException)
            {
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Gateway zone partition {PartitionIndex} event queued write failed for {Channel}.", _index, item.Channel);
            }
        }

        private async Task ProcessEventAsync(QueuedZoneEvent item, CancellationToken cancellationToken)
        {
            if (item.Envelope.DeliveryPolicy == ServerDeliveryPolicy.LatestPerEntity)
            {
                // 이동 AOI는 손실 가능 이벤트이므로 Zone 단위 최신값만 유지
                MergeLatest(item.Channel, item.Envelope);
                return;
            }

            // Reliable AOI가 더 최신 상태를 담고 있으면 pending latest의 낮은 버전을 먼저 제거
            RemoveReliableStateFromLatest(item.Channel, item.Envelope);
            await FlushLatestZoneAsync(item.Channel, cancellationToken);
            await DeliverReliableZoneEnvelopeAsync(item.Channel, item.Envelope, cancellationToken);
        }

        private void MergeLatest(string channel, ServerResEnvelope envelope)
        {
            if (!_latestByZone.TryGetValue(channel, out var pending))
            {
                if (_latestByZone.Count >= _options.NormalizedLatestEventQueueCapacity)
                {
                    LogLatestDrops(envelope.AoiDelta?.Upserts.Count + envelope.AoiDelta?.Removes.Count ?? 1);
                    return;
                }

                pending = new PendingLatestZoneDelta();
                _latestByZone[channel] = pending;
            }

            pending.Merge(envelope);
        }

        private async Task ProcessLatestTickAsync(CancellationToken cancellationToken)
        {
            var budget = _options.LatestEventPartitionProcessingBudget;
            var stopwatch = Stopwatch.StartNew();
            var pairs = _latestByZone.ToArray();
            var dropped = 0;

            for (var i = 0; i < pairs.Length; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (stopwatch.Elapsed >= budget)
                {
                    // 처리 예산을 넘긴 latest-only 이벤트는 버린다
                    dropped += DropRemainingLatest(pairs, i);
                    break;
                }

                if (!pairs[i].Value.TryDrain(out var envelope, out var entityCount))
                {
                    continue;
                }

                var sessions = _subscriptions.GetSessionsForZone(pairs[i].Key);
                if (sessions.Count == 0)
                {
                    dropped += entityCount;
                    continue;
                }

                await _aoiAggregator.EnqueueLatestZoneAsync(envelope, sessions, cancellationToken);
            }

            LogLatestDrops(dropped);
        }

        private int DropRemainingLatest(KeyValuePair<string, PendingLatestZoneDelta>[] pairs, int startIndex)
        {
            var dropped = 0;
            for (var i = startIndex; i < pairs.Length; i++)
            {
                if (pairs[i].Value.TryDrain(out _, out var entityCount))
                {
                    dropped += entityCount;
                }
            }

            return dropped;
        }

        private async Task FlushLatestZoneAsync(string channel, CancellationToken cancellationToken)
        {
            if (!_latestByZone.TryGetValue(channel, out var pending)
                || !pending.TryDrain(out var envelope, out _))
            {
                return;
            }

            var sessions = _subscriptions.GetSessionsForZone(channel);
            await _aoiAggregator.EnqueueLatestZoneAsync(envelope, sessions, cancellationToken);
        }

        private async Task DeliverReliableZoneEnvelopeAsync(
            string channel,
            ServerResEnvelope envelope,
            CancellationToken cancellationToken)
        {
            var sessions = _subscriptions.GetSessionsForZone(channel);
            await _aoiAggregator.EnqueueReliableZoneAsync(envelope, sessions, cancellationToken);
        }

        private void RemoveReliableStateFromLatest(string channel, ServerResEnvelope envelope)
        {
            if (envelope.AoiDelta is not { } delta
                || !_latestByZone.TryGetValue(channel, out var pending))
            {
                return;
            }

            if (delta.Removes.Count > 0)
            {
                pending.Remove(delta.Removes);
            }

            if (delta.Upserts.Count > 0)
            {
                pending.RemoveUpsertsSupersededBy(delta.Upserts);
            }
        }

        private void LogLatestDrops(int dropped)
        {
            if (dropped <= 0)
            {
                return;
            }

            var now = DateTimeOffset.UtcNow;
            if (now - _lastLatestDropLog < TimeSpan.FromSeconds(10))
            {
                return;
            }

            _lastLatestDropLog = now;
            _logger.LogInformation(
                "Dropped {DroppedCount} latest-only AOI entities in zone partition {PartitionIndex}.",
                dropped,
                _index);
        }
    }

    private sealed record QueuedZoneEvent(string Channel, ServerResEnvelope Envelope);

    private sealed class PendingLatestZoneDelta
    {
        private readonly Dictionary<long, PacketEntitySnapshot> _upserts = new();
        private readonly Dictionary<long, AoiRemoveReason> _removes = new();
        private string _mapId = "default";
        private int _zoneX;
        private int _zoneY;
        private long _sequence;

        public void Merge(ServerResEnvelope envelope)
        {
            var delta = envelope.AoiDelta;
            if (delta is null)
            {
                return;
            }

            _mapId = delta.MapId;
            _zoneX = delta.ZoneX;
            _zoneY = delta.ZoneY;
            _sequence = Math.Max(_sequence, delta.Sequence);

            for (var i = 0; i < delta.Removes.Count; i++)
            {
                var remove = delta.Removes[i];
                _upserts.Remove(remove);
                _removes[remove] = RemoveReasonAt(delta, i);
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

        public void Remove(IEnumerable<long> entityIds)
        {
            foreach (var entityId in entityIds)
            {
                _upserts.Remove(entityId);
                _removes.Remove(entityId);
            }
        }

        public bool TryDrain(out ServerResEnvelope envelope, out int entityCount)
        {
            entityCount = _upserts.Count + _removes.Count;
            if (entityCount == 0)
            {
                envelope = new ServerResEnvelope();
                return false;
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

            envelope = new ServerResEnvelope
            {
                Kind = ServerResKind.AoiDelta,
                DeliveryPolicy = ServerDeliveryPolicy.LatestPerEntity,
                AoiDelta = delta
            };
            return true;
        }

        private static AoiRemoveReason RemoveReasonAt(AoiDelta delta, int index)
            => index >= 0 && index < delta.RemoveReasons.Count
                ? delta.RemoveReasons[index]
                : AoiRemoveReason.EntityRemoved;
    }
}
