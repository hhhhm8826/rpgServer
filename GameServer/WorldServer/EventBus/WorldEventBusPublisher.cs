using Google.Protobuf;
using GameServer.Shared.EventBus;
using GameServer.Shared.Grains;
using GameServer.Shared.Protocol;
using StackExchange.Redis;

namespace GameServer.WorldServer.EventBus;

public sealed class WorldEventBusPublisher
{
    private readonly EventBusRedisConnection _redis;

    public WorldEventBusPublisher(EventBusRedisConnection redis)
    {
        _redis = redis;
    }

    public Task PublishGatewayBroadcastAsync(string gatewayId, ServerResEnvelope envelope)
        => PublishAsync(EventBusChannels.GatewayBroadcast(gatewayId), envelope.ToByteArray());

    // 미구현 골격: 저빈도 유저 개인 이벤트와 reliable reconnect 이벤트용
    public Task PublishUserAsync(long userDbId, ServerResEnvelope envelope)
        => PublishAsync(EventBusChannels.User(userDbId), envelope.ToByteArray());

    public Task PublishZoneAsync(ZoneDelta delta)
    {
        // ZoneGrain의 runtime entity delta를 Gateway AOI pipeline으로 전달함
        var envelope = new ServerResEnvelope
        {
            Kind = ServerResKind.AoiDelta,
            DeliveryPolicy = delta.DeliveryPolicy,
            AoiDelta = delta.ToPacketAoiDelta()
        };

        return PublishAsync(EventBusChannels.Zone(delta.MapId, delta.ZoneX, delta.ZoneY), envelope.ToByteArray());
    }

    private Task PublishAsync(string channel, byte[] payload)
        => _redis.Subscriber.PublishAsync(RedisChannel.Sharded(channel), payload);
}
