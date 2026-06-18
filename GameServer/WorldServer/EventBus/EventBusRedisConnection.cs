using StackExchange.Redis;

namespace GameServer.WorldServer.EventBus;

public sealed class EventBusRedisConnection : IAsyncDisposable
{
    private readonly IConnectionMultiplexer _connection;

    public EventBusRedisConnection(string connectionString)
    {
        _connection = ConnectionMultiplexer.Connect(connectionString);
    }

    public IDatabase Database => _connection.GetDatabase();

    public ISubscriber Subscriber => _connection.GetSubscriber();

    public ValueTask DisposeAsync() => _connection.DisposeAsync();
}
