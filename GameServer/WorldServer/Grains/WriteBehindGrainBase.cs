using GameServer.WorldServer.Persistence;
using Orleans;
using Orleans.Runtime;

namespace GameServer.WorldServer.Grains;

public abstract class WriteBehindGrainBase<TState> : Grain
    where TState : class, new()
{
    private readonly WriteBehindGrainStorage _storage;

    protected WriteBehindGrainBase(WriteBehindGrainStorage storage)
    {
        _storage = storage;
    }

    protected TState State { get; private set; } = new();

    protected abstract StorageKey StorageKey { get; }

    public override async Task OnActivateAsync(CancellationToken cancellationToken)
    {
        // 활성화 시 Redis 우선 조회 후 miss면 DynamoDB에서 복구함
        State = await _storage.ReadAsync<TState>(StorageKey, cancellationToken) ?? new TState();
        await base.OnActivateAsync(cancellationToken);
    }

    // Flush 전에 TTL을 걸면 Redis 데이터가 먼저 사라질 수 있음
    public override async Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        // 비활성화 시 DynamoDB flush 후 Redis에 TTL을 부여해 짧은 재활성화 캐시로 남김
        await _storage.FlushAndClearDirtyAsync(StorageKey, cancellationToken);
        await _storage.ExpireRedisAsync(StorageKey, cancellationToken);
        await base.OnDeactivateAsync(reason, cancellationToken);
    }

    protected Task WriteStateAsync(CancellationToken cancellationToken = default)
        => _storage.WriteAsync(StorageKey, State, cancellationToken);

    protected Task WriteStateAsync(TimeSpan flushAfter, CancellationToken cancellationToken = default)
        => _storage.WriteAsync(StorageKey, State, flushAfter, cancellationToken);

    protected Task FlushStateAsync(CancellationToken cancellationToken = default)
        => _storage.FlushAndClearDirtyAsync(StorageKey, cancellationToken);
}
