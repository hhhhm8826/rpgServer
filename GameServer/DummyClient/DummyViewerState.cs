using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using GameServer.Shared.Protocol;

internal sealed class DummyViewerState
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private const float CellSizeMeters = 50f;

    // 관찰자 위치는 MoveNty 기준, 주변 entity는 AOI delta 기준으로만 갱신함
    private readonly ConcurrentDictionary<int, DummyEntity> _entities = new();
    private readonly ConcurrentDictionary<long, DummyEntity> _aoiEntities = new();
    private readonly DummyOptions _options;
    private readonly DummyStats _stats;
    private readonly Stopwatch _stopwatch = Stopwatch.StartNew();
    private readonly ConcurrentDictionary<long, long> _lastViewerUpsertTicks = new();
    private readonly ConcurrentDictionary<long, long> _lastViewerRemoveTicks = new();
    private string _snapshotJson = "{}";
    private string _completionMessage = "";
    private long _viewerAoiDeltas;
    private long _viewerAoiUpserts;
    private long _viewerAoiRemoves;
    private long _viewerRemoveHits;
    private long _viewerRemoveMisses;
    private long _viewerRemoveAfterRecentUpsert;
    private long _viewerUpsertAfterRecentRemove;
    private int _viewerMaxEntities;
    private int _completed;

    public DummyViewerState(DummyOptions options, DummyStats stats)
    {
        _options = options;
        _stats = stats;
    }

    public void Upsert(int index, long userDbId, WorldPosition position, bool connected)
    {
        _entities[index] = new DummyEntity(index, userDbId, position.MapId, position.X, position.Y, position.Z, connected);
    }

    public void MarkDisconnected(int index)
    {
        if (!_entities.TryGetValue(index, out var entity))
        {
            return;
        }

        _entities[index] = entity with { Connected = false };
        if (index == 0)
        {
            _aoiEntities.Clear();
        }
    }

    public void ApplyAoiDelta(AoiDelta? delta)
    {
        if (delta is null)
        {
            return;
        }

        Interlocked.Increment(ref _viewerAoiDeltas);
        foreach (var remove in delta.Removes)
        {
            Interlocked.Increment(ref _viewerAoiRemoves);
            var now = Stopwatch.GetTimestamp();
            if (_lastViewerUpsertTicks.TryGetValue(remove, out var lastUpsert)
                && IsRecent(lastUpsert, now, TimeSpan.FromSeconds(1)))
            {
                Interlocked.Increment(ref _viewerRemoveAfterRecentUpsert);
            }

            if (_aoiEntities.TryRemove(remove, out _))
            {
                Interlocked.Increment(ref _viewerRemoveHits);
            }
            else
            {
                Interlocked.Increment(ref _viewerRemoveMisses);
            }

            _lastViewerRemoveTicks[remove] = now;
        }

        foreach (var upsert in delta.Upserts)
        {
            if (upsert.Position is null)
            {
                continue;
            }

            Interlocked.Increment(ref _viewerAoiUpserts);
            var now = Stopwatch.GetTimestamp();
            if (_lastViewerRemoveTicks.TryGetValue(upsert.EntityId, out var lastRemove)
                && IsRecent(lastRemove, now, TimeSpan.FromSeconds(1)))
            {
                Interlocked.Increment(ref _viewerUpsertAfterRecentRemove);
            }

            _aoiEntities[upsert.EntityId] = new DummyEntity(
                Index: -1,
                UserDbId: upsert.EntityId,
                MapId: upsert.Position.MapId,
                X: upsert.Position.X,
                Y: upsert.Position.Y,
                Z: upsert.Position.Z,
                Connected: true);
            UpdateMaxEntities(_aoiEntities.Count);
        }

    }

    public void MarkCompleted(string message)
    {
        Volatile.Write(ref _completionMessage, message);
        Volatile.Write(ref _completed, 1);
        RefreshSnapshotJson();
    }

    public ViewerSnapshot Snapshot()
    {
        _entities.TryGetValue(0, out var observer);

        var renderEntities = observer is null
            ? Array.Empty<ViewerEntity>()
            : GetRenderEntities(observer);
        var nearbyCount = observer is null
            ? 0
            : renderEntities.Length;

        return new ViewerSnapshot(
            ObserveIndex: observer?.Index ?? 0,
            ViewRadius: _options.ViewRadius,
            ViewExitRadius: Math.Max(_options.ViewRadius, _options.ViewExitRadius),
            CellSize: CellSizeMeters,
            RenderIntervalMs: Math.Clamp(_options.RenderIntervalMs, 200, 2000),
            ElapsedSec: _stopwatch.Elapsed.TotalSeconds,
            DurationSec: _options.Duration.TotalSeconds,
            Completed: Volatile.Read(ref _completed) != 0,
            CompletionMessage: Volatile.Read(ref _completionMessage),
            Stats: new ViewerStats(_stats.ActiveConnected, _stats.PeakConnected, _stats.LoginAccepted, _stats.LoginRejected, _stats.SentMoves, _stats.MoveNty, _stats.AoiDeltas, _stats.Errors, _stats.LoginAttempts, _stats.LoginTimeouts, _stats.LastStatus, _stats.ErrorSummaries.Take(6).ToArray()),
            Aoi: Diagnostics(),
            Observer: observer,
            CurrentZone: observer is null ? null : ToZone(observer),
            Entities: renderEntities,
            NearbyCount: nearbyCount);
    }

    private ViewerEntity[] GetRenderEntities(DummyEntity observer)
    {
        var entities = new List<ViewerEntity>();
        foreach (var entity in _aoiEntities.Values)
        {
            if (entity.UserDbId == observer.UserDbId
                || !entity.Connected
                || entity.MapId != observer.MapId)
            {
                continue;
            }

            entities.Add(new ViewerEntity(entity.UserDbId, entity.X, entity.Y, entity.Connected));
        }

        return entities.ToArray();
    }

    private static ViewerZone ToZone(DummyEntity entity)
    {
        var cellX = FloorToCell(entity.X);
        var cellY = FloorToCell(entity.Y);
        return new ViewerZone(entity.MapId, cellX, cellY, $"{entity.MapId}:{cellX}:{cellY}");
    }

    public string SnapshotJson => Volatile.Read(ref _snapshotJson);

    // HTTP 요청마다 계산하지 않도록 별도 snapshot loop가 JSON 캐시를 갱신함
    public void RefreshSnapshotJson()
        => Volatile.Write(ref _snapshotJson, JsonSerializer.Serialize(Snapshot(), JsonOptions));

    private static int FloorToCell(float value) => (int)MathF.Floor(value / CellSizeMeters);

    public ViewerAoiDiagnostics Diagnostics() => new(
        Deltas: Interlocked.Read(ref _viewerAoiDeltas),
        Upserts: Interlocked.Read(ref _viewerAoiUpserts),
        Removes: Interlocked.Read(ref _viewerAoiRemoves),
        RemoveHits: Interlocked.Read(ref _viewerRemoveHits),
        RemoveMisses: Interlocked.Read(ref _viewerRemoveMisses),
        RemoveAfterRecentUpsert: Interlocked.Read(ref _viewerRemoveAfterRecentUpsert),
        UpsertAfterRecentRemove: Interlocked.Read(ref _viewerUpsertAfterRecentRemove),
        CurrentEntities: _aoiEntities.Count,
        MaxEntities: Volatile.Read(ref _viewerMaxEntities));

    private static bool IsRecent(long thenTicks, long nowTicks, TimeSpan window)
        => Stopwatch.GetElapsedTime(thenTicks, nowTicks) <= window;

    private void UpdateMaxEntities(int current)
    {
        while (true)
        {
            var previous = Volatile.Read(ref _viewerMaxEntities);
            if (current <= previous
                || Interlocked.CompareExchange(ref _viewerMaxEntities, current, previous) == previous)
            {
                return;
            }
        }
    }
}

internal sealed record DummyEntity(int Index, long UserDbId, string MapId, float X, float Y, float Z, bool Connected);

internal sealed record ViewerEntity(long UserDbId, float X, float Y, bool Connected);

internal sealed record ViewerStats(int Connected, int PeakConnected, long LoginAccepted, int LoginRejected, long SentMoves, long MoveNty, long AoiDeltas, long Errors, long LoginAttempts, long LoginTimeouts, string LastStatus, IReadOnlyCollection<ErrorSummary> ErrorSummaries);

internal sealed record ViewerAoiDiagnostics(long Deltas, long Upserts, long Removes, long RemoveHits, long RemoveMisses, long RemoveAfterRecentUpsert, long UpsertAfterRecentRemove, int CurrentEntities, int MaxEntities);

internal sealed record ViewerZone(string MapId, int CellX, int CellY, string Key);

internal sealed record ViewerSnapshot(
    int ObserveIndex,
    float ViewRadius,
    float ViewExitRadius,
    float CellSize,
    int RenderIntervalMs,
    double ElapsedSec,
    double DurationSec,
    bool Completed,
    string CompletionMessage,
    ViewerStats Stats,
    ViewerAoiDiagnostics Aoi,
    DummyEntity? Observer,
    ViewerZone? CurrentZone,
    IReadOnlyCollection<ViewerEntity> Entities,
    int NearbyCount);
