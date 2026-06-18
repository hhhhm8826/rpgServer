using Microsoft.Extensions.Options;

namespace GameServer.WorldServer.Persistence;

public sealed class DynamoFlushWorker : BackgroundService
{
    private readonly WriteBehindGrainStorage _storage;
    private readonly WorldStorageOptions _options;
    private readonly ILogger<DynamoFlushWorker> _logger;

    public DynamoFlushWorker(
        WriteBehindGrainStorage storage,
        IOptions<WorldStorageOptions> options,
        ILogger<DynamoFlushWorker> logger)
    {
        _storage = storage;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Redis dirty set에서 flush 예정 시간이 지난 state만 배치 처리함
                await Task.Delay(_options.DirtyScanInterval, stoppingToken);
                await _storage.FlushDirtyBatchAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Dirty grain state flush failed.");
            }
        }
    }
}
