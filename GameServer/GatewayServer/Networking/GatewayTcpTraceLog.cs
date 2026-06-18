using System.Text.Json;
using Microsoft.Extensions.Options;

namespace GameServer.GatewayServer.Networking;

public sealed class GatewayTcpTraceLog
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = false
    };

    private readonly GatewayOptions _options;
    private readonly SemaphoreSlim _writeLock = new(1, 1);

    public GatewayTcpTraceLog(IOptions<GatewayOptions> options)
    {
        _options = options.Value;
    }

    public async Task WriteAsync(
        string eventName,
        string phase,
        string reason,
        string sessionId,
        long userDbId,
        string remote,
        string? detail = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(_options.TcpTraceLogPath))
        {
            return;
        }

        var path = Path.GetFullPath(_options.TcpTraceLogPath);
        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }

        var line = JsonSerializer.Serialize(new
        {
            timestampUtc = DateTimeOffset.UtcNow,
            eventName,
            phase,
            reason,
            sessionId,
            userDbId,
            remote,
            detail
        }, JsonOptions);

        await _writeLock.WaitAsync(cancellationToken);
        try
        {
            await File.AppendAllTextAsync(path, line + Environment.NewLine, cancellationToken);
        }
        finally
        {
            _writeLock.Release();
        }
    }
}
