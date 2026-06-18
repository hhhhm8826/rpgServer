using System.Collections.Concurrent;
using GameServer.Shared.Protocol;
using ProtocolErrorCode = GameServer.Shared.Protocol.ErrorCode;

internal sealed class DummyStats
{
    // 에러는 메시지별로 집계해 10건 이상 중단 같은 판단에 활용
    private readonly ConcurrentDictionary<string, long> _errorsByMessage = new(StringComparer.Ordinal);
    private int _activeConnected;
    private int _peakConnected;
    private int _loginRejected;
    private long _sentMoves;
    private long _moveNty;
    private long _aoiDeltas;
    private long _errors;
    private long _loginAttempts;
    private long _loginAccepted;
    private long _loginTimeouts;
    private string _lastStatus = "Waiting for first accepted LoginRes.";

    public int ActiveConnected => _activeConnected;

    public int PeakConnected => _peakConnected;

    public long LoginAccepted => _loginAccepted;

    public int LoginRejected => _loginRejected;

    public long SentMoves => _sentMoves;

    public long MoveNty => _moveNty;

    public long AoiDeltas => _aoiDeltas;

    public long Errors => _errors;

    public long LoginTimeouts => _loginTimeouts;

    public long LoginAttempts => _loginAttempts;

    public string LastStatus => Volatile.Read(ref _lastStatus);

    public ErrorSummary[] ErrorSummaries => _errorsByMessage
        .Select(x => new ErrorSummary(x.Key, x.Value))
        .OrderByDescending(x => x.Count)
        .ThenBy(x => x.Key, StringComparer.Ordinal)
        .ToArray();

    public void IncrementConnected()
    {
        Interlocked.Increment(ref _loginAccepted);
        var active = Interlocked.Increment(ref _activeConnected);

        while (true)
        {
            var currentPeak = Volatile.Read(ref _peakConnected);
            if (active <= currentPeak)
            {
                return;
            }

            if (Interlocked.CompareExchange(ref _peakConnected, active, currentPeak) == currentPeak)
            {
                return;
            }
        }
    }

    public void DecrementConnected() => Interlocked.Decrement(ref _activeConnected);

    public void IncrementLoginRejected() => Interlocked.Increment(ref _loginRejected);

    public void IncrementSentMoves() => Interlocked.Increment(ref _sentMoves);

    public void IncrementMoveNty() => Interlocked.Increment(ref _moveNty);

    public void IncrementAoiDeltas() => Interlocked.Increment(ref _aoiDeltas);

    public void RecordClientError(long userDbId, int attempt, Exception exception)
    {
        var key = $"client: {exception.GetType().Name}: {exception.Message}";
        RecordError(key);
        SetStatus($"{userDbId} failed: {exception.GetType().Name}: {exception.Message}. attempt={attempt}");
    }

    public void RecordServerError(ErrorRes? error)
    {
        var key = (error?.Code ?? ProtocolErrorCode.Unknown).ToString();
        if (!string.IsNullOrWhiteSpace(error?.Message))
        {
            key = $"{key}: {error.Message}";
        }

        RecordError(key);
        SetStatus($"Server error: {key}");
    }

    private void RecordError(string key)
    {
        Interlocked.Increment(ref _errors);
        _errorsByMessage.AddOrUpdate(key, 1, static (_, current) => current + 1);
    }

    public void IncrementLoginTimeouts() => Interlocked.Increment(ref _loginTimeouts);

    public void IncrementLoginAttempts() => Interlocked.Increment(ref _loginAttempts);

    public void SetStatus(string status) => Volatile.Write(ref _lastStatus, status);
}

internal sealed record ErrorSummary(string Key, long Count);
