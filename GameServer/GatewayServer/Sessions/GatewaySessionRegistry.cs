using GameServer.Shared.Protocol;

namespace GameServer.GatewayServer.Sessions;

public sealed class GatewaySessionRegistry
{
    // 로컬 Gateway에 붙은 활성 세션만 보관하며 Redis 세션 TTL과 별도로 관리
    private readonly Dictionary<string, GatewaySession> _sessionsById = new(StringComparer.Ordinal);
    // userDbId 기준 단일 세션 조회를 위해 역방향 인덱스를 함께 유지
    private readonly Dictionary<long, string> _sessionIdByUserDbId = new();
    private readonly object _gate = new();

    public int Count
    {
        get
        {
            lock (_gate)
            {
                return _sessionsById.Count;
            }
        }
    }

    public bool TryAdd(GatewaySession session)
    {
        lock (_gate)
        {
            // 한 Gateway 프로세스 안에서는 userDbId/sessionId 모두 중복 등록을 허용하지 않음
            if (_sessionIdByUserDbId.ContainsKey(session.UserDbId) || _sessionsById.ContainsKey(session.SessionId))
            {
                return false;
            }

            _sessionsById[session.SessionId] = session;
            _sessionIdByUserDbId[session.UserDbId] = session.SessionId;
            return true;
        }
    }

    public GatewaySession? Get(string sessionId)
    {
        lock (_gate)
        {
            return _sessionsById.GetValueOrDefault(sessionId);
        }
    }

    public GatewaySession? GetByUserDbId(long userDbId)
    {
        lock (_gate)
        {
            return _sessionIdByUserDbId.TryGetValue(userDbId, out var sessionId)
                ? _sessionsById.GetValueOrDefault(sessionId)
                : null;
        }
    }

    public async Task<bool> SendAsync(string sessionId, ServerResEnvelope envelope, CancellationToken cancellationToken)
    {
        var session = Get(sessionId);
        if (session is null)
        {
            return false;
        }

        await session.SendAsync(envelope, cancellationToken);
        return true;
    }

    public async Task RemoveAsync(string sessionId)
    {
        GatewaySession? session;
        lock (_gate)
        {
            if (!_sessionsById.Remove(sessionId, out session))
            {
                return;
            }

            _sessionIdByUserDbId.Remove(session.UserDbId);
        }

        await session.DisposeAsync();
    }
}
