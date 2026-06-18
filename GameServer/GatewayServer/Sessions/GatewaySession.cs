using System.Net.Sockets;
using GameServer.Shared.Protocol;

namespace GameServer.GatewayServer.Sessions;

public sealed class GatewaySession : IAsyncDisposable
{
    private readonly TcpClient _client;
    private readonly NetworkStream _stream;
    // 같은 TCP stream에 여러 서버 이벤트가 섞이지 않도록 세션 단위 송신만 직렬화한다.
    private readonly SemaphoreSlim _sendLock = new(1, 1);
    // AOI 필터링은 Gateway가 가진 서버 확정 위치를 기준으로 한다.
    private WorldPosition _position = new() { MapId = "default" };
    private int _disposed;

    public GatewaySession(string sessionId, long userDbId, TcpClient client)
    {
        SessionId = sessionId;
        UserDbId = userDbId;
        RemoteEndPoint = client.Client.RemoteEndPoint?.ToString() ?? "unknown";
        _client = client;
        _stream = client.GetStream();
    }

    public string SessionId { get; }

    public long UserDbId { get; }

    public string RemoteEndPoint { get; }

    public NetworkStream Stream => _stream;

    public WorldPosition GetPosition() => _position.Clone();

    public void UpdatePosition(WorldPosition position)
    {
        _position = position.Clone();
    }

    public async Task SendAsync(ServerResEnvelope envelope, CancellationToken cancellationToken)
    {
        await _sendLock.WaitAsync(cancellationToken);
        try
        {
            // 송신 직전에 session/user 식별자를 덮어써 잘못된 envelope 재사용을 막는다.
            envelope.SessionId = SessionId;
            envelope.UserDbId = UserDbId;
            await ProtobufPacketCodec.WriteAsync(_stream, envelope, cancellationToken);
        }
        finally
        {
            _sendLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _sendLock.Dispose();
        await _stream.DisposeAsync();
        _client.Dispose();
    }
}
