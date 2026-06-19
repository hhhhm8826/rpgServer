using System.Net;
using System.Net.Sockets;
using GameServer.GatewayServer.Sessions;
using GameServer.Shared.Grains;
using GameServer.Shared.Protocol;
using Microsoft.Extensions.Options;
using Orleans;
using ProtocolErrorCode = GameServer.Shared.Protocol.ErrorCode;

namespace GameServer.GatewayServer.Networking;

public sealed class TcpGatewayService : BackgroundService
{
    private readonly GatewayEventBus _eventBus;
    private readonly GatewayAoiAggregator _aoiAggregator;
    private readonly IGrainFactory _grainFactory;
    private readonly GatewaySessionRegistry _sessions;
    private readonly RedisSessionStore _sessionStore;
    private readonly GatewayOptions _options;
    private readonly ILogger<TcpGatewayService> _logger;
    private readonly ILogger<GatewayMoveScheduler> _moveSchedulerLogger;
    private readonly GatewayTcpTraceLog _traceLog;
    // 총 동접 제한이 아니라 Gateway 인스턴스별 동시 로그인 처리량 제한.
    private readonly SemaphoreSlim _loginSlots;

    public TcpGatewayService(
        GatewayEventBus eventBus,
        GatewayAoiAggregator aoiAggregator,
        IGrainFactory grainFactory,
        GatewaySessionRegistry sessions,
        RedisSessionStore sessionStore,
        IOptions<GatewayOptions> options,
        ILogger<TcpGatewayService> logger,
        ILogger<GatewayMoveScheduler> moveSchedulerLogger,
        GatewayTcpTraceLog traceLog)
    {
        _eventBus = eventBus;
        _aoiAggregator = aoiAggregator;
        _grainFactory = grainFactory;
        _sessions = sessions;
        _sessionStore = sessionStore;
        _options = options.Value;
        _logger = logger;
        _moveSchedulerLogger = moveSchedulerLogger;
        _traceLog = traceLog;
        _loginSlots = new SemaphoreSlim(Math.Max(1, _options.MaxConcurrentLogins), Math.Max(1, _options.MaxConcurrentLogins));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var address = IPAddress.Parse(_options.ListenAddress);
        var listener = new TcpListener(address, _options.Port);
        listener.Start(Math.Max(128, _options.TcpBacklog));
        _logger.LogInformation("Gateway TCP listener started on {Address}:{Port} with backlog {Backlog}.", _options.ListenAddress, _options.Port, _options.TcpBacklog);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var client = await listener.AcceptTcpClientAsync(stoppingToken);
                    _ = Task.Run(() => HandleClientAsync(client, stoppingToken), CancellationToken.None);
                }
                catch (SocketException ex) when (!stoppingToken.IsCancellationRequested)
                {
                    _logger.LogDebug(ex, "Gateway TCP accept failed; continuing listener loop.");
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        finally
        {
            listener.Stop();
        }
    }

    private async Task HandleClientAsync(TcpClient client, CancellationToken serverStoppingToken)
    {
        var remote = client.Client.RemoteEndPoint?.ToString() ?? "unknown";
        GatewaySession? session = null;
        GatewayMoveScheduler? moveScheduler = null;
        GameServer.Shared.Protocol.WorldPosition? initialPosition = null;
        var disconnectReason = "not-accepted";
        var phase = "read-login";

        try
        {
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(serverStoppingToken);
            var stream = client.GetStream();
            var loginEnvelope = await ProtobufPacketCodec.ReadAsync(stream, ClientReqEnvelope.Parser, linkedCts.Token);
            if (loginEnvelope?.Kind != ClientReqKind.Login || loginEnvelope.Login is null)
            {
                await ProtobufPacketCodec.WriteAsync(stream, Error(ProtocolErrorCode.ProtocolLoginRequired, "First packet must be Login."), linkedCts.Token);
                return;
            }

            var userDbId = loginEnvelope.Login.UserDbId;
            if (userDbId <= 0)
            {
                await ProtobufPacketCodec.WriteAsync(stream, Error(ProtocolErrorCode.ValidationInvalidUserDbId, "UserDbId must be greater than zero."), linkedCts.Token);
                return;
            }

            if (!await _loginSlots.WaitAsync(0, linkedCts.Token))
            {
                await ProtobufPacketCodec.WriteAsync(stream, new ServerResEnvelope
                {
                    Kind = ServerResKind.Login,
                    UserDbId = userDbId,
                    Login = new LoginRes
                    {
                        Accepted = false,
                        ErrorCode = ProtocolErrorCode.LoginServerBusyRetry,
                        Message = "Login is busy. Retry later."
                    }
                }, linkedCts.Token);
                return;
            }

            try
            {
                var sessionId = Guid.NewGuid().ToString("N");
                var registeredInRedis = await _sessionStore.TryRegisterAsync(userDbId, sessionId);
                if (!registeredInRedis)
                {
                    await ProtobufPacketCodec.WriteAsync(stream, new ServerResEnvelope
                    {
                        Kind = ServerResKind.Login,
                        UserDbId = userDbId,
                        SessionId = sessionId,
                        Login = new LoginRes
                        {
                            Accepted = false,
                            SessionId = sessionId,
                            ErrorCode = ProtocolErrorCode.LoginDuplicate,
                            Message = "Duplicate login."
                        }
                    }, linkedCts.Token);
                    return;
                }

                session = new GatewaySession(sessionId, userDbId, client);
                if (!_sessions.TryAdd(session))
                {
                    await _sessionStore.RemoveAsync(userDbId, sessionId);
                    await session.DisposeAsync();
                    return;
                }

                phase = "login-grain";
                var loginResult = await _grainFactory.GetGrain<IUserGrain>(userDbId)
                    .LoginAsync(new LoginCommand
                    {
                        UserDbId = userDbId,
                        SessionId = sessionId,
                        GatewayId = _options.GatewayId,
                        SpawnPosition = new GameServer.Shared.World.WorldPosition
                        {
                            MapId = "default",
                            X = 25f,
                            Y = 25f
                        }
                    });
                var loginResultEnvelope = new ServerResEnvelope
                {
                    Kind = ServerResKind.Login,
                    Sequence = loginEnvelope.Sequence,
                    SessionId = sessionId,
                    UserDbId = userDbId,
                    Login = new LoginRes
                    {
                        Accepted = loginResult.Accepted,
                        ErrorCode = loginResult.ErrorCode,
                        Message = loginResult.Message,
                        SessionId = sessionId,
                        SpawnPosition = loginResult.Position.ToPacketPosition()
                    }
                };

                phase = "send-login-res";
                await session.SendAsync(loginResultEnvelope, linkedCts.Token);

                if (!loginResult.Accepted)
                {
                    disconnectReason = $"login-rejected:{loginResult.ErrorCode}";
                    await _sessionStore.RemoveAsync(userDbId, sessionId);
                    return;
                }

                initialPosition = loginResult.Position.ToPacketPosition();
                session.UpdatePosition(initialPosition);
                // LoginRes 성공 이후에만 user/zone 구독을 등록해 관찰자가 먼저 생기지 않게 함
                await _eventBus.RegisterSessionAsync(session, initialPosition);

                disconnectReason = "active";
                _logger.LogInformation("Accepted user {UserDbId} from {Remote} as session {SessionId}.", userDbId, remote, sessionId);
            }
            finally
            {
                _loginSlots.Release();
            }

            if (session is null)
            {
                return;
            }

            moveScheduler = new GatewayMoveScheduler(
                _eventBus,
                _grainFactory,
                session,
                initialPosition ?? new GameServer.Shared.Protocol.WorldPosition(),
                serverStoppingToken,
                _moveSchedulerLogger);
            var nextSessionRefreshAt = DateTimeOffset.UtcNow + _options.SessionRefreshInterval;

            while (!serverStoppingToken.IsCancellationRequested)
            {
                phase = "read-client-packet";
                var envelope = await ProtobufPacketCodec.ReadAsync(session.Stream, ClientReqEnvelope.Parser, linkedCts.Token);
                if (envelope is null)
                {
                    disconnectReason = "client-closed-stream";
                    break;
                }

                var now = DateTimeOffset.UtcNow;
                if (now >= nextSessionRefreshAt)
                {
                    await _sessionStore.RefreshAsync(userDbId, session.SessionId);
                    nextSessionRefreshAt = now + _options.SessionRefreshInterval;
                }

                phase = $"dispatch-{envelope.Kind}";
                await DispatchAsync(_grainFactory, session, moveScheduler, envelope, linkedCts.Token);
            }
        }
        catch (OperationCanceledException) when (serverStoppingToken.IsCancellationRequested)
        {
            disconnectReason = "server-stopping";
        }
        catch (IOException ex)
        {
            disconnectReason = DescribeTransportFailure("io", phase, ex);
            if (ShouldTraceDisconnect(disconnectReason))
            {
                await _traceLog.WriteAsync(
                    "transport-failure",
                    phase,
                    disconnectReason,
                    session?.SessionId ?? "",
                    session?.UserDbId ?? 0,
                    session?.RemoteEndPoint ?? remote);
                _logger.LogWarning(
                    ex,
                    "TCP session transport failure at {Phase} for session {SessionId}, user {UserDbId}, remote {Remote}: {Reason}",
                    phase,
                    session?.SessionId ?? "",
                    session?.UserDbId ?? 0,
                    session?.RemoteEndPoint ?? remote,
                    disconnectReason);
            }
            else
            {
                _logger.LogDebug(
                    ex,
                    "TCP session closed during transport at {Phase} for session {SessionId}, user {UserDbId}, remote {Remote}: {Reason}",
                    phase,
                    session?.SessionId ?? "",
                    session?.UserDbId ?? 0,
                    session?.RemoteEndPoint ?? remote,
                    disconnectReason);
            }
        }
        catch (SocketException ex)
        {
            disconnectReason = $"socket:{phase}:{ex.SocketErrorCode}";
            if (ShouldTraceDisconnect(disconnectReason))
            {
                await _traceLog.WriteAsync(
                    "transport-failure",
                    phase,
                    disconnectReason,
                    session?.SessionId ?? "",
                    session?.UserDbId ?? 0,
                    session?.RemoteEndPoint ?? remote);
                _logger.LogWarning(
                    ex,
                    "TCP session socket failure at {Phase} for session {SessionId}, user {UserDbId}, remote {Remote}: {SocketError}",
                    phase,
                    session?.SessionId ?? "",
                    session?.UserDbId ?? 0,
                    session?.RemoteEndPoint ?? remote,
                    ex.SocketErrorCode);
            }
            else
            {
                _logger.LogDebug(
                    ex,
                    "TCP session socket closed at {Phase} for session {SessionId}, user {UserDbId}, remote {Remote}: {SocketError}",
                    phase,
                    session?.SessionId ?? "",
                    session?.UserDbId ?? 0,
                    session?.RemoteEndPoint ?? remote,
                    ex.SocketErrorCode);
            }
        }
        catch (Exception ex)
        {
            disconnectReason = $"{ex.GetType().Name}:{phase}:{ex.Message}";
            await _traceLog.WriteAsync(
                "session-failure",
                phase,
                disconnectReason,
                session?.SessionId ?? "",
                session?.UserDbId ?? 0,
                session?.RemoteEndPoint ?? remote);
            _logger.LogWarning(ex, "Client handling failed for {Remote}.", remote);
        }
        finally
        {
            if (session is not null)
            {
                if (moveScheduler is not null)
                {
                    await moveScheduler.StopAsync();
                }

                try
                {
                    await _grainFactory.GetGrain<IUserGrain>(session.UserDbId)
                        .LogoutAsync(new LogoutCommand
                        {
                            UserDbId = session.UserDbId,
                            SessionId = session.SessionId,
                            Reason = disconnectReason
                        });
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "Failed to call logout grain for session {SessionId}.", session.SessionId);
                }

                await _eventBus.UnregisterSessionAsync(session);
                try
                {
                    // 세션 종료 시 session별 AOI pending/visibility 상태까지 정리
                    await _aoiAggregator.FlushSessionAsync(session.SessionId, session.UserDbId);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "Failed to flush AOI state for session {SessionId}.", session.SessionId);
                }

                await _sessionStore.RemoveAsync(session.UserDbId, session.SessionId);
                await _sessions.RemoveAsync(session.SessionId);
                if (ShouldTraceDisconnect(disconnectReason))
                {
                    _logger.LogInformation(
                        "Closed session {SessionId} for user {UserDbId}, remote {Remote}, reason {Reason}.",
                        session.SessionId,
                        session.UserDbId,
                        session.RemoteEndPoint,
                        disconnectReason);
                }
                else
                {
                    _logger.LogDebug(
                        "Closed session {SessionId} for user {UserDbId}, remote {Remote}, reason {Reason}.",
                        session.SessionId,
                        session.UserDbId,
                        session.RemoteEndPoint,
                        disconnectReason);
                }
            }
            else
            {
                client.Dispose();
            }
        }
    }

    private static ServerResEnvelope Error(ProtocolErrorCode code, string message) => new()
    {
        Kind = ServerResKind.Error,
        Error = new ErrorRes
        {
            Code = code,
            Message = message
        }
    };

    private static string DescribeTransportFailure(string category, string phase, IOException exception)
    {
        var socket = exception.InnerException as SocketException;
        if (socket is not null)
        {
            return $"{category}:{phase}:{socket.SocketErrorCode}:{exception.Message}";
        }

        return $"{category}:{phase}:{exception.Message}";
    }

    private static bool ShouldTraceDisconnect(string reason)
    {
        if (string.IsNullOrWhiteSpace(reason)
            || reason is "active" or "client-closed-stream" or "server-stopping"
            || reason.StartsWith("login-rejected:", StringComparison.Ordinal))
        {
            return false;
        }

        return !reason.Contains("ConnectionAborted", StringComparison.OrdinalIgnoreCase)
            && !reason.Contains("ConnectionReset", StringComparison.OrdinalIgnoreCase)
            && !reason.Contains("OperationAborted", StringComparison.OrdinalIgnoreCase);
    }

    private static async Task DispatchAsync(
        IGrainFactory grainFactory,
        GatewaySession session,
        GatewayMoveScheduler moveScheduler,
        ClientReqEnvelope envelope,
        CancellationToken cancellationToken)
    {
        switch (envelope.Kind)
        {
            case ClientReqKind.Move when envelope.Move is not null:
            {
                moveScheduler.Submit(envelope.Sequence, envelope.Move.Position);
                break;
            }
            case ClientReqKind.Ack when envelope.Ack is not null:
                await grainFactory.GetGrain<IUserGrain>(session.UserDbId)
                    .AckServerEventAsync(envelope.Ack.EventId);
                break;
            case ClientReqKind.Logout:
                await grainFactory.GetGrain<IUserGrain>(session.UserDbId)
                    .LogoutAsync(new LogoutCommand
                    {
                        UserDbId = session.UserDbId,
                        SessionId = session.SessionId,
                        Reason = envelope.Logout?.Reason ?? "client-logout"
                    });
                break;
            default:
                await session.SendAsync(Error(ProtocolErrorCode.ProtocolUnsupportedPacket, $"Unsupported packet: {envelope.Kind}"), cancellationToken);
                break;
        }
    }
}
