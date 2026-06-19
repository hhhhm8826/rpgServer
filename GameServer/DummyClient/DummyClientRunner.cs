using System.Diagnostics;
using System.Net.Sockets;
using GameServer.Shared.Protocol;
using ProtocolErrorCode = GameServer.Shared.Protocol.ErrorCode;

internal static class DummyClientRunner
{
    // Dummy 이동 요청은 서버 coalescing/AOI 부하를 보기 위한 랜덤 입력으로 보냄
    private const int MinMoveDelayMs = 100;
    private const int MaxMoveDelayMs = 500;
    private const float MoveSpeedMetersPerSecond = 6.0f;
    private const double KeepDirectionChance = 0.70;
    private const double MaxTurnRadians = Math.PI * 2.0 / 3.0;

    public static async Task RunAsync(
        int index,
        DummyOptions options,
        DummyStats stats,
        DummyViewerState viewerState,
        CancellationToken cancellationToken)
    {
        var userDbId = options.BaseUserDbId + index;
        var random = new Random(index);
        var attempt = 0;

        while (!cancellationToken.IsCancellationRequested)
        {
            attempt++;
            var completedSession = await TryRunClientSessionAsync(index, userDbId, attempt, random, options, stats, viewerState, cancellationToken);
            if (completedSession || cancellationToken.IsCancellationRequested)
            {
                return;
            }

            var retryDelay = options.LoginRetryDelay + TimeSpan.FromMilliseconds(random.Next(0, Math.Max(1, options.LoginRetryJitterMs)));
            stats.SetStatus($"Retrying {userDbId} login attempt {attempt + 1} in {retryDelay.TotalSeconds:0.0}s.");
            await Task.Delay(retryDelay, cancellationToken);
        }
    }

    private static async Task<bool> TryRunClientSessionAsync(
        int index,
        long userDbId,
        int attempt,
        Random random,
        DummyOptions options,
        DummyStats stats,
        DummyViewerState viewerState,
        CancellationToken cancellationToken)
    {
        var accepted = false;
        var gateway = options.GatewayForIndex(index);
        try
        {
            stats.IncrementLoginAttempts();
            if (index == 0)
            {
                stats.SetStatus($"Connecting first observer candidate {userDbId} to {gateway.Host}:{gateway.Port}. attempt={attempt}");
            }

            using var tcpClient = new TcpClient();
            await tcpClient.ConnectAsync(gateway.Host, gateway.Port, cancellationToken);
            var stream = tcpClient.GetStream();

            if (index == 0)
            {
                stats.SetStatus($"Connected TCP for {userDbId}. Waiting for LoginRes.");
            }

            await ProtobufPacketCodec.WriteAsync(stream, new ClientReqEnvelope
            {
                Sequence = 1,
                Kind = ClientReqKind.Login,
                Login = new LoginReq
                {
                    UserDbId = userDbId,
                    Token = "dummy"
                }
            }, cancellationToken);

            using var loginTimeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            loginTimeoutCts.CancelAfter(options.LoginTimeout);
            var login = await ProtobufPacketCodec.ReadAsync(stream, ServerResEnvelope.Parser, loginTimeoutCts.Token);
            if (login?.Login?.Accepted != true)
            {
                stats.IncrementLoginRejected();
                var errorCode = login?.Login?.ErrorCode ?? login?.Error?.Code ?? ProtocolErrorCode.Unknown;
                var message = login?.Login?.Message ?? login?.Error?.Message ?? "missing-login-res";
                stats.SetStatus($"Login rejected for {userDbId}: {errorCode} {message}. attempt={attempt}");
                return false;
            }

            stats.IncrementConnected();
            accepted = true;
            if (index == 0)
            {
                stats.SetStatus($"Login accepted for first observer {userDbId}.");
            }

            var position = login.Login.SpawnPosition;
            viewerState.Upsert(index, userDbId, position, connected: true);

            var reader = ReadLoopAsync(index, userDbId, stream, stats, viewerState, cancellationToken);
            if (index == 0)
            {
                // 첫 번째 로그인이 성공하면 client는 관찰자로 고정하고 이동시키지 않음
                await reader;
                return true;
            }

            var sequence = 2L;
            var hasMoveDirection = false;
            var headingRadians = 0.0;
            var directionX = 0f;
            var directionY = 0f;
            var moveStopwatch = Stopwatch.StartNew();
            var movePosition = new WorldPosition
            {
                MapId = options.MapId,
                X = position.X,
                Y = position.Y,
                Z = position.Z
            };
            var moveEnvelope = new ClientReqEnvelope
            {
                Kind = ClientReqKind.Move,
                Move = new MoveReq
                {
                    Position = movePosition
                }
            };

            while (!cancellationToken.IsCancellationRequested)
            {
                var delay = RandomMoveDelay(random);
                await Task.Delay(delay, cancellationToken);

                if (!hasMoveDirection)
                {
                    headingRadians = random.NextDouble() * Math.Tau;
                    hasMoveDirection = true;
                }
                else if (random.NextDouble() >= KeepDirectionChance)
                {
                    // 목적지 없이 같은 방향을 주로 유지하되 가끔 큰 각도로 회전함
                    headingRadians += (random.NextDouble() * 2.0 - 1.0) * MaxTurnRadians;
                }

                directionX = (float)Math.Cos(headingRadians);
                directionY = (float)Math.Sin(headingRadians);

                var elapsedSeconds = (float)moveStopwatch.Elapsed.TotalSeconds;
                moveStopwatch.Restart();
                var distance = MoveSpeedMetersPerSecond * elapsedSeconds;

                // DummyClient는 송신 완료 후 다음 tick에서만 갱신하므로 protobuf 객체를 재사용해도 안전함
                movePosition.X += directionX * distance;
                movePosition.Y += directionY * distance;
                movePosition.Z = 0;
                moveEnvelope.Sequence = sequence++;

                await ProtobufPacketCodec.WriteAsync(stream, moveEnvelope, cancellationToken);

                stats.IncrementSentMoves();
            }

            await reader;
            return true;
        }
        catch (OperationCanceledException)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return true;
            }

            stats.IncrementLoginTimeouts();
            stats.SetStatus($"Login timed out for {userDbId} after {options.LoginTimeout}. attempt={attempt}");
            return false;
        }
        catch (Exception ex)
        {
            stats.RecordClientError(userDbId, attempt, ex);
            return false;
        }
        finally
        {
            if (accepted)
            {
                stats.DecrementConnected();
            }

            viewerState.MarkDisconnected(index);
        }
    }

    private static async Task ReadLoopAsync(
        int index,
        long userDbId,
        NetworkStream stream,
        DummyStats stats,
        DummyViewerState viewerState,
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var envelope = await ProtobufPacketCodec.ReadAsync(stream, ServerResEnvelope.Parser, cancellationToken);
            if (envelope is null)
            {
                return;
            }

            switch (envelope.Kind)
            {
                case ServerResKind.MoveNty:
                    stats.IncrementMoveNty();
                    if (envelope.Move is not null)
                    {
                        // viewer 위치는 client 예측값이 아니라 서버가 확정한 위치만 사용함
                        viewerState.Upsert(index, userDbId, envelope.Move.AuthoritativePosition, connected: true);
                    }

                    break;
                case ServerResKind.AoiDelta:
                    stats.IncrementAoiDeltas();
                    if (index == 0)
                    {
                        // 화면에는 관찰자가 실제로 받은 AOI delta만 반영함
                        viewerState.ApplyAoiDelta(envelope.AoiDelta);
                    }

                    break;
                case ServerResKind.Error:
                    stats.RecordServerError(envelope.Error);
                    break;
            }
        }
    }

    private static TimeSpan RandomMoveDelay(Random random)
        => TimeSpan.FromMilliseconds(random.Next(MinMoveDelayMs, MaxMoveDelayMs + 1));
}
