using GameServer.Shared.Grains;
using GameServer.Shared.World;
using GameServer.WorldServer.EventBus;
using GameServer.WorldServer.Persistence;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Runtime;
using PacketErrorCode = GameServer.Shared.Protocol.ErrorCode;
using PacketErrorRes = GameServer.Shared.Protocol.ErrorRes;
using PacketServerResEnvelope = GameServer.Shared.Protocol.ServerResEnvelope;
using PacketServerResKind = GameServer.Shared.Protocol.ServerResKind;

namespace GameServer.WorldServer.Grains;

public sealed class UserGrain : WriteBehindGrainBase<UserState>, IUserGrain
{
    private readonly WorldStorageOptions _storageOptions;
    private readonly WorldEventBusPublisher _publisher;
    private readonly ILogger<UserGrain> _logger;
    // 기본 상태와 이동 상태는 중요도별 저장 주기가 달라 별도 timer 사용함
    private IGrainTimer? _defaultSaveTimer;
    private IGrainTimer? _lowPrioritySaveTimer;
    // LoginRes 선반환 후 Zone 입장을 Orleans timer로 분리함
    private IGrainTimer? _presenceInitializationTimer;
    private string _pendingPresenceSessionId = "";
    private long _pendingPresenceStateVersion;
    private bool _defaultSavePending;
    private bool _lowPrioritySavePending;

    public UserGrain(
        WriteBehindGrainStorage storage,
        IOptions<WorldStorageOptions> storageOptions,
        WorldEventBusPublisher publisher,
        ILogger<UserGrain> logger)
        : base(storage)
    {
        _storageOptions = storageOptions.Value;
        _publisher = publisher;
        _logger = logger;
    }

    protected override StorageKey StorageKey => StorageKey.User(this.GetPrimaryKeyLong());

    public override async Task OnActivateAsync(CancellationToken cancellationToken)
    {
        await base.OnActivateAsync(cancellationToken);
        _defaultSaveTimer = this.RegisterGrainTimer(
            SaveDefaultStateIfDirtyAsync,
            new GrainTimerCreationOptions
            {
                DueTime = _storageOptions.UserDefaultSaveInterval,
                Period = _storageOptions.UserDefaultSaveInterval,
                Interleave = false, // grain turn 순서 보장함
                KeepAlive = false // grain 소멸과 함께 timer도 정리함
            });
        _lowPrioritySaveTimer = this.RegisterGrainTimer(
            SaveLowPriorityStateIfDirtyAsync,
            new GrainTimerCreationOptions
            {
                DueTime = _storageOptions.UserLowPrioritySaveInterval,
                Period = _storageOptions.UserLowPrioritySaveInterval,
                Interleave = false, // grain turn 순서 보장함
                KeepAlive = false // grain 소멸과 함께 timer도 정리함
            });
    }

    public override async Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        _defaultSaveTimer?.Dispose();
        _defaultSaveTimer = null;
        _lowPrioritySaveTimer?.Dispose();
        _lowPrioritySaveTimer = null;
        CancelPresenceInitialization();

        if (_defaultSavePending || _lowPrioritySavePending)
        {
            await SavePendingStateAsync(cancellationToken);
        }

        await base.OnDeactivateAsync(reason, cancellationToken);
    }

    public async Task<LoginResult> LoginAsync(LoginCommand command)
    {
        if (command.UserDbId <= 0 || command.UserDbId != this.GetPrimaryKeyLong())
        {
            return new LoginResult
            {
                Accepted = false,
                ErrorCode = PacketErrorCode.ValidationInvalidUserDbId,
                Message = "UserDbId must match the target UserGrain key."
            };
        }

        var isNewUser = State.UserDbId <= 0;

        State.UserDbId = command.UserDbId;
        State.SessionId = command.SessionId;
        State.GatewayId = command.GatewayId;
        State.IsOnline = true;
        State.Position = isNewUser
            ? command.SpawnPosition.Clone()
            : State.Position;
        State.Version++;
        MarkDefaultSavePending();

        // 로그인 응답은 빠르게 반환하고 Zone 입장/AOI 초기화는 timer에서 처리함
        SchedulePresenceInitialization();

        return new LoginResult
        {
            Accepted = true,
            ErrorCode = PacketErrorCode.None,
            Position = State.Position.Clone()
        };
    }

    private void SchedulePresenceInitialization()
    {
        _pendingPresenceSessionId = State.SessionId;
        _pendingPresenceStateVersion = State.Version;

        _presenceInitializationTimer?.Dispose();
        _presenceInitializationTimer = this.RegisterGrainTimer(
            RunPresenceInitializationAsync,
            new GrainTimerCreationOptions
            {
                DueTime = TimeSpan.Zero,
                Period = TimeSpan.FromDays(1),
                Interleave = false,
                KeepAlive = false
            });
    }

    private async Task RunPresenceInitializationAsync(CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return;
        }

        // 세션과 상태 버전을 함께 확인해 오래된 presence timer 실행 방지함
        var sessionId = _pendingPresenceSessionId;
        var stateVersion = _pendingPresenceStateVersion;
        CancelPresenceInitialization();

        if (string.IsNullOrWhiteSpace(sessionId))
        {
            return;
        }

        await InitializePresenceAsync(sessionId, stateVersion);
    }

    private void CancelPresenceInitialization()
    {
        _presenceInitializationTimer?.Dispose();
        _presenceInitializationTimer = null;
        _pendingPresenceSessionId = "";
        _pendingPresenceStateVersion = 0;
    }

    private async Task InitializePresenceAsync(string sessionId, long stateVersion)
    {
        if (!State.IsOnline
            || State.SessionId != sessionId
            || State.Version != stateVersion)
        {
            return;
        }

        try
        {
            await EnterCurrentZoneAsync();
            MarkDefaultSavePending();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Presence initialization failed for user {UserDbId}, session {SessionId}. Forcing logout.",
                State.UserDbId,
                sessionId);

            try
            {
                await ForceLogoutAfterPresenceInitializationFailureAsync(sessionId, stateVersion);
            }
            catch (Exception logoutEx)
            {
                _logger.LogWarning(
                    logoutEx,
                    "Failed to complete forced logout after presence initialization failure for user {UserDbId}, session {SessionId}.",
                    State.UserDbId,
                    sessionId);
            }
        }
    }

    public async Task<MoveResult> MoveAsync(MoveCommand command)
    {
        if (!State.IsOnline || command.SessionId != State.SessionId)
        {
            return new MoveResult
            {
                Accepted = false,
                ErrorCode = PacketErrorCode.SessionNotActive,
                Message = "Session is not active.",
                AuthoritativePosition = State.Position.Clone()
            };
        }

        var oldZoneKey = State.CurrentZoneKey;
        State.Position = command.Position.Clone();
        State.Version++;

        var newZone = ZoneMath.FromPosition(State.Position);
        var newZoneKey = newZone.ToKey();
        var zoneChanged = string.IsNullOrWhiteSpace(oldZoneKey) || oldZoneKey != newZoneKey;

        // Zone 변경 시에만 Leave/Enter를 호출하고 같은 Zone 이동은 Move만 전달함
        if (string.IsNullOrWhiteSpace(oldZoneKey))
        {
            await GrainFactory.GetGrain<IZoneGrain>(newZoneKey).EnterAsync(ToSnapshot());
        }
        else if (oldZoneKey != newZoneKey)
        {
            await GrainFactory.GetGrain<IZoneGrain>(oldZoneKey).LeaveAsync(State.UserDbId);
            await GrainFactory.GetGrain<IZoneGrain>(newZoneKey).EnterAsync(ToSnapshot());
        }
        else
        {
            await GrainFactory.GetGrain<IZoneGrain>(newZoneKey).MoveAsync(ToSnapshot());
        }

        State.CurrentZoneKey = newZoneKey;
        MarkLowPrioritySavePending();

        return new MoveResult
        {
            Accepted = true,
            ErrorCode = PacketErrorCode.None,
            AuthoritativePosition = State.Position.Clone()
        };
    }

    public async Task LogoutAsync(LogoutCommand command)
    {
        if (command.SessionId != State.SessionId)
        {
            return;
        }

        CancelPresenceInitialization();

        if (!string.IsNullOrWhiteSpace(State.CurrentZoneKey))
        {
            await GrainFactory.GetGrain<IZoneGrain>(State.CurrentZoneKey).LeaveAsync(State.UserDbId);
        }

        State.IsOnline = false;
        State.SessionId = "";
        State.GatewayId = "";
        State.CurrentZoneKey = "";
        State.Version++;

        // 로그아웃은 온라인 상태를 즉시 영속화해 재접속 시 ghost session 방지함
        await WriteStateAsync();
        ClearSavePending();
        await FlushStateAsync();
    }

    public async Task AckServerEventAsync(string eventId)
    {
        if (string.IsNullOrWhiteSpace(eventId))
        {
            return;
        }

        var removed = State.PendingReconnectReliableEvents.RemoveAll(x => x.EventId == eventId);
        if (removed > 0)
        {
            // reliable ack는 재전송 여부에 영향이 커 critical flush 대상으로 취급함
            await WriteCriticalStateAsync();
        }
    }

    private async Task EnterCurrentZoneAsync()
    {
        var zone = ZoneMath.FromPosition(State.Position);
        State.CurrentZoneKey = zone.ToKey();
        await GrainFactory.GetGrain<IZoneGrain>(State.CurrentZoneKey).EnterAsync(ToSnapshot());
    }

    private async Task ForceLogoutAfterPresenceInitializationFailureAsync(string sessionId, long stateVersion)
    {
        if (!State.IsOnline
            || State.SessionId != sessionId
            || State.Version != stateVersion)
        {
            return;
        }

        var userDbId = State.UserDbId;
        var gatewayId = State.GatewayId;

        State.IsOnline = false;
        State.SessionId = "";
        State.GatewayId = "";
        State.CurrentZoneKey = "";
        State.Version++;

        try
        {
            await WriteStateAsync();
            ClearSavePending();
            await FlushStateAsync();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to persist forced logout for user {UserDbId}, session {SessionId}.",
                userDbId,
                sessionId);
        }

        if (string.IsNullOrWhiteSpace(gatewayId))
        {
            return;
        }

        await _publisher.PublishGatewayBroadcastAsync(gatewayId, new PacketServerResEnvelope
        {
            Kind = PacketServerResKind.Error,
            SessionId = sessionId,
            UserDbId = userDbId,
            Error = new PacketErrorRes
            {
                Code = PacketErrorCode.LoginPresenceInitFailed,
                Message = "Login initialization failed. Session will be closed."
            }
        });
    }

    private EntitySnapshot ToSnapshot() => new()
    {
        EntityId = State.UserDbId,
        Kind = EntityKind.User,
        DisplayName = State.UserDbId.ToString(),
        Position = State.Position.Clone(),
        Version = State.Version
    };

    private void MarkDefaultSavePending() => _defaultSavePending = true;

    private void MarkLowPrioritySavePending() => _lowPrioritySavePending = true;

    private async Task SaveDefaultStateIfDirtyAsync(CancellationToken cancellationToken)
    {
        if (!_defaultSavePending)
        {
            return;
        }

        await SavePendingStateAsync(cancellationToken);
    }

    private async Task SaveLowPriorityStateIfDirtyAsync(CancellationToken cancellationToken)
    {
        if (!_lowPrioritySavePending)
        {
            return;
        }

        await SavePendingStateAsync(cancellationToken);
    }

    private async Task SavePendingStateAsync(CancellationToken cancellationToken = default)
    {
        // timer 저장은 Redis write + dirty set 등록까지만 수행함
        await WriteStateAsync(cancellationToken);
        ClearSavePending();
    }

    private async Task WriteCriticalStateAsync(CancellationToken cancellationToken = default)
    {
        await WriteStateAsync(_storageOptions.CriticalStateFlushDelay, cancellationToken);
        ClearSavePending();
    }

    private void ClearSavePending()
    {
        _defaultSavePending = false;
        _lowPrioritySavePending = false;
    }
}
