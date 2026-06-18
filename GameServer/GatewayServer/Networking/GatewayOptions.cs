namespace GameServer.GatewayServer.Networking;

public sealed class GatewayOptions
{
    public const string SectionName = "Gateway";

    public string GatewayId { get; set; } = "gateway-local-1";

    public string ListenAddress { get; set; } = "0.0.0.0";

    public int Port { get; set; } = 7777;

    public int TcpBacklog { get; set; } = 2048;

    public int MaxConcurrentLogins { get; set; } = 16;

    public int SessionTTLSec { get; set; } = 30;

    public int SessionRefreshIntervalSec { get; set; } = 10;

    public int GatewayHeartbeatIntervalSec { get; set; } = 1;

    public int GatewayHeartbeatTTLSec { get; set; } = 3;

    public int EventQueueCapacity { get; set; } = 4096;

    public int EventQueueWorkerCount { get; set; } = 4;

    public int LatestEventTickMs { get; set; } = 200;

    public int LatestEventQueueCapacity { get; set; } = 4096;

    public int LatestEventPartitionCount { get; set; } = 4;

    public int LatestEventProcessingBudgetMs { get; set; } = 100;

    public int SessionAoiPartitionCount { get; set; } = 4;

    public int SessionAoiQueueCapacity { get; set; } = 4096;

    public int SessionAoiSendConcurrency { get; set; } = 32;

    public float AoiEnterRadiusMeters { get; set; } = 70f;

    public float AoiExitRadiusMeters { get; set; } = 80f;

    public string TcpTraceLogPath { get; set; } = "";

    public TimeSpan SessionTTL => TimeSpan.FromSeconds(Math.Max(5, SessionTTLSec));

    public TimeSpan SessionRefreshInterval
        => TimeSpan.FromSeconds(Math.Clamp(SessionRefreshIntervalSec, 1, Math.Max(1, SessionTTLSec / 2)));

    public TimeSpan GatewayHeartbeatInterval => TimeSpan.FromSeconds(Math.Max(1, GatewayHeartbeatIntervalSec));

    public TimeSpan GatewayHeartbeatTTL => TimeSpan.FromSeconds(Math.Max(2, GatewayHeartbeatTTLSec));

    public int NormalizedEventQueueCapacity => Math.Max(1024, EventQueueCapacity);

    public int NormalizedEventQueueWorkerCount => Math.Clamp(EventQueueWorkerCount, 1, 8);

    public TimeSpan LatestEventTick => TimeSpan.FromMilliseconds(Math.Clamp(LatestEventTickMs, 50, 1000));

    public int NormalizedLatestEventQueueCapacity => Math.Max(128, LatestEventQueueCapacity);

    public int NormalizedLatestEventPartitionCount => Math.Clamp(LatestEventPartitionCount, 1, 16);

    public int NormalizedSessionAoiPartitionCount => Math.Clamp(SessionAoiPartitionCount, 1, 16);

    public int NormalizedSessionAoiQueueCapacity => Math.Max(1024, SessionAoiQueueCapacity);

    public int NormalizedSessionAoiSendConcurrency => Math.Clamp(SessionAoiSendConcurrency, 1, 128);

    public float NormalizedAoiEnterRadiusMeters => Math.Clamp(AoiEnterRadiusMeters, 1f, 1000f);

    public float NormalizedAoiExitRadiusMeters
        => Math.Max(NormalizedAoiEnterRadiusMeters, Math.Clamp(AoiExitRadiusMeters, 1f, 1200f));

    public double AoiEnterRadiusSquared => NormalizedAoiEnterRadiusMeters * NormalizedAoiEnterRadiusMeters;

    public double AoiExitRadiusSquared => NormalizedAoiExitRadiusMeters * NormalizedAoiExitRadiusMeters;

    public TimeSpan LatestEventProcessingBudget
        => TimeSpan.FromMilliseconds(Math.Clamp(LatestEventProcessingBudgetMs, 1, Math.Max(1, LatestEventTickMs)));

    public TimeSpan LatestEventPartitionProcessingBudget
        => TimeSpan.FromTicks(Math.Max(1, LatestEventProcessingBudget.Ticks / NormalizedLatestEventPartitionCount));
}
