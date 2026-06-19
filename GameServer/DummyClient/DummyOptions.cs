internal sealed class DummyOptions
{
    public string Host { get; init; } = "localhost";

    public int Port { get; init; } = 7777;

    public IReadOnlyList<GatewayEndpoint> Gateways { get; init; } = [];

    public int Clients { get; init; } = 1000;

    public int LoginBatchSize { get; init; } = 100;

    public TimeSpan LoginBatchInterval { get; init; } = TimeSpan.FromSeconds(5);

    public TimeSpan Duration { get; init; } = TimeSpan.FromMinutes(5);

    public TimeSpan LoginTimeout { get; init; } = TimeSpan.FromSeconds(30);

    public TimeSpan LoginRetryDelay { get; init; } = TimeSpan.FromSeconds(3);

    public int LoginRetryJitterMs { get; init; } = 2000;

    public TimeSpan GatewayReadyTimeout { get; init; } = TimeSpan.FromSeconds(30);

    public string MapId { get; init; } = "default";

    public long BaseUserDbId { get; init; } = 1_000_000_000;

    public int ViewerPort { get; init; }

    public string ViewerHost { get; init; } = "127.0.0.1";

    public float ViewRadius { get; init; } = 70f;

    public float ViewExitRadius { get; init; } = 80f;

    public int RenderIntervalMs { get; init; } = 100;

    public string GatewaySummary => Gateways.Count == 0
        ? $"{Host}:{Port}"
        : string.Join(",", Gateways.Select(x => $"{x.Host}:{x.Port}"));

    public int NormalizedLoginBatchSize => Math.Clamp(LoginBatchSize, 1, Math.Max(1, Clients));

    public TimeSpan NormalizedLoginBatchInterval => LoginBatchInterval < TimeSpan.Zero
        ? TimeSpan.Zero
        : LoginBatchInterval;

    // 여러 Gateway를 지정하면 index 기준 RR로 분산 접속
    public GatewayEndpoint GatewayForIndex(int index)
        => Gateways.Count == 0
            ? new GatewayEndpoint(Host, Port)
            : Gateways[index % Gateways.Count];

    public IReadOnlyList<GatewayEndpoint> GatewayReadyTargets
        => Gateways.Count == 0
            ? [new GatewayEndpoint(Host, Port)]
            : Gateways.Distinct().ToArray();

    public static DummyOptions Parse(string[] args)
    {
        var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < args.Length - 1; i++)
        {
            if (args[i].StartsWith("--", StringComparison.Ordinal))
            {
                values[args[i][2..]] = args[i + 1];
                i++;
            }
        }

        var host = values.GetValueOrDefault("host") ?? "localhost";
        var port = int.TryParse(values.GetValueOrDefault("port"), out var parsedPort) ? parsedPort : 7777;

        return new DummyOptions
        {
            Host = host,
            Port = port,
            Gateways = ParseGateways(values.GetValueOrDefault("gateways")),
            Clients = int.TryParse(values.GetValueOrDefault("clients"), out var clients) ? clients : 1000,
            LoginBatchSize = int.TryParse(values.GetValueOrDefault("login-batch-size"), out var loginBatchSize) ? loginBatchSize : 100,
            LoginBatchInterval = TimeSpan.TryParse(values.GetValueOrDefault("login-batch-interval"), out var loginBatchInterval) ? loginBatchInterval : TimeSpan.FromSeconds(5),
            Duration = TimeSpan.TryParse(values.GetValueOrDefault("duration"), out var duration) ? duration : TimeSpan.FromMinutes(5),
            LoginTimeout = TimeSpan.TryParse(values.GetValueOrDefault("login-timeout"), out var loginTimeout) ? loginTimeout : TimeSpan.FromSeconds(30),
            LoginRetryDelay = TimeSpan.TryParse(values.GetValueOrDefault("login-retry-delay"), out var loginRetryDelay) ? loginRetryDelay : TimeSpan.FromSeconds(3),
            LoginRetryJitterMs = int.TryParse(values.GetValueOrDefault("login-retry-jitter-ms"), out var loginRetryJitterMs) ? loginRetryJitterMs : 2000,
            GatewayReadyTimeout = TimeSpan.TryParse(values.GetValueOrDefault("gateway-ready-timeout"), out var gatewayReadyTimeout) ? gatewayReadyTimeout : TimeSpan.FromSeconds(30),
            MapId = values.GetValueOrDefault("map-id") ?? "default",
            BaseUserDbId = long.TryParse(values.GetValueOrDefault("base-user-db-id"), out var baseUserDbId) ? baseUserDbId : 1_000_000_000,
            ViewerPort = int.TryParse(values.GetValueOrDefault("viewer-port"), out var viewerPort) ? viewerPort : 0,
            ViewerHost = values.GetValueOrDefault("viewer-host") ?? "127.0.0.1",
            ViewRadius = float.TryParse(values.GetValueOrDefault("view-radius"), out var viewRadius) ? viewRadius : 70f,
            ViewExitRadius = float.TryParse(values.GetValueOrDefault("view-exit-radius"), out var viewExitRadius) ? viewExitRadius : 80f,
            RenderIntervalMs = int.TryParse(values.GetValueOrDefault("render-interval-ms"), out var renderIntervalMs) ? renderIntervalMs : 100
        };
    }

    private static IReadOnlyList<GatewayEndpoint> ParseGateways(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return [];
        }

        return value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(ParseGateway)
            .ToArray();
    }

    private static GatewayEndpoint ParseGateway(string value)
    {
        var parts = value.Split(':', 2, StringSplitOptions.TrimEntries);
        if (parts.Length == 2 && int.TryParse(parts[1], out var port))
        {
            return new GatewayEndpoint(parts[0], port);
        }

        return new GatewayEndpoint(value, 7777);
    }
}

internal sealed record GatewayEndpoint(string Host, int Port);
