using GameServer.GatewayServer.Networking;
using GameServer.GatewayServer.Sessions;
using Orleans.Configuration;
using Orleans.Hosting;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

builder.AddServiceDefaults();

builder.Services.Configure<GameServer.GatewayServer.Networking.GatewayOptions>(
    builder.Configuration.GetSection(GameServer.GatewayServer.Networking.GatewayOptions.SectionName));

var redisConnectionString = builder.Configuration.GetConnectionString("redis")
    ?? builder.Configuration["Redis:ConnectionString"]
    ?? "localhost:6379";
var eventBusConnectionString = builder.Configuration.GetConnectionString("eventbus")
    ?? builder.Configuration["EventBus:ConnectionString"]
    ?? redisConnectionString;

builder.UseOrleansClient(clientBuilder =>
{
    var clusterId = builder.Configuration["Orleans:ClusterId"] ?? "dev";
    var serviceId = builder.Configuration["Orleans:ServiceId"] ?? "GameServer";

    clientBuilder
        .Configure<ClusterOptions>(options =>
        {
            options.ClusterId = clusterId;
            options.ServiceId = serviceId;
        })
        .UseRedisClustering(options =>
        {
            options.ConfigurationOptions = ConfigurationOptions.Parse(redisConnectionString);
        });
});

builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(redisConnectionString));
builder.Services.AddSingleton(_ => new EventBusRedisConnection(eventBusConnectionString));
builder.Services.AddSingleton<GatewaySessionRegistry>();
builder.Services.AddSingleton<GatewayTcpTraceLog>();
builder.Services.AddSingleton<GatewayAoiAggregator>();
builder.Services.AddSingleton<GatewaySubscriptionManager>();
builder.Services.AddSingleton<GatewayZoneEventRouter>();
builder.Services.AddSingleton<GatewayEventBus>();
builder.Services.AddSingleton<RedisSessionStore>();
builder.Services.AddSingleton<ILogger<GatewayMoveScheduler>>(sp => sp.GetRequiredService<ILoggerFactory>().CreateLogger<GatewayMoveScheduler>());
builder.Services.AddHostedService(sp => sp.GetRequiredService<GatewaySubscriptionManager>());
builder.Services.AddHostedService(sp => sp.GetRequiredService<GatewayAoiAggregator>());
builder.Services.AddHostedService(sp => sp.GetRequiredService<GatewayZoneEventRouter>());
builder.Services.AddHostedService(sp => sp.GetRequiredService<GatewayEventBus>());
builder.Services.AddHostedService<TcpGatewayService>();

var app = builder.Build();

app.MapDefaultEndpoints();
app.MapGet("/", (GatewaySessionRegistry sessions) => Results.Ok(new
{
    Service = "GatewayServer",
    ActiveSessions = sessions.Count
}));

app.Run();
