using System.Net;
using Amazon.DynamoDBv2;
using Amazon.Runtime;
using GameServer.WorldServer.EventBus;
using GameServer.WorldServer.Persistence;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Hosting;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

builder.AddServiceDefaults();

builder.Services.Configure<WorldStorageOptions>(builder.Configuration.GetSection(WorldStorageOptions.SectionName));

var redisConnectionString = builder.Configuration.GetConnectionString("redis")
    ?? builder.Configuration["Redis:ConnectionString"]
    ?? "localhost:6379";
var eventBusConnectionString = builder.Configuration.GetConnectionString("eventbus")
    ?? builder.Configuration["EventBus:ConnectionString"]
    ?? redisConnectionString;

builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(redisConnectionString));
builder.Services.AddSingleton(_ => new EventBusRedisConnection(eventBusConnectionString));
builder.Services.AddSingleton<WorldEventBusPublisher>();
builder.Services.AddSingleton<WriteBehindGrainStorage>();
builder.Services.AddHostedService<DynamoDbTableInitializer>();
builder.Services.AddHostedService<DynamoFlushWorker>();
builder.Services.AddSingleton<IAmazonDynamoDB>(sp =>
{
    var options = sp.GetRequiredService<IOptions<WorldStorageOptions>>().Value;
    var dynamo = options.DynamoDb;
    var credentials = new BasicAWSCredentials(dynamo.AccessKey, dynamo.SecretKey);
    var config = new AmazonDynamoDBConfig
    {
        AuthenticationRegion = dynamo.Region,
        MaxErrorRetry = 1
    };

    if (!string.IsNullOrWhiteSpace(dynamo.ServiceUrl))
    {
        config.ServiceURL = dynamo.ServiceUrl;
    }
    else
    {
        config.RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(dynamo.Region);
    }

    return new AmazonDynamoDBClient(credentials, config);
});

builder.UseOrleans(siloBuilder =>
{
    var clusterId = builder.Configuration["Orleans:ClusterId"] ?? "dev";
    var serviceId = builder.Configuration["Orleans:ServiceId"] ?? "GameServer";
    var siloPort = int.TryParse(builder.Configuration["Orleans:SiloPort"], out var configuredSiloPort)
        ? configuredSiloPort
        : 11111;
    var gatewayPort = int.TryParse(builder.Configuration["Orleans:GatewayPort"], out var configuredGatewayPort)
        ? configuredGatewayPort
        : 30000;

    siloBuilder
        .Configure<ClusterOptions>(options =>
        {
            options.ClusterId = clusterId;
            options.ServiceId = serviceId;
        })
        .Configure<EndpointOptions>(options =>
        {
            options.AdvertisedIPAddress = IPAddress.Loopback;
            options.SiloPort = siloPort;
            options.GatewayPort = gatewayPort;
        })
        .UseRedisClustering(options =>
        {
            options.ConfigurationOptions = ConfigurationOptions.Parse(redisConnectionString);
        });
});

var app = builder.Build();

app.MapDefaultEndpoints();
app.MapGet("/", () => Results.Ok("WorldServer Orleans silo is running."));

app.Run();
