using Aspire.Hosting.ApplicationModel;

var builder = DistributedApplication.CreateBuilder(args);
var gatewayTcpTraceLogPath = Path.GetFullPath(Path.Combine("..", "..", ".runlogs", "gateway-tcp-trace.log"));

var redis = builder.AddContainer("redis", "redis", "7.4")
    .WithLifetime(ContainerLifetime.Session)
    .WithArgs("redis-server", "--appendonly", "yes", "--appendfsync", "everysec")
    .WithEndpoint(port: 6379, targetPort: 6379, name: "tcp");

var eventBusRedis = builder.AddContainer("redis-eventbus", "redis", "7.4")
    .WithLifetime(ContainerLifetime.Session)
    .WithArgs("redis-server")
    .WithEndpoint(port: 6380, targetPort: 6379, name: "tcp");

var dynamoDbLocal = builder.AddContainer("dynamodb-local", "amazon/dynamodb-local", "latest")
    .WithLifetime(ContainerLifetime.Session)
    .WithArgs("-jar", "DynamoDBLocal.jar", "-sharedDb", "-inMemory")
    .WithHttpEndpoint(port: 8000, targetPort: 8000, name: "http");

builder.AddProject<Projects.WorldServer>("worldserver-1", launchProfileName: null)
    .WithEnvironment("ASPNETCORE_ENVIRONMENT", "Development")
    .WithEnvironment("DOTNET_ENVIRONMENT", "Development")
    .WithEnvironment("ASPNETCORE_URLS", "http://127.0.0.1:5023")
    .WithEnvironment("ConnectionStrings__redis", redis.GetEndpoint("tcp").Property(EndpointProperty.HostAndPort))
    .WithEnvironment("ConnectionStrings__eventbus", eventBusRedis.GetEndpoint("tcp").Property(EndpointProperty.HostAndPort))
    .WithEnvironment("WorldStorage__DynamoDb__ServiceUrl", dynamoDbLocal.GetEndpoint("http"))
    .WithEnvironment("Orleans__SiloPort", "11111")
    .WithEnvironment("Orleans__GatewayPort", "30000")
    .WaitFor(redis)
    .WaitFor(eventBusRedis)
    .WaitFor(dynamoDbLocal);

builder.AddProject<Projects.WorldServer>("worldserver-2", launchProfileName: null)
    .WithEnvironment("ASPNETCORE_ENVIRONMENT", "Development")
    .WithEnvironment("DOTNET_ENVIRONMENT", "Development")
    .WithEnvironment("ASPNETCORE_URLS", "http://127.0.0.1:5024")
    .WithEnvironment("ConnectionStrings__redis", redis.GetEndpoint("tcp").Property(EndpointProperty.HostAndPort))
    .WithEnvironment("ConnectionStrings__eventbus", eventBusRedis.GetEndpoint("tcp").Property(EndpointProperty.HostAndPort))
    .WithEnvironment("WorldStorage__DynamoDb__ServiceUrl", dynamoDbLocal.GetEndpoint("http"))
    .WithEnvironment("Orleans__SiloPort", "11112")
    .WithEnvironment("Orleans__GatewayPort", "30001")
    .WaitFor(redis)
    .WaitFor(eventBusRedis)
    .WaitFor(dynamoDbLocal);

builder.AddProject<Projects.GatewayServer>("gatewayserver-1")
    .WithEnvironment("ASPNETCORE_ENVIRONMENT", "Development")
    .WithEnvironment("DOTNET_ENVIRONMENT", "Development")
    .WithEnvironment("ASPNETCORE_URLS", "http://127.0.0.1:5031")
    .WithEnvironment("ConnectionStrings__redis", redis.GetEndpoint("tcp").Property(EndpointProperty.HostAndPort))
    .WithEnvironment("ConnectionStrings__eventbus", eventBusRedis.GetEndpoint("tcp").Property(EndpointProperty.HostAndPort))
    .WithEnvironment("Gateway__GatewayId", "gateway-local-1")
    .WithEnvironment("Gateway__Port", "7777")
    .WithEnvironment("Gateway__TcpTraceLogPath", gatewayTcpTraceLogPath)
    .WaitFor(redis)
    .WaitFor(eventBusRedis)
    .WaitFor(dynamoDbLocal);

builder.AddProject<Projects.GatewayServer>("gatewayserver-2")
    .WithEnvironment("ASPNETCORE_ENVIRONMENT", "Development")
    .WithEnvironment("DOTNET_ENVIRONMENT", "Development")
    .WithEnvironment("ASPNETCORE_URLS", "http://127.0.0.1:5032")
    .WithEnvironment("ConnectionStrings__redis", redis.GetEndpoint("tcp").Property(EndpointProperty.HostAndPort))
    .WithEnvironment("ConnectionStrings__eventbus", eventBusRedis.GetEndpoint("tcp").Property(EndpointProperty.HostAndPort))
    .WithEnvironment("Gateway__GatewayId", "gateway-local-2")
    .WithEnvironment("Gateway__Port", "7778")
    .WithEnvironment("Gateway__TcpTraceLogPath", gatewayTcpTraceLogPath)
    .WaitFor(redis)
    .WaitFor(eventBusRedis)
    .WaitFor(dynamoDbLocal);

builder.AddProject<Projects.GatewayServer>("gatewayserver-3")
    .WithEnvironment("ASPNETCORE_ENVIRONMENT", "Development")
    .WithEnvironment("DOTNET_ENVIRONMENT", "Development")
    .WithEnvironment("ASPNETCORE_URLS", "http://127.0.0.1:5033")
    .WithEnvironment("ConnectionStrings__redis", redis.GetEndpoint("tcp").Property(EndpointProperty.HostAndPort))
    .WithEnvironment("ConnectionStrings__eventbus", eventBusRedis.GetEndpoint("tcp").Property(EndpointProperty.HostAndPort))
    .WithEnvironment("Gateway__GatewayId", "gateway-local-3")
    .WithEnvironment("Gateway__Port", "7779")
    .WithEnvironment("Gateway__TcpTraceLogPath", gatewayTcpTraceLogPath)
    .WaitFor(redis)
    .WaitFor(eventBusRedis)
    .WaitFor(dynamoDbLocal);

builder.Build().Run();
