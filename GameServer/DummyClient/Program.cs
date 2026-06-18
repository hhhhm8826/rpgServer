using System.Diagnostics;
using System.Net;
using System.Net.Sockets;

var completionNoticeHold = TimeSpan.FromSeconds(10);
var options = DummyOptions.Parse(args);
var stats = new DummyStats();
var viewerState = new DummyViewerState(options, stats);
using var viewerCts = new CancellationTokenSource();

Task? viewerTask = null;
if (options.ViewerPort > 0)
{
    viewerTask = DummyViewer.StartAsync(viewerState, options.ViewerHost, options.ViewerPort, viewerCts.Token);
    var displayHost = options.ViewerHost is "0.0.0.0" or "+" or "*" ? "localhost" : options.ViewerHost;
    Console.WriteLine($"Canvas viewer: http://{displayHost}:{options.ViewerPort} (bind={options.ViewerHost})");
}

try
{
    await WaitForGatewaysReadyAsync(options, stats, viewerCts.Token);
}
catch (TimeoutException ex)
{
    Console.WriteLine(ex.Message);
    await StopViewerAsync(viewerTask, viewerCts);
    return;
}

using var cts = new CancellationTokenSource(options.Duration);

Console.WriteLine($"Starting {options.Clients} dummy clients to {options.GatewaySummary}, loginRamp={options.NormalizedLoginBatchSize}/{options.NormalizedLoginBatchInterval}, viewRadius={options.ViewRadius}m, moveInterval=100-500ms random, moveSpeed=6m/s, duration={options.Duration}.");

var stopwatch = Stopwatch.StartNew();
var tasks = new List<Task>(options.Clients);

try
{
    // 로그인은 한 번에 몰아치지 않고 batch 단위로 시작해 기본 부하 테스트를 재현함
    await StartClientsAsync(tasks, options, stats, viewerState, cts.Token);
}
catch (OperationCanceledException)
{
}

try
{
    await Task.WhenAll(tasks);
}
catch (OperationCanceledException)
{
}

stopwatch.Stop();
viewerState.MarkCompleted($"Completed in {stopwatch.Elapsed:hh\\:mm\\:ss}. accepted={stats.LoginAccepted}, peak={stats.PeakConnected}, errors={stats.Errors}");
Console.WriteLine($"Completed in {stopwatch.Elapsed}.");
Console.WriteLine($"ActiveConnected={stats.ActiveConnected}, PeakConnected={stats.PeakConnected}, LoginAccepted={stats.LoginAccepted}, LoginAttempts={stats.LoginAttempts}, LoginRejected={stats.LoginRejected}, LoginTimeouts={stats.LoginTimeouts}, SentMoves={stats.SentMoves}, MoveNty={stats.MoveNty}, AoiDeltas={stats.AoiDeltas}, Errors={stats.Errors}");
foreach (var error in stats.ErrorSummaries)
{
    Console.WriteLine($"Error[{error.Count}] {error.Key}");
}

if (viewerTask is not null)
{
    Console.WriteLine($"Viewer completion notice stays visible for {completionNoticeHold.TotalSeconds:0}s.");
    await Task.Delay(completionNoticeHold);
}

await StopViewerAsync(viewerTask, viewerCts);

static async Task WaitForGatewaysReadyAsync(
    DummyOptions options,
    DummyStats stats,
    CancellationToken cancellationToken)
{
    var pending = options.GatewayReadyTargets.ToList();
    var deadline = DateTimeOffset.UtcNow + options.GatewayReadyTimeout;
    Console.WriteLine($"Waiting for Gateway TCP readiness: {string.Join(",", pending.Select(x => $"{x.Host}:{x.Port}"))}");

    while (pending.Count > 0)
    {
        cancellationToken.ThrowIfCancellationRequested();

        for (var i = pending.Count - 1; i >= 0; i--)
        {
            if (!await CanConnectAsync(pending[i], TimeSpan.FromMilliseconds(500), cancellationToken))
            {
                continue;
            }

            pending.RemoveAt(i);
        }

        if (pending.Count == 0)
        {
            stats.SetStatus("All Gateway TCP endpoints are ready.");
            Console.WriteLine("Gateway TCP readiness confirmed.");
            return;
        }

        if (DateTimeOffset.UtcNow >= deadline)
        {
            var waiting = string.Join(",", pending.Select(x => $"{x.Host}:{x.Port}"));
            throw new TimeoutException($"Gateway TCP readiness timed out after {options.GatewayReadyTimeout}. waiting={waiting}");
        }

        stats.SetStatus($"Waiting for Gateway TCP readiness: {string.Join(",", pending.Select(x => $"{x.Host}:{x.Port}"))}");
        await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
    }
}

static async Task<bool> CanConnectAsync(
    GatewayEndpoint gateway,
    TimeSpan timeout,
    CancellationToken cancellationToken)
{
    IReadOnlyList<IPAddress> addresses;
    try
    {
        addresses = await ResolveGatewayAddressesAsync(gateway.Host, cancellationToken);
    }
    catch (SocketException)
    {
        return false;
    }

    foreach (var address in addresses)
    {
        try
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(timeout);
            using var client = new TcpClient(address.AddressFamily);
            await client.ConnectAsync(address, gateway.Port, timeoutCts.Token);
            return true;
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
        }
        catch (SocketException)
        {
        }
    }

    return false;
}

static async Task<IReadOnlyList<IPAddress>> ResolveGatewayAddressesAsync(string host, CancellationToken cancellationToken)
{
    if (host is "localhost")
    {
        return [IPAddress.Loopback, IPAddress.IPv6Loopback];
    }

    if (host is "0.0.0.0" or "+" or "*")
    {
        return [IPAddress.Loopback];
    }

    if (IPAddress.TryParse(host, out var address))
    {
        return address.Equals(IPAddress.Any) || address.Equals(IPAddress.IPv6Any)
            ? [IPAddress.Loopback]
            : [address];
    }

    var addresses = await Dns.GetHostAddressesAsync(host, cancellationToken);
    return addresses
        .OrderBy(x => x.AddressFamily == AddressFamily.InterNetwork ? 0 : 1)
        .ToArray();
}

static async Task StopViewerAsync(Task? viewerTask, CancellationTokenSource viewerCts)
{
    if (viewerTask is null)
    {
        return;
    }

    await viewerCts.CancelAsync();
    try
    {
        await viewerTask;
    }
    catch (OperationCanceledException)
    {
    }
    catch (SocketException)
    {
    }
}

static async Task StartClientsAsync(
    List<Task> tasks,
    DummyOptions options,
    DummyStats stats,
    DummyViewerState viewerState,
    CancellationToken cancellationToken)
{
    var batchSize = options.NormalizedLoginBatchSize;
    for (var start = 0; start < options.Clients; start += batchSize)
    {
        var endExclusive = Math.Min(start + batchSize, options.Clients);
        for (var index = start; index < endExclusive; index++)
        {
            tasks.Add(DummyClientRunner.RunAsync(index, options, stats, viewerState, cancellationToken));
        }

        stats.SetStatus($"Started login batch {start / batchSize + 1}: {start + 1}-{endExclusive} / {options.Clients}.");
        if (endExclusive >= options.Clients)
        {
            return;
        }

        await Task.Delay(options.NormalizedLoginBatchInterval, cancellationToken);
    }
}
