using System.Net;
using System.Net.Sockets;
using System.Text;

internal static class DummyViewer
{
    // viewer 렌더링용 snapshot은 서버 AOI tick과 같은 200ms 주기로 갱신함
    private static readonly TimeSpan SnapshotInterval = TimeSpan.FromMilliseconds(200);

    public static Task StartAsync(DummyViewerState state, string host, int port, CancellationToken cancellationToken)
    {
        var html = File.ReadAllText(ResolveIndexPath());
        state.RefreshSnapshotJson();

        var listener = new TcpListener(ResolveAddress(host), port);
        listener.Start(backlog: 32);

        var stopped = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancellationRegistration = cancellationToken.Register(static target =>
        {
            ((TcpListener)target!).Stop();
        }, listener);

        // dummy networking 부하와 viewer HTTP 응답이 서로 막지 않도록 전용 thread 사용함
        var snapshotThread = new Thread(() => RunSnapshotLoop(state, cancellationToken))
        {
            IsBackground = true,
            Name = "DummyViewer Snapshot"
        };
        var serverThread = new Thread(() =>
        {
            try
            {
                RunServerLoop(listener, html, state, cancellationToken);
                stopped.TrySetResult();
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                stopped.TrySetException(ex);
            }
            finally
            {
                cancellationRegistration.Dispose();
                listener.Stop();
            }
        })
        {
            IsBackground = true,
            Name = "DummyViewer HTTP"
        };

        snapshotThread.Start();
        serverThread.Start();
        return stopped.Task;
    }

    private static void RunSnapshotLoop(DummyViewerState state, CancellationToken cancellationToken)
    {
        while (!cancellationToken.WaitHandle.WaitOne(SnapshotInterval))
        {
            state.RefreshSnapshotJson();
        }
    }

    private static void RunServerLoop(
        TcpListener listener,
        string html,
        DummyViewerState state,
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            TcpClient? client = null;
            try
            {
                client = listener.AcceptTcpClient();
                HandleClient(client, html, state);
            }
            catch (SocketException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (ObjectDisposedException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch
            {
                client?.Dispose();
            }
        }
    }

    private static void HandleClient(TcpClient client, string html, DummyViewerState state)
    {
        using (client)
        {
            client.NoDelay = true;
            using var stream = client.GetStream();
            stream.ReadTimeout = 1000;
            stream.WriteTimeout = 2000;

            var path = ReadRequestPath(stream);
            if (path == "/")
            {
                WriteResponse(stream, "200 OK", "text/html; charset=utf-8", html);
                return;
            }

            if (path == "/state")
            {
                WriteResponse(stream, "200 OK", "application/json; charset=utf-8", state.SnapshotJson);
                return;
            }

            WriteResponse(stream, "404 Not Found", "text/plain; charset=utf-8", "not found");
        }
    }

    private static string ReadRequestPath(NetworkStream stream)
    {
        Span<byte> buffer = stackalloc byte[4096];
        var length = 0;

        while (length < buffer.Length)
        {
            var read = stream.Read(buffer[length..]);
            if (read <= 0)
            {
                break;
            }

            length += read;
            if (ContainsHeaderEnd(buffer[..length]))
            {
                break;
            }
        }

        var request = Encoding.ASCII.GetString(buffer[..length]);
        var firstLineEnd = request.IndexOf("\r\n", StringComparison.Ordinal);
        var firstLine = firstLineEnd >= 0 ? request[..firstLineEnd] : request;
        var parts = firstLine.Split(' ', 3, StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 2 || !string.Equals(parts[0], "GET", StringComparison.OrdinalIgnoreCase))
        {
            return "";
        }

        var path = parts[1];
        var queryIndex = path.IndexOf('?', StringComparison.Ordinal);
        return queryIndex >= 0 ? path[..queryIndex] : path;
    }

    private static bool ContainsHeaderEnd(ReadOnlySpan<byte> buffer)
    {
        for (var i = 3; i < buffer.Length; i++)
        {
            if (buffer[i - 3] == '\r'
                && buffer[i - 2] == '\n'
                && buffer[i - 1] == '\r'
                && buffer[i] == '\n')
            {
                return true;
            }
        }

        return false;
    }

    private static void WriteResponse(NetworkStream stream, string status, string contentType, string body)
    {
        var bodyBytes = Encoding.UTF8.GetBytes(body);
        var header =
            $"HTTP/1.1 {status}\r\n" +
            $"Content-Type: {contentType}\r\n" +
            $"Content-Length: {bodyBytes.Length}\r\n" +
            "Cache-Control: no-store\r\n" +
            "Connection: close\r\n" +
            "\r\n";
        var headerBytes = Encoding.ASCII.GetBytes(header);
        stream.Write(headerBytes);
        stream.Write(bodyBytes);
        stream.Flush();
    }

    private static IPAddress ResolveAddress(string host)
        => host switch
        {
            "0.0.0.0" or "+" or "*" => IPAddress.Any,
            "localhost" => IPAddress.Loopback,
            _ when IPAddress.TryParse(host, out var address) => address,
            _ => IPAddress.Loopback
        };

    private static string ResolveIndexPath()
    {
        var candidates = new[]
        {
            Path.Combine(AppContext.BaseDirectory, "Viewer", "index.html"),
            Path.Combine(Directory.GetCurrentDirectory(), "Viewer", "index.html"),
            Path.Combine(Directory.GetCurrentDirectory(), "GameServer", "DummyClient", "Viewer", "index.html")
        };

        foreach (var candidate in candidates)
        {
            if (File.Exists(candidate))
            {
                return candidate;
            }
        }

        throw new FileNotFoundException("DummyClient viewer HTML file was not found.", "Viewer/index.html");
    }
}
