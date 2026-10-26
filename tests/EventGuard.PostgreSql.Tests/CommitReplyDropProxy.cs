using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace EventGuard.PostgreSql.Tests;

// A real loopback TCP fault: forward COMMIT to PostgreSQL, observe its successful response,
// then disconnect before forwarding that response. This is not an exception-only simulation.
internal sealed class CommitReplyDropProxy : IAsyncDisposable
{
    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource _stop = new();
    private readonly Task _run;
    public TaskCompletionSource Dropped { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

    public CommitReplyDropProxy(int upstreamPort)
    {
        _listener.Start();
        _run = RelayAsync(upstreamPort);
    }

    private async Task RelayAsync(int upstreamPort)
    {
        try
        {
            using var client = await _listener.AcceptTcpClientAsync(_stop.Token);
            using var upstream = new TcpClient();
            await upstream.ConnectAsync(IPAddress.Loopback, upstreamPort, _stop.Token);
            var incoming = client.GetStream(); var outgoing = upstream.GetStream();
            var requests = ForwardRequestsAsync(incoming, outgoing, _stop.Token);
            var responses = ForwardResponsesAsync(outgoing, incoming, _stop.Token);
            await Task.WhenAny(requests, responses);
            _stop.Cancel();
            client.Close(); upstream.Close();
            try { await Task.WhenAll(requests, responses); }
            catch (Exception e) when (e is IOException or OperationCanceledException or ObjectDisposedException) { }
        }
        catch (Exception e) when (_stop.IsCancellationRequested && e is OperationCanceledException or SocketException) { }
    }

    private static async Task ForwardRequestsAsync(NetworkStream incoming, NetworkStream outgoing, CancellationToken ct)
    {
        // SSL and GSS encryption are explicitly disabled on this local test connection.
        byte[] header = new byte[4];
        await incoming.ReadExactlyAsync(header, ct);
        var length = BinaryPrimitives.ReadInt32BigEndian(header);
        if (length < 8 || length > 1024 * 1024) throw new InvalidDataException("Invalid startup frame.");
        var startup = new byte[length];
        header.CopyTo(startup, 0);
        await incoming.ReadExactlyAsync(startup.AsMemory(4), ct);
        await outgoing.WriteAsync(startup, ct);
        while (await ReadFrameAsync(incoming, ct) is { } frame)
            await outgoing.WriteAsync(frame, ct);
    }

    private async Task ForwardResponsesAsync(NetworkStream outgoing, NetworkStream incoming, CancellationToken ct)
    {
        while (await ReadFrameAsync(outgoing, ct) is { } frame)
        {
            if (frame[0] == (byte)'C' && Encoding.UTF8.GetString(frame, 5, frame.Length - 5) == "COMMIT\0")
            {
                Dropped.TrySetResult();
                return;
            }
            await incoming.WriteAsync(frame, ct);
        }
    }

    private static async Task<byte[]?> ReadFrameAsync(NetworkStream stream, CancellationToken ct)
    {
        byte[] header = new byte[5];
        if (await stream.ReadAsync(header.AsMemory(0, 1), ct) == 0) return null;
        await stream.ReadExactlyAsync(header.AsMemory(1), ct);
        var length = BinaryPrimitives.ReadInt32BigEndian(header.AsSpan(1));
        if (length < 4 || length > 64 * 1024 * 1024) throw new InvalidDataException("Invalid PostgreSQL frame.");
        var frame = new byte[length + 1];
        header.CopyTo(frame, 0);
        await stream.ReadExactlyAsync(frame.AsMemory(5), ct);
        return frame;
    }

    public async ValueTask DisposeAsync()
    {
        _stop.Cancel();
        _listener.Stop();
        await _run;
        _stop.Dispose();
    }
}
