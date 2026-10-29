using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;

namespace EventGuard.TestSupport;

// Loopback-only protocol counting for the local benchmark. No SQL text, values or credentials are recorded.
// PostgreSQL counts Sync/simple-query requests; SQL Server counts complete SQL/RPC/transaction messages.
internal sealed class DatabaseExchangeCounter : IAsyncDisposable
{
    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource _stop = new();
    private readonly Task _run;
    private long _exchanges;
    public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;
    public long Exchanges => Interlocked.Read(ref _exchanges);

    public DatabaseExchangeCounter(int upstreamPort, bool postgres)
    {
        _listener.Start();
        _run = Relay(upstreamPort, postgres);
    }
    private async Task Relay(int port, bool postgres)
    {
        try
        {
            using var client = await _listener.AcceptTcpClientAsync(_stop.Token);
            using var upstream = new TcpClient { NoDelay = true };
            client.NoDelay = true;
            await upstream.ConnectAsync(IPAddress.Loopback, port, _stop.Token);
            var requests = Requests(client.GetStream(), upstream.GetStream(), postgres, _stop.Token);
            var replies = upstream.GetStream().CopyToAsync(client.GetStream(), _stop.Token);
            await Task.WhenAny(requests, replies);
            _stop.Cancel(); client.Close(); upstream.Close();
            try { await Task.WhenAll(requests, replies); }
            catch (Exception e) when (e is IOException or OperationCanceledException or ObjectDisposedException or SocketException) { }
        }
        catch (Exception e) when (_stop.IsCancellationRequested && e is OperationCanceledException or SocketException) { }
    }
    private async Task Requests(NetworkStream input, NetworkStream output, bool postgres, CancellationToken ct)
    {
        if (postgres)
        {
            var header = new byte[4]; await input.ReadExactlyAsync(header, ct);
            var size = BinaryPrimitives.ReadInt32BigEndian(header);
            if (size is < 8 or > 1024 * 1024) throw new InvalidDataException("Unexpected local startup frame.");
            var startup = new byte[size]; header.CopyTo(startup, 0);
            await input.ReadExactlyAsync(startup.AsMemory(4), ct); await output.WriteAsync(startup, ct);
        }
        var first = new byte[1];
        while (await input.ReadAsync(first, ct) != 0)
        {
            var tls = !postgres && first[0] is >= 0x14 and <= 0x17;
            var header = new byte[postgres || tls ? 5 : 8]; header[0] = first[0];
            await input.ReadExactlyAsync(header.AsMemory(1), ct);
            var size = postgres ? BinaryPrimitives.ReadInt32BigEndian(header.AsSpan(1)) + 1
                : tls ? 5 + BinaryPrimitives.ReadUInt16BigEndian(header.AsSpan(3)) : BinaryPrimitives.ReadUInt16BigEndian(header.AsSpan(2));
            if (size < header.Length || size > 128 * 1024 * 1024) throw new InvalidDataException("Unexpected local protocol frame.");
            var frame = new byte[size]; header.CopyTo(frame, 0);
            await input.ReadExactlyAsync(frame.AsMemory(header.Length), ct);
            if (postgres ? frame[0] is (byte)'S' or (byte)'Q' : !tls && (frame[1] & 1) != 0 && frame[0] is 1 or 3 or 14)
                Interlocked.Increment(ref _exchanges);
            await output.WriteAsync(frame, ct);
        }
    }
    public async ValueTask DisposeAsync()
    {
        _stop.Cancel(); _listener.Stop();
        await _run;
        _stop.Dispose();
    }
}
