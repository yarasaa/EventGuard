using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;

namespace EventGuard.SqlServer.Tests;

// Local TDS 7.x only, MARS off and Encrypt=False (login TLS is forwarded untouched).
// Observe TM_COMMIT_XACT, then drop its actual server reply. Tests independently verify commit.
internal sealed class CommitReplyDropProxy : IAsyncDisposable
{
    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource _stop = new();
    private readonly Task _run;
    private int _commit;
    public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;
    public TaskCompletionSource Dropped { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public CommitReplyDropProxy()
    {
        _listener.Start();
        _run = RelayAsync();
    }
    private async Task RelayAsync()
    {
        try
        {
            using var client = await _listener.AcceptTcpClientAsync(_stop.Token);
            using var server = new TcpClient();
            await server.ConnectAsync(IPAddress.Loopback, 51433, _stop.Token);
            var requests = ForwardAsync(client.GetStream(), server.GetStream(), true, _stop.Token);
            var replies = ForwardAsync(server.GetStream(), client.GetStream(), false, _stop.Token);
            await Task.WhenAny(requests, replies);
            _stop.Cancel();
            client.Close(); server.Close();
            try { await Task.WhenAll(requests, replies); }
            catch (Exception error) when (error is IOException or OperationCanceledException or ObjectDisposedException or SocketException) { }
        }
        catch (Exception error) when (_stop.IsCancellationRequested && error is OperationCanceledException or SocketException) { }
    }
    private async Task ForwardAsync(NetworkStream input, NetworkStream output, bool fromClient, CancellationToken ct)
    {
        while (await ReadPacketAsync(input, ct) is { } packet)
        {
            if (fromClient && packet[0] == 0x0e && packet.Length >= 14)
            {
                var headers = BinaryPrimitives.ReadInt32LittleEndian(packet.AsSpan(8));
                if (headers >= 4 && headers + 10 <= packet.Length && BinaryPrimitives.ReadUInt16LittleEndian(packet.AsSpan(8 + headers)) == 7)
                    Volatile.Write(ref _commit, 1);
            }
            if (!fromClient && packet[0] == 4 && Volatile.Read(ref _commit) == 1)
            {
                Dropped.TrySetResult();
                return;
            }
            await output.WriteAsync(packet, ct);
        }
    }
    private static async Task<byte[]?> ReadPacketAsync(NetworkStream input, CancellationToken ct)
    {
        var first = new byte[1];
        if (await input.ReadAsync(first, ct) == 0) return null;
        var tls = first[0] is >= 0x14 and <= 0x17;
        var header = new byte[tls ? 5 : 8];
        header[0] = first[0];
        await input.ReadExactlyAsync(header.AsMemory(1), ct);
        var length = tls ? 5 + BinaryPrimitives.ReadUInt16BigEndian(header.AsSpan(3)) : BinaryPrimitives.ReadUInt16BigEndian(header.AsSpan(2));
        if (length < header.Length || length > 65540) throw new InvalidDataException("Invalid local TDS/TLS packet length.");
        var packet = new byte[length];
        header.CopyTo(packet, 0);
        await input.ReadExactlyAsync(packet.AsMemory(header.Length), ct);
        return packet;
    }
    public async ValueTask DisposeAsync()
    {
        _stop.Cancel(); _listener.Stop();
        await _run;
        _stop.Dispose();
    }
}
