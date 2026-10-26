using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace EventGuard.Kafka.Tests;

// The separate broker listener advertises this fixed loopback port, so metadata/coordinator connections also traverse the proxy.
// Kafka plaintext frames only; never inspect or rewrite record bodies. Drop actual broker responses (or commit requests).
internal sealed class KafkaReplyDropProxy : IAsyncDisposable
{
    public const string Address = "127.0.0.1:29092";
    private readonly TcpListener _listener = new(IPAddress.Loopback, 29092);
    private readonly CancellationTokenSource _stop = new();
    private readonly short _api;
    private readonly bool _request;
    private readonly Task _run;
    public TaskCompletionSource Dropped { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public KafkaReplyDropProxy(short api, bool request = false)
    {
        _api = api; _request = request; _listener.Start(); _run = AcceptAsync();
    }
    private async Task AcceptAsync()
    {
        var relays = new List<Task>();
        try
        {
            while (!_stop.IsCancellationRequested) relays.Add(RelayAsync(await _listener.AcceptTcpClientAsync(_stop.Token)));
        }
        catch (Exception error) when (_stop.IsCancellationRequested && error is OperationCanceledException or SocketException) { }
        finally { await Task.WhenAll(relays); }
    }
    private async Task RelayAsync(TcpClient client)
    {
        using (client)
        using (var server = new TcpClient())
        using (var stop = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token))
        {
            try
            {
                await server.ConnectAsync(IPAddress.Loopback, 19094, stop.Token);
                var correlations = new ConcurrentDictionary<int, short>();
                var send = ForwardAsync(client.GetStream(), server.GetStream(), true, correlations, stop.Token);
                var receive = ForwardAsync(server.GetStream(), client.GetStream(), false, correlations, stop.Token);
                await Task.WhenAny(send, receive);
                stop.Cancel(); client.Close(); server.Close();
                try { await Task.WhenAll(send, receive); }
                catch (Exception e) when (e is IOException or SocketException or OperationCanceledException or ObjectDisposedException) { }
            }
            catch (Exception e) when (_stop.IsCancellationRequested && e is OperationCanceledException or SocketException) { }
        }
    }
    private async Task ForwardAsync(NetworkStream input, NetworkStream output, bool request, ConcurrentDictionary<int, short> correlations, CancellationToken ct)
    {
        while (true)
        {
            var header = new byte[4];
            if (await input.ReadAsync(header.AsMemory(0, 1), ct) == 0) return;
            await input.ReadExactlyAsync(header.AsMemory(1), ct);
            var length = BinaryPrimitives.ReadInt32BigEndian(header);
            if (length < (request ? 8 : 4) || length > 16 * 1024 * 1024) throw new InvalidDataException("Kafka frame exceeds test profile.");
            var body = new byte[length]; await input.ReadExactlyAsync(body, ct);
            short api;
            if (request)
            {
                api = BinaryPrimitives.ReadInt16BigEndian(body);
                correlations[BinaryPrimitives.ReadInt32BigEndian(body.AsSpan(4))] = api;
            }
            else if (!correlations.TryRemove(BinaryPrimitives.ReadInt32BigEndian(body), out api)) throw new InvalidDataException("Unknown Kafka correlation.");
            if (request == _request && api == _api) { Dropped.TrySetResult(); continue; }
            await output.WriteAsync(header, ct); await output.WriteAsync(body, ct);
        }
    }
    public async ValueTask DisposeAsync()
    {
        _stop.Cancel(); _listener.Stop(); await _run; _stop.Dispose();
    }
}
