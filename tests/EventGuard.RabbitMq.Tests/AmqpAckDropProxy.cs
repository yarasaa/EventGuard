using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;

namespace EventGuard.RabbitMq.Tests;

// Plaintext loopback AMQP only. Drop one actual basic.ack frame, not an injected client exception.
internal sealed class AmqpAckDropProxy : IAsyncDisposable
{
    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource _stop = new();
    private readonly Task _run;
    private readonly bool _publisherConfirm;
    private readonly bool _disconnect;
    private readonly TimeSpan _forwardedConfirmDelay;
    private int _dropped;
    public TaskCompletionSource Dropped { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;
    public AmqpAckDropProxy(bool publisherConfirm, bool disconnect = true, TimeSpan? forwardedConfirmDelay = null)
    {
        _publisherConfirm = publisherConfirm;
        _disconnect = disconnect;
        _forwardedConfirmDelay = forwardedConfirmDelay ?? TimeSpan.Zero;
        _listener.Start();
        _run = AcceptAsync();
    }
    private async Task AcceptAsync()
    {
        var relays = new List<Task>();
        try
        {
            while (!_stop.IsCancellationRequested)
                relays.Add(RelayAsync(await _listener.AcceptTcpClientAsync(_stop.Token)));
        }
        catch (Exception e) when (_stop.IsCancellationRequested && e is OperationCanceledException or SocketException) { }
        finally { await Task.WhenAll(relays); }
    }
    private async Task RelayAsync(TcpClient client)
    {
        using (client)
        using (var upstream = new TcpClient())
        using (var stop = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token))
        {
            try
            {
                // Only the selected protocol fault should delay traffic, not Nagle buffering.
                client.NoDelay = true;
                upstream.NoDelay = true;
                await upstream.ConnectAsync(IPAddress.Loopback, 5673, stop.Token);
                var requests = ForwardAsync(client.GetStream(), upstream.GetStream(), fromClient: true, stop.Token);
                var replies = ForwardAsync(upstream.GetStream(), client.GetStream(), fromClient: false, stop.Token);
                await Task.WhenAny(requests, replies);
                stop.Cancel();
                client.Close();
                upstream.Close();
                try { await Task.WhenAll(requests, replies); }
                catch (Exception e) when (e is IOException or OperationCanceledException or ObjectDisposedException or SocketException) { }
            }
            catch (Exception e) when (_stop.IsCancellationRequested && e is OperationCanceledException or SocketException) { }
        }
    }
    private async Task ForwardAsync(NetworkStream input, NetworkStream output, bool fromClient, CancellationToken ct)
    {
        if (fromClient)
        {
            var protocol = new byte[8];
            await input.ReadExactlyAsync(protocol, ct);
            await output.WriteAsync(protocol, ct);
        }
        while (true)
        {
            var header = new byte[7];
            if (await input.ReadAsync(header.AsMemory(0, 1), ct) == 0) return;
            await input.ReadExactlyAsync(header.AsMemory(1), ct);
            var size = BinaryPrimitives.ReadUInt32BigEndian(header.AsSpan(3));
            if (size > 16 * 1024 * 1024) throw new InvalidDataException("AMQP frame exceeds test limit.");
            var payload = new byte[size + 1];
            await input.ReadExactlyAsync(payload, ct);
            if (payload[^1] != 0xce) throw new InvalidDataException("Invalid AMQP frame end.");
            var targetedAck = fromClient != _publisherConfirm && header[0] == 1 && size >= 4
                && BinaryPrimitives.ReadUInt16BigEndian(payload) == 60 && BinaryPrimitives.ReadUInt16BigEndian(payload.AsSpan(2)) == 80;
            if (targetedAck && Interlocked.CompareExchange(ref _dropped, 1, 0) == 0)
            {
                Dropped.TrySetResult();
                if (_disconnect) return;
                continue;
            }
            if (targetedAck && _publisherConfirm && _forwardedConfirmDelay > TimeSpan.Zero)
                await Task.Delay(_forwardedConfirmDelay, ct);
            await output.WriteAsync(header, ct);
            await output.WriteAsync(payload, ct);
        }
    }
    public async ValueTask DisposeAsync()
    {
        _stop.Cancel();
        _listener.Stop();
        await _run;
        _stop.Dispose();
    }
}
