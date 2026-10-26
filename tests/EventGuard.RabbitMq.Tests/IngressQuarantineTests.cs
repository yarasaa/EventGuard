using System.Text;
using EventGuard.PostgreSql;
using RabbitMQ.Client;

namespace EventGuard.RabbitMq.Tests;

public sealed class IngressQuarantineTests(BrokerFixture broker) : IClassFixture<BrokerFixture>
{
    [Fact]
    public async Task Permanent_delivery_preserves_body_and_typed_properties_before_individual_ack()
    {
        var queue = await broker.QueueAsync(); var scope = BrokerFixture.Scope();
        // Untracked confirms avoid adding a different publisher sequence header to each physical send.
        await using var publisher = await broker.Native.CreateChannelAsync(new CreateChannelOptions(true, false));
        var properties = new BasicProperties { Persistent = true, ContentType = "application/json", Headers = new Dictionary<string, object?>
        { ["binary"] = new byte[] { 0, 255 }, ["nested"] = new Dictionary<string, object?> { ["count"] = 42, ["flag"] = true } } };
        await publisher.BasicPublishAsync("", queue, true, properties, new byte[] { 6 });
        await publisher.BasicPublishAsync("", queue, true, properties, new byte[] { 6 });
        await publisher.BasicPublishAsync("", queue, true, new BasicProperties { MessageId = Guid.NewGuid().ToString("D") }, new byte[] { 0 });
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        await Assert.ThrowsAsync<InvalidOperationException>(() => new RabbitMqIngress(broker.Connection, broker.Store, scope, BrokerFixture.Policy,
            queue, delivery => delivery.BasicProperties.MessageId is null
                ? throw new IngressMessageRejectedException("invalid-contract") : throw new InvalidOperationException("sentinel"),
            quarantine: new("test-broker")).RunAsync(stop.Token));
        await using var query = broker.DataSource.CreateCommand($"SELECT id FROM {broker.Schema}.ingress_quarantine WHERE scope_id=@scope");
        query.Parameters.AddWithValue("scope", scope.Id);
        Guid id;
        await using (var rows = await query.ExecuteReaderAsync()) { Assert.True(await rows.ReadAsync()); id = rows.GetGuid(0); Assert.False(await rows.ReadAsync()); }
        var raw = (await broker.Store.ReadQuarantineDataAsync(scope, id))!;
        Assert.Equal(new byte[] { 6 }, raw.CopyBody());
        using var reader = new BinaryReader(new MemoryStream(raw.CopyMetadata()), Encoding.UTF8);
        Assert.Equal(queue, reader.ReadString()); Assert.Equal("", reader.ReadString()); Assert.Equal(queue, reader.ReadString());
        IAmqpWriteable expected = properties; var bytes = new byte[expected.GetRequiredBufferSize()]; expected.WriteTo(bytes);
        Assert.Equal(bytes.Length, reader.ReadInt32()); Assert.Equal(bytes, reader.ReadBytes(bytes.Length));
        var sentinel = await broker.Admin.BasicGetAsync(queue, autoAck: true);
        Assert.NotNull(sentinel); Assert.Equal(new byte[] { 0 }, sentinel.Body.ToArray());
        Assert.Null(await broker.Admin.BasicGetAsync(queue, autoAck: true));
    }

    [Fact]
    public async Task Native_database_failure_during_quarantine_leaves_raw_message_unacknowledged()
    {
        var queue = await broker.QueueAsync(); await broker.RawAsync(queue, null, [9]);
        var broken = new PostgreSqlWorkStore(broker.DataSource, "eg_missing_" + Guid.NewGuid().ToString("N"));
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        await Assert.ThrowsAsync<Npgsql.PostgresException>(() => new RabbitMqIngress(broker.Connection, broken, BrokerFixture.Scope(),
            BrokerFixture.Policy, queue, quarantine: new("test-broker")).RunAsync(stop.Token));
        var pending = await broker.Admin.BasicGetAsync(queue, autoAck: true);
        Assert.NotNull(pending); Assert.True(pending.Redelivered); Assert.Equal(new byte[] { 9 }, pending.Body.ToArray());
    }

    [Fact]
    public async Task Lost_native_ack_redelivers_into_the_same_durable_quarantine_record()
    {
        var queue = await broker.QueueAsync(); var scope = BrokerFixture.Scope(); await broker.RawAsync(queue, null, [8]);
        await using (var proxy = new AmqpAckDropProxy(publisherConfirm: false))
        await using (var connection = await RabbitMqConnection.OpenAsync(BrokerFixture.Factory(proxy.Port)))
        {
            using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            var running = new RabbitMqIngress(connection, broker.Store, scope, BrokerFixture.Policy, queue, quarantine: new("test-broker")).RunAsync(stop.Token);
            await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await Assert.ThrowsAnyAsync<Exception>(() => running.WaitAsync(TimeSpan.FromSeconds(10)));
        }
        await broker.RawAsync(queue, Guid.NewGuid().ToString("D"), [0]);
        using var again = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        await Assert.ThrowsAsync<InvalidOperationException>(() => new RabbitMqIngress(broker.Connection, broker.Store, scope, BrokerFixture.Policy, queue,
            delivery => delivery.BasicProperties.MessageId is null ? throw new IngressMessageRejectedException("invalid-message-id")
                : throw new InvalidOperationException("sentinel"), quarantine: new("test-broker")).RunAsync(again.Token));
        await using var query = broker.DataSource.CreateCommand($"SELECT count(*) FROM {broker.Schema}.ingress_quarantine WHERE scope_id=@scope");
        query.Parameters.AddWithValue("scope", scope.Id); Assert.Equal(1L, await query.ExecuteScalarAsync());
        var pending = await broker.Admin.BasicGetAsync(queue, true); Assert.NotNull(pending); Assert.Equal(new byte[] { 0 }, pending.Body.ToArray());
        Assert.Null(await broker.Admin.BasicGetAsync(queue, true));
    }
}
