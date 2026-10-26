using EventGuard.RabbitMq;
using EventGuard.Runtime;
using RabbitMQ.Client;

namespace EventGuard.SqlServer.Tests;

public sealed class RabbitMqTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    [Fact]
    public async Task Real_rabbitmq_duplicate_delivery_creates_one_sqlserver_work_and_business_effect()
    {
        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = 5673,
            VirtualHost = "eventguard_test",
            UserName = "eventguard",
            Password = "eventguard-local-only",
            AutomaticRecoveryEnabled = false,
            TopologyRecoveryEnabled = false
        };
        await using var adminConnection = await factory.CreateConnectionAsync();
        await using var admin = await adminConnection.CreateChannelAsync(new CreateChannelOptions(true, true));
        var queue = "eg_sql_rmq_" + Guid.NewGuid().ToString("N");
        await admin.QueueDeclareAsync(queue, true, false, false,
            new Dictionary<string, object?> { ["x-queue-type"] = "quorum", ["x-delivery-limit"] = -1 });
        try
        {
            await using var connection = await RabbitMqConnection.OpenAsync(factory);
            await using var publisher = new RabbitMqPublisher(connection, "sql-local", "v1");
            var consumer = DatabaseFixture.Scope();
            var producer = new ProducerScope(consumer.Tenant, consumer.Consumer);
            var message = new OutgoingMessage(producer, Guid.NewGuid(), Guid.NewGuid(),
                new("rabbitmq", "sql-local", "v1", RabbitMqPublisher.DefaultExchange, queue), [1]);
            await db.EnqueueAsync(message);
            var publication = await new OutboxDispatcher(db.Outbox, producer, [publisher]).DispatchNextAsync();
            Assert.Equal(OutboxState.Published, publication!.Record.State);
            Assert.Equal(PublishStatus.Accepted, (await publisher.PublishAsync(message)).Status);
            // A sequential invalid sentinel is reached only after the preceding valid deliveries were acked.
            await admin.BasicPublishAsync("", queue, true, new BasicProperties { Persistent = true }, new byte[] { 0 });
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await Assert.ThrowsAsync<IngressMessageRejectedException>(() => new RabbitMqIngress(connection, db.Store, consumer,
                new("message-id/v1", "body/v1"), queue).RunAsync(timeout.Token));
            var sentinel = await admin.BasicGetAsync(queue, true);
            Assert.NotNull(sentinel);
            Assert.Null(sentinel.BasicProperties.MessageId);
            Assert.Null(await admin.BasicGetAsync(queue, true));
            var claim = (await db.Claim(consumer))!;
            Assert.Equal(message.MessageId, claim.Work.MessageId);
            Assert.Equal(ExecutionStatus.Completed, (await db.Store.ExecuteAsync(claim, db.Effect, new())).Status);
            Assert.Equal(1, await db.EffectCount(claim.Work.Id));
            Assert.Null(await db.Claim(consumer));
        }
        finally { await admin.QueueDeleteAsync(queue); }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Raw_quarantine_controls_native_rabbitmq_ack_with_sqlserver(bool brokenStore)
    {
        var factory = new ConnectionFactory { HostName = "127.0.0.1", Port = 5673, VirtualHost = "eventguard_test",
            UserName = "eventguard", Password = "eventguard-local-only", AutomaticRecoveryEnabled = false, TopologyRecoveryEnabled = false };
        await using var native = await factory.CreateConnectionAsync();
        await using var channel = await native.CreateChannelAsync(new CreateChannelOptions(true, true));
        var queue = "eg_sql_rmq_quarantine_" + Guid.NewGuid().ToString("N");
        await channel.QueueDeclareAsync(queue, true, false, false);
        try
        {
            var scope = DatabaseFixture.Scope();
            await channel.BasicPublishAsync("", queue, true, new BasicProperties { Persistent = true }, new byte[] { 9 });
            await channel.BasicPublishAsync("", queue, true, new BasicProperties { MessageId = Guid.NewGuid().ToString("D") }, new byte[] { 0 });
            await using var connection = await RabbitMqConnection.OpenAsync(factory);
            var store = brokenStore ? new SqlServerWorkStore(db.DataSource, "eg_missing_" + Guid.NewGuid().ToString("N")) : db.Store;
            using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            var running = new RabbitMqIngress(connection, store, scope, new("v1", "sha256-bytes/v1"), queue,
                delivery => delivery.BasicProperties.MessageId is null ? throw new IngressMessageRejectedException("invalid-message-id")
                    : throw new InvalidOperationException("sentinel"), quarantine: new("test-broker")).RunAsync(stop.Token);
            if (brokenStore) await Assert.ThrowsAsync<Microsoft.Data.SqlClient.SqlException>(() => running);
            else await Assert.ThrowsAsync<InvalidOperationException>(() => running);
            var pending = await channel.BasicGetAsync(queue, true); Assert.NotNull(pending);
            Assert.Equal(new byte[] { brokenStore ? (byte)9 : (byte)0 }, pending.Body.ToArray());
            await using var sql = await db.DataSource.OpenConnectionAsync();
            await using var count = new Microsoft.Data.SqlClient.SqlCommand($"SELECT COUNT_BIG(*) FROM {db.Schema}.ingress_quarantine WHERE scope_id=@scope", sql);
            count.Parameters.AddWithValue("scope", scope.Id);
            Assert.Equal(brokenStore ? 0L : 1L, await count.ExecuteScalarAsync());
        }
        finally { await channel.QueueDeleteAsync(queue); }
    }
}
