using EventGuard;
using EventGuard.PostgreSql;
using EventGuard.RabbitMq;
using EventGuard.Runtime;
using Npgsql;
using RabbitMQ.Client;

// This composition root only connects to the repository's local development services.
const string schema = "eventguard_rabbit_sample";
await using var data = NpgsqlDataSource.Create("Host=127.0.0.1;Port=55432;Database=eventguard_test;Username=eventguard;Password=eventguard-local-only;Maximum Pool Size=8");
var store = new PostgreSqlWorkStore(data, schema);
await store.InitializeAsync();
var outbox = new PostgreSqlOutboxStore(data, schema);
await using (var setup = data.CreateCommand($"CREATE TABLE IF NOT EXISTS {schema}.effects(message_id uuid NOT NULL, stage text NOT NULL)"))
    await setup.ExecuteNonQueryAsync();
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
await using var admin = await adminConnection.CreateChannelAsync();
var runId = Guid.NewGuid().ToString("N");
var queue = "eg_rabbit_sample_" + runId;
// Topology belongs to the application/deployment. A single-node quorum is not an HA demonstration.
await admin.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: false,
    new Dictionary<string, object?> { ["x-queue-type"] = "quorum", ["x-delivery-limit"] = -1 });
try
{
    await using var connection = await RabbitMqConnection.OpenAsync(factory);
    await using var publisher = new RabbitMqPublisher(connection, "local", "v1", channelCount: 2);
    // Demo-only isolation. Production scopes must stay stable across application restarts.
    var producerScope = new ProducerScope("demo", "file-service-" + runId);
    var consumerScope = new WorkScope("demo", "indexer-" + runId, "rabbitmq/local/v1", "file.updated");
    var message = new OutgoingMessage(producerScope, Guid.NewGuid(), Guid.NewGuid(),
        new("rabbitmq", "local", "v1", RabbitMqPublisher.DefaultExchange, queue), "{\"file\":\"sample.txt\"}"u8);
    var request = new ProducerRequest(producerScope, Guid.NewGuid(), ContentFingerprint.FromBytes(message.CopyPayload()));
    var produced = await outbox.ProduceAsync(request, async (session, ct) =>
    {
        await using var command = session.CreateCommand($"INSERT INTO {schema}.effects VALUES(@id,'producer')");
        command.Parameters.AddWithValue("id", message.MessageId);
        await command.ExecuteNonQueryAsync(ct);
        await session.Outbox.EnqueueAsync(message, ct);
    });
    var repeated = await outbox.ProduceAsync(request, (_, _) => throw new InvalidOperationException("A committed request must not run twice."));
    Console.WriteLine($"Producer: {produced.Status}; repeated request: {repeated.Status}");
    var published = await new OutboxDispatcher(outbox, producerScope, [publisher]).DispatchNextAsync();
    if (published?.Record.State != OutboxState.Published) throw new InvalidOperationException("Broker acceptance was not confirmed.");
    Console.WriteLine($"RabbitMQ: {published.Record.State}; reason: {published.Record.LastReason}");

    using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(30));
    var ingress = new RabbitMqIngress(connection, store, consumerScope, new("message-id/v1", "body/v1"), queue);
    var pump = new WorkPump<PostgreSqlSession>(store, consumerScope, async (work, _, session, ct) =>
    {
        await using var command = session.CreateCommand($"INSERT INTO {schema}.effects VALUES(@id,'consumer')");
        command.Parameters.AddWithValue("id", work.MessageId);
        await command.ExecuteNonQueryAsync(ct);
    }, options: new WorkPumpOptions(maximumConcurrency: 2));
    var accepting = ingress.RunAsync(stop.Token);
    var processing = pump.RunAsync(stop.Token);
    try
    {
        while (true)
        {
            await using var count = data.CreateCommand($"SELECT count(*) FROM {schema}.effects WHERE message_id=@id AND stage='consumer'");
            count.Parameters.AddWithValue("id", message.MessageId);
            var effects = (long)(await count.ExecuteScalarAsync(stop.Token))!;
            if (effects == 1) break; // Business SQL and Completed are visible from the same committed transaction.
            if (effects > 1) throw new InvalidOperationException("Unexpected repeated consumer effect.");
            if (accepting.IsCompleted) { await accepting; throw new InvalidOperationException("Ingress stopped before completion."); }
            if (processing.IsCompleted) { await processing; throw new InvalidOperationException("Worker stopped before completion."); }
            await Task.Delay(25, stop.Token);
        }
    }
    finally
    {
        stop.Cancel();
        await Task.WhenAll(accepting, processing);
    }
    Console.WriteLine("Consumer: Completed; one business effect committed through PostgreSQL.");
}
finally { await admin.QueueDeleteAsync(queue); }
