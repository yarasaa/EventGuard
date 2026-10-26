using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using EventGuard;
using EventGuard.Kafka;
using EventGuard.Kafka.Sample.Contracts;
using EventGuard.PostgreSql;
using EventGuard.Runtime;
using Npgsql;

// Local composition root. Production configuration/credentials and topology belong to the application.
const string broker = "127.0.0.1:19092";
const string schema = "eventguard_kafka_sample";
await using var data = NpgsqlDataSource.Create("Host=127.0.0.1;Port=55432;Database=eventguard_test;Username=eventguard;Password=eventguard-local-only;Maximum Pool Size=8");
var store = new PostgreSqlWorkStore(data, schema);
await store.InitializeAsync();
var outbox = new PostgreSqlOutboxStore(data, schema);
await using (var setup = data.CreateCommand($"""
    CREATE TABLE IF NOT EXISTS {schema}.files(file_id text PRIMARY KEY, revision bigint NOT NULL, path text NOT NULL);
    CREATE TABLE IF NOT EXISTS {schema}.requested_events(event_id uuid NOT NULL, revision bigint NOT NULL);
    """)) await setup.ExecuteNonQueryAsync();
using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = broker }).Build();
var runId = Guid.NewGuid().ToString("N");
var topic = "eg_kafka_sample_" + runId;
var group = "eg_kafka_sample_" + runId;
await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }]);
try
{
    // Demo isolation only. Real consumer/source/policy identities remain stable across restarts.
    var scope = new WorkScope("demo", "file-indexer-" + runId, "file-service", "file.updated");
    var policy = new IdentityPolicy("event-body-id/v1", "body/v1");
    var producerScope = new ProducerScope("demo", "file-service-" + runId);
    var file = "file-" + runId;
    var events = new[]
    {
        new FileUpdated(Guid.NewGuid(), 1, file, 18, "current.txt"),
        new FileUpdated(Guid.NewGuid(), 1, file, 17, "old.txt")
    };
    await using var publisher = new KafkaPublisher(new ProducerConfig { BootstrapServers = broker }, "local", "v1");
    foreach (var model in events)
    {
        var payload = model.Revision == 18 ? JsonSerializer.SerializeToUtf8Bytes(model)
            : JsonSerializer.SerializeToUtf8Bytes(new FileUpdatedV2(model.EventId, 2, model.FileId, model.Revision, model.Path));
        var message = new OutgoingMessage(producerScope, model.EventId, model.EventId,
            new("kafka", "local", "v1", topic, model.FileId), payload);
        var request = new ProducerRequest(producerScope, model.EventId, ContentFingerprint.FromBytes(payload));
        var result = await outbox.ProduceAsync(request, async (session, ct) =>
        {
            await using var command = session.CreateCommand($"INSERT INTO {schema}.requested_events VALUES(@id,@revision)");
            command.Parameters.AddWithValue("id", model.EventId);
            command.Parameters.AddWithValue("revision", model.Revision);
            await command.ExecuteNonQueryAsync(ct);
            await session.Outbox.EnqueueAsync(message, ct);
        });
        var repeated = await outbox.ProduceAsync(request, (_, _) => throw new InvalidOperationException("Producer request repeated its business effect."));
        Console.WriteLine($"Revision {model.Revision}: producer {result.Status}; repeat {repeated.Status}");
        var publication = await new OutboxDispatcher(outbox, producerScope, [publisher]).DispatchNextAsync();
        if (publication?.Record.State != OutboxState.Published) throw new InvalidOperationException("Kafka acceptance was not confirmed.");
    }

    // JSON is one application choice. A different mapper can use Avro, Protobuf, existing IDs or native headers.
    WorkSubmission Map(ConsumeResult<byte[], byte[]> record)
    {
        if (record.Message.Value is not { } body) throw new InvalidOperationException("This contract has no tombstones.");
        return FileUpdatedJson.Contract.Map(scope, policy, body, model => (model.EventId.ToString("D"), model.EventId));
    }
    var ingress = new KafkaIngress(new ConsumerConfig
    {
        BootstrapServers = broker,
        GroupId = group,
        SessionTimeoutMs = 10000,
        HeartbeatIntervalMs = 3000,
        AutoOffsetReset = AutoOffsetReset.Earliest
    }, store, scope, policy, [topic], Map);
    var pump = new WorkPump<PostgreSqlSession>(store, scope, async (_, payload, session, ct) =>
    {
        var model = FileUpdatedJson.Decode(payload.Span);
        // This example is a last-state projection. Revision is application logic, not a universal event requirement.
        // The DB evaluates the revision condition atomically, even if workers finish out of order.
        await using var command = session.CreateCommand($"""
            INSERT INTO {schema}.files(file_id,revision,path) VALUES(@file,@revision,@path)
            ON CONFLICT(file_id) DO UPDATE SET revision=EXCLUDED.revision,path=EXCLUDED.path
            WHERE {schema}.files.revision < EXCLUDED.revision
            """);
        command.Parameters.AddWithValue("file", model.FileId);
        command.Parameters.AddWithValue("revision", model.Revision);
        command.Parameters.AddWithValue("path", model.Path);
        await command.ExecuteNonQueryAsync(ct);
    }, options: new WorkPumpOptions(maximumConcurrency: 2));
    using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(30));
    var accepting = ingress.RunAsync(stop.Token);
    var processing = pump.RunAsync(stop.Token);
    try
    {
        using var offsets = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig { BootstrapServers = broker, GroupId = group, EnableAutoCommit = false }).Build();
        while (true)
        {
            await using var command = data.CreateCommand($"SELECT count(*) FROM {schema}.works WHERE scope_id=@scope AND state=3");
            command.Parameters.AddWithValue("scope", scope.Id);
            var completed = (long)(await command.ExecuteScalarAsync(stop.Token))!;
            if (completed == events.Length && offsets.Committed([new(topic, 0)], TimeSpan.FromSeconds(5)).Single().Offset.Value == events.Length) break;
            if (accepting.IsCompleted) { await accepting; throw new InvalidOperationException("Ingress stopped before completion."); }
            if (processing.IsCompleted) { await processing; throw new InvalidOperationException("Worker stopped before completion."); }
            await Task.Delay(25, stop.Token);
        }
    }
    finally { stop.Cancel(); await Task.WhenAll(accepting, processing); }
    await using var final = data.CreateCommand($"SELECT revision,path FROM {schema}.files WHERE file_id=@file");
    final.Parameters.AddWithValue("file", file);
    await using var projection = await final.ExecuteReaderAsync();
    if (!await projection.ReadAsync() || projection.GetInt64(0) != 18 || projection.GetString(1) != "current.txt")
        throw new InvalidOperationException("An older event regressed the file projection.");
    Console.WriteLine("Kafka: both records committed; consumer: Completed; file revision remains 18 after revision 17.");
}
finally
{
    // The adapter disposes without draining shutdown rebalance callbacks. Coordinator
    // removal can take a session timeout; only this run's temporary group is deleted.
    var cleanupDeadline = DateTime.UtcNow.AddSeconds(15);
    while (true)
    {
        try { await admin.DeleteGroupsAsync([group]); break; }
        catch (DeleteGroupsException error) when (error.Results.All(r => r.Error.Code == ErrorCode.GroupIdNotFound)) { break; }
        catch (DeleteGroupsException error) when (DateTime.UtcNow < cleanupDeadline && error.Results.All(r => r.Error.Code == ErrorCode.NonEmptyGroup))
        { await Task.Delay(200); }
    }
    await admin.DeleteTopicsAsync([topic]);
}
