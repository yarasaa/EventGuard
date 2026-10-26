using EventGuard;
using EventGuard.PostgreSql;
using EventGuard.Runtime;
using Npgsql;

var connectionString = Environment.GetEnvironmentVariable("EVENTGUARD_SAMPLE_CONNECTION")
    ?? "Host=127.0.0.1;Port=55432;Database=eventguard_test;Username=eventguard;Password=eventguard-local-only";
var settings = new NpgsqlConnectionStringBuilder(connectionString);
if (settings.Host is not ("127.0.0.1" or "localhost") || settings.Database?.StartsWith("eventguard_test", StringComparison.Ordinal) != true)
    throw new InvalidOperationException("The demo only writes to a local eventguard_test database.");
await using var dataSource = NpgsqlDataSource.Create(connectionString);
var store = new PostgreSqlWorkStore(dataSource, "eventguard_sample");
await store.InitializeAsync();
await using (var command = dataSource.CreateCommand("CREATE TABLE IF NOT EXISTS eventguard_sample.files_processed(work_id uuid NOT NULL, processed_at timestamptz NOT NULL DEFAULT clock_timestamp())"))
    await command.ExecuteNonQueryAsync();

// Isolate each demo run. Real applications keep the consumer scope stable across restarts.
var runId = Guid.NewGuid().ToString("N");
var scope = new WorkScope("demo", "file-indexer-" + runId, "local", "file.created");
var key = "sample-" + runId;
byte[] payload = "{\"path\":\"sample.txt\"}"u8.ToArray();
var submission = new WorkSubmission(new(scope, new("file-key/v1", "sha256-bytes/v1"), key),
    Guid.NewGuid(), payload, ContentFingerprint.FromBytes(payload));
var accepted = await store.AcceptAsync(submission);
Console.WriteLine($"Accept: {accepted.Status}; repeated accept: {(await store.AcceptAsync(submission)).Status}");

var claim = await store.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromSeconds(30)) ?? throw new InvalidOperationException("No work.");
var failure = await store.ExecuteAsync(claim, (_, _, _, _) => throw new PermanentWorkException("demo-configuration-error"), new());
Console.WriteLine($"Failure: {failure.Status}; explanation: {WorkDiagnostics.Explain(failure.Work, DateTimeOffset.UtcNow).Code}");
var replayRequest = new ReplayRequest(scope, Guid.NewGuid(), failure.Work.Id, failure.Work.Version, "local-demo", "configuration corrected");
var replay = await store.ReplayAsync(replayRequest);
var repeated = await store.ReplayAsync(replayRequest);
Console.WriteLine($"Replay: {replay.Status}; generation: {replay.Generation}; repeated request: {repeated.RepeatedRequest}");
var second = await store.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromSeconds(30)) ?? throw new InvalidOperationException("No replay work.");
var producerScope = new ProducerScope(scope.Tenant, scope.Consumer);
// Capture stable IDs outside the retried business handler. Each destination has its own intent ID.
var outgoing = new OutgoingMessage(producerScope, Guid.NewGuid(), Guid.NewGuid(),
    new("demo", "local", "v1", "files.processed"), payload);
var completed = await store.ExecuteAsync(second, async (work, _, session, ct) =>
{
    await using var command = session.CreateCommand("INSERT INTO eventguard_sample.files_processed(work_id) VALUES(@id)");
    command.Parameters.AddWithValue("id", work.Id);
    await command.ExecuteNonQueryAsync(ct);
    await session.Outbox.EnqueueAsync(outgoing, ct);
}, new());
Console.WriteLine($"Result: {completed.Status}; business write, completion and outgoing intent committed together.");
var outbox = new PostgreSqlOutboxStore(dataSource, "eventguard_sample");
Console.WriteLine($"Outbox before dispatch: {(await outbox.FindAsync(producerScope, outgoing.IntentId))!.State}");

// A producer operation can also commit its business SQL, receipt and outgoing intent atomically.
var request = new ProducerRequest(producerScope, Guid.NewGuid(), ContentFingerprint.FromBytes(payload));
var produced = new OutgoingMessage(producerScope, Guid.NewGuid(), Guid.NewGuid(), outgoing.Route, payload);
var producerResult = await outbox.ProduceAsync(request, async (session, ct) =>
{
    await using var command = session.CreateCommand("INSERT INTO eventguard_sample.files_processed(work_id) VALUES(@id)");
    command.Parameters.AddWithValue("id", request.RequestId);
    await command.ExecuteNonQueryAsync(ct);
    await session.Outbox.EnqueueAsync(produced, ct);
});
var repeatedProducer = await outbox.ProduceAsync(request, (_, _) => throw new InvalidOperationException("Must not rerun committed request."));
Console.WriteLine($"Producer: {producerResult.Status}; repeated request: {repeatedProducer.Status}");
var rejected = (await new OutboxDispatcher(outbox, producerScope, [new DemoTransport(reject: true)]).DispatchNextAsync())!.Record;
var recovery = new PostgreSqlOutboxRecoveryStore(dataSource, "eventguard_sample");
var publicationReplay = new OutboxReplayRequest(producerScope, Guid.NewGuid(), rejected.Message.IntentId,
    rejected.Version, "local-demo", "demo transport configuration corrected");
var reopened = await recovery.ReplayAsync(publicationReplay);
var repeatedPublication = await recovery.ReplayAsync(publicationReplay);
Console.WriteLine($"Outbox failure: {rejected.State}; replay: {reopened.Status}; repeated request: {repeatedPublication.RepeatedRequest}");
var dispatcher = new OutboxDispatcher(outbox, producerScope, [new DemoTransport()]);
while (await dispatcher.DispatchNextAsync() is { } publication)
    Console.WriteLine($"Demo transport result: {publication.Record.State}; uncertain: {publication.Record.DeliveryUncertain}");

// Local demonstration only. No real Kafka/RabbitMQ broker is contacted or acknowledged here.
sealed class DemoTransport(bool reject = false) : IOutboundTransport
{
    public string Transport => "demo";
    public string Connection => "local";
    public string ConfigurationVersion => "v1";
    public Task<PublishOutcome> PublishAsync(OutgoingMessage message, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (reject) return Task.FromResult(new PublishOutcome(PublishStatus.NotSentPermanent, "demo-configuration-error"));
        Console.WriteLine($"Demo accepts message {message.MessageId} for {message.Route.Address}");
        return Task.FromResult(new PublishOutcome(PublishStatus.Accepted, "demo-accepted"));
    }
}
