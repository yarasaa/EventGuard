using System.Text.Json;
using System.Text.RegularExpressions;
using EventGuard;
using EventGuard.PostgreSql;
using EventGuard.Runtime;
using Npgsql;

var connection = Environment.GetEnvironmentVariable("EVENTGUARD_TEST_CONNECTION") ?? "";
var schema = Environment.GetEnvironmentVariable("EVENTGUARD_TEST_SCHEMA") ?? "";
var settings = new NpgsqlConnectionStringBuilder(connection);
if (settings.Host != "eventguard-test-database.eventguard-qualification.svc"
    || settings.Database != "eventguard_test" || settings.Port != 5432
    || !Regex.IsMatch(schema, "^eg_cluster_[a-f0-9]{32}$"))
    throw new InvalidOperationException("Only the dedicated qualification fixture is allowed.");
await using var data = NpgsqlDataSource.Create(connection);
var store = new PostgreSqlWorkStore(data, schema);
var scope = new WorkScope("qualification", "node-recovery", "synthetic", "document.indexed");
using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(8));
var ct = deadline.Token;
var role = args.Single();
var owner = Environment.GetEnvironmentVariable("HOSTNAME") ?? "unknown";
// Newly created Service/EndpointSlice rules may not be installed before a pod starts.
// Bound fixture startup separately; do not mistake that race for transaction recovery.
using (var startup = CancellationTokenSource.CreateLinkedTokenSource(ct))
{
    startup.CancelAfter(TimeSpan.FromSeconds(60));
    while (true)
    {
        try
        {
            await using var ready = await data.OpenConnectionAsync(startup.Token);
            break;
        }
        catch (NpgsqlException) when (!startup.IsCancellationRequested)
        { await Task.Delay(TimeSpan.FromSeconds(1), startup.Token); }
    }
}

async ValueTask Effect(WorkItem work, ReadOnlyMemory<byte> payload, PostgreSqlSession session, CancellationToken token)
{
    await using var effect = session.CreateCommand($"INSERT INTO {schema}.effects(work_id,fingerprint,worker) VALUES(@id,@fingerprint,@worker)");
    effect.Parameters.AddWithValue("id", work.Id);
    effect.Parameters.AddWithValue("fingerprint", ContentFingerprint.FromBytes(payload.Span).Value);
    effect.Parameters.AddWithValue("worker", owner);
    await effect.ExecuteNonQueryAsync(token);
    // Random IDs and no effect uniqueness constraint expose any repeated committed handler.
    await session.Outbox.EnqueueAsync(new(new(scope.Tenant, scope.Consumer), Guid.NewGuid(), work.Id,
        new("qualification", "local", "v1", "document.indexed"), payload.Span), token);
}

if (role == "initialize")
{
    await store.InitializeAsync(ct);
    await using var tables = data.CreateCommand($"CREATE TABLE {schema}.effects(work_id uuid NOT NULL,fingerprint text NOT NULL,worker text NOT NULL); CREATE TABLE {schema}.signals(stage text NOT NULL,work_id uuid NOT NULL,worker text NOT NULL)");
    await tables.ExecuteNonQueryAsync(ct);
    var payload = "{\"documentId\":\"qualification-document\",\"revision\":18}"u8.ToArray();
    var work = await store.AcceptAsync(new(new(scope, new("message/v1", "sha256/v1"), "node-failure"),
        Guid.NewGuid(), payload, ContentFingerprint.FromBytes(payload)), ct);
    Console.WriteLine(JsonSerializer.Serialize(new { stage = "accepted", workId = work.Work.Id }));
}
else if (role == "hold")
{
    var claim = await store.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromSeconds(3), ct)
        ?? throw new InvalidOperationException("Expected one fixture work item.");
    await store.ExecuteAsync(claim, async (work, payload, session, token) =>
    {
        await Effect(work, payload, session, token);
        // This signal commits on a separate connection after the uncommitted effect exists.
        await using var signal = data.CreateCommand($"INSERT INTO {schema}.signals VALUES('transaction-open',@id,@worker)");
        signal.Parameters.AddWithValue("id", work.Id); signal.Parameters.AddWithValue("worker", owner);
        await signal.ExecuteNonQueryAsync(token);
        Console.WriteLine("transaction-open"); await Console.Out.FlushAsync(token);
        await Task.Delay(Timeout.InfiniteTimeSpan, token);
    }, new(), ct);
    throw new InvalidOperationException("The held transaction must be interrupted by the node crash.");
}
else if (role == "peer")
    await new WorkPump<PostgreSqlSession>(store, scope, Effect,
        options: new(maximumConcurrency: 2, leaseDuration: TimeSpan.FromSeconds(10), idleDelay: TimeSpan.FromMilliseconds(100))).RunAsync(ct);
else throw new ArgumentException("Use initialize, hold or peer.");
