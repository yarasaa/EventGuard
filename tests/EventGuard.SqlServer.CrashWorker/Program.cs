using System.Text.Json;
using EventGuard;
using EventGuard.SqlServer;
using Microsoft.Data.SqlClient;

string Required(string key) => Environment.GetEnvironmentVariable(key) ?? throw new InvalidOperationException(key);
var options = new SqlConnectionStringBuilder(Required("EVENTGUARD_SQLSERVER_TEST_CONNECTION"));
var schema = Required("EVENTGUARD_TEST_SCHEMA");
if (options.DataSource != "127.0.0.1,51433" || options.InitialCatalog != "eventguard_test" || !schema.StartsWith("eg_sql_test_", StringComparison.Ordinal))
    throw new InvalidOperationException("Crash worker only uses local test resources.");
var source = new SqlServerDataSource(options.ConnectionString);
var store = new SqlServerWorkStore(source, schema);
var scope = new WorkScope("tenant", Required("EVENTGUARD_TEST_CONSUMER"), "sample", "file.created");
if (Environment.GetEnvironmentVariable("EVENTGUARD_TEST_MODE") == "bulk-replay")
    return await EventGuard.ProcessSupport.RecoveryWorker.BulkAsync(new SqlServerBulkReplayStore(source, schema), scope);
if (Environment.GetEnvironmentVariable("EVENTGUARD_TEST_MODE") == "budget-claim")
    return await EventGuard.ProcessSupport.RecoveryWorker.ClaimAsync(store, scope);
if (Environment.GetEnvironmentVariable("EVENTGUARD_TEST_MODE") == "retention")
    return await EventGuard.ProcessSupport.RecoveryWorker.RetainAsync(store, scope);
if (Environment.GetEnvironmentVariable("EVENTGUARD_TEST_MODE") == "outbox-repair")
    return await EventGuard.ProcessSupport.RecoveryWorker.RepairAsync(new SqlServerOutboxRecoveryStore(source, schema), scope);
if (Environment.GetEnvironmentVariable("EVENTGUARD_TEST_MODE") == "ingress-release")
    return await EventGuard.ProcessSupport.RecoveryWorker.RunAsync(store, scope);
if (Environment.GetEnvironmentVariable("EVENTGUARD_TEST_MODE") is { } mode)
    return await EventGuard.ProcessSupport.CoordinatedWorker.RunAsync(mode, store, scope, async (work, payload, session, ct) =>
    {
        await using var command = session.CreateCommand($"INSERT INTO {schema}.business_effects VALUES (@id,@note)");
        command.Parameters.AddWithValue("id", work.Id);
        command.Parameters.AddWithValue("note", $"{Environment.ProcessId}|{ContentFingerprint.FromBytes(payload.Span).Value}");
        await command.ExecuteNonQueryAsync(ct);
        // A new intent per invocation exposes repeated handler effects instead of
        // hiding them behind outbox deduplication with a constant intent ID.
        var intent = Guid.NewGuid();
        await session.Outbox.EnqueueAsync(new(new(scope.Tenant, scope.Consumer), intent, work.Id,
            new("test", "local", "v1", "files.processed"), payload.Span), ct);
        return intent;
    });
var claim = await store.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromSeconds(1)) ?? throw new InvalidOperationException("No claim.");
await store.ExecuteAsync(claim, async (work, payload, session, ct) =>
{
    await using var command = session.CreateCommand($"INSERT INTO {schema}.business_effects VALUES(@id,'before-kill')");
    command.Parameters.AddWithValue("id", work.Id);
    await command.ExecuteNonQueryAsync(ct);
    await session.Outbox.EnqueueAsync(new(new(scope.Tenant, scope.Consumer), work.Id, work.Id, new("test", "local", "v1", "files.created"), [1]), ct);
    Console.WriteLine(JsonSerializer.Serialize(new { stage = "transaction-open", workId = work.Id }));
    await Console.Out.FlushAsync(ct);
    await Task.Delay(Timeout.InfiniteTimeSpan, ct);
}, new());

return 0;
