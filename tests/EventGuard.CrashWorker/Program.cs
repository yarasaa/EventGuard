using System.Text.Json;
using EventGuard;
using EventGuard.PostgreSql;
using Npgsql;

var connection = Environment.GetEnvironmentVariable("EVENTGUARD_TEST_CONNECTION") ?? throw new InvalidOperationException("Local test connection required.");
var schema = Environment.GetEnvironmentVariable("EVENTGUARD_TEST_SCHEMA") ?? throw new InvalidOperationException("Test schema required.");
var consumer = Environment.GetEnvironmentVariable("EVENTGUARD_TEST_CONSUMER") ?? throw new InvalidOperationException("Test scope required.");
var settings = new NpgsqlConnectionStringBuilder(connection);
if (settings.Host is not ("127.0.0.1" or "localhost") || !settings.Database!.StartsWith("eventguard_test", StringComparison.Ordinal))
    throw new InvalidOperationException("Crash worker accepts only a local test database.");
await using var dataSource = NpgsqlDataSource.Create(connection);
var store = new PostgreSqlWorkStore(dataSource, schema);
var scope = new WorkScope("tenant", consumer, "sample", "file.created");
if (Environment.GetEnvironmentVariable("EVENTGUARD_TEST_MODE") == "bulk-replay")
    return await EventGuard.ProcessSupport.RecoveryWorker.BulkAsync(new PostgreSqlBulkReplayStore(dataSource, schema), scope);
if (Environment.GetEnvironmentVariable("EVENTGUARD_TEST_MODE") == "budget-claim")
    return await EventGuard.ProcessSupport.RecoveryWorker.ClaimAsync(store, scope);
if (Environment.GetEnvironmentVariable("EVENTGUARD_TEST_MODE") == "retention")
    return await EventGuard.ProcessSupport.RecoveryWorker.RetainAsync(store, scope);
if (Environment.GetEnvironmentVariable("EVENTGUARD_TEST_MODE") == "outbox-repair")
    return await EventGuard.ProcessSupport.RecoveryWorker.RepairAsync(new PostgreSqlOutboxRecoveryStore(dataSource, schema), scope);
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
var claim = await store.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromSeconds(1))
    ?? throw new InvalidOperationException("No work available.");
await store.ExecuteAsync(claim, async (work, _, session, ct) =>
{
    await using var command = session.CreateCommand($"INSERT INTO {schema}.business_effects VALUES (@id,'child-before-commit')");
    command.Parameters.AddWithValue("id", work.Id);
    await command.ExecuteNonQueryAsync(ct);
    await session.Outbox.EnqueueAsync(new(new(scope.Tenant, scope.Consumer), work.Id, work.Id,
        new("test", "local", "v1", "files.processed"), [1]), ct);
    Console.WriteLine(JsonSerializer.Serialize(new { stage = "transaction-open", workId = work.Id, leaseUntil = claim.LeaseUntil }));
    await Console.Out.FlushAsync(ct);
    await Task.Delay(Timeout.Infinite, ct);
}, new());

return 0;
