using Npgsql;
using EventGuard.Store.Conformance;
namespace EventGuard.PostgreSql.Tests;

public sealed class OutboxRepairProcessTests(DatabaseFixture db, Xunit.Abstractions.ITestOutputHelper output) : IClassFixture<DatabaseFixture>
{
    [Fact]
    public async Task Kill_after_envelope_update_before_receipt_commit_preserves_quarantine()
    {
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(25)); var ct = stop.Token;
        var scope = DatabaseFixture.Scope();
        await db.Store.ConfigureBudgetAsync(new(Guid.NewGuid(), new(scope.Tenant, 86400, maximumRecoveries: 1), -1, "operator", "test budget"), ct);
        var message = new OutgoingMessage(new(scope.Tenant, scope.Consumer), Guid.NewGuid(), Guid.NewGuid(), new("test", "local", "v1", "files.created"), [1, 2]);
        await db.EnqueueAsync(message);
        await using (var corruptConnection = await db.DataSource.OpenConnectionAsync(ct))
        await using (var corrupt = new NpgsqlCommand($"UPDATE {db.Schema}.outbox_messages SET headers='[]' WHERE scope_id=@scope AND id=@id", corruptConnection))
        {
            corrupt.Parameters.AddWithValue("scope", message.Scope.Id); corrupt.Parameters.AddWithValue("id", message.IntentId);
            await corrupt.ExecuteNonQueryAsync(ct);
        }
        Assert.Null(await db.Outbox.ClaimNextAsync(message.Scope, Guid.NewGuid(), TimeSpan.FromSeconds(10), ct));
        var record = (await db.Recovery.FindMetadataAsync(message.Scope, message.IntentId, ct))!;
        var request = new OutboxRepairRequest(Guid.NewGuid(), record.QuarantineId!.Value, message, record.Version, "operator", "fixed");
        var app = "eg-repair-kill-" + Guid.NewGuid().ToString("N");
        var settings = new NpgsqlConnectionStringBuilder(db.ConnectionString) { ApplicationName = app };
        await using var connection = await db.DataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await using var barrier = new NpgsqlCommand($"LOCK TABLE {db.Schema}.outbox_repair_requests IN SHARE MODE", connection, transaction);
        await barrier.ExecuteNonQueryAsync(ct);
        await using var child = NativeWorkerProcess.Start("EventGuard.CrashWorker", new Dictionary<string, string>
        {
            ["EVENTGUARD_TEST_CONNECTION"] = settings.ConnectionString, ["EVENTGUARD_TEST_SCHEMA"] = db.Schema,
            ["EVENTGUARD_TEST_CONSUMER"] = scope.Consumer, ["EVENTGUARD_TEST_MODE"] = "outbox-repair",
            ["EVENTGUARD_TEST_QUARANTINE"] = record.QuarantineId!.Value.ToString("D"), ["EVENTGUARD_TEST_MESSAGE"] = message.MessageId.ToString("D"),
            ["EVENTGUARD_TEST_REQUEST"] = request.RequestId.ToString("D"),
            ["EVENTGUARD_TEST_INTENT"] = message.IntentId.ToString("D"), ["EVENTGUARD_TEST_VERSION"] = record.Version.ToString(System.Globalization.CultureInfo.InvariantCulture)
        });
        await child.ReadAsync("repair-start", ct);
        await using var observer = await db.DataSource.OpenConnectionAsync(ct);
        await using var wait = new NpgsqlCommand("SELECT count(*) FROM pg_stat_activity WHERE application_name=@app AND wait_event_type='Lock' AND query LIKE '%INSERT INTO%outbox_repair_requests%'", observer);
        wait.Parameters.AddWithValue("app", app);
        while ((long)(await wait.ExecuteScalarAsync(ct))! == 0) { Assert.False(child.HasExited); await Task.Delay(20, ct); }
        // The native server is waiting at the final receipt INSERT; envelope and state updates are uncommitted.
        await child.KillAsync(ct);
        await transaction.RollbackAsync(ct);
        Assert.Equal(OutboxState.Quarantined, (await db.Recovery.FindMetadataAsync(message.Scope, message.IntentId, ct))!.State);
        Assert.Null(await db.Outbox.ClaimNextAsync(message.Scope, Guid.NewGuid(), TimeSpan.FromSeconds(10), ct));
        Assert.Equal(0, (await db.Store.FindBudgetAsync(scope.Tenant, ct))!.Recoveries);
        var retried = await ((IOutboxRepairStore)db.Recovery).RepairAsync(request, ct);
        Assert.Equal(1, (await db.Store.FindBudgetAsync(scope.Tenant, ct))!.Recoveries);
        Assert.Equal(OutboxRepairStatus.Accepted, retried.Status); Assert.False(retried.RepeatedRequest);
        var claim = (await db.Outbox.ClaimNextAsync(message.Scope, Guid.NewGuid(), TimeSpan.FromSeconds(10), ct))!;
        Assert.Equal(message.Fingerprint, claim.Record.Message.Fingerprint);
        await db.Outbox.SettleAsync(claim, new(PublishStatus.Accepted, "confirmed"), new(), ct);
        Assert.Null(await db.Outbox.ClaimNextAsync(message.Scope, Guid.NewGuid(), TimeSpan.FromSeconds(10), ct));
        output.WriteLine($"Killed native repair process {child.Id} at receipt INSERT in {db.Schema}; rollback and single recovery verified.");
    }
}
