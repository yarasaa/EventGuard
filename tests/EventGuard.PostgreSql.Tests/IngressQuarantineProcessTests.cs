using Npgsql;
using EventGuard.Store.Conformance;
namespace EventGuard.PostgreSql.Tests;

public sealed class IngressQuarantineProcessTests(DatabaseFixture db, Xunit.Abstractions.ITestOutputHelper output) : IClassFixture<DatabaseFixture>
{
    [Fact]
    public async Task Kill_after_replacement_insert_before_receipt_commit_rolls_back_the_entire_release()
    {
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(25)); var ct = stop.Token;
        var scope = DatabaseFixture.Scope();
        await db.Store.ConfigureBudgetAsync(new(Guid.NewGuid(), new(scope.Tenant, 86400, maximumRecoveries: 1), -1, "operator", "test budget"), ct);
        var record = await db.Store.QuarantineAsync(scope, new("v1", "sha256-bytes/v1"), new("broker", "position", "codec/v1", [1], []), "invalid-contract", ct);
        var submission = new WorkSubmission(new(scope, new("v1", "sha256-bytes/v1"), "file-1"), Guid.NewGuid(), [8], ContentFingerprint.FromBytes([8]));
        var request = new IngressReleaseRequest(Guid.NewGuid(), record.Item.Id, 0, submission, "operator", "fixed");
        var app = "eg-ingress-kill-" + Guid.NewGuid().ToString("N");
        var settings = new NpgsqlConnectionStringBuilder(db.ConnectionString) { ApplicationName = app };
        await using var connection = await db.DataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await using var barrier = new NpgsqlCommand($"LOCK TABLE {db.Schema}.ingress_release_requests IN SHARE MODE", connection, transaction);
        await barrier.ExecuteNonQueryAsync(ct);
        await using var child = NativeWorkerProcess.Start("EventGuard.CrashWorker", new Dictionary<string, string>
        {
            ["EVENTGUARD_TEST_CONNECTION"] = settings.ConnectionString, ["EVENTGUARD_TEST_SCHEMA"] = db.Schema,
            ["EVENTGUARD_TEST_CONSUMER"] = scope.Consumer, ["EVENTGUARD_TEST_MODE"] = "ingress-release",
            ["EVENTGUARD_TEST_QUARANTINE"] = record.Item.Id.ToString("D"), ["EVENTGUARD_TEST_MESSAGE"] = submission.MessageId.ToString("D"),
            ["EVENTGUARD_TEST_REQUEST"] = request.RequestId.ToString("D")
        });
        await child.ReadAsync("release-start", ct);
        await using var observer = await db.DataSource.OpenConnectionAsync(ct);
        await using var wait = new NpgsqlCommand("SELECT count(*) FROM pg_stat_activity WHERE application_name=@app AND wait_event_type='Lock' AND query LIKE '%INSERT INTO%ingress_release_requests%'", observer);
        wait.Parameters.AddWithValue("app", app);
        while ((long)(await wait.ExecuteScalarAsync(ct))! == 0) { Assert.False(child.HasExited); await Task.Delay(20, ct); }
        // The native server is waiting at the final receipt INSERT; acceptance and row update are uncommitted.
        await child.KillAsync(ct);
        await transaction.RollbackAsync(ct);
        Assert.Null((await db.Store.FindQuarantineAsync(scope, record.Item.Id, ct))!.ReleasedWorkId);
        Assert.Null(await db.Claim(scope));
        Assert.Equal(0, (await db.Store.FindBudgetAsync(scope.Tenant, ct))!.Recoveries);
        var retried = await db.Store.ReleaseAsync(request, ct);
        Assert.Equal(IngressReleaseStatus.Accepted, retried.Status); Assert.False(retried.RepeatedRequest);
        Assert.Equal(1, (await db.Store.FindBudgetAsync(scope.Tenant, ct))!.Recoveries);
        var claim = (await db.Claim(scope))!;
        await db.Store.ExecuteAsync(claim, db.Effect, new(), ct);
        Assert.Equal(1, await db.EffectCount(claim.Work.Id)); Assert.Null(await db.Claim(scope));
        output.WriteLine($"Killed native release process {child.Id} at receipt INSERT in {db.Schema}; rollback and single recovery verified.");
    }
}
