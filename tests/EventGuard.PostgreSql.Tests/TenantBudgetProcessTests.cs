using Npgsql;
using EventGuard.Store.Conformance;
namespace EventGuard.PostgreSql.Tests;
public sealed class TenantBudgetProcessTests(DatabaseFixture db, Xunit.Abstractions.ITestOutputHelper output) : IClassFixture<DatabaseFixture>
{
    [Fact]
    public async Task Kill_after_counter_and_work_update_before_attempt_insert_rolls_back_admission()
    {
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(25)); var ct = stop.Token;
        var scope = DatabaseFixture.Scope();
        await db.Store.ConfigureBudgetAsync(new(Guid.NewGuid(), new(scope.Tenant, 86400, maximumClaims: 1), -1, "operator", "test budget"), ct);
        var accepted = await db.Store.AcceptAsync(DatabaseFixture.Submission(scope), ct); var request = Guid.NewGuid();
        var app = "eg-budget-kill-" + Guid.NewGuid().ToString("N");
        var settings = new NpgsqlConnectionStringBuilder(db.ConnectionString) { ApplicationName = app };
        await using var connection = await db.DataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await using var barrier = new NpgsqlCommand($"LOCK TABLE {db.Schema}.attempts IN SHARE MODE", connection, transaction); await barrier.ExecuteNonQueryAsync(ct);
        await using var child = NativeWorkerProcess.Start("EventGuard.CrashWorker", new Dictionary<string, string>
        {
            ["EVENTGUARD_TEST_CONNECTION"] = settings.ConnectionString, ["EVENTGUARD_TEST_SCHEMA"] = db.Schema,
            ["EVENTGUARD_TEST_CONSUMER"] = scope.Consumer, ["EVENTGUARD_TEST_MODE"] = "budget-claim", ["EVENTGUARD_TEST_REQUEST"] = request.ToString("D")
        });
        await child.ReadAsync("budget-claim-start", ct);
        await using var observer = await db.DataSource.OpenConnectionAsync(ct);
        await using var wait = new NpgsqlCommand("SELECT count(*) FROM pg_stat_activity WHERE application_name=@app AND wait_event_type='Lock' AND query LIKE '%INSERT INTO%attempts%'", observer); wait.Parameters.AddWithValue("app", app);
        while ((long)(await wait.ExecuteScalarAsync(ct))! == 0) { Assert.False(child.HasExited); await Task.Delay(20, ct); }
        await child.KillAsync(ct); await transaction.RollbackAsync(ct);
        Assert.Equal(0, (await db.Store.FindBudgetAsync(scope.Tenant, ct))!.Claims);
        Assert.Equal(WorkState.Pending, (await db.Store.FindAsync(scope, accepted.Work.Id, ct))!.State);
        var claim = (await db.Store.ClaimNextAsync(scope, request, TimeSpan.FromMinutes(1), ct))!;
        Assert.NotNull(claim); Assert.Equal(1, (await db.Store.FindBudgetAsync(scope.Tenant, ct))!.Claims);
        await db.Store.ExecuteAsync(claim, db.Effect, new(), ct);
        Assert.Equal(1, await db.EffectCount(accepted.Work.Id));
        Assert.Null(await db.Store.ClaimNextAsync(scope, request, TimeSpan.FromMinutes(1), ct));
        output.WriteLine($"Killed native claim process {child.Id} before attempt receipt; work and shared counter rolled back together.");
    }
}
