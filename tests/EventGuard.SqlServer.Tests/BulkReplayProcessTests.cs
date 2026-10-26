using Microsoft.Data.SqlClient;
using EventGuard.Store.Conformance;
namespace EventGuard.SqlServer.Tests;
public sealed class BulkReplayProcessTests(DatabaseFixture db, Xunit.Abstractions.ITestOutputHelper output) : IClassFixture<DatabaseFixture>
{
    [Fact]
    public async Task Kill_mid_batch_preserves_committed_members_and_resume_finishes_once()
    {
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(25)); var ct = stop.Token;
        var scope = DatabaseFixture.Scope(); var items = new List<BulkReplayItem>();
        for (var i = 0; i < 3; i++)
        {
            await db.Store.AcceptAsync(StoreContractData.Submission(scope, "bulk-" + i), ct);
            var claim = (await db.Store.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromMinutes(1), ct))!;
            var failed = await db.Store.ExecuteAsync(claim, (_, _, _, _) => throw new PermanentWorkException("test"), new(), ct);
            items.Add(new(failed.Work.Id, failed.Work.Version));
        }
        var request = new BulkReplayRequest(scope, Guid.NewGuid(), items, "operator", "reviewed selection");
        var bulk = new SqlServerBulkReplayStore(db.DataSource, db.Schema);
        await bulk.PrepareBulkAsync(request, ct);
        await db.Store.ReplayAsync(request.ConsumerRequest(0), ct);
        var app = "eg-bulk-kill-" + Guid.NewGuid().ToString("N");
        var settings = new SqlConnectionStringBuilder(db.ConnectionString) { ApplicationName = app };
        await using var connection = await db.DataSource.OpenConnectionAsync(ct);
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, ct);
        await using var barrier = new SqlCommand($"SELECT COUNT_BIG(*) FROM {db.Schema}.replay_requests WITH(TABLOCK,HOLDLOCK)", connection, transaction); await barrier.ExecuteNonQueryAsync(ct);
        await using var child = NativeWorkerProcess.Start("EventGuard.SqlServer.CrashWorker", new Dictionary<string, string>
        {
            ["EVENTGUARD_SQLSERVER_TEST_CONNECTION"] = settings.ConnectionString, ["EVENTGUARD_TEST_SCHEMA"] = db.Schema,
            ["EVENTGUARD_TEST_ITEMS"] = System.Text.Json.JsonSerializer.Serialize(items),
            ["EVENTGUARD_TEST_CONSUMER"] = scope.Consumer, ["EVENTGUARD_TEST_MODE"] = "bulk-replay", ["EVENTGUARD_TEST_REQUEST"] = request.RequestId.ToString("D")
        });
        await child.ReadAsync("bulk-start", ct);
        await using var observer = await db.DataSource.OpenConnectionAsync(ct);
        await using var wait = new SqlCommand("SELECT COUNT_BIG(*) FROM sys.dm_exec_requests r JOIN sys.dm_exec_sessions s ON s.session_id=r.session_id CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t WHERE s.program_name=@app AND r.blocking_session_id<>0 AND t.text LIKE '%INSERT INTO%replay_requests%'", observer); wait.Parameters.AddWithValue("app", app);
        while ((long)(await wait.ExecuteScalarAsync(ct))! == 0) { Assert.False(child.HasExited); await Task.Delay(20, ct); }
        await child.KillAsync(ct); await transaction.RollbackAsync(ct);
        var partial = (await bulk.FindBulkAsync(scope, request.RequestId, request.Actor, ct))!;
        Assert.Equal("Accepted", partial.Entries[0].Status); Assert.All(partial.Entries.Skip(1), e => Assert.Null(e.Status));
        Assert.Equal(WorkState.DeadLetter, (await db.Store.FindAsync(scope, items[1].Id, ct))!.State);
        await bulk.RunBulkAsync(request, ct);
        var completed = (await bulk.FindBulkAsync(scope, request.RequestId, request.Actor, ct))!;
        Assert.True(completed.Complete); Assert.All(completed.Entries, e => { Assert.Equal("Accepted", e.Status); Assert.Equal(1, e.Generation); });
        for (var i = 0; i < 3; i++)
            await db.Store.ExecuteAsync((await db.Store.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromMinutes(1), ct))!, db.Effect, new(), ct);
        await bulk.RunBulkAsync(request, ct);
        foreach (var item in items) Assert.Equal(1, await db.EffectCount(item.Id));
        output.WriteLine($"Killed native bulk process {child.Id} during member receipt insertion; committed member kept, uncommitted member rolled back, resume produced one effect each.");
    }
}
