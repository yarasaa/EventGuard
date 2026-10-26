using Microsoft.Data.SqlClient;
using EventGuard.Store.Conformance;
namespace EventGuard.SqlServer.Tests;

public sealed class RetentionProcessTests(DatabaseFixture db, Xunit.Abstractions.ITestOutputHelper output) : IClassFixture<DatabaseFixture>
{
    [Fact]
    public async Task Kill_after_data_removal_before_receipt_commit_restores_the_original_data()
    {
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(25)); var ct = stop.Token;
        var scope = DatabaseFixture.Scope();
        var accepted = await db.Store.AcceptAsync(DatabaseFixture.Submission(scope, payload: [1, 2]), ct);
        var originalClaim = (await db.Claim(scope))!; await db.Store.ExecuteAsync(originalClaim, db.Effect, new(), ct);
        await using (var ageConnection = await db.DataSource.OpenConnectionAsync(ct))
        await using (var age = new SqlCommand($"UPDATE {db.Schema}.works SET created_at=@old,completed_at=@old WHERE id=@id", ageConnection))
        {
            age.Parameters.AddWithValue("id", accepted.Work.Id); age.Parameters.AddWithValue("old", new DateTimeOffset(2020, 1, 1, 0, 0, 0, TimeSpan.Zero));
            await age.ExecuteNonQueryAsync(ct);
        }
        var request = new RetentionRequest(scope, Guid.NewGuid(), RetentionTarget.ConsumerPayload, new(2021, 1, 1, 0, 0, 0, TimeSpan.Zero), "operator", "configured retention");
        var app = "eg-retention-kill-" + Guid.NewGuid().ToString("N");
        var settings = new SqlConnectionStringBuilder(db.ConnectionString) { ApplicationName = app };
        await using var connection = await db.DataSource.OpenConnectionAsync(ct);
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, ct);
        await using var barrier = new SqlCommand($"SELECT COUNT_BIG(*) FROM {db.Schema}.retention_requests WITH(TABLOCK,HOLDLOCK)", connection, transaction);
        await barrier.ExecuteNonQueryAsync(ct);
        await using var child = NativeWorkerProcess.Start("EventGuard.SqlServer.CrashWorker", new Dictionary<string, string>
        {
            ["EVENTGUARD_SQLSERVER_TEST_CONNECTION"] = settings.ConnectionString, ["EVENTGUARD_TEST_SCHEMA"] = db.Schema,
            ["EVENTGUARD_TEST_CONSUMER"] = scope.Consumer, ["EVENTGUARD_TEST_MODE"] = "retention",
            ["EVENTGUARD_TEST_REQUEST"] = request.RequestId.ToString("D")
        });
        await child.ReadAsync("retention-start", ct);
        await using var observer = await db.DataSource.OpenConnectionAsync(ct);
        await using var wait = new SqlCommand("SELECT COUNT_BIG(*) FROM sys.dm_exec_requests r JOIN sys.dm_exec_sessions s ON s.session_id=r.session_id CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t WHERE s.program_name=@app AND r.blocking_session_id<>0 AND t.text LIKE '%INSERT INTO%retention_requests%'", observer);
        wait.Parameters.AddWithValue("app", app);
        while ((long)(await wait.ExecuteScalarAsync(ct))! == 0) { Assert.False(child.HasExited); await Task.Delay(20, ct); }
        // The native server is waiting at the final receipt INSERT; data removal and version updates are uncommitted.
        await child.KillAsync(ct);
        await transaction.RollbackAsync(ct);
        await using var read = new SqlCommand($"SELECT payload,payload_purged_at FROM {db.Schema}.works WHERE id=@id", observer);
        read.Parameters.AddWithValue("id", accepted.Work.Id);
        await using (var reader = await read.ExecuteReaderAsync(ct)) { Assert.True(await reader.ReadAsync(ct)); Assert.Equal(new byte[] { 1, 2 }, (byte[])reader.GetValue(0)); Assert.True(reader.IsDBNull(1)); }
        var retried = await db.Store.RetainAsync(request, ct);
        Assert.Equal(RetentionStatus.Completed, retried.Status); Assert.False(retried.RepeatedRequest);
        Assert.Equal(new[] { accepted.Work.Id }, retried.PurgedIds);
        Assert.Equal(1, await db.EffectCount(accepted.Work.Id));
        Assert.Null(await db.Store.ClaimNextAsync(scope, originalClaim.Token, TimeSpan.FromMinutes(1), ct));
        output.WriteLine($"Killed native retention process {child.Id} at receipt INSERT in {db.Schema}; rollback and single recovery verified.");
    }
}
