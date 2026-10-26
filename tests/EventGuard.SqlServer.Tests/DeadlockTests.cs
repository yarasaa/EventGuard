using System.Data;
using Microsoft.Data.SqlClient;

namespace EventGuard.SqlServer.Tests;

public sealed class DeadlockTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    [Fact]
    public async Task Engine_deadlock_victim_rolls_back_business_and_schedules_a_retry()
    {
        await using var blocker = await db.DataSource.OpenConnectionAsync();
        await using (var setup = new SqlCommand($"CREATE TABLE {db.Schema}.deadlock_rows(id int PRIMARY KEY,value int NOT NULL); INSERT INTO {db.Schema}.deadlock_rows VALUES(1,0),(2,0)", blocker))
            await setup.ExecuteNonQueryAsync();
        var scope = DatabaseFixture.Scope();
        var work = (await db.Store.AcceptAsync(DatabaseFixture.Submission(scope))).Work;
        var claim = (await db.Claim(scope))!;
        await using var transaction = (SqlTransaction)await blocker.BeginTransactionAsync(IsolationLevel.ReadCommitted);
        await using (var hold = new SqlCommand($"UPDATE {db.Schema}.deadlock_rows WITH(ROWLOCK) SET value=value+1 WHERE id=2", blocker, transaction))
            await hold.ExecuteNonQueryAsync();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int? errorNumber = null;
        var executing = db.Store.ExecuteAsync(claim, async (w, p, session, ct) =>
        {
            await db.Effect(w, p, session, ct);
            await using var first = session.CreateCommand($"SET DEADLOCK_PRIORITY LOW; UPDATE {db.Schema}.deadlock_rows WITH(ROWLOCK) SET value=value+1 WHERE id=1");
            await first.ExecuteNonQueryAsync(ct);
            entered.SetResult();
            await using var second = session.CreateCommand($"UPDATE {db.Schema}.deadlock_rows WITH(ROWLOCK) SET value=value+1 WHERE id=2");
            try { await second.ExecuteNonQueryAsync(ct); }
            catch (SqlException error) { errorNumber = error.Number; throw; }
        }, new());
        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await using var cycle = new SqlCommand($"UPDATE {db.Schema}.deadlock_rows WITH(ROWLOCK) SET value=value+1 WHERE id=1", blocker, transaction);
            await cycle.ExecuteNonQueryAsync().WaitAsync(TimeSpan.FromSeconds(15));
            await transaction.CommitAsync();
            var result = await executing.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(1205, errorNumber);
            Assert.Equal(ExecutionStatus.RetryScheduled, result.Status);
            Assert.Equal(0, await db.EffectCount(work.Id));
        }
        finally
        {
            if (transaction.Connection is not null) await transaction.RollbackAsync();
            if (!executing.IsCompleted) await executing;
        }
    }
}
