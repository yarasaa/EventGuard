namespace EventGuard.PostgreSql.Tests;

public sealed class ClaimEligibilityTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    [Fact]
    public async Task Deadline_cleanup_waits_for_a_live_lease_and_never_invokes_the_expired_handler()
    {
        var scope = DatabaseFixture.Scope();
        var work = await db.Store.AcceptAsync(DatabaseFixture.Submission(scope, expiresAt: DateTimeOffset.UtcNow.AddHours(1)));
        var original = (await db.Claim(scope, TimeSpan.FromMinutes(2)))!;
        // Model passage of the work deadline independently of the still-live lease.
        await using (var command = db.DataSource.CreateCommand($"UPDATE {db.Schema}.works SET expires_at=clock_timestamp()-interval '1 minute' WHERE id=@id"))
        { command.Parameters.AddWithValue("id", work.Work.Id); await command.ExecuteNonQueryAsync(); }
        Assert.Null(await db.Claim(scope));
        var fresh = await db.Store.AcceptAsync(DatabaseFixture.Submission(scope, "fresh"));
        var next = (await db.Claim(scope))!;
        Assert.Equal(fresh.Work.Id, next.Work.Id);
        await db.Store.ExecuteAsync(next, db.Effect, new());
        await using (var command = db.DataSource.CreateCommand($"UPDATE {db.Schema}.works SET lease_until=clock_timestamp()-interval '1 second' WHERE id=@id"))
        { command.Parameters.AddWithValue("id", work.Work.Id); await command.ExecuteNonQueryAsync(); }
        var recovered = (await db.Claim(scope))!;
        Assert.Equal(original.Work.Id, recovered.Work.Id);
        Assert.Equal(2, recovered.Work.AttemptCount);
        Assert.Equal(ExecutionStatus.LeaseLost, (await db.Store.ExecuteAsync(original, db.Effect, new())).Status);
        Assert.Equal(ExecutionStatus.Expired, (await db.Store.ExecuteAsync(recovered, db.Effect, new())).Status);
        Assert.Equal(0, await db.EffectCount(work.Work.Id));
        Assert.Equal(1, await db.EffectCount(fresh.Work.Id));
    }
}
