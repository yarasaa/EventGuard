using Npgsql;
namespace EventGuard.PostgreSql.Tests;
public sealed class TenantBudgetFaultTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    private async Task<(WorkScope Scope, Guid Id)> Prepare()
    {
        var scope = new WorkScope(Guid.NewGuid().ToString("N"), "worker", "sample", "file.created");
        await db.Store.ConfigureBudgetAsync(new(Guid.NewGuid(), new(scope.Tenant, 86400, maximumClaims: 1), -1, "operator", "test budget"));
        var accepted = await db.Store.AcceptAsync(DatabaseFixture.Submission(scope)); return (scope, accepted.Work.Id);
    }
    [Fact]
    public async Task Claim_commit_reply_loss_does_not_charge_the_resolved_request_twice()
    {
        var (scope, id) = await Prepare(); var request = Guid.NewGuid();
        var settings = new NpgsqlConnectionStringBuilder(db.ConnectionString);
        await using var proxy = new CommitReplyDropProxy(settings.Port);
        settings.Port = proxy.Port; settings.Pooling = false; settings.SslMode = SslMode.Disable; settings.DisableGssEncryptionForFaultProxy();
        await using var source = NpgsqlDataSource.Create(settings.ConnectionString);
        var interrupted = new PostgreSqlWorkStore(source, db.Schema);
        var error = await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => interrupted.ClaimNextAsync(scope, request, TimeSpan.FromMinutes(1)));
        Assert.Equal(request, error.OperationId); await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var resolved = (await db.Store.ClaimNextAsync(scope, request, TimeSpan.FromMinutes(1)))!;
        Assert.Equal(id, resolved.Work.Id); Assert.Equal(1, (await db.Store.FindBudgetAsync(scope.Tenant))!.Claims);
        await db.Store.ExecuteAsync(resolved, db.Effect, new());
        Assert.Null(await db.Store.ClaimNextAsync(scope, request, TimeSpan.FromMinutes(1)));
        Assert.Equal(1, (await db.Store.FindBudgetAsync(scope.Tenant))!.Claims); Assert.Equal(1, await db.EffectCount(id));
    }
    [Fact]
    public async Task Native_attempt_insert_failure_rolls_back_the_counter_and_work_claim()
    {
        var (scope, id) = await Prepare(); var request = Guid.NewGuid();
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var command = new NpgsqlCommand($"ALTER TABLE {db.Schema}.attempts ADD CONSTRAINT reject_budget_claim CHECK(id <> '{request:D}')", connection);
        await command.ExecuteNonQueryAsync();
        try { await Assert.ThrowsAnyAsync<System.Data.Common.DbException>(() => db.Store.ClaimNextAsync(scope, request, TimeSpan.FromMinutes(1))); }
        finally { command.CommandText = $"ALTER TABLE {db.Schema}.attempts DROP CONSTRAINT reject_budget_claim"; await command.ExecuteNonQueryAsync(); }
        Assert.Equal(WorkState.Pending, (await db.Store.FindAsync(scope, id))!.State); Assert.Equal(0, (await db.Store.FindBudgetAsync(scope.Tenant))!.Claims);
        Assert.NotNull(await db.Store.ClaimNextAsync(scope, request, TimeSpan.FromMinutes(1))); Assert.Equal(1, (await db.Store.FindBudgetAsync(scope.Tenant))!.Claims);
    }
    [Fact]
    public async Task Configuration_commit_reply_loss_resolves_without_resetting_a_later_policy()
    {
        var request = new TenantBudgetRequest(Guid.NewGuid(), new(Guid.NewGuid().ToString("N"), maximumClaims: 1), -1, "operator", "initial");
        var settings = new NpgsqlConnectionStringBuilder(db.ConnectionString);
        await using var proxy = new CommitReplyDropProxy(settings.Port);
        settings.Port = proxy.Port; settings.Pooling = false; settings.SslMode = SslMode.Disable; settings.DisableGssEncryptionForFaultProxy();
        await using var source = NpgsqlDataSource.Create(settings.ConnectionString);
        var interrupted = new PostgreSqlWorkStore(source, db.Schema);
        await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => interrupted.ConfigureBudgetAsync(request));
        await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True((await db.Store.ConfigureBudgetAsync(request)).RepeatedRequest);
        await db.Store.ConfigureBudgetAsync(new(Guid.NewGuid(), new(request.Policy.Tenant, maximumClaims: 2), 0, "operator", "increase"));
        Assert.Equal(0, (await db.Store.ConfigureBudgetAsync(request)).Version);
        Assert.Equal(2, (await db.Store.FindBudgetAsync(request.Policy.Tenant))!.Policy.MaximumClaims);
    }
}
