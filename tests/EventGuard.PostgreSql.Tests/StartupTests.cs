using EventGuard.Runtime;
using Npgsql;
namespace EventGuard.PostgreSql.Tests;
public sealed class StartupTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    [Fact]
    public async Task Required_budget_and_capabilities_are_checked_before_claims_and_fault_callbacks()
    {
        var scope = DatabaseFixture.Scope(); var accepted = await db.Store.AcceptAsync(DatabaseFixture.Submission(scope));
        var faults = 0; var invoked = 0;
        var options = new WorkPumpOptions(new StoreStartupOptions(requireTenantBudget: true), maximumConcurrency: 1);
        var pump = new WorkPump<PostgreSqlSession>(db.Store, scope, (_, _, _, _) => { invoked++; return ValueTask.CompletedTask; }, options: options, onFault: _ => faults++);
        await Assert.ThrowsAsync<InvalidOperationException>(() => pump.RunAsync(CancellationToken.None));
        Assert.Equal(0, faults); Assert.Equal(0, invoked); Assert.Equal(WorkState.Pending, (await db.Store.FindAsync(scope, accepted.Work.Id))!.State);
        Assert.Null(await db.Store.FindBudgetAsync(scope.Tenant));
        await db.Store.ConfigureBudgetAsync(new(Guid.NewGuid(), new(scope.Tenant), -1, "operator", "unlimited"));
        await Assert.ThrowsAsync<InvalidOperationException>(() => pump.RunAsync(CancellationToken.None));
        await db.Store.ConfigureBudgetAsync(new(Guid.NewGuid(), new(scope.Tenant, maximumClaims: 5), 0, "operator", "configured"));
        var report = await db.Store.ValidateStartupAsync(new(scope.Tenant, StoreRole.Consumer, new((StoreCapabilities)255, true)));
        Assert.Equal(11, report.SchemaVersion); Assert.True(report.TenantBudgetConfigured); Assert.Equal("PostgreSql", report.Provider); Assert.NotEmpty(report.EngineVersion);
        Assert.Equal(0, (await db.Store.FindBudgetAsync(scope.Tenant))!.Claims);
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(5)); var run = pump.RunAsync(stop.Token);
        while ((await db.Store.FindAsync(scope, accepted.Work.Id, stop.Token))!.State != WorkState.Completed) await Task.Delay(20, stop.Token);
        stop.Cancel(); await run; Assert.Equal(1, invoked); Assert.Equal(0, faults);
    }
    [Fact]
    public async Task Outbox_requires_its_own_role_and_checks_budget_before_dispatch()
    {
        var producer = new ProducerScope(Guid.NewGuid().ToString("N"), "publisher");
        var message = new OutgoingMessage(producer, Guid.NewGuid(), Guid.NewGuid(), new("test", "local", "v1", "events"), [1]); await db.EnqueueAsync(message);
        var store = new PostgreSqlOutboxStore(db.DataSource, db.Schema);
        var dispatcher = new OutboxDispatcher(store, producer, [], options: new(new StoreStartupOptions(requireTenantBudget: true)));
        await Assert.ThrowsAsync<InvalidOperationException>(() => dispatcher.RunAsync(CancellationToken.None));
        Assert.Equal(OutboxState.Pending, (await store.FindAsync(producer, message.IntentId))!.State);
        Assert.Throws<NotSupportedException>(() => { _ = store.ValidateStartupAsync(new(producer.Tenant, StoreRole.Consumer)); });
        var report = await store.ValidateStartupAsync(new(producer.Tenant, StoreRole.Outbox)); Assert.False(report.TenantBudgetConfigured);
    }
    [Fact]
    public async Task Missing_migration_is_rejected_without_creating_any_schema_objects()
    {
        var schema = db.Schema + "_absent"; var store = new PostgreSqlWorkStore(db.DataSource, schema);
        await Assert.ThrowsAnyAsync<Exception>(() => store.ValidateStartupAsync(new("tenant", StoreRole.Consumer)));
        
        await using var check = db.DataSource.CreateCommand("SELECT to_regclass(@name)::text");
        check.Parameters.AddWithValue("name", schema + ".schema_version"); Assert.Equal(DBNull.Value, await check.ExecuteScalarAsync());
    }
    [Fact]
    public async Task Disabled_synchronous_commit_fails_before_work_is_claimed()
    {
        var settings = new NpgsqlConnectionStringBuilder(db.ConnectionString) { Options = "-c synchronous_commit=off" };
        await using var source = NpgsqlDataSource.Create(settings.ConnectionString);
        var store = new PostgreSqlWorkStore(source, db.Schema);
        await Assert.ThrowsAsync<NotSupportedException>(() => store.ValidateStartupAsync(new("tenant", StoreRole.Consumer)));
    }

}
