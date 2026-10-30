using EventGuard.Runtime;
using Microsoft.Data.SqlClient;

namespace EventGuard.SqlServer.Tests;

public sealed class ExternalCallLimitTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    private static ExternalCallLimit Limit(int calls = 3, int concurrency = 2, int failures = 2) =>
        new("app", Guid.NewGuid().ToString("N"), calls, TimeSpan.FromMinutes(1), concurrency, failures, TimeSpan.FromMinutes(1));
    private Task<ExternalCallPermit?> Acquire(ExternalCallLimit limit) => db.Store.TryAcquireCallAsync(limit, Guid.NewGuid(), TimeSpan.FromSeconds(30));
    private async Task Past(ExternalCallLimit limit, string column)
    {
        await using var c = await db.DataSource.OpenConnectionAsync();
        await using var sql = new SqlCommand($"UPDATE {db.Schema}.external_limits SET {column}=@past WHERE id=@id", c);
        sql.Parameters.AddWithValue("past", DateTimeOffset.UtcNow.AddHours(-1)); sql.Parameters.AddWithValue("id", limit.Id); await sql.ExecuteNonQueryAsync();
    }
    [Fact]
    public async Task Concurrent_workers_share_rate_and_concurrency_and_unused_permits_refund_only_once()
    {
        await db.Store.InitializeExternalActionsAsync(); var limit = Limit();
        var grants = (await Task.WhenAll(Enumerable.Range(0, 12).Select(_ => Acquire(limit)))).Where(x => x is not null).Cast<ExternalCallPermit>().ToArray();
        Assert.Equal(2, grants.Length);
        await db.Store.CompleteCallAsync(grants[0], ExternalCallOutcome.Unused); await db.Store.CompleteCallAsync(grants[0], ExternalCallOutcome.Unused);
        var third = (await Acquire(limit))!; Assert.NotNull(third);
        await db.Store.CompleteCallAsync(grants[1], ExternalCallOutcome.Healthy); await db.Store.CompleteCallAsync(third, ExternalCallOutcome.Healthy);
        var last = (await Acquire(limit))!; Assert.NotNull(last); await db.Store.CompleteCallAsync(last, ExternalCallOutcome.Healthy);
        Assert.Null(await Acquire(limit));
        var state = (await db.Store.ReadCallLimitAsync(limit))!; Assert.Equal(3, state.Used); Assert.Equal(0, state.Active);
        await Past(limit, "window_started_at"); Assert.NotNull(await Acquire(limit));
        await Assert.ThrowsAsync<InvalidOperationException>(() => Acquire(new(limit.Namespace, limit.Endpoint, 100, limit.Window)));
    }
    [Fact]
    public async Task Circuit_opens_and_allows_only_one_recovery_probe()
    {
        await db.Store.InitializeExternalActionsAsync(); var limit = Limit(100, 4);
        var first = (await Acquire(limit))!; var second = (await Acquire(limit))!;
        await db.Store.CompleteCallAsync(first, ExternalCallOutcome.Unhealthy); await db.Store.CompleteCallAsync(second, ExternalCallOutcome.Unhealthy);
        Assert.Null(await Acquire(limit)); Assert.NotNull((await db.Store.ReadCallLimitAsync(limit))!.OpenUntil);
        await Past(limit, "open_until");
        var probes = await Task.WhenAll(Enumerable.Range(0, 10).Select(_ => Acquire(limit)));
        var probe = Assert.Single(probes, value => value is not null)!;
        await db.Store.CompleteCallAsync(probe, ExternalCallOutcome.Healthy);
        Assert.Null((await db.Store.ReadCallLimitAsync(limit))!.OpenUntil); Assert.NotNull(await Acquire(limit));
    }
    [Fact]
    public async Task Denied_permit_does_not_claim_or_consume_the_external_action_attempt_budget()
    {
        await db.Store.InitializeExternalActionsAsync(); var limit = Limit(1, 1);
        var used = (await Acquire(limit))!; await db.Store.CompleteCallAsync(used, ExternalCallOutcome.Healthy);
        var scope = new ProducerScope("tenant", Guid.NewGuid().ToString("N"));
        var action = new ExternalAction(scope, Guid.NewGuid(), "service", "v1", [1]);
        await db.Outbox.ProduceAsync(new(scope, action.Id, ContentFingerprint.FromBytes([1])), async (session, ct) => { await session.ExternalActions.EnqueueAsync(action, ct); });
        var service = new Service(); var worker = new ExternalActionWorker(db.Store, scope, service, ExternalPhase.Submit, limit, db.Store);
        for (var n = 0; n < 10; n++) Assert.Null(await worker.RunOnceAsync());
        var pending = (await db.Store.FindActionAsync(scope, action.Id))!;
        Assert.Equal(ExternalActionState.Ready, pending.State); Assert.Equal(0, pending.SubmissionAttempts); Assert.Equal(0, service.Calls);
        await Past(limit, "window_started_at"); Assert.Equal(ExternalActionState.Accepted, (await worker.RunOnceAsync())!.Record.State);
        Assert.Equal(1, service.Calls);
    }
    [Fact]
    public async Task Idle_worker_returns_capacity_without_a_remote_call()
    {
        await db.Store.InitializeExternalActionsAsync(); var limit = Limit();
        var service = new Service(); var worker = new ExternalActionWorker(db.Store, new("tenant", "empty"), service, ExternalPhase.Submit, limit, db.Store);
        for (var n = 0; n < 5; n++) Assert.Null(await worker.RunOnceAsync());
        Assert.Null(await db.Store.ReadCallLimitAsync(limit)); Assert.Equal(0, service.Calls);
    }
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Lost_permit_commit_reply_is_resolved_without_double_accounting(bool completing)
    {
        await db.Store.InitializeExternalActionsAsync(); var limit = Limit(); var id = Guid.NewGuid();
        var permit = completing ? (await db.Store.TryAcquireCallAsync(limit, id, TimeSpan.FromSeconds(30)))! : null;
        await using var proxy = new CommitReplyDropProxy();
        var settings = new SqlConnectionStringBuilder(db.ConnectionString) { DataSource = "127.0.0.1," + proxy.Port, Encrypt = false, Pooling = false };
        var source = new SqlServerDataSource(settings.ConnectionString);
        var interrupted = new SqlServerWorkStore(source, db.Schema);
        await Assert.ThrowsAsync<CommitOutcomeUnknownException>(async () =>
        {
            if (completing) await interrupted.CompleteCallAsync(permit!, ExternalCallOutcome.Healthy);
            else await interrupted.TryAcquireCallAsync(limit, id, TimeSpan.FromSeconds(30));
        });
        await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(5));
        if (completing) await db.Store.CompleteCallAsync(permit!, ExternalCallOutcome.Healthy);
        else Assert.NotNull(await db.Store.TryAcquireCallAsync(limit, id, TimeSpan.FromSeconds(30)));
        var state = (await db.Store.ReadCallLimitAsync(limit))!; Assert.Equal(1, state.Used); Assert.Equal(completing ? 0 : 1, state.Active);
    }
    private sealed class Service : IExternalServiceAdapter
    {
        public int Calls;
        public string Binding => "service"; public string BindingVersion => "v1"; public bool CanReconcile => false; public bool CanObserve => false;
        public Task<ExternalResult> SubmitAsync(ExternalAction action, CancellationToken cancellationToken) { Calls++; return Task.FromResult(new ExternalResult(ExternalResultKind.Accepted, "accepted")); }
        public Task<ExternalResult> ReconcileAsync(ExternalActionRecord action, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<ExternalResult> ObserveAsync(ExternalActionRecord action, CancellationToken cancellationToken) => throw new NotSupportedException();
    }
}
