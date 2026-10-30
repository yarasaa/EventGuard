using EventGuard.Runtime;
using Npgsql;

namespace EventGuard.PostgreSql.Tests;

public sealed class ExternalPermitMaintenanceTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    [Fact]
    public async Task Idle_batch_worker_does_not_create_permit_receipts()
    {
        await db.Store.InitializeExternalActionsAsync();
        var limit = new ExternalCallLimit("idle", Guid.NewGuid().ToString("N"), 100, TimeSpan.FromMinutes(1));
        var adapter = new EmptyAdapter();
        var worker = new ExternalBatchWorker(db.Store, new("idle", "empty"), adapter, ExternalPhase.Submit, limit: limit, limiter: db.Store);
        for (var i = 0; i < 20; i++) Assert.Null(await worker.RunOnceAsync());
        await using var c = await db.DataSource.OpenConnectionAsync();
        await using var command = new NpgsqlCommand($"SELECT COUNT(*) FROM {db.Schema}.external_permits WHERE limit_id=@limit", c);
        command.Parameters.AddWithValue("limit", limit.Id);
        Assert.Equal(0, Convert.ToInt32(await command.ExecuteScalarAsync())); Assert.Equal(0, adapter.Calls);
    }

    [Fact]
    public async Task Bounded_retirement_preserves_active_legacy_and_retry_safety()
    {
        await db.Store.InitializeExternalActionsAsync();
        var limit = new ExternalCallLimit("retire", Guid.NewGuid().ToString("N"), 100, TimeSpan.FromMinutes(1), 20);
        var permits = new List<ExternalCallPermit>();
        for (var i = 0; i < 3; i++)
        {
            var value = ExternalCallRequestIds.New().ToString("N");
            var oldId = Guid.ParseExact(DateTimeOffset.UtcNow.AddDays(-2).ToUnixTimeMilliseconds().ToString("x12") + value[12..], "N");
            var permit = (await db.Store.TryAcquireCallAsync(limit, oldId, TimeSpan.FromSeconds(30)))!;
            await db.Store.CompleteCallAsync(permit, ExternalCallOutcome.Unused); permits.Add(permit);
        }
        var legacy = (await db.Store.TryAcquireCallAsync(limit, Guid.NewGuid(), TimeSpan.FromSeconds(30)))!;
        await db.Store.CompleteCallAsync(legacy, ExternalCallOutcome.Unused);
        var active = (await db.Store.TryAcquireCallAsync(limit, ExternalCallRequestIds.New(), TimeSpan.FromMinutes(1)))!;
        await using var c = await db.DataSource.OpenConnectionAsync();
        await using (var expire = new NpgsqlCommand($"UPDATE {db.Schema}.external_permits SET expires_at=clock_timestamp()-interval '1 day' WHERE limit_id=@limit AND outcome=0", c))
        { expire.Parameters.AddWithValue("limit", limit.Id); await expire.ExecuteNonQueryAsync(); }
        Assert.Equal(2, await db.Store.PruneCallPermitsAsync(limit, TimeSpan.FromDays(1), 2));
        Assert.Equal(1, await db.Store.PruneCallPermitsAsync(limit, TimeSpan.FromDays(1), 2));
        Assert.Equal(0, await db.Store.PruneCallPermitsAsync(limit, TimeSpan.FromDays(1), 2));
        foreach (var permit in permits)
        {
            Assert.Null(await db.Store.TryAcquireCallAsync(limit, permit.Id, TimeSpan.FromSeconds(30)));
            await Assert.ThrowsAsync<ExternalCallPermitRetiredException>(() => db.Store.CompleteCallAsync(permit, ExternalCallOutcome.Unused));
        }
        await db.Store.CompleteCallAsync(legacy, ExternalCallOutcome.Unused);
        Assert.Equal(active, await db.Store.TryAcquireCallAsync(limit, active.Id, TimeSpan.FromMinutes(1)));
        Assert.Equal(1, (await db.Store.ReadCallLimitAsync(limit))!.Used);
    }

    private sealed class EmptyAdapter : IExternalBatchServiceAdapter
    {
        public int Calls; public string Binding => "service"; public string BindingVersion => "v1";
        public bool CanReconcile => false; public bool CanObserve => false;
        public Task<IReadOnlyList<ExternalBatchItemResult>> ExecuteAsync(ExternalBatchRequest request, CancellationToken ct)
        { Calls++; throw new InvalidOperationException("Empty queues must not invoke the adapter."); }
    }
}
