using EventGuard.Runtime;
namespace EventGuard.PostgreSql.Tests;
// Runtime contract doubles only; StartupTests separately exercises real database validation.
public sealed class RuntimeStartupTests
{
    private class Store : IOutboxStore
    {
        public int Claims;
        public Task<OutboxRecord?> FindAsync(ProducerScope scope, Guid intentId, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<OutboxClaim?> ClaimNextAsync(ProducerScope scope, Guid requestId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
        { Claims++; return Task.FromResult<OutboxClaim?>(null); }
        public Task<OutboxSettlement> SettleAsync(OutboxClaim claim, PublishOutcome outcome, RetryPolicy retryPolicy, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    }
    private sealed class CheckedStore : Store, IStoreStartupCheck
    {
        public StoreCapabilities Capabilities = StoreCapabilities.OutboxDispatch; public bool Budget; public bool Wait;
        public async Task<StoreStartupReport> ValidateStartupAsync(StoreStartupRequest request, CancellationToken cancellationToken = default)
        {
            Assert.Equal(StoreRole.Outbox, request.Role); Assert.Equal("tenant", request.Tenant);
            if (Wait) await Task.Delay(Timeout.Infinite, cancellationToken);
            return new("test-double", "test", 1, Capabilities, Budget);
        }
    }
    [Fact]
    public async Task Explicit_requirements_cannot_be_silently_ignored_by_custom_stores()
    {
        var store = new Store(); var pump = new OutboxDispatcher(store, new("tenant", "publisher"), [], options: new(new StoreStartupOptions(requireTenantBudget: true)));
        await Assert.ThrowsAsync<NotSupportedException>(() => pump.RunAsync(CancellationToken.None)); Assert.Equal(0, store.Claims);
        var checkedStore = new CheckedStore(); var required = new OutboxDispatcher(checkedStore, new("tenant", "publisher"), [], options: new(new StoreStartupOptions(StoreCapabilities.OutboxRepair)));
        await Assert.ThrowsAsync<NotSupportedException>(() => required.RunAsync(CancellationToken.None)); Assert.Equal(0, checkedStore.Claims);
        var budget = new OutboxDispatcher(checkedStore, new("tenant", "publisher"), [], options: new(new StoreStartupOptions(requireTenantBudget: true)));
        await Assert.ThrowsAsync<NotSupportedException>(() => budget.RunAsync(CancellationToken.None)); Assert.Equal(0, checkedStore.Claims);
    }
    [Fact]
    public async Task Startup_has_a_total_deadline_before_any_dispatch_loop_runs()
    {
        var store = new CheckedStore { Wait = true, Budget = true }; var pump = new OutboxDispatcher(store, new("tenant", "publisher"), []);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pump.RunAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(8)));
        Assert.Equal(0, store.Claims);
    }
}
