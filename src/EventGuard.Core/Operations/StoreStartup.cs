namespace EventGuard;

[Flags]
public enum StoreCapabilities
{
    None = 0, ConsumerTransactions = 1, OutboxDispatch = 2, DurableReplay = 4,
    IngressQuarantine = 8, OutboxRepair = 16, Retention = 32, TenantBudgets = 64, BulkReplay = 128
}
public enum StoreRole { Consumer, Outbox }
public sealed class StoreStartupOptions
{
    public StoreCapabilities RequiredCapabilities { get; }
    public bool RequireTenantBudget { get; }
    public StoreStartupOptions(StoreCapabilities requiredCapabilities = StoreCapabilities.None, bool requireTenantBudget = false)
    {
        if (((int)requiredCapabilities & ~255) != 0) throw new ArgumentOutOfRangeException(nameof(requiredCapabilities));
        RequiredCapabilities = requiredCapabilities; RequireTenantBudget = requireTenantBudget;
    }
}
public sealed class StoreStartupRequest
{
    public string Tenant { get; }
    public StoreRole Role { get; }
    public StoreStartupOptions Options { get; }
    public StoreStartupRequest(string tenant, StoreRole role, StoreStartupOptions? options = null)
    {
        Tenant = IdentityRules.Required(tenant, nameof(tenant));
        if (!Enum.IsDefined(role)) throw new ArgumentOutOfRangeException(nameof(role));
        Role = role; Options = options ?? new();
    }
}
public sealed record StoreStartupReport(string Provider, string EngineVersion, int SchemaVersion, StoreCapabilities Capabilities, bool TenantBudgetConfigured);
/// <summary>Read-only startup validation. Does not migrate, prove business SQL grants, or replace native provider qualification.</summary>
public interface IStoreStartupCheck
{
    Task<StoreStartupReport> ValidateStartupAsync(StoreStartupRequest request, CancellationToken cancellationToken = default);
}
