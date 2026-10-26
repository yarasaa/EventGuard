namespace EventGuard.Runtime;
internal static class StoreStartup
{
    internal static async Task ValidateAsync(object store, string tenant, StoreRole role, StoreStartupOptions options, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        if (store is not IStoreStartupCheck check)
        {
            if (options.RequiredCapabilities != StoreCapabilities.None || options.RequireTenantBudget)
                throw new NotSupportedException("The selected store cannot validate the required startup capabilities.");
            return; // Existing custom stores remain usable; no validation or native guarantee is claimed for them.
        }
        using var budget = CancellationTokenSource.CreateLinkedTokenSource(ct); budget.CancelAfter(TimeSpan.FromSeconds(5));
        var report = await check.ValidateStartupAsync(new(tenant, role, options), budget.Token).ConfigureAwait(false);
        budget.Token.ThrowIfCancellationRequested();
        var required = options.RequiredCapabilities | (role == StoreRole.Consumer ? StoreCapabilities.ConsumerTransactions : StoreCapabilities.OutboxDispatch);
        if ((report.Capabilities & required) != required || options.RequireTenantBudget && !report.TenantBudgetConfigured)
            throw new NotSupportedException("The selected store does not satisfy the required startup capabilities or tenant budget.");
    }
}
