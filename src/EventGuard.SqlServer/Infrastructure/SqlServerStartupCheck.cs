using Microsoft.Data.SqlClient;
namespace EventGuard.SqlServer;
internal static class SqlServerStartupCheck
{
    internal static async Task<StoreStartupReport> ValidateAsync(SqlServerDataSource source, string schema, StoreStartupRequest request, CancellationToken ct)
    {
        await using var connection = await source.OpenConnectionAsync(ct);
        string engine;
        await using (var profile = new SqlCommand("""
                SELECT CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128)), CAST(CASE WHEN
                CAST(SERVERPROPERTY('ProductMajorVersion') AS int)>=16 AND is_read_committed_snapshot_on=1
                AND delayed_durability_desc='DISABLED' AND is_read_only=0 THEN 1 ELSE 0 END AS bit)
                FROM sys.databases WHERE database_id=DB_ID()
                """, connection) { CommandTimeout = 3 })
        await using (var reader = await profile.ExecuteReaderAsync(ct))
        {
            if (!await reader.ReadAsync(ct) || !reader.GetBoolean(1)) throw new NotSupportedException("The database does not satisfy the required local durability, writable-primary and isolation profile.");
            engine = reader.GetString(0);
        }
        await using (var version = new SqlCommand($"SELECT version FROM {schema}.schema_version WHERE singleton=1", connection) { CommandTimeout = 3 })
            if (await version.ExecuteScalarAsync(ct) is not int value || value != 8)
                throw new InvalidOperationException("Apply the current EventGuard migrations before starting workers. Startup never executes DDL.");
        var required = request.Options.RequiredCapabilities | (request.Role == StoreRole.Consumer ? StoreCapabilities.ConsumerTransactions : StoreCapabilities.OutboxDispatch);
        var tables = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
        void Need(string table, params string[] permissions)
        {
            if (!tables.TryGetValue(table, out var grants)) tables[table] = grants = new(StringComparer.Ordinal);
            foreach (var permission in permissions) grants.Add(permission);
        }
        bool Has(StoreCapabilities capability) => (required & capability) != 0;
        Need("tenant_budgets", "SELECT", "UPDATE"); // Every native claim consults the shared policy, including an absent policy.
        if (Has(StoreCapabilities.ConsumerTransactions))
        {
            Need("works", "SELECT", "INSERT", "UPDATE"); Need("attempts", "SELECT", "INSERT", "UPDATE");
            Need("policy_bindings", "SELECT", "INSERT"); Need("conflicts", "SELECT", "INSERT");
        }
        if (Has(StoreCapabilities.OutboxDispatch))
        {
            Need("outbox_messages", "SELECT", "INSERT", "UPDATE"); Need("outbox_attempts", "SELECT", "INSERT", "UPDATE");
            Need("outbox_quarantine", "SELECT", "INSERT"); Need("producer_requests", "SELECT", "INSERT", "UPDATE");
        }
        if (Has(StoreCapabilities.DurableReplay) || Has(StoreCapabilities.BulkReplay))
        {
            Need(request.Role == StoreRole.Consumer ? "replay_requests" : "outbox_replay_requests", "SELECT", "INSERT");
            if (Has(StoreCapabilities.BulkReplay)) Need("bulk_replay_requests", "SELECT", "INSERT");
        }
        if (Has(StoreCapabilities.IngressQuarantine))
        { Need("ingress_quarantine", "SELECT", "INSERT", "UPDATE"); Need("ingress_release_requests", "SELECT", "INSERT"); }
        if (Has(StoreCapabilities.OutboxRepair))
        { Need("outbox_quarantine", "SELECT"); Need("outbox_repair_requests", "SELECT", "INSERT"); Need("outbox_messages", "SELECT", "UPDATE"); }
        if (Has(StoreCapabilities.Retention))
        {
            Need("retention_requests", "SELECT", "INSERT");
            foreach (var table in new[] { "works", "conflicts", "outbox_messages", "ingress_quarantine", "outbox_quarantine" }) Need(table, "SELECT", "UPDATE");
        }
        if (Has(StoreCapabilities.TenantBudgets))
        { Need("tenant_budgets", "INSERT"); Need("tenant_budget_requests", "SELECT", "INSERT"); }
        foreach (var (table, grants) in tables)
        foreach (var permission in grants)
        {
            await using var check = new SqlCommand("SELECT CAST(CASE WHEN HAS_PERMS_BY_NAME(@table,'OBJECT',@permission)=1 THEN 1 ELSE 0 END AS bit)", connection) { CommandTimeout = 3 };
            check.Parameters.AddWithValue("table", schema + "." + table); check.Parameters.AddWithValue("permission", permission);
            if (await check.ExecuteScalarAsync(ct) is not true) throw new InvalidOperationException("Required EventGuard protocol permissions are missing.");
        }
        // Validate current columns under the real identity, without returning payloads or opening a transaction.
        await using (var shape = new SqlCommand(request.Role == StoreRole.Consumer ? $"SELECT payload_purged_at,version,state FROM {schema}.works WHERE 1=0" : $"SELECT payload_purged_at,version,state,quarantine_id FROM {schema}.outbox_messages WHERE 1=0", connection) { CommandTimeout = 3 }) await using (var reader = await shape.ExecuteReaderAsync(ct)) { }
        var budget = await SqlServerTenantBudgetProtocol.ReadAsync(connection, null, schema, request.Tenant, false, ct);
        var configured = budget is not null && (budget.Policy.MaximumClaims.HasValue || budget.Policy.MaximumRetries.HasValue || budget.Policy.MaximumRecoveries.HasValue);
        if (request.Options.RequireTenantBudget && !configured) throw new InvalidOperationException("Configure at least one finite tenant budget before starting this worker.");
        return new("SqlServer", engine, 8, (StoreCapabilities)255, configured);
    }
}
