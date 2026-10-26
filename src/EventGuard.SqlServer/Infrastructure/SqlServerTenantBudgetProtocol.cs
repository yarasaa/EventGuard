using Microsoft.Data.SqlClient;
namespace EventGuard.SqlServer;

internal static class SqlServerTenantBudgetProtocol
{
    private const string Columns = "tenant,version,window_seconds,maximum_claims,maximum_retries,maximum_recoveries,window_started_at,claims,retries,recoveries";
    internal static async Task<TenantBudgetSnapshot?> ReadAsync(SqlConnection connection, SqlTransaction? transaction,
        string schema, string tenant, bool forUpdate, CancellationToken ct, bool advisory = false)
    {
        var identity = new TenantBudgetPolicy(tenant);
        await using var command = new SqlCommand($"SELECT {Columns} FROM {schema}.tenant_budgets{(forUpdate ? " WITH(UPDLOCK,ROWLOCK)" : advisory ? " WITH(READUNCOMMITTED)" : "")} WHERE tenant_id=@tenant", connection, transaction);
        command.Parameters.AddValue("tenant", identity.Id);
        await using var reader = await command.ExecuteReaderCheckedAsync(ct);
        if (!await reader.ReadCheckedAsync(ct)) return null;
        if (reader.GetString(0) != tenant) throw new InvalidOperationException("Tenant budget identity mismatch.");
        return new(new(tenant, reader.GetInt32(2), reader.IsDBNull(3) ? null : reader.GetInt64(3),
            reader.IsDBNull(4) ? null : reader.GetInt64(4), reader.IsDBNull(5) ? null : reader.GetInt64(5)),
            reader.GetInt64(1), reader.GetFieldValue<DateTimeOffset>(6), reader.GetInt64(7), reader.GetInt64(8), reader.GetInt64(9));
    }
    internal static async Task<(bool Fresh, bool Retry)> AvailableAsync(SqlConnection connection, SqlTransaction transaction,
        string schema, string tenant, CancellationToken ct)
    {
        // Candidate filtering is advisory. SQL Server's dirty snapshot here never authorizes a transition;
        // ConsumeAsync re-reads the committed counter under an update lock after the work row is locked.
        var current = await ReadAsync(connection, transaction, schema, tenant, false, ct, advisory: true);
        if (current is null) return (true, true);
        var now = await SqlServerProtocol.NowAsync(connection, transaction, ct);
        return (TenantBudgets.Admit(current, TenantBudgetKind.Claim, now).Allowed, TenantBudgets.Admit(current, TenantBudgetKind.RetryClaim, now).Allowed);
    }
    internal static async Task<bool> ConsumeAsync(SqlConnection connection, SqlTransaction transaction,
        string schema, string tenant, TenantBudgetKind kind, CancellationToken ct)
    {
        var current = await ReadAsync(connection, transaction, schema, tenant, true, ct);
        if (current is null) return true; // Configure before starting workers when a limit is required.
        var now = await SqlServerProtocol.NowAsync(connection, transaction, ct);
        var decision = TenantBudgets.Admit(current, kind, now);
        if (!decision.Allowed) return false;
        await PersistAsync(connection, transaction, schema, current, decision.After, ct);
        return true;
    }
    internal static async Task PersistAsync(SqlConnection connection, SqlTransaction transaction, string schema,
        TenantBudgetSnapshot? before, TenantBudgetSnapshot after, CancellationToken ct)
    {
        var sql = before is null ? $"""
            INSERT INTO {schema}.tenant_budgets(tenant_id,tenant,version,window_seconds,maximum_claims,maximum_retries,maximum_recoveries,window_started_at,claims,retries,recoveries)
            VALUES(@id,@tenant,@version,@seconds,@maxClaims,@maxRetries,@maxRecoveries,@window,@claims,@retries,@recoveries)
            """ : $"""
            UPDATE {schema}.tenant_budgets SET version=@version,window_seconds=@seconds,maximum_claims=@maxClaims,maximum_retries=@maxRetries,
                maximum_recoveries=@maxRecoveries,window_started_at=@window,claims=@claims,retries=@retries,recoveries=@recoveries
            WHERE tenant_id=@id AND tenant=@tenant AND version=@expected AND window_started_at=@oldWindow
                AND claims=@oldClaims AND retries=@oldRetries AND recoveries=@oldRecoveries
            """;
        await using var command = new SqlCommand(sql, connection, transaction);
        command.Parameters.AddValue("id", after.Policy.Id); command.Parameters.AddValue("tenant", after.Policy.Tenant);
        command.Parameters.AddValue("version", after.Version); command.Parameters.AddValue("seconds", after.Policy.WindowSeconds);
        AddLimit(command, "maxClaims", after.Policy.MaximumClaims); AddLimit(command, "maxRetries", after.Policy.MaximumRetries); AddLimit(command, "maxRecoveries", after.Policy.MaximumRecoveries);
        command.Parameters.AddValue("window", after.WindowStartedAt); command.Parameters.AddValue("claims", after.Claims);
        command.Parameters.AddValue("retries", after.Retries); command.Parameters.AddValue("recoveries", after.Recoveries);
        if (before is not null)
        {
            command.Parameters.AddValue("expected", before.Version); command.Parameters.AddValue("oldWindow", before.WindowStartedAt);
            command.Parameters.AddValue("oldClaims", before.Claims); command.Parameters.AddValue("oldRetries", before.Retries); command.Parameters.AddValue("oldRecoveries", before.Recoveries);
        }
        if (await command.ExecuteNonQueryCheckedAsync(ct) != 1) throw new InvalidOperationException("Tenant budget changed under lock.");
    }
    private static void AddLimit(SqlCommand command, string name, long? value) => SqlServerProtocol.Optional(command, name, System.Data.SqlDbType.BigInt, value);
}
