using Npgsql;
namespace EventGuard.PostgreSql;

internal static class PostgreSqlTenantBudgetProtocol
{
    private const string Columns = "tenant,version,window_seconds,maximum_claims,maximum_retries,maximum_recoveries,window_started_at,claims,retries,recoveries";
    internal static async Task<TenantBudgetSnapshot?> ReadAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction,
        string schema, string tenant, bool forUpdate, CancellationToken ct, bool advisory = false)
    {
        var identity = new TenantBudgetPolicy(tenant);
        await using var command = new NpgsqlCommand($"SELECT {Columns} FROM {schema}.tenant_budgets WHERE tenant_id=@tenant{(forUpdate ? " FOR UPDATE" : "")}", connection, transaction);
        command.Parameters.AddWithValue("tenant", identity.Id);
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) return null;
        if (reader.GetString(0) != tenant) throw new InvalidOperationException("Tenant budget identity mismatch.");
        return new(new(tenant, reader.GetInt32(2), reader.IsDBNull(3) ? null : reader.GetInt64(3),
            reader.IsDBNull(4) ? null : reader.GetInt64(4), reader.IsDBNull(5) ? null : reader.GetInt64(5)),
            reader.GetInt64(1), reader.GetFieldValue<DateTimeOffset>(6), reader.GetInt64(7), reader.GetInt64(8), reader.GetInt64(9));
    }
    internal static async Task<(bool Fresh, bool Retry)> AvailableAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        string schema, string tenant, CancellationToken ct)
    {
        // Candidate filtering is advisory. SQL Server's dirty snapshot here never authorizes a transition;
        // ConsumeAsync re-reads the committed counter under an update lock after the work row is locked.
        var current = await ReadAsync(connection, transaction, schema, tenant, false, ct, advisory: true);
        if (current is null) return (true, true);
        var now = await PostgreSqlProtocol.NowAsync(connection, transaction, ct);
        return (TenantBudgets.Admit(current, TenantBudgetKind.Claim, now).Allowed, TenantBudgets.Admit(current, TenantBudgetKind.RetryClaim, now).Allowed);
    }
    internal static async Task<bool> ConsumeAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        string schema, string tenant, TenantBudgetKind kind, CancellationToken ct)
    {
        var current = await ReadAsync(connection, transaction, schema, tenant, true, ct);
        if (current is null) return true; // Configure before starting workers when a limit is required.
        var now = await PostgreSqlProtocol.NowAsync(connection, transaction, ct);
        var decision = TenantBudgets.Admit(current, kind, now);
        if (!decision.Allowed) return false;
        await PersistAsync(connection, transaction, schema, current, decision.After, ct);
        return true;
    }
    internal static async Task PersistAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string schema,
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
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue("id", after.Policy.Id); command.Parameters.AddWithValue("tenant", after.Policy.Tenant);
        command.Parameters.AddWithValue("version", after.Version); command.Parameters.AddWithValue("seconds", after.Policy.WindowSeconds);
        AddLimit(command, "maxClaims", after.Policy.MaximumClaims); AddLimit(command, "maxRetries", after.Policy.MaximumRetries); AddLimit(command, "maxRecoveries", after.Policy.MaximumRecoveries);
        command.Parameters.AddWithValue("window", after.WindowStartedAt); command.Parameters.AddWithValue("claims", after.Claims);
        command.Parameters.AddWithValue("retries", after.Retries); command.Parameters.AddWithValue("recoveries", after.Recoveries);
        if (before is not null)
        {
            command.Parameters.AddWithValue("expected", before.Version); command.Parameters.AddWithValue("oldWindow", before.WindowStartedAt);
            command.Parameters.AddWithValue("oldClaims", before.Claims); command.Parameters.AddWithValue("oldRetries", before.Retries); command.Parameters.AddWithValue("oldRecoveries", before.Recoveries);
        }
        if (await command.ExecuteNonQueryAsync(ct) != 1) throw new InvalidOperationException("Tenant budget changed under lock.");
    }
    private static void AddLimit(NpgsqlCommand command, string name, long? value) => command.Parameters.AddWithValue(name, NpgsqlTypes.NpgsqlDbType.Bigint, (object?)value ?? DBNull.Value);
}
