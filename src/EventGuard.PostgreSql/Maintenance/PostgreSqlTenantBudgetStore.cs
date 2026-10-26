using Npgsql;
namespace EventGuard.PostgreSql;

public sealed partial class PostgreSqlWorkStore : ITenantBudgetStore
{
    public async Task<TenantBudgetSnapshot?> FindBudgetAsync(string tenant, CancellationToken cancellationToken = default)
    {
        _ = new TenantBudgetPolicy(tenant);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        return await PostgreSqlTenantBudgetProtocol.ReadAsync(connection, null, _schema, tenant, false, cancellationToken);
    }
    public async Task<TenantBudgetResult> ConfigureBudgetAsync(TenantBudgetRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await PostgreSqlProtocol.LockAsync(connection, transaction, "budget-config:" + _schema + ":" + request.Policy.Id, cancellationToken);
        await using (var previous = new NpgsqlCommand($"SELECT fingerprint,result_status,result_version FROM {_schema}.tenant_budget_requests WHERE tenant_id=@tenant AND id=@id", connection, transaction))
        {
            previous.Parameters.AddWithValue("tenant", request.Policy.Id); previous.Parameters.AddWithValue("id", request.RequestId);
            await using var reader = await previous.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
                return TenantBudgets.Repeat(request, reader.GetString(0), new((TenantBudgetStatus)reader.GetInt32(1), reader.GetInt64(2)));
        }
        var current = await PostgreSqlTenantBudgetProtocol.ReadAsync(connection, transaction, _schema, request.Policy.Tenant, true, cancellationToken);
        var now = await PostgreSqlProtocol.NowAsync(connection, transaction, cancellationToken);
        var decision = TenantBudgets.Configure(request, current, now);
        if (decision.After is not null) await PostgreSqlTenantBudgetProtocol.PersistAsync(connection, transaction, _schema, current, decision.After, cancellationToken);
        await using var receipt = new NpgsqlCommand($"INSERT INTO {_schema}.tenant_budget_requests(tenant_id,id,fingerprint,actor,reason,result_status,result_version,expected_version,requested_window_seconds,requested_max_claims,requested_max_retries,requested_max_recoveries) VALUES(@tenant,@id,@fingerprint,@actor,@reason,@status,@version,@expected,@seconds,@claims,@retries,@recoveries)", connection, transaction);
        receipt.Parameters.AddWithValue("tenant", request.Policy.Id); receipt.Parameters.AddWithValue("id", request.RequestId);
        receipt.Parameters.AddWithValue("fingerprint", request.Fingerprint); receipt.Parameters.AddWithValue("actor", request.Actor); receipt.Parameters.AddWithValue("reason", request.Reason);
        receipt.Parameters.AddWithValue("status", (int)decision.Result.Status); receipt.Parameters.AddWithValue("version", decision.Result.Version);
        receipt.Parameters.AddWithValue("expected", request.ExpectedVersion); receipt.Parameters.AddWithValue("seconds", request.Policy.WindowSeconds);
        receipt.Parameters.AddWithValue("claims", NpgsqlTypes.NpgsqlDbType.Bigint, (object?)request.Policy.MaximumClaims ?? DBNull.Value); receipt.Parameters.AddWithValue("retries", NpgsqlTypes.NpgsqlDbType.Bigint, (object?)request.Policy.MaximumRetries ?? DBNull.Value); receipt.Parameters.AddWithValue("recoveries", NpgsqlTypes.NpgsqlDbType.Bigint, (object?)request.Policy.MaximumRecoveries ?? DBNull.Value);
        await receipt.ExecuteNonQueryAsync(cancellationToken);
        await PostgreSqlProtocol.CommitAsync(transaction, request.RequestId, cancellationToken);
        return decision.Result;
    }
}
