using Microsoft.Data.SqlClient;
namespace EventGuard.SqlServer;

public sealed partial class SqlServerWorkStore : ITenantBudgetStore
{
    public async Task<TenantBudgetSnapshot?> FindBudgetAsync(string tenant, CancellationToken cancellationToken = default)
    {
        _ = new TenantBudgetPolicy(tenant);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        return await SqlServerTenantBudgetProtocol.ReadAsync(connection, null, _schema, tenant, false, cancellationToken);
    }
    public async Task<TenantBudgetResult> ConfigureBudgetAsync(TenantBudgetRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginLocalAsync(cancellationToken);
        await SqlServerProtocol.LockAsync(connection, transaction, "budget-config:" + _schema + ":" + request.Policy.Id, cancellationToken);
        await using (var previous = new SqlCommand($"SELECT fingerprint,result_status,result_version FROM {_schema}.tenant_budget_requests WHERE tenant_id=@tenant AND id=@id", connection, transaction))
        {
            previous.Parameters.AddValue("tenant", request.Policy.Id); previous.Parameters.AddValue("id", request.RequestId);
            await using var reader = await previous.ExecuteReaderCheckedAsync(cancellationToken);
            if (await reader.ReadCheckedAsync(cancellationToken))
                return TenantBudgets.Repeat(request, reader.GetString(0), new((TenantBudgetStatus)reader.GetInt32(1), reader.GetInt64(2)));
        }
        var current = await SqlServerTenantBudgetProtocol.ReadAsync(connection, transaction, _schema, request.Policy.Tenant, true, cancellationToken);
        var now = await SqlServerProtocol.NowAsync(connection, transaction, cancellationToken);
        var decision = TenantBudgets.Configure(request, current, now);
        if (decision.After is not null) await SqlServerTenantBudgetProtocol.PersistAsync(connection, transaction, _schema, current, decision.After, cancellationToken);
        await using var receipt = new SqlCommand($"INSERT INTO {_schema}.tenant_budget_requests(tenant_id,id,fingerprint,actor,reason,result_status,result_version,expected_version,requested_window_seconds,requested_max_claims,requested_max_retries,requested_max_recoveries) VALUES(@tenant,@id,@fingerprint,@actor,@reason,@status,@version,@expected,@seconds,@claims,@retries,@recoveries)", connection, transaction);
        receipt.Parameters.AddValue("tenant", request.Policy.Id); receipt.Parameters.AddValue("id", request.RequestId);
        receipt.Parameters.AddValue("fingerprint", request.Fingerprint); receipt.Parameters.AddValue("actor", request.Actor); receipt.Parameters.AddValue("reason", request.Reason);
        receipt.Parameters.AddValue("status", (int)decision.Result.Status); receipt.Parameters.AddValue("version", decision.Result.Version);
        receipt.Parameters.AddValue("expected", request.ExpectedVersion); receipt.Parameters.AddValue("seconds", request.Policy.WindowSeconds);
        SqlServerProtocol.Optional(receipt, "claims", System.Data.SqlDbType.BigInt, request.Policy.MaximumClaims); SqlServerProtocol.Optional(receipt, "retries", System.Data.SqlDbType.BigInt, request.Policy.MaximumRetries); SqlServerProtocol.Optional(receipt, "recoveries", System.Data.SqlDbType.BigInt, request.Policy.MaximumRecoveries);
        await receipt.ExecuteNonQueryCheckedAsync(cancellationToken);
        await SqlServerProtocol.CommitAsync(transaction, request.RequestId, cancellationToken);
        return decision.Result;
    }
}
