using Microsoft.Data.SqlClient;
using System.Data;

namespace EventGuard.SqlServer;

internal sealed class SqlServerExternalActionWriter(SqlConnection connection, SqlTransaction transaction,
    string schema, Guid? originWorkId, ProducerRequest? producerRequest) : IExternalActionResultWriter
{
    public Task<OutboxEnqueueStatus> EnqueueAsync(ExternalAction action, CancellationToken cancellationToken = default) =>
        EnqueueCoreAsync(action, null, cancellationToken);

    public Task<OutboxEnqueueStatus> EnqueueAsync(ExternalAction action, ExternalResultTarget target, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(target);
        return EnqueueCoreAsync(action, target, cancellationToken);
    }

    private async Task<OutboxEnqueueStatus> EnqueueCoreAsync(ExternalAction action, ExternalResultTarget? target, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(action);
        if (target is not null && target.Scope != action.Scope) throw new InvalidOperationException("Result target must use the action producer scope.");
        _ = transaction.IsolationLevel;
        if (!ReferenceEquals(transaction.Connection, connection)) throw new InvalidOperationException("The owning transaction is no longer active.");
        if (producerRequest is not null && action.Scope != producerRequest.Scope)
            throw new InvalidOperationException("External action must use the producer request scope.");
        if (originWorkId is Guid work)
        {
            await using var owner = new SqlCommand($"SELECT b.tenant FROM {schema}.works w JOIN {schema}.policy_bindings b ON b.scope_id=w.scope_id WHERE w.id=@id", connection, transaction);
            owner.Parameters.AddValue("id", work);
            if (await owner.ExecuteScalarCheckedAsync(cancellationToken) as string != action.Scope.Tenant)
                throw new InvalidOperationException("External action must use the work tenant.");
        }
        var fingerprint = Fingerprint(action, originWorkId, producerRequest?.RequestId, target);
        await SqlServerProtocol.LockAsync(connection, transaction, "external-intent:" + schema + ":" + action.Scope.Id + ":" + action.Id, cancellationToken);
        await using (var existing = new SqlCommand($"SELECT fingerprint FROM {schema}.external_actions WHERE scope_id=@scope AND id=@id", connection, transaction))
        {
            existing.Parameters.AddValue("scope", action.Scope.Id); existing.Parameters.AddValue("id", action.Id);
            if (await existing.ExecuteScalarCheckedAsync(cancellationToken) is string saved)
            {
                if (saved != fingerprint) throw new ExternalActionConflictException(action.Id);
                return OutboxEnqueueStatus.AlreadyEnqueued;
            }
        }
        await using var insert = new SqlCommand($"""
            INSERT INTO {schema}.external_actions(scope_id,id,tenant,producer,binding_name,binding_version,payload,
                observation_timeout,fingerprint,origin_work_id,origin_request_id,result_handler,result_handler_version)
            VALUES(@scope,@id,@tenant,@producer,@binding,@binding_version,@payload,@timeout,@fingerprint,@work,@request,@result_handler,@result_handler_version)
            """, connection, transaction);
        insert.Parameters.AddValue("scope", action.Scope.Id); insert.Parameters.AddValue("id", action.Id);
        insert.Parameters.AddValue("tenant", action.Scope.Tenant); insert.Parameters.AddValue("producer", action.Scope.Producer);
        insert.Parameters.AddValue("binding", action.Binding); insert.Parameters.AddValue("binding_version", action.BindingVersion);
        insert.Parameters.AddValue("payload", action.CopyPayload()); insert.Parameters.AddValue("fingerprint", fingerprint);
        SqlServerProtocol.Optional(insert, "timeout", SqlDbType.BigInt, action.ObservationTimeout is { } timeout ? (long)timeout.TotalMilliseconds : null);
        SqlServerProtocol.Optional(insert, "work", SqlDbType.UniqueIdentifier, originWorkId);
        SqlServerProtocol.Optional(insert, "request", SqlDbType.UniqueIdentifier, producerRequest?.RequestId);
        SqlServerProtocol.Optional(insert, "result_handler", SqlDbType.NVarChar, target?.Name);
        SqlServerProtocol.Optional(insert, "result_handler_version", SqlDbType.NVarChar, target?.HandlerVersion);
        await insert.ExecuteNonQueryCheckedAsync(cancellationToken);
        return OutboxEnqueueStatus.Enqueued;
    }

    internal static string Fingerprint(ExternalAction action, Guid? work, Guid? request, ExternalResultTarget? target = null)
    {
        var origin = StableHash.Of("external/origin/v1", action.Fingerprint, work?.ToString("D") ?? "", request?.ToString("D") ?? "");
        return target is null ? origin : StableHash.Of("external/origin-results/v1", origin, target.Id);
    }
}
