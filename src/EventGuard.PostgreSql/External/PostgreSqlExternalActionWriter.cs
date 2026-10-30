using Npgsql;
using NpgsqlTypes;

namespace EventGuard.PostgreSql;

internal sealed class PostgreSqlExternalActionWriter(NpgsqlConnection connection, NpgsqlTransaction transaction,
    string schema, Guid? originWorkId, ProducerRequest? producerRequest) : IExternalActionResultWriter, IExternalActionBatchWriter
{
    public Task<OutboxEnqueueStatus> EnqueueAsync(ExternalAction action, CancellationToken cancellationToken = default) =>
        EnqueueCoreAsync(action, null, cancellationToken);

    public Task<OutboxEnqueueStatus> EnqueueAsync(ExternalAction action, ExternalResultTarget target, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(target);
        return EnqueueCoreAsync(action, target, cancellationToken);
    }

    public async Task<IReadOnlyList<ExternalActionEnqueueResult>> EnqueueBatchAsync(ExternalActionEnqueueBatch batch,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(batch); _ = transaction.IsolationLevel;
        if (!ReferenceEquals(transaction.Connection,connection)) throw new InvalidOperationException("The owning transaction is no longer active.");
        var scope = batch.Items[0].Action.Scope;
        if (producerRequest is not null && scope != producerRequest.Scope) throw new InvalidOperationException("External action must use the producer request scope.");
        if (originWorkId is Guid work)
        {
            await using var owner = new NpgsqlCommand($"SELECT b.tenant FROM {schema}.works w JOIN {schema}.policy_bindings b ON b.scope_id=w.scope_id WHERE w.id=@id",connection,transaction);
            owner.Parameters.AddWithValue("id",work);
            if (await owner.ExecuteScalarAsync(cancellationToken) as string != scope.Tenant) throw new InvalidOperationException("External action must use the work tenant.");
        }
        using var table = new System.Data.DataTable();
        table.Columns.Add("id",typeof(Guid));
        table.Columns.Add("binding_name",typeof(string));
        table.Columns.Add("binding_version",typeof(string));
        table.Columns.Add("payload",typeof(byte[]));
        table.Columns.Add("observation_timeout",typeof(long));
        table.Columns.Add("fingerprint",typeof(string));
        table.Columns.Add("result_handler",typeof(string));
        table.Columns.Add("result_handler_version",typeof(string));
        var fingerprints = new Dictionary<Guid,string>();
        foreach (var item in batch.Items)
        {
            var a = item.Action; var target = item.ResultTarget;
            var fingerprint = Fingerprint(a,originWorkId,producerRequest?.RequestId,target);
            fingerprints.Add(a.Id,fingerprint);
            table.Rows.Add(a.Id,a.Binding,a.BindingVersion,a.CopyPayload(),
                a.ObservationTimeout is { } time ? (long)time.TotalMilliseconds : DBNull.Value,
                fingerprint,(object?)target?.Name ?? DBNull.Value,(object?)target?.HandlerVersion ?? DBNull.Value);
        }
        await using var command = new NpgsqlCommand { Connection = connection, Transaction = transaction };
        var input = PostgreSqlWorkStore.AddExternalTable(command,table);
        command.Parameters.AddWithValue("scope",scope.Id); command.Parameters.AddWithValue("tenant",scope.Tenant);
        command.Parameters.AddWithValue("producer",scope.Producer); command.Parameters.AddWithValue("prefix","external-intent:"+schema+":"+scope.Id+":");
        PostgreSqlProtocol.Optional(command,"work",NpgsqlDbType.Uuid,originWorkId);
        PostgreSqlProtocol.Optional(command,"request",NpgsqlDbType.Uuid,producerRequest?.RequestId);
        command.CommandText = $"""
            WITH ordered AS MATERIALIZED (SELECT id FROM ({input}) x ORDER BY id::text)
            SELECT pg_advisory_xact_lock(hashtextextended(@prefix||id::text,0)) FROM ordered;
            """;
        await command.ExecuteNonQueryAsync(cancellationToken);
        command.CommandText = $"SELECT a.id,a.fingerprint FROM {schema}.external_actions a JOIN ({input}) x ON x.id=a.id WHERE a.scope_id=@scope";
        var existing = new HashSet<Guid>();
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            while (await reader.ReadAsync(cancellationToken))
            {
                var id=reader.GetGuid(0);
                if (reader.GetString(1)!=fingerprints[id]) throw new ExternalActionConflictException(id);
                existing.Add(id);
            }
        command.CommandText = $"""
            INSERT INTO {schema}.external_actions(scope_id,id,tenant,producer,binding_name,binding_version,payload,
                observation_timeout,fingerprint,origin_work_id,origin_request_id,result_handler,result_handler_version)
            SELECT @scope,x.id,@tenant,@producer,x.binding_name,x.binding_version,x.payload,
                x.observation_timeout,x.fingerprint,@work,@request,x.result_handler,x.result_handler_version FROM ({input}) x
            WHERE NOT EXISTS(SELECT 1 FROM {schema}.external_actions a WHERE a.scope_id=@scope AND a.id=x.id)
            """;
        await command.ExecuteNonQueryAsync(cancellationToken);
        return Array.AsReadOnly(batch.Items.Select(i => new ExternalActionEnqueueResult(i.Action.Id,
            existing.Contains(i.Action.Id) ? OutboxEnqueueStatus.AlreadyEnqueued : OutboxEnqueueStatus.Enqueued)).ToArray());
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
            await using var owner = new NpgsqlCommand($"SELECT b.tenant FROM {schema}.works w JOIN {schema}.policy_bindings b ON b.scope_id=w.scope_id WHERE w.id=@id", connection, transaction);
            owner.Parameters.AddWithValue("id", work);
            if (await owner.ExecuteScalarAsync(cancellationToken) as string != action.Scope.Tenant)
                throw new InvalidOperationException("External action must use the work tenant.");
        }
        var fingerprint = Fingerprint(action, originWorkId, producerRequest?.RequestId, target);
        await PostgreSqlProtocol.LockAsync(connection, transaction, "external-intent:" + schema + ":" + action.Scope.Id + ":" + action.Id, cancellationToken);
        await using (var existing = new NpgsqlCommand($"SELECT fingerprint FROM {schema}.external_actions WHERE scope_id=@scope AND id=@id", connection, transaction))
        {
            existing.Parameters.AddWithValue("scope", action.Scope.Id); existing.Parameters.AddWithValue("id", action.Id);
            if (await existing.ExecuteScalarAsync(cancellationToken) is string saved)
            {
                if (saved != fingerprint) throw new ExternalActionConflictException(action.Id);
                return OutboxEnqueueStatus.AlreadyEnqueued;
            }
        }
        await using var insert = new NpgsqlCommand($"""
            INSERT INTO {schema}.external_actions(scope_id,id,tenant,producer,binding_name,binding_version,payload,
                observation_timeout,fingerprint,origin_work_id,origin_request_id,result_handler,result_handler_version)
            VALUES(@scope,@id,@tenant,@producer,@binding,@binding_version,@payload,@timeout,@fingerprint,@work,@request,@result_handler,@result_handler_version)
            """, connection, transaction);
        insert.Parameters.AddWithValue("scope", action.Scope.Id); insert.Parameters.AddWithValue("id", action.Id);
        insert.Parameters.AddWithValue("tenant", action.Scope.Tenant); insert.Parameters.AddWithValue("producer", action.Scope.Producer);
        insert.Parameters.AddWithValue("binding", action.Binding); insert.Parameters.AddWithValue("binding_version", action.BindingVersion);
        insert.Parameters.AddWithValue("payload", action.CopyPayload()); insert.Parameters.AddWithValue("fingerprint", fingerprint);
        PostgreSqlProtocol.Optional(insert, "timeout", NpgsqlDbType.Bigint, action.ObservationTimeout is { } timeout ? (long)timeout.TotalMilliseconds : null);
        PostgreSqlProtocol.Optional(insert, "work", NpgsqlDbType.Uuid, originWorkId);
        PostgreSqlProtocol.Optional(insert, "request", NpgsqlDbType.Uuid, producerRequest?.RequestId);
        PostgreSqlProtocol.Optional(insert, "result_handler", NpgsqlDbType.Text, target?.Name);
        PostgreSqlProtocol.Optional(insert, "result_handler_version", NpgsqlDbType.Text, target?.HandlerVersion);
        await insert.ExecuteNonQueryAsync(cancellationToken);
        return OutboxEnqueueStatus.Enqueued;
    }

    internal static string Fingerprint(ExternalAction action, Guid? work, Guid? request, ExternalResultTarget? target = null)
    {
        var origin = StableHash.Of("external/origin/v1", action.Fingerprint, work?.ToString("D") ?? "", request?.ToString("D") ?? "");
        return target is null ? origin : StableHash.Of("external/origin-results/v1", origin, target.Id);
    }
}
