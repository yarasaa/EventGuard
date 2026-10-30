using Microsoft.Data.SqlClient;
using System.Data;

namespace EventGuard.SqlServer;

internal sealed class SqlServerExternalActionWriter(SqlConnection connection, SqlTransaction transaction,
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
            await using var owner = new SqlCommand($"SELECT b.tenant FROM {schema}.works w JOIN {schema}.policy_bindings b ON b.scope_id=w.scope_id WHERE w.id=@id",connection,transaction);
            owner.Parameters.AddWithValue("id",work);
            if (await owner.ExecuteScalarCheckedAsync(cancellationToken) as string != scope.Tenant) throw new InvalidOperationException("External action must use the work tenant.");
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
        await using var command = new SqlCommand { Connection = connection, Transaction = transaction };
        command.Parameters.Add(new SqlParameter("rows", System.Data.SqlDbType.Structured) { TypeName = schema + ".external_action_inputs", Value = table });
        const string input = "SELECT * FROM @rows";
        command.Parameters.AddWithValue("scope",scope.Id); command.Parameters.AddWithValue("tenant",scope.Tenant);
        command.Parameters.AddWithValue("producer",scope.Producer); command.Parameters.AddWithValue("prefix","external-intent:"+schema+":"+scope.Id+":");
        SqlServerProtocol.Optional(command,"work",System.Data.SqlDbType.UniqueIdentifier,originWorkId);
        SqlServerProtocol.Optional(command,"request",System.Data.SqlDbType.UniqueIdentifier,producerRequest?.RequestId);
        command.CommandText = $"""
            DECLARE @id uniqueidentifier,@resource nvarchar(255),@result int;
            DECLARE action_keys CURSOR LOCAL FAST_FORWARD FOR SELECT id FROM @rows ORDER BY CONVERT(char(36),id);
            OPEN action_keys; FETCH NEXT FROM action_keys INTO @id;
            WHILE @@FETCH_STATUS=0 BEGIN
                SET @resource=@prefix+LOWER(CONVERT(char(36),@id));
                EXEC @result=sys.sp_getapplock @Resource=@resource,@LockMode='Exclusive',@LockOwner='Transaction',@LockTimeout=15000;
                IF @result<0 THROW 51002,'External intent lock unavailable.',1;
                FETCH NEXT FROM action_keys INTO @id;
            END;
            CLOSE action_keys; DEALLOCATE action_keys;
            """;
        await command.ExecuteNonQueryCheckedAsync(cancellationToken);
        command.CommandText = $"SELECT a.id,a.fingerprint FROM {schema}.external_actions a JOIN ({input}) x ON x.id=a.id WHERE a.scope_id=@scope";
        var existing = new HashSet<Guid>();
        await using (var reader = await command.ExecuteReaderCheckedAsync(cancellationToken))
            while (await reader.ReadCheckedAsync(cancellationToken))
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
        await command.ExecuteNonQueryCheckedAsync(cancellationToken);
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
