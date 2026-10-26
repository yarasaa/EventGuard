using System.Text.Json;
using System.Data;
using Microsoft.Data.SqlClient;
namespace EventGuard.SqlServer;

public sealed partial class SqlServerWorkStore
{
    public async Task<RetentionResult> RetainAsync(RetentionRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginLocalAsync(IsolationLevel.RepeatableRead, cancellationToken);
        await SqlServerProtocol.LockAsync(connection, transaction, "retention:" + _schema + ":" + request.ScopeId + ":" + request.RequestId, cancellationToken);
        await using (var previous = new SqlCommand($"SELECT fingerprint,purged_ids FROM {_schema}.retention_requests WHERE scope_id=@scope AND id=@id", connection, transaction))
        {
            previous.Parameters.AddValue("scope", request.ScopeId); previous.Parameters.AddValue("id", request.RequestId);
            await using var reader = await previous.ExecuteReaderCheckedAsync(cancellationToken);
            if (await reader.ReadCheckedAsync(cancellationToken))
            {
                var ids = JsonSerializer.Deserialize<Guid[]>(reader.GetString(1)) ?? throw new InvalidOperationException("Invalid retention receipt.");
                if (ids.Length > 1000) throw new InvalidOperationException("Retention receipt exceeds its bound.");
                return Retention.Repeat(request, reader.GetString(0), Array.AsReadOnly(ids));
            }
        }
        var now = await SqlServerProtocol.NowAsync(connection, transaction, cancellationToken);
        Retention.ValidateWindow(request, now);
        var plan = RetentionPlan(request.Target);
        var selections = request.Target == RetentionTarget.ConsumerPayload
            ? new[] { plan.Select, RetentionPlan(request.Target, expired: true).Select } : new[] { plan.Select };
        var rows = new List<(Guid Id, long Version, RetentionCandidate Candidate)>();
        foreach (var selection in selections)
        {
        if (rows.Count == request.BatchSize) break;
        await using (var select = new SqlCommand(selection, connection, transaction))
        {
            select.Parameters.AddValue("scope", request.ScopeId); select.Parameters.AddValue("before", request.Before); select.Parameters.AddValue("take", request.BatchSize - rows.Count);
            await using var reader = await select.ExecuteReaderCheckedAsync(cancellationToken);
            while (await reader.ReadCheckedAsync(cancellationToken))
            {
                var state = reader.GetInt32(2);
                WorkState? workState = request.Target is RetentionTarget.ConsumerPayload or RetentionTarget.ConflictPayload ? (WorkState)state : null;
                OutboxState? outboxState = request.Target is RetentionTarget.PublishedOutboxPayload or RetentionTarget.PublishedOutboxQuarantineData ? (OutboxState)state : null;
                rows.Add((reader.GetGuid(0), reader.GetInt64(1), new(reader.GetGuid(0), false, workState, outboxState,
                    reader.IsDBNull(3) ? null : reader.GetFieldValue<DateTimeOffset>(3), reader.GetFieldValue<DateTimeOffset>(4))));
            }
        }
        }
        var purged = new List<Guid>();
        foreach (var row in rows)
        {
            if (!Retention.CanPurge(request, row.Candidate, now)) continue;
            await using var update = new SqlCommand(plan.Update, connection, transaction);
            update.Parameters.AddValue("id", row.Id); update.Parameters.AddValue("scope", request.ScopeId);
            update.Parameters.AddValue("version", row.Version); update.Parameters.AddValue("now", now);
            if (await update.ExecuteNonQueryCheckedAsync(cancellationToken) != 1) throw new InvalidOperationException("Retention candidate changed under lock.");
            purged.Add(row.Id);
        }
        await using var receipt = new SqlCommand($"INSERT INTO {_schema}.retention_requests(scope_id,id,fingerprint,target,actor,reason,purged_ids) VALUES(@scope,@id,@fingerprint,@target,@actor,@reason,@ids)", connection, transaction);
        receipt.Parameters.AddValue("scope", request.ScopeId); receipt.Parameters.AddValue("id", request.RequestId);
        receipt.Parameters.AddValue("fingerprint", request.Fingerprint); receipt.Parameters.AddValue("target", (int)request.Target);
        receipt.Parameters.AddValue("actor", request.Actor); receipt.Parameters.AddValue("reason", request.Reason);
        receipt.Parameters.AddValue("ids", JsonSerializer.Serialize(purged));
        await receipt.ExecuteNonQueryCheckedAsync(cancellationToken);
        await SqlServerProtocol.CommitAsync(transaction, request.RequestId, cancellationToken);
        return new(RetentionStatus.Completed, purged.AsReadOnly());
    }

    private (string Select, string Update) RetentionPlan(RetentionTarget target, bool expired = false)
    {
        const string terminal = "CASE WHEN w.state=3 THEN w.completed_at ELSE w.expires_at END";
        const string workFilter = "((w.state=3 AND w.completed_at<=@before) OR (w.state IN (4,5) AND w.expires_at<=@before))";
        string table, fields, joins, filter, update;
        switch (target)
        {
            case RetentionTarget.ConsumerPayload:
                table = "works";
                var terminalColumn = expired ? "r.expires_at" : "r.completed_at";
                fields = $"r.id,r.version,r.state,{terminalColumn} AS terminal_at,r.created_at";
                joins = ""; filter = $"r.scope_id=@scope AND r.payload_purged_at IS NULL AND r.created_at<=@before AND {(expired ? "r.state IN (4,5)" : "r.state=3")} AND {terminalColumn}<=@before";
                update = $"UPDATE {_schema}.works SET payload=0x,payload_purged_at=@now,version=version+1 WHERE scope_id=@scope AND id=@id AND version=@version AND payload_purged_at IS NULL";
                break;
            case RetentionTarget.PublishedOutboxPayload:
                table = "outbox_messages"; fields = "r.id,r.version,r.state,r.published_at AS terminal_at,r.created_at";
                joins = ""; filter = "r.scope_id=@scope AND r.state=3 AND r.payload_purged_at IS NULL AND r.published_at<=@before";
                update = $"UPDATE {_schema}.outbox_messages SET payload=0x,headers='{{}}',payload_purged_at=@now,version=version+1 WHERE scope_id=@scope AND id=@id AND version=@version AND payload_purged_at IS NULL AND state=3";
                break;
            case RetentionTarget.ConflictPayload:
                table = "conflicts"; fields = $"r.id,CAST(0 AS bigint),w.state,{terminal} AS terminal_at,r.received_at";
                joins = $" JOIN {_schema}.works w ON w.id=r.work_id";
                filter = $"w.scope_id=@scope AND r.payload_purged_at IS NULL AND r.received_at<=@before AND {workFilter}";
                update = $"UPDATE {_schema}.conflicts SET payload=0x,payload_purged_at=@now WHERE id=@id AND payload_purged_at IS NULL";
                break;
            case RetentionTarget.ReleasedIngressData:
                table = "ingress_quarantine"; fields = "r.id,r.version,0,r.released_at AS terminal_at,r.created_at";
                joins = ""; filter = "r.scope_id=@scope AND r.data_purged_at IS NULL AND r.released_work_id IS NOT NULL AND r.released_at<=@before";
                update = $"UPDATE {_schema}.ingress_quarantine SET body=0x,metadata=0x,data_purged_at=@now,version=version+1 WHERE scope_id=@scope AND id=@id AND version=@version AND data_purged_at IS NULL AND released_work_id IS NOT NULL";
                break;
            case RetentionTarget.PublishedOutboxQuarantineData:
                table = "outbox_quarantine"; fields = "r.id,CAST(0 AS bigint),o.state,o.published_at AS terminal_at,r.detected_at";
                joins = $" JOIN {_schema}.outbox_messages o ON o.scope_id=r.scope_id AND o.id=r.intent_id";
                filter = "r.scope_id=@scope AND r.data_purged_at IS NULL AND o.state=3 AND o.published_at<=@before AND r.detected_at<=@before";
                update = $"UPDATE {_schema}.outbox_quarantine SET original_record='{{}}',data_purged_at=@now WHERE scope_id=@scope AND id=@id AND data_purged_at IS NULL";
                break;
            default: throw new ArgumentOutOfRangeException(nameof(target));
        }
        var select = $"SELECT TOP(@take) {fields} FROM {_schema}.{table} r WITH(UPDLOCK,READPAST,ROWLOCK){joins} WHERE {filter} ORDER BY terminal_at,r.id OPTION(MAXDOP 1)";
        return (select, update);
    }
}
