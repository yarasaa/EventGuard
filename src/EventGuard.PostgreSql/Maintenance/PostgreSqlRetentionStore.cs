using System.Text.Json;
using System.Data;
using Npgsql;
namespace EventGuard.PostgreSql;

public sealed partial class PostgreSqlWorkStore
{
    public async Task<RetentionResult> RetainAsync(RetentionRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);
        await PostgreSqlProtocol.LockAsync(connection, transaction, "retention:" + _schema + ":" + request.ScopeId + ":" + request.RequestId, cancellationToken);
        await using (var previous = new NpgsqlCommand($"SELECT fingerprint,purged_ids FROM {_schema}.retention_requests WHERE scope_id=@scope AND id=@id", connection, transaction))
        {
            previous.Parameters.AddWithValue("scope", request.ScopeId); previous.Parameters.AddWithValue("id", request.RequestId);
            await using var reader = await previous.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                var ids = JsonSerializer.Deserialize<Guid[]>(reader.GetString(1)) ?? throw new InvalidOperationException("Invalid retention receipt.");
                if (ids.Length > 1000) throw new InvalidOperationException("Retention receipt exceeds its bound.");
                return Retention.Repeat(request, reader.GetString(0), Array.AsReadOnly(ids));
            }
        }
        var now = await PostgreSqlProtocol.NowAsync(connection, transaction, cancellationToken);
        Retention.ValidateWindow(request, now);
        var plan = RetentionPlan(request.Target);
        var selections = request.Target == RetentionTarget.ConsumerPayload
            ? new[] { plan.Select, RetentionPlan(request.Target, expired: true).Select } : new[] { plan.Select };
        var rows = new List<(Guid Id, long Version, RetentionCandidate Candidate)>();
        foreach (var selection in selections)
        {
        if (rows.Count == request.BatchSize) break;
        await using (var select = new NpgsqlCommand(selection, connection, transaction))
        {
            select.Parameters.AddWithValue("scope", request.ScopeId); select.Parameters.AddWithValue("before", request.Before); select.Parameters.AddWithValue("take", request.BatchSize - rows.Count);
            await using var reader = await select.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
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
            await using var update = new NpgsqlCommand(plan.Update, connection, transaction);
            update.Parameters.AddWithValue("id", row.Id); update.Parameters.AddWithValue("scope", request.ScopeId);
            update.Parameters.AddWithValue("version", row.Version); update.Parameters.AddWithValue("now", now);
            if (await update.ExecuteNonQueryAsync(cancellationToken) != 1) throw new InvalidOperationException("Retention candidate changed under lock.");
            purged.Add(row.Id);
        }
        await using var receipt = new NpgsqlCommand($"INSERT INTO {_schema}.retention_requests(scope_id,id,fingerprint,target,actor,reason,purged_ids) VALUES(@scope,@id,@fingerprint,@target,@actor,@reason,@ids)", connection, transaction);
        receipt.Parameters.AddWithValue("scope", request.ScopeId); receipt.Parameters.AddWithValue("id", request.RequestId);
        receipt.Parameters.AddWithValue("fingerprint", request.Fingerprint); receipt.Parameters.AddWithValue("target", (int)request.Target);
        receipt.Parameters.AddWithValue("actor", request.Actor); receipt.Parameters.AddWithValue("reason", request.Reason);
        receipt.Parameters.AddWithValue("ids", NpgsqlTypes.NpgsqlDbType.Jsonb, JsonSerializer.Serialize(purged));
        await receipt.ExecuteNonQueryAsync(cancellationToken);
        await PostgreSqlProtocol.CommitAsync(transaction, request.RequestId, cancellationToken);
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
                update = $"UPDATE {_schema}.works SET payload=decode('','hex'),payload_purged_at=@now,version=version+1 WHERE scope_id=@scope AND id=@id AND version=@version AND payload_purged_at IS NULL";
                break;
            case RetentionTarget.PublishedOutboxPayload:
                table = "outbox_messages"; fields = "r.id,r.version,r.state,r.published_at AS terminal_at,r.created_at";
                joins = ""; filter = "r.scope_id=@scope AND r.state=3 AND r.payload_purged_at IS NULL AND r.published_at<=@before";
                update = $"UPDATE {_schema}.outbox_messages SET payload=decode('','hex'),headers='{{}}',payload_purged_at=@now,version=version+1 WHERE scope_id=@scope AND id=@id AND version=@version AND payload_purged_at IS NULL AND state=3";
                break;
            case RetentionTarget.ConflictPayload:
                table = "conflicts"; fields = $"r.id,CAST(0 AS bigint),w.state,{terminal} AS terminal_at,r.received_at";
                joins = $" JOIN {_schema}.works w ON w.id=r.work_id";
                filter = $"w.scope_id=@scope AND r.payload_purged_at IS NULL AND r.received_at<=@before AND {workFilter}";
                update = $"UPDATE {_schema}.conflicts SET payload=decode('','hex'),payload_purged_at=@now WHERE id=@id AND payload_purged_at IS NULL";
                break;
            case RetentionTarget.ReleasedIngressData:
                table = "ingress_quarantine"; fields = "r.id,r.version,0,r.released_at AS terminal_at,r.created_at";
                joins = ""; filter = "r.scope_id=@scope AND r.data_purged_at IS NULL AND r.released_work_id IS NOT NULL AND r.released_at<=@before";
                update = $"UPDATE {_schema}.ingress_quarantine SET body=decode('','hex'),metadata=decode('','hex'),data_purged_at=@now,version=version+1 WHERE scope_id=@scope AND id=@id AND version=@version AND data_purged_at IS NULL AND released_work_id IS NOT NULL";
                break;
            case RetentionTarget.PublishedOutboxQuarantineData:
                table = "outbox_quarantine"; fields = "r.id,CAST(0 AS bigint),o.state,o.published_at AS terminal_at,r.detected_at";
                joins = $" JOIN {_schema}.outbox_messages o ON o.scope_id=r.scope_id AND o.id=r.intent_id";
                filter = "r.scope_id=@scope AND r.data_purged_at IS NULL AND o.state=3 AND o.published_at<=@before AND r.detected_at<=@before";
                update = $"UPDATE {_schema}.outbox_quarantine SET original_record='{{}}',data_purged_at=@now WHERE scope_id=@scope AND id=@id AND data_purged_at IS NULL";
                break;
            default: throw new ArgumentOutOfRangeException(nameof(target));
        }
        var select = $"SELECT {fields} FROM {_schema}.{table} r{joins} WHERE {filter} ORDER BY terminal_at,r.id LIMIT @take FOR UPDATE OF r SKIP LOCKED";
        return (select, update);
    }
}
