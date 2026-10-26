using Npgsql;
namespace EventGuard.Operations.PostgreSql;
public sealed partial class PostgreSqlWorkReader
{
    private string AuditUnion(bool outbox) => (outbox ? $"""
            SELECT id,2 AS kind,intent_id AS subject_id,left(actor,1024) AS actor,left(reason,1024) AS reason,result_status AS outcome,acknowledge_possible_duplicate AS acknowledge_possible_duplicate,created_at FROM {_schema}.outbox_replay_requests WHERE scope_id=@scope AND created_at>=@from AND created_at<@until
            UNION ALL SELECT id,3 AS kind,intent_id AS subject_id,left(actor,1024) AS actor,left(reason,1024) AS reason,result_status AS outcome,acknowledge_possible_duplicate AS acknowledge_possible_duplicate,created_at FROM {_schema}.outbox_repair_requests WHERE scope_id=@scope AND created_at>=@from AND created_at<@until
            """ : $"""
            SELECT id,0 AS kind,work_id AS subject_id,left(actor,1024) AS actor,left(reason,1024) AS reason,result_status AS outcome,false AS acknowledge_possible_duplicate,created_at FROM {_schema}.replay_requests WHERE scope_id=@scope AND created_at>=@from AND created_at<@until
            UNION ALL SELECT id,1 AS kind,quarantine_id AS subject_id,left(actor,1024) AS actor,left(reason,1024) AS reason,result_status AS outcome,false AS acknowledge_possible_duplicate,created_at FROM {_schema}.ingress_release_requests WHERE scope_id=@scope AND created_at>=@from AND created_at<@until
            """) + $"""

            UNION ALL SELECT id,4 AS kind,NULL::uuid AS subject_id,left(actor,1024) AS actor,left(reason,1024) AS reason,target AS outcome,false AS acknowledge_possible_duplicate,created_at FROM {_schema}.retention_requests WHERE scope_id=@scope AND created_at>=@from AND created_at<@until
            UNION ALL SELECT id,5 AS kind,NULL::uuid AS subject_id,left(actor,1024) AS actor,left(reason,1024) AS reason,0 AS outcome,acknowledge_possible_duplicate AS acknowledge_possible_duplicate,created_at FROM {_schema}.bulk_replay_requests WHERE scope_id=@scope AND created_at>=@from AND created_at<@until AND kind=@kind
            """;
    public async Task ValidateAuditAsync(bool consumer, bool outbox, CancellationToken ct = default)
    {
        if (!consumer && !outbox) throw new ArgumentException("Select an audit capability.");
        
        await using var version = _data.CreateCommand($"SELECT version FROM {_schema}.schema_version WHERE singleton");
        if (await version.ExecuteScalarAsync(ct) is not int number || number != 11)
            throw new InvalidOperationException("Audit requires the current migrated schema.");
        var tables = new List<string> { "retention_requests", "bulk_replay_requests" };
        if (consumer) tables.AddRange(["replay_requests", "ingress_release_requests"]);
        if (outbox) tables.AddRange(["outbox_replay_requests", "outbox_repair_requests"]);
        foreach (var table in tables)
        {
            await using var index = _data.CreateCommand($"SELECT EXISTS(SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=@schema AND c.relname=@index AND i.indisvalid AND i.indisready)");
            index.Parameters.AddWithValue("schema", _schema.Trim('"')); index.Parameters.AddWithValue("index", "ix_audit_" + table);
            if (await index.ExecuteScalarAsync(ct) is not true) throw new InvalidOperationException("Install the explicit audit read indexes first.");
        }
        // Compile and execute an empty time window under the actual read identity to validate column access.
        foreach (var outgoing in new[] { false, true }.Where(o => o ? outbox : consumer))
        {
            await using var access = _data.CreateCommand(AuditUnion(outgoing));
            access.Parameters.AddWithValue("scope", "audit-health"); access.Parameters.AddWithValue("from", DateTimeOffset.UnixEpoch);
            access.Parameters.AddWithValue("until", DateTimeOffset.UnixEpoch); access.Parameters.AddWithValue("kind", outgoing ? 1 : 0);
            await using var probe = await access.ExecuteReaderAsync(ct);
        }
    }
    public async Task<AuditPage> ListAuditAsync(AuditQuery query, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        
        var comparison = query.After is null ? "<" : ">";
        var order = query.After is null ? "DESC" : "ASC";
        var seek = query.Before is not null || query.After is not null
            ? $"WHERE created_at{comparison}=@created AND (created_at{comparison}@created OR (created_at=@created AND (kind{comparison}@positionKind OR (kind=@positionKind AND id{comparison}@id))))" : "";
        await using var command = _data.CreateCommand($"SELECT id,kind,subject_id,actor,reason,outcome,acknowledge_possible_duplicate,created_at FROM ({AuditUnion(query.Outbox)}) entries {seek} ORDER BY created_at {order},kind {order},id {order} LIMIT @take");
        command.Parameters.AddWithValue("scope", query.ScopeId); command.Parameters.AddWithValue("from", query.From);
        command.Parameters.AddWithValue("until", query.Until); command.Parameters.AddWithValue("kind", query.Outbox ? 1 : 0);
        command.Parameters.AddWithValue("take", query.PageSize + 1);
        if ((query.Before ?? query.After) is { } position)
        {
            command.Parameters.AddWithValue("created", position.CreatedAt.ToUniversalTime()); command.Parameters.AddWithValue("positionKind", (int)position.Kind);
            command.Parameters.AddWithValue("id", position.Id);
        }
        var rows = new List<AuditEntry>(); await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var kind = (AuditKind)reader.GetInt32(1);
            rows.Add(new(reader.GetGuid(0), kind, reader.IsDBNull(2) ? null : reader.GetGuid(2), reader.GetString(3), reader.GetString(4),
                AuditOutcomes.Read(kind, reader.GetInt32(5)), reader.GetBoolean(6), reader.GetFieldValue<DateTimeOffset>(7)));
        }
        return AuditPage.FromWindow(rows, query);
    }
}
