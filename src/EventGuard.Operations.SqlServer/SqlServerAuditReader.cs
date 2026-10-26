using Microsoft.Data.SqlClient;
namespace EventGuard.Operations.SqlServer;
public sealed partial class SqlServerWorkReader
{
    private string AuditUnion(bool outbox) => (outbox ? $"""
            SELECT id,2 AS kind,intent_id AS subject_id,left(actor,1024) AS actor,left(reason,1024) AS reason,result_status AS outcome,acknowledge_possible_duplicate AS acknowledge_possible_duplicate,created_at FROM {_schema}.outbox_replay_requests WHERE scope_id=@scope AND created_at>=@from AND created_at<@until
            UNION ALL SELECT id,3 AS kind,intent_id AS subject_id,left(actor,1024) AS actor,left(reason,1024) AS reason,result_status AS outcome,acknowledge_possible_duplicate AS acknowledge_possible_duplicate,created_at FROM {_schema}.outbox_repair_requests WHERE scope_id=@scope AND created_at>=@from AND created_at<@until
            """ : $"""
            SELECT id,0 AS kind,work_id AS subject_id,left(actor,1024) AS actor,left(reason,1024) AS reason,result_status AS outcome,CAST(0 AS bit) AS acknowledge_possible_duplicate,created_at FROM {_schema}.replay_requests WHERE scope_id=@scope AND created_at>=@from AND created_at<@until
            UNION ALL SELECT id,1 AS kind,quarantine_id AS subject_id,left(actor,1024) AS actor,left(reason,1024) AS reason,result_status AS outcome,CAST(0 AS bit) AS acknowledge_possible_duplicate,created_at FROM {_schema}.ingress_release_requests WHERE scope_id=@scope AND created_at>=@from AND created_at<@until
            """) + $"""

            UNION ALL SELECT id,4 AS kind,CAST(NULL AS uniqueidentifier) AS subject_id,left(actor,1024) AS actor,left(reason,1024) AS reason,target AS outcome,CAST(0 AS bit) AS acknowledge_possible_duplicate,created_at FROM {_schema}.retention_requests WHERE scope_id=@scope AND created_at>=@from AND created_at<@until
            UNION ALL SELECT id,5 AS kind,CAST(NULL AS uniqueidentifier) AS subject_id,left(actor,1024) AS actor,left(reason,1024) AS reason,0 AS outcome,acknowledge_possible_duplicate AS acknowledge_possible_duplicate,created_at FROM {_schema}.bulk_replay_requests WHERE scope_id=@scope AND created_at>=@from AND created_at<@until AND kind=@kind
            """;
    public async Task ValidateAuditAsync(bool consumer, bool outbox, CancellationToken ct = default)
    {
        if (!consumer && !outbox) throw new ArgumentException("Select an audit capability.");
        await using var connection = await OpenAsync(ct);
        await using var version = new SqlCommand($"SELECT version FROM {_schema}.schema_version WHERE singleton=1", connection) { CommandTimeout = 2 };
        if (await version.ExecuteScalarAsync(ct) is not int number || number != 8)
            throw new InvalidOperationException("Audit requires the current migrated schema.");
        var tables = new List<string> { "retention_requests", "bulk_replay_requests" };
        if (consumer) tables.AddRange(["replay_requests", "ingress_release_requests"]);
        if (outbox) tables.AddRange(["outbox_replay_requests", "outbox_repair_requests"]);
        foreach (var table in tables)
        {
            await using var index = new SqlCommand("SELECT CAST(CASE WHEN EXISTS(SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(@table) AND name=@index AND is_disabled=0) THEN 1 ELSE 0 END AS bit)", connection) { CommandTimeout = 2 };
            index.Parameters.AddWithValue("table", _schema + "." + table); index.Parameters.AddWithValue("index", "ix_audit_" + table);
            if (await index.ExecuteScalarAsync(ct) is not true) throw new InvalidOperationException("Install the explicit audit read indexes first.");
        }
        // Compile and execute an empty time window under the actual read identity to validate column access.
        foreach (var outgoing in new[] { false, true }.Where(o => o ? outbox : consumer))
        {
            await using var access = new SqlCommand(AuditUnion(outgoing), connection) { CommandTimeout = 2 };
            access.Parameters.AddWithValue("scope", "audit-health"); access.Parameters.AddWithValue("from", DateTimeOffset.UnixEpoch);
            access.Parameters.AddWithValue("until", DateTimeOffset.UnixEpoch); access.Parameters.AddWithValue("kind", outgoing ? 1 : 0);
            await using var probe = await access.ExecuteReaderAsync(ct);
        }
    }
    public async Task<AuditPage> ListAuditAsync(AuditQuery query, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        await using var connection = await OpenAsync(ct);
        var comparison = query.After is null ? "<" : ">";
        var order = query.After is null ? "DESC" : "ASC";
        var seek = query.Before is not null || query.After is not null
            ? $"WHERE created_at{comparison}=@created AND (created_at{comparison}@created OR (created_at=@created AND (kind{comparison}@positionKind OR (kind=@positionKind AND id{comparison}@id))))" : "";
        await using var command = new SqlCommand($"SELECT TOP (@take) id,kind,subject_id,actor,reason,outcome,acknowledge_possible_duplicate,created_at FROM ({AuditUnion(query.Outbox)}) entries {seek} ORDER BY created_at {order},kind {order},id {order}", connection) { CommandTimeout = 2 };
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
