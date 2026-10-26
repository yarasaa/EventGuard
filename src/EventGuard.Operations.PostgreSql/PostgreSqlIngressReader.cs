using Npgsql;
namespace EventGuard.Operations.PostgreSql;

public sealed partial class PostgreSqlWorkReader
{
    private const string IngressColumns = "id,version,left(key_version,1024),left(fingerprint_version,1024),left(reason_code,64),released_work_id,data_purged_at IS NULL,created_at";
    public async Task ValidateRecoveryAsync(CancellationToken ct = default)
    {
        
        await using var version = _data.CreateCommand($"SELECT version FROM {_schema}.schema_version WHERE singleton");
        if (await version.ExecuteScalarAsync(ct) is not int schema || schema != 11)
            throw new InvalidOperationException("Recovery requires the current migrated schema.");
        // Compile all metadata columns using the actual read identity, without fetching raw data.
        await using var access = _data.CreateCommand($"SELECT {IngressColumns} FROM {_schema}.ingress_quarantine WHERE 1=0");
        await using var reader = await access.ExecuteReaderAsync(ct);
    }
    public async Task<IngressSummary?> FindIngressAsync(WorkScope scope, Guid id, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        
        await using var command = _data.CreateCommand($"SELECT {IngressColumns} FROM {_schema}.ingress_quarantine WHERE scope_id=@scope AND id=@id");
        command.Parameters.AddWithValue("scope", scope.Id); command.Parameters.AddWithValue("id", id);
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? ReadIngress(reader) : null;
    }
    public async Task<IngressPage> ListIngressAsync(IngressQuery query, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        var window = query.Window;
        
        await using var command = _data.CreateCommand($"""
            SELECT {IngressColumns} FROM {_schema}.ingress_quarantine
            WHERE scope_id=@scope AND created_at<=@cutoff
            {(query.Released is { } released ? released ? "AND released_work_id IS NOT NULL" : "AND released_work_id IS NULL" : "")}
            {(window.Filters.WorkId.HasValue ? "AND id=@lookup" : "")}
            {(window.Before is not null ? "AND created_at<=@created AND (created_at<@created OR id<@id)" : "")}
            {(window.After is not null ? "AND created_at>=@created AND (created_at>@created OR id>@id)" : "")}
            ORDER BY created_at {(window.After is null ? "DESC" : "ASC")},id {(window.After is null ? "DESC" : "ASC")} LIMIT @take
            """);
        command.Parameters.AddWithValue("scope", window.Scope.Id); command.Parameters.AddWithValue("cutoff", window.CreatedBefore);
        command.Parameters.AddWithValue("take", window.PageSize + 1);
        if (window.Filters.WorkId is { } lookup) command.Parameters.AddWithValue("lookup", lookup);
        if ((window.Before ?? window.After) is { } position)
        { command.Parameters.AddWithValue("created", position.CreatedAt); command.Parameters.AddWithValue("id", position.Id); }
        var rows = new List<IngressSummary>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct)) rows.Add(ReadIngress(reader));
        return IngressPage.FromWindow(rows, query);
    }
    private static IngressSummary ReadIngress(System.Data.Common.DbDataReader reader) => new(reader.GetGuid(0), reader.GetInt64(1),
        reader.GetString(2), reader.GetString(3), IngressReason(reader.GetString(4)), reader.IsDBNull(5) ? null : reader.GetGuid(5),
        reader.GetBoolean(6), reader.GetFieldValue<DateTimeOffset>(7));
    private static string IngressReason(string reason) => reason switch
    {
        "contract-version-unsupported" or "contract-name-mismatch" or "contract-invalid" or "invalid-message-id" or "null-body"
            or "contract-payload-too-large" or "missing-contract" or "wrong-contract" or "unsupported-contract-version" or "missing-contract-model" or "invalid-contract" => reason,
        _ => "mapping-rejected"
    };
}
