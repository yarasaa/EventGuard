using Npgsql;

namespace EventGuard.Operations.PostgreSql;

/// <summary>Dedicated bounded read pool. Supply a SELECT-only database role in deployments.</summary>
public sealed partial class PostgreSqlWorkReader : IStoreHealthCheck, IWorkReadStore, IWorkDataReadStore, IIngressReadStore, IRecoveryHealthCheck, IAuditReadStore, IAsyncDisposable
{
    private const string Columns = "id,state,version,attempt_count,replay_generation,created_at,next_attempt_at,expires_at,completed_at,left(last_reason,64)";
    private readonly NpgsqlDataSource _data;
    private readonly string _schema;

    public PostgreSqlWorkReader(string connectionString, string schema = "eventguard")
    {
        _schema = OperationsIndexes.Quote(schema);
        var options = new NpgsqlConnectionStringBuilder(connectionString)
        {
            Pooling = true, Multiplexing = false, MaxPoolSize = 4, MinPoolSize = 0, Timeout = 3, CommandTimeout = 2, Enlist = false,
            ApplicationName = "EventGuard.Operations",
            Options = "-c default_transaction_read_only=on -c statement_timeout=2000 -c lock_timeout=1000"
        };
        _data = NpgsqlDataSource.Create(options.ConnectionString);
    }

    public async Task CheckHealthAsync(bool consumer, bool outbox, CancellationToken ct = default)
    {
        if (!consumer && !outbox) throw new ArgumentException("Select a store capability to validate.");
        if (consumer) await ValidateAsync(ct).ConfigureAwait(false);
        if (outbox) await ValidateOutboxAsync(ct).ConfigureAwait(false);
    }

    public async Task ValidateAsync(CancellationToken ct = default)
    {
        await using var command = _data.CreateCommand($"""
            SELECT version IN (3,4,5,6,7,8,9,10,11) AND (SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
                JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname=@schema AND c.relname IN ('ix_ops_created','ix_ops_state_created','ix_ops_replays')
                AND i.indisvalid AND i.indisready)=3 FROM {_schema}.schema_version WHERE singleton
            """);
        command.Parameters.AddWithValue("schema", _schema.Trim('"'));
        if (await command.ExecuteScalarAsync(ct).ConfigureAwait(false) is not true)
            throw new InvalidOperationException("Operations requires a supported PostgreSQL schema and its three read indexes. Apply the explicit setup script first.");
        await ValidateMetadataAccessAsync(false, ct);
    }

    public async Task<WorkPage> ListAsync(WorkQuery query, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        await using var command = _data.CreateCommand($"""
            SELECT {Columns} FROM {_schema}.works WHERE scope_id=@scope AND created_at<=@cutoff
            {(query.State.HasValue ? "AND state=@state" : "")}
            {(query.Filters.CreatedFrom.HasValue ? "AND created_at>=@from" : "")}
            {(query.Filters.CreatedUntil.HasValue ? "AND created_at<@until" : "")}
            {(query.Filters.WorkId.HasValue ? "AND id=@work" : "")}
            {(query.Filters.MinimumAttempts.HasValue ? "AND attempt_count>=@attempts" : "")}
            {(query.Filters.Replayed is { } replayed ? replayed ? "AND replay_generation>0" : "AND replay_generation=0" : "")}
            {(query.Before is not null ? "AND (created_at,id)<(@created,@id)" : "")}
            {(query.After is not null ? "AND (created_at,id)>(@created,@id)" : "")}
            ORDER BY created_at {(query.After is null ? "DESC" : "ASC")},id {(query.After is null ? "DESC" : "ASC")} LIMIT @limit
            """);
        command.Parameters.AddWithValue("scope", query.Scope.Id);
        command.Parameters.AddWithValue("cutoff", query.CreatedBefore);
        command.Parameters.AddWithValue("limit", query.PageSize + 1);
        if (query.State.HasValue) command.Parameters.AddWithValue("state", (int)query.State.Value);
        if (query.Filters.CreatedFrom is { } from) command.Parameters.AddWithValue("from", from.ToUniversalTime());
        if (query.Filters.CreatedUntil is { } until) command.Parameters.AddWithValue("until", until.ToUniversalTime());
        if (query.Filters.WorkId is { } work) command.Parameters.AddWithValue("work", work);
        if (query.Filters.MinimumAttempts is { } attempts) command.Parameters.AddWithValue("attempts", attempts);
        if ((query.Before ?? query.After) is { } before)
        {
            command.Parameters.AddWithValue("created", before.CreatedAt.ToUniversalTime());
            command.Parameters.AddWithValue("id", before.Id);
        }
        var rows = new List<WorkSummary>();
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false)) rows.Add(MetadataRows.Work(reader));
        return WorkPage.FromWindow(rows, query);
    }

    public async Task<WorkDetail?> FindAsync(WorkScope scope, Guid id, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        await using var connection = await _data.OpenConnectionAsync(ct).ConfigureAwait(false);
        WorkSummary work;
        await using (var command = new NpgsqlCommand($"SELECT {Columns} FROM {_schema}.works WHERE scope_id=@scope AND id=@id", connection))
        {
            command.Parameters.AddWithValue("scope", scope.Id);
            command.Parameters.AddWithValue("id", id);
            await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false)) return null;
            work = MetadataRows.Work(reader);
        }
        var attempts = new List<AttemptSummary>();
        await using (var command = new NpgsqlCommand($"""
            SELECT number,generation,left(status,64),started_at,finished_at,left(reason,64) FROM {_schema}.attempts
            WHERE work_id=@id AND EXISTS (SELECT 1 FROM {_schema}.works WHERE id=@id AND scope_id=@scope)
            ORDER BY number DESC LIMIT 51
            """, connection))
        {
            command.Parameters.AddWithValue("scope", scope.Id);
            command.Parameters.AddWithValue("id", id);
            await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false)) attempts.Add(MetadataRows.Attempt(reader));
        }
        var replays = new List<ReplaySummary>();
        await using (var command = new NpgsqlCommand($"""
            SELECT id,result_status,result_generation,result_version,created_at FROM {_schema}.replay_requests
            WHERE scope_id=@scope AND work_id=@id ORDER BY created_at DESC,id DESC LIMIT 21
            """, connection))
        {
            command.Parameters.AddWithValue("scope", scope.Id);
            command.Parameters.AddWithValue("id", id);
            await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false)) replays.Add(MetadataRows.Replay(reader));
        }
        return new(work, attempts.Take(50).ToArray(), attempts.Count > 50, replays.Take(20).ToArray(), replays.Count > 20);
    }

    public async Task<WorkData?> ReadDataAsync(WorkScope scope, Guid id, WorkDataSelection selection, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        ArgumentNullException.ThrowIfNull(selection);
        await using var versionQuery = _data.CreateCommand($"SELECT version FROM {_schema}.schema_version WHERE singleton");
        var hasRetention = await versionQuery.ExecuteScalarAsync(ct).ConfigureAwait(false) is int version && version >= 8;
        await using var command = _data.CreateCommand($"""
            SELECT CASE WHEN octet_length(payload)<=@limit THEN payload ELSE NULL END,
                {(hasRetention ? "payload_purged_at IS NOT NULL" : "false")}
            FROM {_schema}.works WHERE scope_id=@scope AND id=@id
            """);
        command.Parameters.AddWithValue("scope", scope.Id);
        command.Parameters.AddWithValue("id", id);
        command.Parameters.AddWithValue("limit", WorkDataSelection.MaximumPayloadBytes);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false)) return null;
        if (reader.GetBoolean(1)) return new(WorkDataStatus.Removed, []);
        return reader.IsDBNull(0) ? new(WorkDataStatus.TooLarge, [])
            : WorkDataProjection.FromJson(reader.GetFieldValue<byte[]>(0), selection);
    }

    public ValueTask DisposeAsync() => _data.DisposeAsync();
}
