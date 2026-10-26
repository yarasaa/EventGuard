using Microsoft.Data.SqlClient;

namespace EventGuard.Operations.SqlServer;

/// <summary>Dedicated bounded read pool. Supply a SELECT-only database role in deployments.</summary>
public sealed partial class SqlServerWorkReader : IStoreHealthCheck, IWorkReadStore, IWorkDataReadStore, IIngressReadStore, IRecoveryHealthCheck, IAuditReadStore, IAsyncDisposable
{
    private const string Columns = "id,state,version,attempt_count,replay_generation,created_at,next_attempt_at,expires_at,completed_at,left(last_reason,64)";
    private readonly string _connectionString;
    private readonly string _schema;

    public SqlServerWorkReader(string connectionString, string schema = "eventguard")
    {
        _schema = OperationsIndexes.Quote(schema);
        var options = new SqlConnectionStringBuilder(connectionString)
        {
            Pooling = true, MaxPoolSize = 4, MinPoolSize = 0, ConnectTimeout = 3, ConnectRetryCount = 0,
            Enlist = false, MultipleActiveResultSets = false,
            ApplicationName = "EventGuard.Operations." + Guid.NewGuid().ToString("N")
        };
        if (string.IsNullOrWhiteSpace(options.InitialCatalog)) throw new ArgumentException("Specify the application database.", nameof(connectionString));
        _connectionString = options.ConnectionString;
    }

    private async Task<SqlConnection> OpenAsync(CancellationToken ct)
    {
        var connection = new SqlConnection(_connectionString);
        try
        {
            await connection.OpenAsync(ct).ConfigureAwait(false);
            await using var reset = new SqlCommand("SET TRANSACTION ISOLATION LEVEL READ COMMITTED; SET LOCK_TIMEOUT 1000;", connection) { CommandTimeout = 2 };
            await reset.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            return connection;
        }
        catch { await connection.DisposeAsync().ConfigureAwait(false); throw; }
    }

    public async Task CheckHealthAsync(bool consumer, bool outbox, CancellationToken ct = default)
    {
        if (!consumer && !outbox) throw new ArgumentException("Select a store capability to validate.");
        if (consumer) await ValidateAsync(ct).ConfigureAwait(false);
        if (outbox) await ValidateOutboxAsync(ct).ConfigureAwait(false);
    }

    public async Task ValidateAsync(CancellationToken ct = default)
    {
        await using var connection = await OpenAsync(ct).ConfigureAwait(false);
        await using var command = new SqlCommand($"""
            SELECT CASE WHEN version IN (1,2,3,4,5,6,7,8)
                AND (SELECT is_read_committed_snapshot_on FROM sys.databases WHERE database_id=DB_ID())=1
                AND (SELECT count(*) FROM sys.indexes WHERE is_disabled=0 AND
                    ((object_id=OBJECT_ID(@works) AND name IN ('ix_ops_created','ix_ops_state_created'))
                    OR (object_id=OBJECT_ID(@replays) AND name='ix_ops_replays')))=3 THEN 1 ELSE 0 END
            FROM {_schema}.schema_version WHERE singleton=1
            """, connection) { CommandTimeout = 2 };
        command.Parameters.AddWithValue("works", _schema + ".works");
        command.Parameters.AddWithValue("replays", _schema + ".replay_requests");
        if (await command.ExecuteScalarAsync(ct).ConfigureAwait(false) is not 1)
            throw new InvalidOperationException("Operations requires a supported SQL Server schema, RCSI and its three read indexes. Apply the explicit setup script first.");
        await ValidateMetadataAccessAsync(false, ct);
    }

    public async Task<WorkPage> ListAsync(WorkQuery query, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        await using var connection = await OpenAsync(ct).ConfigureAwait(false);
        await using var command = new SqlCommand($"""
            SELECT TOP (@limit) {Columns} FROM {_schema}.works WHERE scope_id=@scope AND created_at<=@cutoff
            {(query.State.HasValue ? "AND state=@state" : "")}
            {(query.Filters.CreatedFrom.HasValue ? "AND created_at>=@from" : "")}
            {(query.Filters.CreatedUntil.HasValue ? "AND created_at<@until" : "")}
            {(query.Filters.WorkId.HasValue ? "AND id=@work" : "")}
            {(query.Filters.MinimumAttempts.HasValue ? "AND attempt_count>=@attempts" : "")}
            {(query.Filters.Replayed is { } replayed ? replayed ? "AND replay_generation>0" : "AND replay_generation=0" : "")}
            {(query.Before is not null ? "AND created_at<=@created AND (created_at<@created OR id<@id)" : "")}
            {(query.After is not null ? "AND created_at>=@created AND (created_at>@created OR id>@id)" : "")}
            ORDER BY created_at {(query.After is null ? "DESC" : "ASC")},id {(query.After is null ? "DESC" : "ASC")}
            """, connection) { CommandTimeout = 2 };
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
        await using var connection = await OpenAsync(ct).ConfigureAwait(false);
        WorkSummary work;
        await using (var command = new SqlCommand($"SELECT {Columns} FROM {_schema}.works WHERE scope_id=@scope AND id=@id", connection) { CommandTimeout = 2 })
        {
            command.Parameters.AddWithValue("scope", scope.Id);
            command.Parameters.AddWithValue("id", id);
            await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false)) return null;
            work = MetadataRows.Work(reader);
        }
        var attempts = new List<AttemptSummary>();
        await using (var command = new SqlCommand($"""
            SELECT TOP (51) number,generation,left(status,64),started_at,finished_at,left(reason,64) FROM {_schema}.attempts
            WHERE work_id=@id AND EXISTS (SELECT 1 FROM {_schema}.works WHERE id=@id AND scope_id=@scope)
            ORDER BY number DESC
            """, connection) { CommandTimeout = 2 })
        {
            command.Parameters.AddWithValue("scope", scope.Id);
            command.Parameters.AddWithValue("id", id);
            await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false)) attempts.Add(MetadataRows.Attempt(reader));
        }
        var replays = new List<ReplaySummary>();
        await using (var command = new SqlCommand($"""
            SELECT TOP (21) id,result_status,result_generation,result_version,created_at FROM {_schema}.replay_requests
            WHERE scope_id=@scope AND work_id=@id ORDER BY created_at DESC,id DESC
            """, connection) { CommandTimeout = 2 })
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
        await using var connection = await OpenAsync(ct).ConfigureAwait(false);
        await using var versionQuery = new SqlCommand($"SELECT version FROM {_schema}.schema_version WHERE singleton=1", connection) { CommandTimeout = 2 };
        var hasRetention = await versionQuery.ExecuteScalarAsync(ct).ConfigureAwait(false) is int version && version >= 6;
        await using var command = new SqlCommand($"""
            SELECT CASE WHEN DATALENGTH(payload)<=@limit THEN payload ELSE NULL END,
                {(hasRetention ? "CASE WHEN payload_purged_at IS NULL THEN 0 ELSE 1 END" : "0")}
            FROM {_schema}.works WHERE scope_id=@scope AND id=@id
            """, connection) { CommandTimeout = 2 };
        command.Parameters.AddWithValue("scope", scope.Id);
        command.Parameters.AddWithValue("id", id);
        command.Parameters.AddWithValue("limit", WorkDataSelection.MaximumPayloadBytes);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false)) return null;
        if (reader.GetInt32(1) == 1) return new(WorkDataStatus.Removed, []);
        return reader.IsDBNull(0) ? new(WorkDataStatus.TooLarge, [])
            : WorkDataProjection.FromJson(reader.GetFieldValue<byte[]>(0), selection);
    }

    public ValueTask DisposeAsync()
    {
        using var connection = new SqlConnection(_connectionString);
        SqlConnection.ClearPool(connection);
        return ValueTask.CompletedTask;
    }
}
