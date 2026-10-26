using EventGuard.SqlServer;
using Microsoft.Data.SqlClient;

namespace EventGuard.Operations.Replay.SqlServer;

/// <summary>Dedicated command pool; reuses the worker store's locked, durable replay protocol without initializing schema.</summary>
public sealed partial class SqlServerReplayStore : IStoreHealthCheck, IWorkReplayStore, IOutboxReplayStore, IRecoveryCommandStore, IBulkReplayStore, IAsyncDisposable
{
    private readonly string _connectionString;
    private readonly SqlServerDataSource _data;
    private readonly SqlServerWorkStore _store;
    private readonly string _schema;
    private readonly SqlServerOutboxRecoveryStore _outbox;
    private readonly SqlServerBulkReplayStore _bulk;

    public SqlServerReplayStore(string readConnectionString, string replayConnectionString, string schema = "eventguard")
    {
        var read = new SqlConnectionStringBuilder(readConnectionString);
        var options = new SqlConnectionStringBuilder(replayConnectionString);
        if (!string.Equals(read.DataSource, options.DataSource, StringComparison.OrdinalIgnoreCase) || read.InitialCatalog != options.InitialCatalog)
            throw new ArgumentException("Replay and metadata connections must target the same server and database.");
        options.Pooling = true; options.MaxPoolSize = 2; options.MinPoolSize = 0; options.ConnectTimeout = 3;
        options.CommandTimeout = 2; options.ConnectRetryCount = 0; options.Enlist = false; options.MultipleActiveResultSets = false;
        options.ApplicationIntent = ApplicationIntent.ReadWrite;
        options.ApplicationName = "EventGuard.Operations.Replay." + Guid.NewGuid().ToString("N");
        _connectionString = options.ConnectionString;
        _data = new(_connectionString);
        _outbox = new(_data, schema);
        _bulk = new(_data, schema);
        _store = new(_data, schema);
        _schema = "[" + schema + "]";
    }

    public Task CheckHealthAsync(bool consumer, bool outbox, CancellationToken ct = default) => ValidateAsync(ct);

    public async Task ValidateAsync(CancellationToken ct = default)
    {
        await using var connection = await _data.OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = new SqlCommand($"""
            SELECT CASE WHEN version=8 AND EXISTS(SELECT 1 FROM sys.databases WHERE database_id=DB_ID()
            AND is_read_only=0 AND is_read_committed_snapshot_on=1 AND delayed_durability=0) THEN 1 ELSE 0 END
            FROM {_schema}.schema_version WHERE singleton=1
            """, connection) { CommandTimeout = 2 };
        if (await command.ExecuteScalarAsync(ct).ConfigureAwait(false) is not 1)
            throw new InvalidOperationException("Replay requires writable SQL Server schema 8 with RCSI and full durability.");
    }

    public Task<ReplayResult> ReplayAsync(ReplayRequest request, CancellationToken ct = default) => _store.ReplayAsync(request, ct);

    public async Task<ReplayReceipt?> FindReceiptAsync(WorkScope scope, Guid workId, Guid requestId, string actor, CancellationToken ct = default)
    {
        await using var connection = await _data.OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = new SqlCommand($"""
            SELECT result_status,result_generation,result_version,left(reason,1024),created_at FROM {_schema}.replay_requests
            WHERE scope_id=@scope AND work_id=@work AND id=@request AND actor=@actor COLLATE Latin1_General_100_BIN2
            """, connection) { CommandTimeout = 2 };
        command.Parameters.AddWithValue("scope", scope.Id);
        command.Parameters.AddWithValue("work", workId);
        command.Parameters.AddWithValue("request", requestId);
        command.Parameters.AddWithValue("actor", actor);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false)) return null;
        var status = (ReplayStatus)reader.GetInt32(0);
        if (!Enum.IsDefined(status)) throw new InvalidOperationException("Unknown replay receipt status.");
        return new(requestId, new(status, workId, reader.GetInt32(1), reader.GetInt64(2)), reader.GetString(3), reader.GetFieldValue<DateTimeOffset>(4));
    }

    public ValueTask DisposeAsync()
    {
        using var connection = new SqlConnection(_connectionString);
        SqlConnection.ClearPool(connection);
        return ValueTask.CompletedTask;
    }
}
