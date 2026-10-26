using EventGuard.PostgreSql;
using Npgsql;

namespace EventGuard.Operations.Replay.PostgreSql;

/// <summary>Dedicated command pool; reuses the worker store's locked, durable replay protocol without initializing schema.</summary>
public sealed partial class PostgreSqlReplayStore : IStoreHealthCheck, IWorkReplayStore, IOutboxReplayStore, IRecoveryCommandStore, IBulkReplayStore, IAsyncDisposable
{
    private readonly NpgsqlDataSource _data;
    private readonly PostgreSqlWorkStore _store;
    private readonly string _schema;
    private readonly PostgreSqlOutboxRecoveryStore _outbox;
    private readonly PostgreSqlBulkReplayStore _bulk;

    public PostgreSqlReplayStore(string readConnectionString, string replayConnectionString, string schema = "eventguard")
    {
        var read = new NpgsqlConnectionStringBuilder(readConnectionString);
        var options = new NpgsqlConnectionStringBuilder(replayConnectionString);
        if (string.IsNullOrWhiteSpace(read.Database) || string.IsNullOrWhiteSpace(options.Database)
            || !string.Equals(read.Host, options.Host, StringComparison.OrdinalIgnoreCase) || read.Port != options.Port || read.Database != options.Database)
            throw new ArgumentException("Replay and metadata connections must target the same server and database.");
        options.Pooling = true; options.Multiplexing = false; options.MaxPoolSize = 2; options.MinPoolSize = 0;
        options.Timeout = 3; options.CommandTimeout = 2; options.Enlist = false;
        options.ApplicationName = "EventGuard.Operations.Replay";
        options.Options = "-c statement_timeout=2000 -c lock_timeout=1000";
        _data = NpgsqlDataSource.Create(options.ConnectionString);
        _outbox = new(_data, schema);
        _bulk = new(_data, schema);
        _store = new(_data, schema); // Validates and quotes the existing worker schema name.
        _schema = "\"" + schema + "\"";
    }

    public Task CheckHealthAsync(bool consumer, bool outbox, CancellationToken ct = default) => ValidateAsync(ct);

    public async Task ValidateAsync(CancellationToken ct = default)
    {
        await using var command = _data.CreateCommand($"SELECT version=11 AND current_setting('transaction_read_only')='off' FROM {_schema}.schema_version WHERE singleton");
        if (await command.ExecuteScalarAsync(ct).ConfigureAwait(false) is not true)
            throw new InvalidOperationException("Replay requires writable PostgreSQL schema 11.");
    }

    public Task<ReplayResult> ReplayAsync(ReplayRequest request, CancellationToken ct = default) => _store.ReplayAsync(request, ct);

    public async Task<ReplayReceipt?> FindReceiptAsync(WorkScope scope, Guid workId, Guid requestId, string actor, CancellationToken ct = default)
    {
        await using var command = _data.CreateCommand($"""
            SELECT result_status,result_generation,result_version,left(reason,1024),created_at FROM {_schema}.replay_requests
            WHERE scope_id=@scope AND work_id=@work AND id=@request AND actor=@actor
            """);
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

    public ValueTask DisposeAsync() => _data.DisposeAsync();
}
