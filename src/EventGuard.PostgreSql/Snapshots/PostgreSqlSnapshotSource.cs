using System.Data;
using System.Globalization;
using System.Text;
using Npgsql;

namespace EventGuard.PostgreSql;

/// <summary>A read-only repeatable view. An interrupted capture restarts before any batch is exposed.</summary>
public sealed class PostgreSqlSnapshotSource : ISnapshotSource
{
    private readonly NpgsqlDataSource _source;
    private readonly Func<NpgsqlConnection, NpgsqlTransaction, ReadOnlyMemory<byte>, NpgsqlCommand> _query;
    private readonly Func<NpgsqlDataReader, SnapshotItem> _map;
    public string Name { get; }
    public string Version { get; }
    public SnapshotConsistency Consistency => SnapshotConsistency.SessionBound;
    public PostgreSqlSnapshotSource(NpgsqlDataSource source, string name, string version,
        Func<NpgsqlConnection, NpgsqlTransaction, ReadOnlyMemory<byte>, NpgsqlCommand> query,
        Func<NpgsqlDataReader, SnapshotItem> map)
    {
        _source = source ?? throw new ArgumentNullException(nameof(source));
        _query = query ?? throw new ArgumentNullException(nameof(query)); _map = map ?? throw new ArgumentNullException(nameof(map));
        var binding = new ProducerScope(name, version); Name = binding.Tenant; Version = binding.Producer;
    }
    public async Task<ISnapshotReader> OpenAsync(ReadOnlyMemory<byte> parameters, string? resumeView, CancellationToken cancellationToken = default)
    {
        if (resumeView is not null) throw new InvalidOperationException("A connection-bound snapshot cannot resume after losing its transaction.");
        var connection = await _source.OpenConnectionAsync(cancellationToken);
        NpgsqlTransaction? transaction = null; NpgsqlCommand? command = null;
        try
        {
            transaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead, cancellationToken);
            await using (var readOnly = new NpgsqlCommand("SET TRANSACTION READ ONLY", connection, transaction))
                await readOnly.ExecuteNonQueryAsync(cancellationToken);
            command = _query(connection, transaction, parameters.ToArray()) ?? throw new InvalidOperationException("Source query is required.");
            if (!ReferenceEquals(command.Connection, connection) || !ReferenceEquals(command.Transaction, transaction))
                throw new InvalidOperationException("Source query must use the supplied snapshot transaction.");
            var reader = await command.ExecuteReaderAsync(cancellationToken);
            return new Reader(connection, transaction, command, reader, _map);
        }
        catch
        {
            if (command is not null) await command.DisposeAsync();
            if (transaction is not null) await transaction.DisposeAsync();
            await connection.DisposeAsync(); throw;
        }
    }
    private sealed class Reader(NpgsqlConnection connection, NpgsqlTransaction transaction, NpgsqlCommand command,
        NpgsqlDataReader reader, Func<NpgsqlDataReader, SnapshotItem> map) : ISnapshotReader
    {
        private long _page;
        private SnapshotItem? _pending;
        private bool _done;
        public string View { get; } = Guid.NewGuid().ToString("D");
        public async Task<SnapshotPage> ReadAsync(ReadOnlyMemory<byte> cursor, int maximumItems, CancellationToken cancellationToken = default)
        {
            var expected = _page == 0 ? "" : _page.ToString(CultureInfo.InvariantCulture);
            if (Encoding.UTF8.GetString(cursor.Span) != expected || _done) throw new InvalidOperationException("Source cursor is not current.");
            if (maximumItems is < 1 or > 1000) throw new ArgumentOutOfRangeException(nameof(maximumItems));
            var items = new List<SnapshotItem>(); var size = 0;
            while (true)
            {
                if (_pending is null)
                {
                    if (!await reader.ReadAsync(cancellationToken)) { _done = true; break; }
                    _pending = map(reader) ?? throw new InvalidOperationException("Source mapping returned no item.");
                }
                var bytes = _pending.CopyPayload().Length;
                if (items.Count == maximumItems || size + bytes > 4 * 1024 * 1024) break;
                items.Add(_pending); _pending = null; size += bytes;
            }
            _page = checked(_page + 1);
            return new(items, Encoding.UTF8.GetBytes(_page.ToString(CultureInfo.InvariantCulture)), _done);
        }
        public async ValueTask DisposeAsync()
        {
            try { await reader.DisposeAsync(); }
            finally
            {
                try { await command.DisposeAsync(); }
                finally { try { await transaction.DisposeAsync(); } finally { await connection.DisposeAsync(); } }
            }
        }
    }
}
