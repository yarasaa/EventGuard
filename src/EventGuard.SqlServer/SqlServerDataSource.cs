using Microsoft.Data.SqlClient;

namespace EventGuard.SqlServer;

/// <summary>Creates independent, locally owned connections; pooling remains owned by SqlClient.</summary>
public sealed class SqlServerDataSource
{
    private readonly string _connectionString;

    public SqlServerDataSource(string connectionString)
    {
        var options = new SqlConnectionStringBuilder(connectionString);
        if (string.IsNullOrWhiteSpace(options.InitialCatalog)) throw new ArgumentException("Specify the application database.", nameof(connectionString));
        if (options.MultipleActiveResultSets) throw new NotSupportedException("MARS is not supported by this transaction profile.");
        // No ambient enlistment or transparent reconnection around a durable operation.
        options.Enlist = false;
        options.ConnectRetryCount = 0;
        _connectionString = options.ConnectionString;
    }

    public async Task<SqlConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
    {
        var connection = new SqlConnection(_connectionString);
        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            // Isolation can survive a pooled checkout after a RepeatableRead claim transaction.
            // Standalone reads must regain RCSI semantics instead of waiting on uncommitted business writes.
            await using var reset = new SqlCommand("SET TRANSACTION ISOLATION LEVEL READ COMMITTED", connection);
            await reset.ExecuteNonQueryCheckedAsync(cancellationToken).ConfigureAwait(false);
            return connection;
        }
        catch { await connection.DisposeAsync().ConfigureAwait(false); throw; }
    }
}
