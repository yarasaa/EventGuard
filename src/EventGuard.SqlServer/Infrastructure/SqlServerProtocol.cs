using System.Data;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;

namespace EventGuard.SqlServer;

internal static class SqlServerProtocol
{
    // SqlClient can report cancellation as SqlException or an InvalidOperationException during a packet flush.
    // Normalize only command/reader paths;
    // COMMIT has its own unknown-outcome handling and must never be collapsed into cancellation.
    public static async Task<int> ExecuteNonQueryCheckedAsync(this SqlCommand command, CancellationToken ct)
    {
        try { return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false); }
        catch (Exception error) when (IsCommandCancellation(error, ct)) { throw new OperationCanceledException("SQL command cancelled.", error, ct); }
    }
    public static async Task<object?> ExecuteScalarCheckedAsync(this SqlCommand command, CancellationToken ct)
    {
        try { return await command.ExecuteScalarAsync(ct).ConfigureAwait(false); }
        catch (Exception error) when (IsCommandCancellation(error, ct)) { throw new OperationCanceledException("SQL command cancelled.", error, ct); }
    }
    public static async Task<SqlDataReader> ExecuteReaderCheckedAsync(this SqlCommand command, CancellationToken ct)
    {
        try { return await command.ExecuteReaderAsync(ct).ConfigureAwait(false); }
        catch (Exception error) when (IsCommandCancellation(error, ct)) { throw new OperationCanceledException("SQL command cancelled.", error, ct); }
    }
    public static async Task<bool> ReadCheckedAsync(this SqlDataReader reader, CancellationToken ct)
    {
        try { return await reader.ReadAsync(ct).ConfigureAwait(false); }
        catch (Exception error) when (IsCommandCancellation(error, ct)) { throw new OperationCanceledException("SQL reader cancelled.", error, ct); }
    }
    public static async Task<bool> NextResultCheckedAsync(this SqlDataReader reader, CancellationToken ct)
    {
        try { return await reader.NextResultAsync(ct).ConfigureAwait(false); }
        catch (Exception error) when (IsCommandCancellation(error, ct)) { throw new OperationCanceledException("SQL reader cancelled.", error, ct); }
    }

    internal static bool IsCommandCancellation(Exception error, CancellationToken ct)
    {
        if (!ct.IsCancellationRequested) return false;
        if (error is SqlException) return true;
        // The pinned SqlClient driver throws SQL.OperationCancelled() at these two sites.
        // Its message is localized and has no error code. Keep the match narrow: an unrelated
        // InvalidOperationException must still fail, even when shutdown happens concurrently.
        // A driver update that changes these sites fails closed and needs the regression probe updated.
        var site = error.TargetSite;
        return error is InvalidOperationException
            && site?.DeclaringType?.Assembly == typeof(SqlConnection).Assembly
            && site.DeclaringType.FullName == "Microsoft.Data.SqlClient.TdsParserStateObject"
            && site.Name is "ExecuteFlush" or "CancelWritePacket";
    }
    public static string QuoteSchema(string schema)
    {
        if (schema is null || !Regex.IsMatch(schema, "^[a-z][a-z0-9_]{0,62}$", RegexOptions.CultureInvariant))
            throw new ArgumentException("Schema must be a lowercase identifier of at most 63 characters.", nameof(schema));
        return "[" + schema + "]";
    }

    public static async Task<SqlTransaction> BeginLocalAsync(this SqlConnection connection, CancellationToken ct) =>
        (SqlTransaction)await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct).ConfigureAwait(false);
    public static async Task<SqlTransaction> BeginLocalAsync(this SqlConnection connection, IsolationLevel isolation, CancellationToken ct) =>
        (SqlTransaction)await connection.BeginTransactionAsync(isolation, ct).ConfigureAwait(false);

    public static async Task LockAsync(SqlConnection connection, SqlTransaction transaction, string key, CancellationToken ct)
    {
        await using var command = new SqlCommand("""
            DECLARE @result int;
            EXEC @result=sys.sp_getapplock @Resource=@key, @LockMode='Exclusive', @LockOwner='Transaction', @LockTimeout=10000;
            IF @result < 0 THROW 51001, 'EventGuard operation lock could not be acquired.', 1;
            """, connection, transaction);
        command.Parameters.AddValue("key", StableHash.Of("sqlserver-lock/v1", key));
        await command.ExecuteNonQueryCheckedAsync(ct).ConfigureAwait(false);
    }

    public static async Task<DateTimeOffset> NowAsync(SqlConnection connection, SqlTransaction transaction, CancellationToken ct)
    {
        await using var command = new SqlCommand("SELECT TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00')", connection, transaction);
        return (DateTimeOffset)(await command.ExecuteScalarCheckedAsync(ct).ConfigureAwait(false))!;
    }

    public static async Task CommitAsync(SqlTransaction transaction, Guid operationId, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        try { await transaction.CommitAsync(ct).ConfigureAwait(false); }
        catch (Exception error) { throw new CommitOutcomeUnknownException(operationId, error); }
    }

    public static void Optional(SqlCommand command, string name, SqlDbType type, object? value)
    {
        var parameter = command.Parameters.Add(name, type);
        if (type is SqlDbType.NVarChar or SqlDbType.VarBinary) parameter.Size = -1;
        parameter.Value = value ?? DBNull.Value;
    }

    // Stable Unicode size buckets avoid value-length plan proliferation and varchar conversion on hash indexes.
    public static SqlParameter AddValue(this SqlParameterCollection parameters, string name, object value)
    {
        var type = value switch
        {
            string => SqlDbType.NVarChar,
            Guid => SqlDbType.UniqueIdentifier,
            byte[] => SqlDbType.VarBinary,
            int => SqlDbType.Int,
            long => SqlDbType.BigInt,
            bool => SqlDbType.Bit,
            DateTimeOffset => SqlDbType.DateTimeOffset,
            _ => throw new ArgumentException("Unsupported SQL parameter type.", nameof(value))
        };
        var parameter = parameters.Add(name, type);
        if (value is string text) parameter.Size = text.Length <= 64 ? 64 : text.Length <= 1024 ? 1024 : -1;
        if (value is byte[]) parameter.Size = -1;
        parameter.Value = value;
        return parameter;
    }
}
