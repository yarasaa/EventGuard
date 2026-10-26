using System.Text.RegularExpressions;
using Npgsql;
using NpgsqlTypes;

namespace EventGuard.PostgreSql;

internal static class PostgreSqlProtocol
{
    public static string QuoteSchema(string schema)
    {
        if (schema is null || !Regex.IsMatch(schema, "^[a-z][a-z0-9_]{0,62}$", RegexOptions.CultureInvariant))
            throw new ArgumentException("Schema must be a lowercase PostgreSQL identifier.", nameof(schema));
        return '"' + schema + '"';
    }

    public static async Task LockAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string key, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT pg_advisory_xact_lock(hashtextextended(@key,0))", connection, transaction);
        command.Parameters.AddWithValue("key", key);
        await command.ExecuteNonQueryAsync(ct);
    }

    public static async Task<DateTimeOffset> NowAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT clock_timestamp()", connection, transaction);
        return new DateTimeOffset((DateTime)(await command.ExecuteScalarAsync(ct))!);
    }

    public static async Task CommitAsync(NpgsqlTransaction transaction, Guid operationId, CancellationToken ct)
    {
        // No COMMIT has been invoked yet; an already cancelled request is a known rollback path.
        ct.ThrowIfCancellationRequested();
        try { await transaction.CommitAsync(ct); }
        catch (Exception error) { throw new CommitOutcomeUnknownException(operationId, error); }
    }

    public static void Optional(NpgsqlCommand command, string name, NpgsqlDbType type, object? value) =>
        command.Parameters.AddWithValue(name, type, value ?? DBNull.Value);
}
