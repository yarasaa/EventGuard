using Npgsql;

namespace EventGuard.PostgreSql;

internal static class PostgreSqlSchema
{
    public static async Task InitializeAsync(NpgsqlDataSource dataSource, string quotedSchema, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await PostgreSqlProtocol.LockAsync(connection, transaction, "schema:" + quotedSchema, ct);
        await using (var exists = new NpgsqlCommand("SELECT to_regclass(@name)::text", connection, transaction))
        {
            exists.Parameters.AddWithValue("name", quotedSchema + ".schema_version");
            if (await exists.ExecuteScalarAsync(ct) is not string)
                await ApplyAsync("001_initial.sql", connection, transaction, quotedSchema, ct);
        }
        int version;
        await using (var check = new NpgsqlCommand($"SELECT version FROM {quotedSchema}.schema_version WHERE singleton", connection, transaction))
            version = await check.ExecuteScalarAsync(ct) is int value ? value : throw new InvalidOperationException("Schema version is missing.");
        if (version is < 1 or > 11) throw new InvalidOperationException("Unsupported EventGuard schema version.");
        if (version == 1) await ApplyAsync("002_outbox.sql", connection, transaction, quotedSchema, ct);
        if (version <= 2) await ApplyAsync("003_outbox_recovery.sql", connection, transaction, quotedSchema, ct);
        if (version <= 3) await ApplyAsync("004_claim_index.sql", connection, transaction, quotedSchema, ct);
        if (version <= 4) await ApplyAsync("005_claim_expiry.sql", connection, transaction, quotedSchema, ct);
        if (version <= 5) await ApplyAsync("006_ingress_quarantine.sql", connection, transaction, quotedSchema, ct);
        if (version <= 6) await ApplyAsync("007_outbox_repair.sql", connection, transaction, quotedSchema, ct);
        if (version <= 7) await ApplyAsync("008_retention.sql", connection, transaction, quotedSchema, ct);
        if (version <= 8) await ApplyAsync("009_tenant_budgets.sql", connection, transaction, quotedSchema, ct);
        if (version <= 9) await ApplyAsync("010_bulk_replay.sql", connection, transaction, quotedSchema, ct);
        if (version <= 10) await ApplyAsync("011_claim_due.sql", connection, transaction, quotedSchema, ct);
        await transaction.CommitAsync(ct);
    }

    private static async Task ApplyAsync(string name, NpgsqlConnection connection, NpgsqlTransaction transaction, string schema, CancellationToken ct)
    {
        await using var stream = typeof(PostgreSqlSchema).Assembly.GetManifestResourceStream("EventGuard.PostgreSql.Migrations." + name)
            ?? throw new InvalidOperationException("Migration not found.");
        using var reader = new StreamReader(stream);
        var sql = (await reader.ReadToEndAsync(ct)).Replace("__SCHEMA__", schema, StringComparison.Ordinal);
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        await command.ExecuteNonQueryAsync(ct);
    }
}
