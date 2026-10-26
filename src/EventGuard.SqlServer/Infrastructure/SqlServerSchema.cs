using Microsoft.Data.SqlClient;

namespace EventGuard.SqlServer;

internal static class SqlServerSchema
{
    public static async Task InitializeAsync(SqlServerDataSource source, string schema, CancellationToken ct)
    {
        await using var connection = await source.OpenConnectionAsync(ct);
        await using (var profile = new SqlCommand("""
            SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS int), is_read_committed_snapshot_on, delayed_durability_desc
            FROM sys.databases WHERE database_id=DB_ID()
            """, connection))
        await using (var reader = await profile.ExecuteReaderCheckedAsync(ct))
        {
            if (!await reader.ReadCheckedAsync(ct) || reader.GetInt32(0) < 16 || !reader.GetBoolean(1) || reader.GetString(2) != "DISABLED")
                throw new NotSupportedException("SQL Server 2022+, READ_COMMITTED_SNAPSHOT ON and DELAYED_DURABILITY DISABLED are required. Configure the database explicitly.");
        }
        await using var transaction = await connection.BeginLocalAsync(ct);
        await SqlServerProtocol.LockAsync(connection, transaction, "schema:" + schema, ct);
        await using (var exists = new SqlCommand("SELECT OBJECT_ID(@name,'U')", connection, transaction))
        {
            exists.Parameters.AddValue("name", schema + ".schema_version");
            if (await exists.ExecuteScalarCheckedAsync(ct) is not int)
            {
                await using var stream = typeof(SqlServerSchema).Assembly.GetManifestResourceStream("EventGuard.SqlServer.Migrations.001_initial.sql")!;
                using var reader = new StreamReader(stream);
                var sql = (await reader.ReadToEndAsync(ct)).Replace("__SCHEMA__", schema, StringComparison.Ordinal);
                await using var command = new SqlCommand(sql, connection, transaction);
                await command.ExecuteNonQueryCheckedAsync(ct);
            }
        }
        await using (var version = new SqlCommand($"SELECT version FROM {schema}.schema_version WHERE singleton=1", connection, transaction))
        {
            if (await version.ExecuteScalarCheckedAsync(ct) is not int value || value is not (1 or 2 or 3 or 4 or 5 or 6 or 7 or 8))
                throw new InvalidOperationException("Unsupported EventGuard SQL Server schema version.");
            foreach (var name in new[] { "002_claim_index.sql", "003_claim_expiry.sql", "004_ingress_quarantine.sql", "005_outbox_repair.sql", "006_retention.sql", "007_tenant_budgets.sql", "008_bulk_replay.sql" }.Skip(value - 1))
            {
                await using var stream = typeof(SqlServerSchema).Assembly.GetManifestResourceStream("EventGuard.SqlServer.Migrations." + name)!;
                using var reader = new StreamReader(stream);
                var sql = (await reader.ReadToEndAsync(ct)).Replace("__SCHEMA__", schema, StringComparison.Ordinal);
                await using var migration = new SqlCommand(sql, connection, transaction);
                await migration.ExecuteNonQueryCheckedAsync(ct);
            }
        }
        await SqlServerProtocol.CommitAsync(transaction, Guid.NewGuid(), ct);
    }
}
