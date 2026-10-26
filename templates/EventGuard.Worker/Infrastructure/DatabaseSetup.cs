using EventGuard;
using EventGuard.PostgreSql;
using EventGuard.SqlServer;
using EventGuardWorker.Configuration;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace EventGuardWorker.Infrastructure;

public static class DatabaseSetup
{
    // Run explicitly with migration credentials. Normal worker startup never calls this method.
    public static async Task MigrateAsync(IServiceProvider services, WorkerSettings settings, CancellationToken ct)
    {
        if (settings.Provider == "PostgreSql")
        {
            await services.GetRequiredService<PostgreSqlWorkStore>().InitializeAsync(ct);
            await using var connection = await services.GetRequiredService<NpgsqlDataSource>().OpenConnectionAsync(ct);
            // Application table/index setup is an independently repeatable step after provider migration.
            await using var command = new NpgsqlCommand($"""
                CREATE TABLE IF NOT EXISTS "{settings.Schema}".sample_products(id varchar(100) PRIMARY KEY, revision bigint NOT NULL, name varchar(200) NOT NULL);
                """ + EventGuard.Operations.PostgreSql.OperationsIndexes.CreateScript(settings.Schema), connection);
            await command.ExecuteNonQueryAsync(ct);
        }
        else
        {
            await services.GetRequiredService<SqlServerWorkStore>().InitializeAsync(ct);
            await using var connection = await services.GetRequiredService<SqlServerDataSource>().OpenConnectionAsync(ct);
            await using var command = new SqlCommand($"""
                IF OBJECT_ID('[{settings.Schema}].sample_products','U') IS NULL
                    CREATE TABLE [{settings.Schema}].sample_products(id nvarchar(100) PRIMARY KEY, revision bigint NOT NULL, name nvarchar(200) NOT NULL);
                """ + EventGuard.Operations.SqlServer.OperationsIndexes.CreateScript(settings.Schema), connection);
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    public static async Task<StoreStartupReport> CheckAsync(IServiceProvider services, WorkerSettings settings, CancellationToken ct)
    {
        IStoreStartupCheck store = settings.Provider == "PostgreSql"
            ? services.GetRequiredService<PostgreSqlWorkStore>() : services.GetRequiredService<SqlServerWorkStore>();
        return await store.ValidateStartupAsync(new(settings.Tenant, StoreRole.Consumer), ct);
    }
}
