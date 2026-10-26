using EventGuard;
using EventGuard.Hosting;
using EventGuard.PostgreSql;
using EventGuard.SqlServer;
using EventGuardWorker.Configuration;
using EventGuardWorker.Contracts;

namespace EventGuardWorker.Handlers;

// Business ordering is explicit: late revisions cannot replace a newer projection.
public sealed class PostgreSqlProductHandler(WorkerSettings settings) : IEventGuardHandler<ProductChanged, PostgreSqlSession>
{
    public async ValueTask HandleAsync(ProductChanged message, WorkItem work, PostgreSqlSession session, CancellationToken ct)
    {
        await using var command = session.CreateCommand($"""
            INSERT INTO "{settings.Schema}".sample_products(id, revision, name) VALUES (@id,@revision,@name)
            ON CONFLICT (id) DO UPDATE SET revision=EXCLUDED.revision, name=EXCLUDED.name
            WHERE sample_products.revision < EXCLUDED.revision
            """);
        command.Parameters.AddWithValue("id", message.ProductId);
        command.Parameters.AddWithValue("revision", message.Revision);
        command.Parameters.AddWithValue("name", message.Name);
        await command.ExecuteNonQueryAsync(ct);
    }
}

public sealed class SqlServerProductHandler(WorkerSettings settings) : IEventGuardHandler<ProductChanged, SqlServerSession>
{
    public async ValueTask HandleAsync(ProductChanged message, WorkItem work, SqlServerSession session, CancellationToken ct)
    {
        await using var command = session.CreateCommand($"""
            IF NOT EXISTS (SELECT 1 FROM [{settings.Schema}].sample_products WITH (UPDLOCK,HOLDLOCK) WHERE id=@id)
                INSERT INTO [{settings.Schema}].sample_products(id,revision,name) VALUES (@id,@revision,@name);
            ELSE
                UPDATE [{settings.Schema}].sample_products SET revision=@revision,name=@name WHERE id=@id AND revision<@revision;
            """);
        command.Parameters.AddWithValue("id", message.ProductId);
        command.Parameters.AddWithValue("revision", message.Revision);
        command.Parameters.AddWithValue("name", message.Name);
        await command.ExecuteNonQueryAsync(ct);
    }
}
