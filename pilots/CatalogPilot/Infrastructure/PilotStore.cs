using System.Text.Json;
using CatalogPilot.Contracts;
using CatalogPilot.Domain;
using CatalogPilot.Hosting;
using EventGuard;
using EventGuard.Operations.PostgreSql;
using EventGuard.PostgreSql;
using EventGuard.RabbitMq;
using Npgsql;

namespace CatalogPilot.Infrastructure;

public sealed class PilotStore(PilotSettings settings) : IAsyncDisposable
{
    public PilotSettings Settings { get; } = settings;
    public NpgsqlDataSource Data { get; } = NpgsqlDataSource.Create(settings.ConnectionString);
    public PostgreSqlWorkStore Work => new(Data, Settings.Schema);
    public PostgreSqlOutboxStore Outbox => new(Data, Settings.Schema);
    public string Schema => Settings.QuotedSchema;
    public PublishRoute Route(string queue) => new("rabbitmq", "pilot", "v1", RabbitMqPublisher.DefaultExchange, queue);

    public async Task InitializeAsync(CancellationToken ct)
    {
        await Work.InitializeAsync(ct);
        await using var command = Data.CreateCommand($"""
            CREATE TABLE IF NOT EXISTS {Schema}.categories(name text PRIMARY KEY);
            INSERT INTO {Schema}.categories VALUES('general') ON CONFLICT DO NOTHING;
            CREATE TABLE IF NOT EXISTS {Schema}.source_events(event_id uuid NOT NULL, product_id text NOT NULL);
            CREATE TABLE IF NOT EXISTS {Schema}.products(product_id text PRIMARY KEY, revision bigint NOT NULL, name text NOT NULL, category text NOT NULL);
            CREATE TABLE IF NOT EXISTS {Schema}.search_projection(product_id text PRIMARY KEY, revision bigint NOT NULL, name text NOT NULL, category text NOT NULL);
            CREATE TABLE IF NOT EXISTS {Schema}.business_effects(event_id uuid NOT NULL, work_id uuid NOT NULL, applied boolean NOT NULL);
            CREATE INDEX IF NOT EXISTS ix_pilot_effect_event ON {Schema}.business_effects(event_id);
            CREATE INDEX IF NOT EXISTS ix_pilot_source_event ON {Schema}.source_events(event_id);
            CREATE INDEX IF NOT EXISTS ix_pilot_work_message ON {Schema}.works(scope_id,message_id);
            """ + OperationsIndexes.CreateScript(Settings.Schema) + OutboxReadIndexes.CreateScript(Settings.Schema) + AuditIndexes.CreateScript(Settings.Schema));
        await command.ExecuteNonQueryAsync(ct);
        await using var connection = await Settings.Broker().CreateConnectionAsync(ct);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: ct);
        foreach (var queue in new[] { Settings.InputQueue, Settings.OutputQueue })
            await channel.QueueDeclareAsync(queue, true, false, false,
                new Dictionary<string, object?> { ["x-queue-type"] = "quorum", ["x-delivery-limit"] = -1 }, cancellationToken: ct);
    }

    public async Task ValidateAsync(CancellationToken ct)
    {
        await Work.ValidateStartupAsync(new("pilot", StoreRole.Consumer,
            new(StoreCapabilities.IngressQuarantine | StoreCapabilities.DurableReplay)), ct);
        await Outbox.ValidateStartupAsync(new("pilot", StoreRole.Outbox), ct);
    }

    public async Task<ProducerResult> SubmitAsync(ProductChanged value, CancellationToken ct)
    {
        ProductContract.Validate(value);
        if (value.ContractVersion != 1) throw new IngressMessageRejectedException("unsupported-api-version");
        var payload = JsonSerializer.SerializeToUtf8Bytes(value, ProductContract.Json);
        return await Outbox.ProduceAsync(new(Settings.Producer, value.EventId, ContentFingerprint.FromBytes(payload)), async (session, token) =>
        {
            await using var insert = session.CreateCommand($"""
                INSERT INTO {Schema}.source_events VALUES(@event,@product);
                INSERT INTO {Schema}.products VALUES(@product,@revision,@name,@category)
                ON CONFLICT(product_id) DO UPDATE SET revision=EXCLUDED.revision,name=EXCLUDED.name,category=EXCLUDED.category
                WHERE {Schema}.products.revision < EXCLUDED.revision;
                """);
            Parameters(insert, value); await insert.ExecuteNonQueryAsync(token);
            await session.Outbox.EnqueueAsync(new(Settings.Producer, value.EventId, value.EventId, Route(Settings.InputQueue), payload), token);
        }, ct);
    }

    public async ValueTask ApplyAsync(WorkItem work, ReadOnlyMemory<byte> payload, PostgreSqlSession session, CancellationToken ct)
    {
        var value = ProductContract.Codec.Decode(payload);
        await using (var category = session.CreateCommand($"SELECT EXISTS(SELECT 1 FROM {Schema}.categories WHERE name=@category)"))
        {
            category.Parameters.AddWithValue("category", value.Category);
            if (!(bool)(await category.ExecuteScalarAsync(ct))!) throw new PermanentWorkException("unknown-category");
        }
        await using (var timeout = session.CreateCommand("SET LOCAL lock_timeout = '400ms'")) await timeout.ExecuteNonQueryAsync(ct);
        int changed;
        await using (var update = session.CreateCommand($"""
            INSERT INTO {Schema}.search_projection VALUES(@product,@revision,@name,@category)
            ON CONFLICT(product_id) DO UPDATE SET revision=EXCLUDED.revision,name=EXCLUDED.name,category=EXCLUDED.category
            WHERE {Schema}.search_projection.revision < EXCLUDED.revision
            """))
        {
            Parameters(update, value); changed = await update.ExecuteNonQueryAsync(ct);
        }
        // No unique constraint here: the pilot must expose, rather than hide, any repeated effect.
        await using (var effect = session.CreateCommand($"INSERT INTO {Schema}.business_effects VALUES(@event,@work,@applied)"))
        {
            effect.Parameters.AddWithValue("event", value.EventId); effect.Parameters.AddWithValue("work", work.Id);
            effect.Parameters.AddWithValue("applied", changed == 1); await effect.ExecuteNonQueryAsync(ct);
        }
        var outgoing = JsonSerializer.SerializeToUtf8Bytes(new { value.EventId, value.ProductId, value.Revision, Applied = changed == 1 }, ProductContract.Json);
        await session.Outbox.EnqueueAsync(new(Settings.IndexedProducer, work.Id, work.Id, Route(Settings.OutputQueue), outgoing), ct);
        // An explicit verification-only barrier lets the external runner kill a real uncommitted transaction.
        if (Environment.GetEnvironmentVariable("PILOT_TEST_CONTROLS") == "1"
            && Environment.GetEnvironmentVariable("PILOT_HOLD_EVENT") == value.EventId.ToString("D"))
        {
            await File.WriteAllTextAsync(Environment.GetEnvironmentVariable("PILOT_HOLD_FILE")!, "business-and-outbox-written-before-commit", ct);
            await Task.Delay(Timeout.InfiniteTimeSpan, ct);
        }
    }

    private static void Parameters(NpgsqlCommand command, ProductChanged value)
    {
        command.Parameters.AddWithValue("event", value.EventId); command.Parameters.AddWithValue("product", value.ProductId);
        command.Parameters.AddWithValue("revision", value.Revision); command.Parameters.AddWithValue("name", value.Name);
        command.Parameters.AddWithValue("category", value.Category);
    }
    public ValueTask DisposeAsync() => Data.DisposeAsync();
}
