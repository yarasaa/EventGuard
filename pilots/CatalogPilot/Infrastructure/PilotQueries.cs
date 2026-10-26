using CatalogPilot.Hosting;
using EventGuard;
using Npgsql;

namespace CatalogPilot.Infrastructure;

// Local pilot diagnostics only. EventGuard state is never modified by these queries.
public sealed class PilotQueries(PilotStore store)
{
    public async Task<WorkItem?> FindEventAsync(Guid eventId, CancellationToken ct)
    {
        await using var command = store.Data.CreateCommand($"SELECT id FROM {store.Schema}.works WHERE scope_id=@scope AND message_id=@event LIMIT 1");
        command.Parameters.AddWithValue("scope", store.Settings.Consumer.Id); command.Parameters.AddWithValue("event", eventId);
        return await command.ExecuteScalarAsync(ct) is Guid id ? await store.Work.FindAsync(store.Settings.Consumer, id, ct) : null;
    }
    public async Task<object> EventAsync(Guid eventId, CancellationToken ct)
    {
        var work = await FindEventAsync(eventId, ct);
        await using var command = store.Data.CreateCommand($"""
            SELECT (SELECT count(*) FROM {store.Schema}.source_events WHERE event_id=@event),
              (SELECT count(*) FROM {store.Schema}.business_effects WHERE event_id=@event),
              (SELECT count(*) FROM {store.Schema}.conflicts WHERE message_id=@event),
              (SELECT count(*) FROM {store.Schema}.outbox_messages WHERE origin_work_id=@work AND state=3),
              (SELECT count(*) FROM {store.Schema}.outbox_messages WHERE origin_work_id=@work)
            """);
        command.Parameters.AddWithValue("event", eventId); command.Parameters.AddWithValue("work", work?.Id ?? Guid.Empty);
        await using var reader = await command.ExecuteReaderAsync(ct); await reader.ReadAsync(ct);
        return new { eventId, work = work is null ? null : new { work.Id, State = work.State.ToString(), work.Version, work.AttemptCount, work.ReplayGeneration, work.LastReason },
            sourceWrites = reader.GetInt64(0), effects = reader.GetInt64(1), conflicts = reader.GetInt64(2),
            completionPublished = reader.GetInt64(3), completionIntents = reader.GetInt64(4) };
    }
    public async Task<object?> ProductAsync(string productId, CancellationToken ct)
    {
        await using var command = store.Data.CreateCommand($"SELECT revision,name,category FROM {store.Schema}.search_projection WHERE product_id=@id");
        command.Parameters.AddWithValue("id", productId);
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? new { productId, revision = reader.GetInt64(0), name = reader.GetString(1), category = reader.GetString(2) } : null;
    }
    public async Task<object> SummaryAsync(CancellationToken ct)
    {
        await using var command = store.Data.CreateCommand($"""
            SELECT (SELECT count(*) FROM {store.Schema}.works),
              (SELECT count(*) FROM {store.Schema}.works WHERE state=3),
              (SELECT count(*) FROM {store.Schema}.business_effects),
              (SELECT count(*) FROM (SELECT event_id FROM {store.Schema}.business_effects GROUP BY event_id HAVING count(*)>1) duplicates),
              (SELECT count(*) FROM {store.Schema}.outbox_messages),
              (SELECT count(*) FROM {store.Schema}.outbox_messages WHERE state=3),
              (SELECT count(*) FROM {store.Schema}.ingress_quarantine WHERE released_work_id IS NULL),
              (SELECT count(*) FROM {store.Schema}.source_events)
            """);
        await using var r = await command.ExecuteReaderAsync(ct); await r.ReadAsync(ct);
        return new { works = r.GetInt64(0), completed = r.GetInt64(1), effects = r.GetInt64(2), duplicateEffects = r.GetInt64(3),
            outbox = r.GetInt64(4), published = r.GetInt64(5), held = r.GetInt64(6), sourceWrites = r.GetInt64(7) };
    }
    public async Task<object> QuarantineAsync(CancellationToken ct)
    {
        await using var command = store.Data.CreateCommand($"SELECT id,reason_code,version,released_work_id FROM {store.Schema}.ingress_quarantine WHERE scope_id=@scope ORDER BY created_at DESC LIMIT 25");
        command.Parameters.AddWithValue("scope", store.Settings.Consumer.Id);
        await using var r = await command.ExecuteReaderAsync(ct); var items = new List<object>();
        while (await r.ReadAsync(ct)) items.Add(new { id = r.GetGuid(0), reason = r.GetString(1), version = r.GetInt64(2), released = !r.IsDBNull(3) });
        return items;
    }
}
