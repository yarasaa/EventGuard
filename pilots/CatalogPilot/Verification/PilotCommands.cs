using System.Text.Json;
using CatalogPilot.Contracts;
using CatalogPilot.Infrastructure;
using EventGuard;
using EventGuard.RabbitMq;

namespace CatalogPilot.Verification;

// Trusted local operator/test commands; never mapped to the application's public HTTP routes.
public static class PilotCommands
{
    public static async Task<object> RunAsync(string[] args, PilotStore store, CancellationToken ct)
    {
        var config = store.Settings;
        switch (args[0])
        {
            case "inject":
                var bytes = await File.ReadAllBytesAsync(args[1], ct);
                await using (var connection = await RabbitMqConnection.OpenAsync(config.Broker(), ct))
                await using (var publisher = new RabbitMqPublisher(connection, "pilot", "v1"))
                {
                    var id = Guid.NewGuid();
                    var count = args.Length > 2 ? int.Parse(args[2]) : 1;
                    if (count is < 1 or > 100) throw new ArgumentOutOfRangeException(nameof(count));
                    for (var i = 0; i < count; i++)
                    {
                        var result = await publisher.PublishAsync(new(config.Producer, id, id, store.Route(config.InputQueue), bytes), ct);
                        if (result.Status != PublishStatus.Accepted) throw new InvalidOperationException("Native injection was not confirmed.");
                    }
                    return new { status = "Accepted", count };
                }
            case "category":
                await using (var command = store.Data.CreateCommand($"INSERT INTO {store.Schema}.categories VALUES(@name) ON CONFLICT DO NOTHING"))
                { command.Parameters.AddWithValue("name", args[1]); await command.ExecuteNonQueryAsync(ct); }
                return new { status = "configured" };
            case "replay":
                var work = await new PilotQueries(store).FindEventAsync(Guid.Parse(args[1]), ct) ?? throw new InvalidOperationException("Work not found.");
                var replay = await store.Work.ReplayAsync(new(config.Consumer, Guid.Parse(args[2]), work.Id, long.Parse(args[3]), "local-pilot-operator", "Category configuration corrected; retry unchanged input."), ct);
                return new { status = replay.Status.ToString(), replay.RepeatedRequest, replay.Generation };
            case "release":
                var payload = await File.ReadAllBytesAsync(args[3], ct);
                var submission = ProductContract.Codec.Map(config.Consumer, config.Policy, payload, v => (v.EventId.ToString("D"), v.EventId));
                var release = await store.Work.ReleaseAsync(new(Guid.Parse(args[2]), Guid.Parse(args[1]), long.Parse(args[4]), submission,
                    "local-pilot-operator", "Unsupported input corrected to the explicit supported contract."), ct);
                return new { status = release.Status.ToString(), release.RepeatedRequest, release.WorkId };
            case "lock":
                if (Environment.GetEnvironmentVariable("PILOT_TEST_CONTROLS") != "1") throw new InvalidOperationException("Test controls are disabled.");
                await using (var connection = await store.Data.OpenConnectionAsync(ct))
                await using (var transaction = await connection.BeginTransactionAsync(ct))
                {
                    await using var command = connection.CreateCommand(); command.Transaction = transaction;
                    command.CommandText = $"SELECT product_id FROM {store.Schema}.search_projection WHERE product_id=@id FOR UPDATE";
                    command.Parameters.AddWithValue("id", args[1]);
                    if (await command.ExecuteScalarAsync(ct) is null) throw new InvalidOperationException("Lock target missing.");
                    await File.WriteAllTextAsync(args[2], "native-row-lock-held", ct);
                    using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct); timeout.CancelAfter(TimeSpan.FromSeconds(60));
                    while (!File.Exists(args[3])) await Task.Delay(50, timeout.Token);
                    await transaction.RollbackAsync(ct);
                }
                return new { status = "lock-released" };
            case "inspect": return await new PilotQueries(store).SummaryAsync(ct);
            case "verify-batch":
                var ids = JsonSerializer.Deserialize<Guid[]>(await File.ReadAllBytesAsync(args[1], ct))!;
                if (ids.Length is < 1 or > 1000 || ids.Distinct().Count() != ids.Length) throw new ArgumentException("Use 1–1000 unique event IDs.");
                await using (var query = store.Data.CreateCommand($"""
                    SELECT (SELECT count(*) FROM {store.Schema}.works WHERE scope_id=@scope AND message_id=ANY(@ids) AND state=3),
                      (SELECT count(*) FROM {store.Schema}.source_events WHERE event_id=ANY(@ids)),
                      (SELECT count(*) FROM (SELECT event_id FROM {store.Schema}.business_effects WHERE event_id=ANY(@ids) GROUP BY event_id HAVING count(*)=1) single_effects),
                      (SELECT count(*) FROM {store.Schema}.outbox_messages o JOIN {store.Schema}.works w ON w.id=o.origin_work_id WHERE w.message_id=ANY(@ids) AND o.state=3)
                    """))
                {
                    query.Parameters.AddWithValue("scope", config.Consumer.Id); query.Parameters.AddWithValue("ids", ids);
                    await using var result = await query.ExecuteReaderAsync(ct); await result.ReadAsync(ct);
                    var counts = Enumerable.Range(0, 4).Select(result.GetInt64).ToArray();
                    if (counts.Any(n => n != ids.Length)) throw new InvalidOperationException("Batch has missing, incomplete or repeated effects.");
                    return new { requested = ids.Length, completed = counts[0], sourceWrites = counts[1], exactlyOneEffect = counts[2], completionPublished = counts[3] };
                }
            case "raw-proof":
                var raw = await store.Work.ReadQuarantineDataAsync(config.Consumer, Guid.Parse(args[1]), ct) ?? throw new InvalidOperationException("Quarantine missing.");
                return new { fingerprint = raw.Fingerprint, bodyHash = ContentFingerprint.FromBytes(raw.CopyBody()).Value };
            default: throw new ArgumentException("Unknown pilot command.");
        }
    }
}
