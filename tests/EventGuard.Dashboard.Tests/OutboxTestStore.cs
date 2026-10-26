using EventGuard.Operations;

namespace EventGuard.Dashboard.Tests;

public sealed partial class MetadataStore : IOutboxReadStore
{
    public int OutboxCalls { get; private set; }
    public OutboxQuery? LastOutboxQuery { get; private set; }
    public string? LastOutboxScope { get; private set; }
    public bool DeliveryUncertain { get; set; } = true;
    public OutboxState OutboxState { get; set; } = OutboxState.Quarantined;
    public Task<OutboxPage> ListOutboxAsync(OutboxQuery query, CancellationToken ct = default)
    {
        OutboxCalls++; LastOutboxScope = query.Scope.Id; LastOutboxQuery = query;
        if (Unavailable) throw new InvalidOperationException("Password=never-display");
        var message = Outgoing();
        return Task.FromResult(new OutboxPage([message], NextPage ? new(message.CreatedAt, message.Id) : null,
            query.Before is not null ? new(message.CreatedAt, message.Id) : null));
    }
    public Task<OutboxDetail?> FindOutboxAsync(ProducerScope scope, Guid id, CancellationToken ct = default)
    {
        OutboxCalls++; LastOutboxScope = scope.Id;
        if (Unavailable) throw new InvalidOperationException("Password=never-display");
        return Task.FromResult<OutboxDetail?>(scope.Tenant == "a" && (id == WorkId || AdditionalWorkIds.Contains(id)) ? new(Outgoing() with { Id = id }, [], false, [], false,
            OutboxState == OutboxState.Quarantined ? new(Guid.NewGuid(), 2, "outbox-envelope-invalid", DateTimeOffset.UtcNow) : null) : null);
    }
    private OutboxSummary Outgoing() => new(WorkId, Guid.Parse("68eec7e8-a00b-4e79-ac69-cc9cbf0b7b6c"), OutboxState,
        3, 1, 0, DeliveryUncertain, DateTimeOffset.Parse("2026-01-01T00:00:00Z"), DateTimeOffset.Parse("2026-01-01T00:00:00Z"),
        null, "outbox-envelope-invalid", "kafka", "<script>binding</script>", "v1");
}
