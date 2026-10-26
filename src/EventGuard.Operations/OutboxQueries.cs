namespace EventGuard.Operations;

public sealed record OutboxPosition(DateTimeOffset CreatedAt, Guid Id);
public sealed record OutboxFilters(DateTimeOffset? CreatedFrom = null, DateTimeOffset? CreatedUntil = null,
    Guid? IntentId = null, int? MinimumAttempts = null, bool? Replayed = null, bool? DeliveryUncertain = null);

public sealed class OutboxQuery
{
    public ProducerScope Scope { get; }
    public OutboxState? State { get; }
    public int PageSize { get; }
    public DateTimeOffset CreatedBefore { get; }
    public OutboxPosition? Before { get; }
    public OutboxPosition? After { get; }
    public OutboxFilters Filters { get; }

    public OutboxQuery(ProducerScope scope, DateTimeOffset createdBefore, OutboxState? state = null,
        int pageSize = 25, OutboxPosition? before = null, OutboxPosition? after = null, OutboxFilters? filters = null)
    {
        ArgumentNullException.ThrowIfNull(scope);
        if (state.HasValue && !Enum.IsDefined(state.Value)) throw new ArgumentOutOfRangeException(nameof(state));
        if (pageSize is < 1 or > 100) throw new ArgumentOutOfRangeException(nameof(pageSize));
        if (createdBefore.Year is < 1970 or > 9998) throw new ArgumentOutOfRangeException(nameof(createdBefore));
        if (before is not null && after is not null) throw new ArgumentException("Use one page direction.");
        var position = before ?? after;
        if (position is not null && (position.Id == Guid.Empty || position.CreatedAt.Year < 1970 || position.CreatedAt > createdBefore))
            throw new ArgumentException("Invalid page position.", nameof(before));
        filters ??= new();
        if (filters.CreatedFrom?.Year is < 1970 or > 9998 || filters.CreatedUntil?.Year is < 1970 or > 9998
            || filters.CreatedFrom >= filters.CreatedUntil || filters.IntentId == Guid.Empty || filters.MinimumAttempts < 0)
            throw new ArgumentException("Invalid outbox filters.", nameof(filters));
        Scope = scope; State = state; PageSize = pageSize; CreatedBefore = createdBefore.ToUniversalTime();
        Before = before; After = after; Filters = filters;
    }
}

// No envelope decoding: payload, headers, raw quarantine snapshot, address/routing key and replay actor/reason are omitted.
public sealed record OutboxSummary(Guid Id, Guid MessageId, OutboxState State, long Version, int AttemptCount,
    int ReplayGeneration, bool DeliveryUncertain, DateTimeOffset CreatedAt, DateTimeOffset NextAttemptAt,
    DateTimeOffset? PublishedAt, string? ReasonCode, string Transport, string Binding, string BindingVersion);
public sealed record OutboxAttemptSummary(int Number, int Generation, string Status, DateTimeOffset StartedAt,
    DateTimeOffset? FinishedAt, string? ReasonCode);
public sealed record OutboxReplaySummary(Guid RequestId, OutboxReplayStatus Status, int Generation, long Version,
    bool AcknowledgePossibleDuplicate, DateTimeOffset CreatedAt);
public sealed record OutboxQuarantineSummary(Guid Id, long ObservedVersion, string? ReasonCode, DateTimeOffset DetectedAt);
public sealed record OutboxDetail(OutboxSummary Message, IReadOnlyList<OutboxAttemptSummary> Attempts, bool HasOlderAttempts,
    IReadOnlyList<OutboxReplaySummary> Replays, bool HasOlderReplays, OutboxQuarantineSummary? Quarantine);
public sealed record OutboxPage(IReadOnlyList<OutboxSummary> Items, OutboxPosition? Next, OutboxPosition? Previous)
{
    public static OutboxPage FromWindow(List<OutboxSummary> rows, OutboxQuery query)
    {
        var more = rows.Count > query.PageSize;
        if (more) rows.RemoveAt(rows.Count - 1);
        if (query.After is not null) rows.Reverse();
        var first = rows.Count == 0 ? query.Before : new OutboxPosition(rows[0].CreatedAt, rows[0].Id);
        var last = rows.Count == 0 ? query.After : new OutboxPosition(rows[^1].CreatedAt, rows[^1].Id);
        return new(rows.AsReadOnly(), (query.After is not null || more) ? last : null,
            (query.Before is not null || (query.After is not null && more)) ? first : null);
    }
}

public interface IOutboxReadStore
{
    Task<OutboxPage> ListOutboxAsync(OutboxQuery query, CancellationToken cancellationToken = default);
    Task<OutboxDetail?> FindOutboxAsync(ProducerScope scope, Guid id, CancellationToken cancellationToken = default);
}

public static class OutboxMetadataCodes
{
    public static string? Reason(string? value) => value switch
    {
        null => null,
        "publish-interrupted" or "outbox-replay-requested" or "outbox-quarantined" or "outbox-envelope-too-large"
            or "outbox-scope-mismatch" or "outbox-fingerprint-mismatch" or "outbox-envelope-invalid"
            or "rabbitmq-binding-mismatch" or "rabbitmq-envelope-unsupported" or "rabbitmq-confirmed"
            or "rabbitmq-unroutable" or "rabbitmq-confirm-unknown" or "rabbitmq-before-publish-failed"
            or "kafka-binding-mismatch" or "kafka-envelope-unsupported" or "kafka-before-publish-cancelled"
            or "kafka-delivery-unknown" or "kafka-before-publish-failed" or "kafka-persisted" or "kafka-not-persisted" => value,
        _ => "unrecognized"
    };
    public static string Attempt(string value) => value switch
    {
        "Started" or "Accepted" or "NotSentTransient" or "NotSentPermanent" or "Unknown" => value,
        _ => "Unrecognized"
    };
}
