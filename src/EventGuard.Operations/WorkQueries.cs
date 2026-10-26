namespace EventGuard.Operations;

// Scope is a query boundary, not authorization. The host must authorize it before calling these ports.
public sealed record WorkPosition(DateTimeOffset CreatedAt, Guid Id);
public sealed record WorkFilters(DateTimeOffset? CreatedFrom = null, DateTimeOffset? CreatedUntil = null,
    Guid? WorkId = null, int? MinimumAttempts = null, bool? Replayed = null);

public sealed class WorkQuery
{
    public WorkScope Scope { get; }
    public WorkState? State { get; }
    public int PageSize { get; }
    public DateTimeOffset CreatedBefore { get; }
    public WorkPosition? Before { get; }
    public WorkPosition? After { get; }
    public WorkFilters Filters { get; }

    public WorkQuery(WorkScope scope, DateTimeOffset createdBefore, WorkState? state = null,
        int pageSize = 25, WorkPosition? before = null, WorkPosition? after = null, WorkFilters? filters = null)
    {
        ArgumentNullException.ThrowIfNull(scope);
        if (state.HasValue && !Enum.IsDefined(state.Value)) throw new ArgumentOutOfRangeException(nameof(state));
        if (pageSize is < 1 or > 100) throw new ArgumentOutOfRangeException(nameof(pageSize));
        if (createdBefore.Year < 1970 || createdBefore.Year > 9998) throw new ArgumentOutOfRangeException(nameof(createdBefore));
        if (before is not null && after is not null) throw new ArgumentException("Use one page direction.");
        var position = before ?? after;
        if (position is not null && (position.Id == Guid.Empty || position.CreatedAt.Year < 1970 || position.CreatedAt > createdBefore))
            throw new ArgumentException("Invalid page position.", nameof(before));
        filters ??= new();
        if (filters.CreatedFrom?.Year is < 1970 or > 9998 || filters.CreatedUntil?.Year is < 1970 or > 9998
            || filters.CreatedFrom >= filters.CreatedUntil || filters.WorkId == Guid.Empty || filters.MinimumAttempts < 0)
            throw new ArgumentException("Invalid work filters.", nameof(filters));
        Scope = scope;
        State = state;
        PageSize = pageSize;
        CreatedBefore = createdBefore.ToUniversalTime();
        Before = before;
        After = after;
        Filters = filters;
    }
}

// Deliberately omit payload, business key, fingerprint, correlation, lease token and replay actor/free text.
public sealed record WorkSummary(Guid Id, WorkState State, long Version, int AttemptCount, int ReplayGeneration,
    DateTimeOffset CreatedAt, DateTimeOffset NextAttemptAt, DateTimeOffset? ExpiresAt,
    DateTimeOffset? CompletedAt, string? ReasonCode);
public sealed record AttemptSummary(int Number, int Generation, string Status,
    DateTimeOffset StartedAt, DateTimeOffset? FinishedAt, string? ReasonCode);
public sealed record ReplaySummary(Guid RequestId, ReplayStatus Status, int Generation, long Version, DateTimeOffset CreatedAt);
public sealed record WorkPage(IReadOnlyList<WorkSummary> Items, WorkPosition? Next, WorkPosition? Previous = null)
{
    // A reverse seek reads ascending, then restores the newest-first display order.
    public static WorkPage FromWindow(List<WorkSummary> rows, WorkQuery query)
    {
        var more = rows.Count > query.PageSize;
        if (more) rows.RemoveAt(rows.Count - 1);
        if (query.After is not null) rows.Reverse();
        var first = rows.Count == 0 ? query.Before : new WorkPosition(rows[0].CreatedAt, rows[0].Id);
        var last = rows.Count == 0 ? query.After : new WorkPosition(rows[^1].CreatedAt, rows[^1].Id);
        return new(rows.AsReadOnly(), (query.After is not null || more) ? last : null,
            (query.Before is not null || (query.After is not null && more)) ? first : null);
    }
}
public sealed record WorkDetail(WorkSummary Work, IReadOnlyList<AttemptSummary> Attempts, bool HasOlderAttempts,
    IReadOnlyList<ReplaySummary> Replays, bool HasOlderReplays);
public sealed record ReadSnapshot<T>(T Value, DateTimeOffset ReadStartedAt, DateTimeOffset ReadFinishedAt);

public interface IWorkReadStore
{
    Task<WorkPage> ListAsync(WorkQuery query, CancellationToken cancellationToken = default);
    Task<WorkDetail?> FindAsync(WorkScope scope, Guid id, CancellationToken cancellationToken = default);
}

public static class MetadataCodes
{
    public static string? Reason(string? value) => value switch
    {
        null => null,
        "expired" or "permanent-failure" or "attempt-budget-exhausted" or "cancelled" or "transient-failure" or "replay-requested" or "lease-recovered" => value,
        _ => "unrecognized"
    };

    public static string Attempt(string value) => value switch
    {
        "Started" or "Abandoned" or "Completed" or "Failed" or "Expired" => value,
        _ => "Unknown"
    };
}
