namespace EventGuard.Operations;

public enum BacklogKind { Consumer, Outbox }
public sealed record BacklogState(int Code, string Name);

/// <summary>A state inventory, not a count of immediately claimable work. No arbitrary filters.</summary>
public sealed class BacklogQuery
{
    public BacklogKind Kind { get; }
    public string ScopeId { get; }
    public int CountLimit { get; }
    public IReadOnlyList<BacklogState> States { get; }
    private BacklogQuery(BacklogKind kind, string scopeId, int countLimit)
    {
        if (countLimit is < 1 or > 10_000) throw new ArgumentOutOfRangeException(nameof(countLimit));
        Kind = kind; ScopeId = scopeId; CountLimit = countLimit;
        States = Array.AsReadOnly(kind == BacklogKind.Consumer
            ? Enum.GetValues<WorkState>().Where(s => s != WorkState.Completed).Select(s => new BacklogState((int)s, s.ToString())).ToArray()
            : Enum.GetValues<OutboxState>().Where(s => s != OutboxState.Published).Select(s => new BacklogState((int)s, s.ToString())).ToArray());
    }
    public static BacklogQuery Consumer(WorkScope scope, int countLimit = 10_000) => new(BacklogKind.Consumer, (scope ?? throw new ArgumentNullException(nameof(scope))).Id, countLimit);
    public static BacklogQuery Outbox(ProducerScope scope, int countLimit = 10_000) => new(BacklogKind.Outbox, (scope ?? throw new ArgumentNullException(nameof(scope))).Id, countLimit);
}

public sealed record BacklogBucket(string State, int Count, bool IsLowerBound, DateTimeOffset? OldestCreatedAt);
public sealed record BacklogSummary(int CountLimit, IReadOnlyList<BacklogBucket> Buckets);
public interface IBacklogReadStore
{
    Task<BacklogSummary> ReadBacklogAsync(BacklogQuery query, CancellationToken cancellationToken = default);
}
