namespace EventGuard.Operations;

public enum AuditKind { ConsumerReplay, IngressRelease, OutboxReplay, OutboxRepair, Retention, BulkPlan }
public sealed record AuditPosition(DateTimeOffset CreatedAt, AuditKind Kind, Guid Id);
public sealed record AuditEntry(Guid RequestId, AuditKind Kind, Guid? SubjectId, string Actor, string Reason,
    string Outcome, bool AcknowledgePossibleDuplicate, DateTimeOffset CreatedAt);

/// <summary>Request metadata only. A time fence is not a database snapshot; late commits may appear on a later read.</summary>
public sealed class AuditQuery
{
    public string ScopeId { get; }
    public bool Outbox { get; }
    public DateTimeOffset From { get; }
    public DateTimeOffset Until { get; }
    public int PageSize { get; }
    public AuditPosition? Before { get; }
    public AuditPosition? After { get; }
    public AuditQuery(WorkScope scope, DateTimeOffset from, DateTimeOffset until, int pageSize = 100, AuditPosition? before = null, AuditPosition? after = null)
        : this((scope ?? throw new ArgumentNullException(nameof(scope))).Id, false, from, until, pageSize, before, after) { }
    public AuditQuery(ProducerScope scope, DateTimeOffset from, DateTimeOffset until, int pageSize = 100, AuditPosition? before = null, AuditPosition? after = null)
        : this((scope ?? throw new ArgumentNullException(nameof(scope))).Id, true, from, until, pageSize, before, after) { }
    private AuditQuery(string scope, bool outbox, DateTimeOffset from, DateTimeOffset until, int size, AuditPosition? before, AuditPosition? after)
    {
        if (size is < 1 or > 100 || until <= from || until - from > TimeSpan.FromDays(31) || before is not null && after is not null)
            throw new ArgumentException("Audit reads require 1–100 rows and a positive window of at most 31 days.");
        if ((before ?? after) is { } position && (position.Id == Guid.Empty || !Enum.IsDefined(position.Kind) || position.CreatedAt < from || position.CreatedAt >= until))
            throw new ArgumentException("Invalid audit position.");
        ScopeId = scope; Outbox = outbox; From = from.ToUniversalTime(); Until = until.ToUniversalTime(); PageSize = size; Before = before; After = after;
    }
}
public sealed record AuditPage(IReadOnlyList<AuditEntry> Items, AuditPosition? Next, AuditPosition? Previous)
{
    public static AuditPage FromWindow(List<AuditEntry> rows, AuditQuery query)
    {
        var more = rows.Count > query.PageSize;
        if (more) rows.RemoveAt(rows.Count - 1);
        if (query.After is not null) rows.Reverse();
        static AuditPosition Position(AuditEntry row) => new(row.CreatedAt, row.Kind, row.RequestId);
        var first = rows.Count == 0 ? query.Before : Position(rows[0]);
        var last = rows.Count == 0 ? query.After : Position(rows[^1]);
        return new(rows.AsReadOnly(), query.After is not null || more ? last : null,
            query.Before is not null || query.After is not null && more ? first : null);
    }
}
/// <summary>Host must separately authorize audit access: actors and operator reasons may be sensitive.</summary>
public interface IAuditReadStore
{
    Task ValidateAuditAsync(bool consumer, bool outbox, CancellationToken ct = default);
    Task<AuditPage> ListAuditAsync(AuditQuery query, CancellationToken ct = default);
}
public sealed partial class OperationsReader
{
    public Task<ReadSnapshot<AuditPage>> ListAuditAsync(AuditQuery query, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        return ReadAsync(token => Audit().ListAuditAsync(query, token), ct);
    }
    public Task<ReadSnapshot<bool>> CheckAuditHealthAsync(bool consumer, bool outbox, CancellationToken ct = default) => ReadAsync(async token =>
    { await Audit().ValidateAuditAsync(consumer, outbox, token); return true; }, ct);
    private IAuditReadStore Audit() => _store as IAuditReadStore ?? throw new NotSupportedException("Audit reads are unavailable.");
}

public static class AuditOutcomes
{
    public static string Read(AuditKind kind, int status) => kind switch
    {
        AuditKind.ConsumerReplay => Known<ReplayStatus>(status), AuditKind.IngressRelease => Known<IngressReleaseStatus>(status),
        AuditKind.OutboxReplay => Known<OutboxReplayStatus>(status), AuditKind.OutboxRepair => Known<OutboxRepairStatus>(status),
        AuditKind.Retention => "Recorded", AuditKind.BulkPlan => "Plan prepared", _ => throw new InvalidDataException("Unknown audit kind.")
    };
    private static string Known<T>(int value) where T : struct, Enum => Enum.IsDefined(typeof(T), value)
        ? ((T)Enum.ToObject(typeof(T), value)).ToString() : throw new InvalidDataException("Unknown audit outcome.");
}
