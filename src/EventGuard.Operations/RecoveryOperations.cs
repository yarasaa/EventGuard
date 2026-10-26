namespace EventGuard.Operations;

public sealed record IngressSummary(Guid Id, long Version, string KeyVersion, string FingerprintVersion,
    string ReasonCode, Guid? ReleasedWorkId, bool DataRetained, DateTimeOffset CreatedAt);
public sealed class IngressQuery
{
    public WorkQuery Window { get; }
    public bool? Released { get; }
    public IngressQuery(WorkScope scope, DateTimeOffset cutoff, bool? released = null, int pageSize = 25,
        WorkPosition? before = null, WorkPosition? after = null, Guid? id = null)
    {
        Window = new(scope, cutoff, pageSize: pageSize, before: before, after: after, filters: new(WorkId: id));
        Released = released;
    }
}
public sealed record IngressPage(IReadOnlyList<IngressSummary> Items, WorkPosition? Next, WorkPosition? Previous)
{
    public static IngressPage FromWindow(List<IngressSummary> rows, IngressQuery query)
    {
        var window = query.Window;
        var more = rows.Count > window.PageSize;
        if (more) rows.RemoveAt(rows.Count - 1);
        if (window.After is not null) rows.Reverse();
        var first = rows.Count == 0 ? window.Before : new WorkPosition(rows[0].CreatedAt, rows[0].Id);
        var last = rows.Count == 0 ? window.After : new WorkPosition(rows[^1].CreatedAt, rows[^1].Id);
        return new(rows.AsReadOnly(), window.After is not null || more ? last : null,
            window.Before is not null || window.After is not null && more ? first : null);
    }
}
/// <summary>Metadata only. Raw snapshots, source coordinates, keys and fingerprints are deliberately omitted.</summary>
public interface IIngressReadStore
{
    Task<IngressPage> ListIngressAsync(IngressQuery query, CancellationToken ct = default);
    Task<IngressSummary?> FindIngressAsync(WorkScope scope, Guid id, CancellationToken ct = default);
}
public interface IRecoveryHealthCheck
{
    Task ValidateRecoveryAsync(CancellationToken ct = default);
}
public sealed record IngressReleaseReceipt(Guid RequestId, IngressReleaseResult Result, string Reason, DateTimeOffset CreatedAt);
public sealed record OutboxRepairReceipt(Guid RequestId, OutboxRepairResult Result, string Reason, bool AcknowledgePossibleDuplicate, DateTimeOffset CreatedAt);
/// <summary>The host must independently authorize recovery, scope and actor before every call.</summary>
public interface IRecoveryCommandStore : IRecoveryHealthCheck
{
    Task<IngressReleaseResult> ReleaseIngressAsync(IngressReleaseRequest request, CancellationToken ct = default);
    Task<OutboxRepairResult> RepairOutboxAsync(OutboxRepairRequest request, CancellationToken ct = default);
    Task<IngressReleaseReceipt?> FindIngressReleaseReceiptAsync(WorkScope scope, Guid id, Guid requestId, string actor, CancellationToken ct = default);
    Task<OutboxRepairReceipt?> FindOutboxRepairReceiptAsync(ProducerScope scope, Guid id, Guid requestId, string actor, CancellationToken ct = default);
}
public sealed partial class OperationsReader
{
    public Task<ReadSnapshot<IngressPage>> ListIngressAsync(IngressQuery query, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        return ReadAsync(token => Ingress().ListIngressAsync(query, token), ct);
    }
    public Task<ReadSnapshot<IngressSummary?>> FindIngressAsync(WorkScope scope, Guid id, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        if (id == Guid.Empty) throw new ArgumentException("A quarantine ID is required.", nameof(id));
        return ReadAsync(token => Ingress().FindIngressAsync(scope, id, token), ct);
    }
    public Task<ReadSnapshot<bool>> CheckIngressHealthAsync(CancellationToken ct = default) => ReadAsync(async token =>
    {
        if (_store is not IRecoveryHealthCheck health) throw new NotSupportedException("Ingress validation is unavailable.");
        await health.ValidateRecoveryAsync(token); return true;
    }, ct);
    private IIngressReadStore Ingress() => _store as IIngressReadStore ?? throw new NotSupportedException("Ingress recovery metadata is unavailable.");
}
public sealed partial class OperationsReplayer
{
    public Task<bool> CheckRecoveryHealthAsync(CancellationToken ct = default) => ReceiptAsync(async token =>
    { await Recovery().ValidateRecoveryAsync(token); return true; }, ct);
    private IRecoveryCommandStore Recovery() => _store as IRecoveryCommandStore ?? throw new NotSupportedException("Recovery commands are unavailable.");
    public Task<IngressReleaseResult> ReleaseIngressAsync(IngressReleaseRequest request, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CommandAsync(request.RequestId, token => Recovery().ReleaseIngressAsync(request, token), ct);
    }
    public Task<OutboxRepairResult> RepairOutboxAsync(OutboxRepairRequest request, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CommandAsync(request.RequestId, token => Recovery().RepairOutboxAsync(request, token), ct);
    }
    public Task<IngressReleaseReceipt?> FindIngressReleaseReceiptAsync(WorkScope scope, Guid id, Guid requestId, string actor, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(scope); ValidateLookup(id, requestId, actor);
        return ReceiptAsync(token => Recovery().FindIngressReleaseReceiptAsync(scope, id, requestId, actor, token), ct);
    }
    public Task<OutboxRepairReceipt?> FindOutboxRepairReceiptAsync(ProducerScope scope, Guid id, Guid requestId, string actor, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(scope); ValidateLookup(id, requestId, actor);
        return ReceiptAsync(token => Recovery().FindOutboxRepairReceiptAsync(scope, id, requestId, actor, token), ct);
    }
}
