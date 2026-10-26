using System.Globalization;
namespace EventGuard;

public enum BulkReplayKind { Consumer, Outbox }
public enum BulkReplayStatus { Prepared, RequestConflict }
public sealed record BulkReplayItem(Guid Id, long ExpectedVersion);
public sealed record BulkReplayPreparation(BulkReplayStatus Status, bool RepeatedRequest = false);
public sealed record BulkReplayEntry(Guid Id, Guid RequestId, long ExpectedVersion, string? Status, int? Generation, long? Version);
public sealed record BulkReplayProgress(BulkReplayRequest Request, IReadOnlyList<BulkReplayEntry> Entries, DateTimeOffset CreatedAt)
{
    public bool Complete => Entries.All(item => item.Status is not null);
}
/// <summary>A frozen, bounded plan. Each member uses the ordinary durable replay protocol in its own transaction.</summary>
public sealed class BulkReplayRequest
{
    public Guid RequestId { get; }
    public WorkScope? ConsumerScope { get; }
    public ProducerScope? OutboxScope { get; }
    public BulkReplayKind Kind { get; }
    public string ScopeId => ConsumerScope?.Id ?? OutboxScope!.Id;
    public IReadOnlyList<BulkReplayItem> Items { get; }
    public string Actor { get; }
    public string Reason { get; }
    public bool AcknowledgePossibleDuplicate { get; }
    public string Fingerprint { get; }
    public BulkReplayRequest(WorkScope scope, Guid requestId, IEnumerable<BulkReplayItem> items, string actor, string reason)
        : this(scope ?? throw new ArgumentNullException(nameof(scope)), null, requestId, items, actor, reason, false) { }
    public BulkReplayRequest(ProducerScope scope, Guid requestId, IEnumerable<BulkReplayItem> items, string actor, string reason, bool acknowledgePossibleDuplicate = false)
        : this(null, scope ?? throw new ArgumentNullException(nameof(scope)), requestId, items, actor, reason, acknowledgePossibleDuplicate) { }
    private BulkReplayRequest(WorkScope? consumer, ProducerScope? outbox, Guid requestId, IEnumerable<BulkReplayItem> items, string actor, string reason, bool acknowledge)
    {
        ArgumentNullException.ThrowIfNull(items);
        var copy = items.Take(101).ToArray();
        if (requestId == Guid.Empty || copy.Length is < 1 or > 100 || copy.Any(i => i is null || i.Id == Guid.Empty || i.ExpectedVersion < 0)
            || copy.Select(i => i.Id).Distinct().Count() != copy.Length) throw new ArgumentException("Select 1–100 unique records with their expected versions and a stable batch ID.");
        ConsumerScope = consumer; OutboxScope = outbox; Kind = consumer is null ? BulkReplayKind.Outbox : BulkReplayKind.Consumer;
        RequestId = requestId; Items = Array.AsReadOnly(copy); Actor = IdentityRules.Required(actor, nameof(actor)); Reason = IdentityRules.Required(reason, nameof(reason));
        AcknowledgePossibleDuplicate = acknowledge;
        var parts = new List<string> { "bulk-replay-plan/v1", ScopeId, Kind.ToString(), Actor, Reason, acknowledge ? "1" : "0" };
        foreach (var item in copy) { parts.Add(item.Id.ToString("D")); parts.Add(item.ExpectedVersion.ToString(CultureInfo.InvariantCulture)); }
        Fingerprint = StableHash.Of(parts.ToArray());
    }
    public Guid ItemRequestId(int index)
    {
        if (index < 0 || index >= Items.Count) throw new ArgumentOutOfRangeException(nameof(index));
        return Guid.ParseExact(StableHash.Of("bulk-replay-item/v1", Kind.ToString(), ScopeId, RequestId.ToString("D"), index.ToString(CultureInfo.InvariantCulture))[..32], "N");
    }
    public ReplayRequest ConsumerRequest(int index) => new(ConsumerScope ?? throw new InvalidOperationException("Not a consumer plan."),
        ItemRequestId(index), Items[index].Id, Items[index].ExpectedVersion, Actor, Reason);
    public OutboxReplayRequest OutboxRequest(int index) => new(OutboxScope ?? throw new InvalidOperationException("Not an outbox plan."),
        ItemRequestId(index), Items[index].Id, Items[index].ExpectedVersion, Actor, Reason, AcknowledgePossibleDuplicate);
}
/// <summary>Trusted command port. Authorize the plan scope and actor in the host; UI must review the exact plan before preparing it.</summary>
public interface IBulkReplayStore
{
    Task<BulkReplayPreparation> PrepareBulkAsync(BulkReplayRequest request, CancellationToken cancellationToken = default);
    Task<BulkReplayPreparation> RunBulkAsync(BulkReplayRequest request, CancellationToken cancellationToken = default);
    Task<BulkReplayProgress?> FindBulkAsync(WorkScope scope, Guid requestId, string actor, CancellationToken cancellationToken = default);
    Task<BulkReplayProgress?> FindBulkAsync(ProducerScope scope, Guid requestId, string actor, CancellationToken cancellationToken = default);
}
