using EventGuard.Operations;
namespace EventGuard.Dashboard.Tests;

// HTTP/auth doubles only. Native recovery transactions and commit loss have separate store tests.
public sealed partial class MetadataStore : IIngressReadStore, IRecoveryHealthCheck
{
    public int IngressCalls { get; private set; }
    public IngressQuery? LastIngressQuery { get; private set; }
    public bool IngressReleased { get; set; }
    public Task ValidateRecoveryAsync(CancellationToken ct = default) => Task.CompletedTask;
    private IngressSummary Input() => new(WorkId, 0, "v1", "sha256-bytes/v1", "mapping-rejected",
        IngressReleased ? WorkId : null, true, DateTimeOffset.Parse("2026-01-01T00:00:00Z"));
    public Task<IngressSummary?> FindIngressAsync(WorkScope scope, Guid id, CancellationToken ct = default)
    {
        IngressCalls++;
        if (Unavailable) throw new InvalidOperationException("secret=never-render");
        return Task.FromResult<IngressSummary?>(scope.Tenant == "a" && id == WorkId ? Input() : null);
    }
    public Task<IngressPage> ListIngressAsync(IngressQuery query, CancellationToken ct = default)
    {
        IngressCalls++; LastIngressQuery = query;
        if (Unavailable) throw new InvalidOperationException("secret=never-render");
        var input = Input();
        return Task.FromResult(new IngressPage([input], NextPage ? new(input.CreatedAt, input.Id) : null,
            query.Window.Before is null ? null : new(input.CreatedAt, input.Id)));
    }
}
public sealed partial class ReplayTestStore : IRecoveryCommandStore
{
    public int RecoveryCalls { get; private set; }
    public int Recoveries { get; private set; }
    public bool LoseNextRecoveryReply { get; set; }
    public IngressReleaseRequest? LastRelease { get; private set; }
    public OutboxRepairRequest? LastRepair { get; private set; }
    private readonly Dictionary<Guid, (IngressReleaseRequest Request, IngressReleaseResult Result)> _releaseReceipts = [];
    private readonly Dictionary<Guid, (OutboxRepairRequest Request, OutboxRepairResult Result)> _repairReceipts = [];
    public Task ValidateRecoveryAsync(CancellationToken ct = default) => HealthUnavailable ? Task.FromException(new InvalidOperationException("secret-recovery-error")) : Task.CompletedTask;
    public Task<IngressReleaseResult> ReleaseIngressAsync(IngressReleaseRequest request, CancellationToken ct = default)
    {
        lock (_gate)
        {
            RecoveryCalls++; LastRelease = request;
            if (_releaseReceipts.TryGetValue(request.RequestId, out var saved)) return Task.FromResult(IngressRecovery.Repeat(request, saved.Request.Fingerprint, saved.Result));
            var result = new IngressReleaseResult(IngressReleaseStatus.Accepted, request.QuarantineId, MetadataStore.WorkId, request.ExpectedVersion + 1);
            _releaseReceipts.Add(request.RequestId, (request, result)); Recoveries++; LoseReply(); return Task.FromResult(result);
        }
    }
    public Task<OutboxRepairResult> RepairOutboxAsync(OutboxRepairRequest request, CancellationToken ct = default)
    {
        lock (_gate)
        {
            RecoveryCalls++; LastRepair = request;
            if (_repairReceipts.TryGetValue(request.RequestId, out var saved)) return Task.FromResult(OutboxRecovery.RepeatRepair(request, saved.Request.Fingerprint, saved.Result));
            var result = new OutboxRepairResult(OutboxRepairStatus.Accepted, request.Message.IntentId, 1, request.ExpectedVersion + 1);
            _repairReceipts.Add(request.RequestId, (request, result)); Recoveries++; LoseReply(); return Task.FromResult(result);
        }
    }
    private void LoseReply()
    {
        if (LoseNextRecoveryReply) { LoseNextRecoveryReply = false; throw new CommitOutcomeUnknownException(Guid.NewGuid(), new Exception("secret=never-render")); }
    }
    public Task<IngressReleaseReceipt?> FindIngressReleaseReceiptAsync(WorkScope scope, Guid id, Guid requestId, string actor, CancellationToken ct = default)
    {
        lock (_gate) return Task.FromResult<IngressReleaseReceipt?>(_releaseReceipts.TryGetValue(requestId, out var saved)
            && saved.Request.Submission.Identity.Scope == scope && saved.Request.QuarantineId == id && saved.Request.Actor == actor
            ? new(requestId, saved.Result, saved.Request.Reason, DateTimeOffset.UtcNow) : null);
    }
    public Task<OutboxRepairReceipt?> FindOutboxRepairReceiptAsync(ProducerScope scope, Guid id, Guid requestId, string actor, CancellationToken ct = default)
    {
        lock (_gate) return Task.FromResult<OutboxRepairReceipt?>(_repairReceipts.TryGetValue(requestId, out var saved)
            && saved.Request.Message.Scope == scope && saved.Request.Message.IntentId == id && saved.Request.Actor == actor
            ? new(requestId, saved.Result, saved.Request.Reason, saved.Request.AcknowledgePossibleDuplicate, DateTimeOffset.UtcNow) : null);
    }
}
