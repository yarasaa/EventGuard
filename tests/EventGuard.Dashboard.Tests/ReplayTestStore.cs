using EventGuard.Operations;

namespace EventGuard.Dashboard.Tests;

// Web security/flow double. Transaction races and lost COMMIT replies are exercised against real DBs in Operations.Tests.
public sealed partial class ReplayTestStore : IWorkReplayStore, IStoreHealthCheck
{
    public int HealthCalls { get; private set; }
    public bool HealthUnavailable { get; set; }
    public Task CheckHealthAsync(bool consumer, bool outbox, CancellationToken ct = default)
    { HealthCalls++; if (HealthUnavailable) throw new InvalidOperationException("secret"); return Task.CompletedTask; }
    private readonly object _gate = new();
    private readonly Dictionary<Guid, (ReplayRequest Request, ReplayResult Result)> _receipts = [];
    public int Calls { get; private set; }
    public int Generations { get; private set; }
    public ReplayRequest? LastRequest { get; private set; }
    public bool LoseNextReply { get; set; }
    public ReplayStatus NextStatus { get; set; } = ReplayStatus.Accepted;

    public Task<ReplayResult> ReplayAsync(ReplayRequest request, CancellationToken ct = default)
    {
        lock (_gate)
        {
            Calls++; LastRequest = request;
            if (_receipts.TryGetValue(request.RequestId, out var old))
                return Task.FromResult(WorkLifecycle.RepeatReplay(request, old.Request.Fingerprint, old.Result));
            if (NextStatus == ReplayStatus.Accepted) Generations++;
            var result = new ReplayResult(NextStatus, request.WorkId, Generations, request.ExpectedVersion + 1);
            _receipts.Add(request.RequestId, (request, result));
            if (LoseNextReply)
            {
                LoseNextReply = false;
                throw new CommitOutcomeUnknownException(request.RequestId, new InvalidOperationException("Password=never-render"));
            }
            return Task.FromResult(result);
        }
    }
    public Task<ReplayReceipt?> FindReceiptAsync(WorkScope scope, Guid workId, Guid requestId, string actor, CancellationToken ct = default)
    {
        lock (_gate)
            return Task.FromResult<ReplayReceipt?>(_receipts.TryGetValue(requestId, out var saved)
                && saved.Request.Scope.Id == scope.Id && saved.Request.WorkId == workId && saved.Request.Actor == actor
                ? new(requestId, saved.Result, saved.Request.Reason, DateTimeOffset.UtcNow) : null);
    }
}
