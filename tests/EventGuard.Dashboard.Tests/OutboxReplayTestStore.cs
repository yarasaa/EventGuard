using EventGuard.Operations;

namespace EventGuard.Dashboard.Tests;

public sealed partial class ReplayTestStore : IOutboxReplayStore
{
    private readonly Dictionary<Guid, (OutboxReplayRequest Request, OutboxReplayResult Result)> _outboxReceipts = [];
    public int OutboxCalls { get; private set; }
    public int OutboxGenerations { get; private set; }
    public OutboxReplayRequest? LastOutboxRequest { get; private set; }
    public bool LoseNextOutboxReply { get; set; }
    public OutboxReplayStatus NextOutboxStatus { get; set; } = OutboxReplayStatus.Accepted;

    public Task<OutboxReplayResult> ReplayOutboxAsync(OutboxReplayRequest request, CancellationToken ct = default)
    {
        lock (_gate)
        {
            OutboxCalls++; LastOutboxRequest = request;
            if (_outboxReceipts.TryGetValue(request.RequestId, out var old))
                return Task.FromResult(OutboxRecovery.RepeatReplay(request, old.Request.Fingerprint, old.Result));
            if (NextOutboxStatus == OutboxReplayStatus.Accepted) OutboxGenerations++;
            var result = new OutboxReplayResult(NextOutboxStatus, request.IntentId, OutboxGenerations, request.ExpectedVersion + 1);
            _outboxReceipts.Add(request.RequestId, (request, result));
            if (LoseNextOutboxReply)
            {
                LoseNextOutboxReply = false;
                throw new CommitOutcomeUnknownException(request.RequestId, new InvalidOperationException("Password=never-render"));
            }
            return Task.FromResult(result);
        }
    }
    public Task<OutboxReplayReceipt?> FindOutboxReceiptAsync(ProducerScope scope, Guid intentId, Guid requestId, string actor, CancellationToken ct = default)
    {
        lock (_gate)
            return Task.FromResult<OutboxReplayReceipt?>(_outboxReceipts.TryGetValue(requestId, out var saved)
                && saved.Request.Scope.Id == scope.Id && saved.Request.IntentId == intentId && saved.Request.Actor == actor
                ? new(requestId, saved.Result, saved.Request.Reason, saved.Request.AcknowledgePossibleDuplicate, DateTimeOffset.UtcNow) : null);
    }
}
