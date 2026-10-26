namespace EventGuard.Dashboard.Tests;
// HTTP orchestration double. Native bulk transactions, partial progress and reply loss are tested separately.
public sealed partial class ReplayTestStore : IBulkReplayStore
{
    private readonly Dictionary<Guid, BulkReplayRequest> _bulkPlans = [];
    public int BulkPlans { get { lock (_gate) return _bulkPlans.Count; } }
    public Task<BulkReplayPreparation> PrepareBulkAsync(BulkReplayRequest request, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (_bulkPlans.TryGetValue(request.RequestId, out var existing))
                return Task.FromResult(existing.Fingerprint == request.Fingerprint ? new BulkReplayPreparation(BulkReplayStatus.Prepared, true) : new(BulkReplayStatus.RequestConflict));
            _bulkPlans.Add(request.RequestId, request); return Task.FromResult(new BulkReplayPreparation(BulkReplayStatus.Prepared));
        }
    }
    public async Task<BulkReplayPreparation> RunBulkAsync(BulkReplayRequest request, CancellationToken cancellationToken = default)
    {
        var result = await PrepareBulkAsync(request, cancellationToken);
        if (result.Status == BulkReplayStatus.RequestConflict) return result;
        for (var i = 0; i < request.Items.Count; i++)
        {
            if (request.Kind == BulkReplayKind.Consumer) await ReplayAsync(request.ConsumerRequest(i), cancellationToken);
            else await ReplayOutboxAsync(request.OutboxRequest(i), cancellationToken);
        }
        return result;
    }
    public Task<BulkReplayProgress?> FindBulkAsync(WorkScope scope, Guid requestId, string actor, CancellationToken cancellationToken = default) => FindBulk(scope.Id, BulkReplayKind.Consumer, requestId, actor);
    public Task<BulkReplayProgress?> FindBulkAsync(ProducerScope scope, Guid requestId, string actor, CancellationToken cancellationToken = default) => FindBulk(scope.Id, BulkReplayKind.Outbox, requestId, actor);
    private Task<BulkReplayProgress?> FindBulk(string scopeId, BulkReplayKind kind, Guid id, string actor)
    {
        lock (_gate)
        {
            if (!_bulkPlans.TryGetValue(id, out var request) || request.ScopeId != scopeId || request.Kind != kind || request.Actor != actor) return Task.FromResult<BulkReplayProgress?>(null);
            var rows = request.Items.Select((item, index) =>
            {
                var entry = new BulkReplayEntry(item.Id, request.ItemRequestId(index), item.ExpectedVersion, null, null, null);
                if (kind == BulkReplayKind.Consumer && _receipts.TryGetValue(entry.RequestId, out var consumer)) return entry with
                { Status = consumer.Result.Status.ToString(), Generation = consumer.Result.Generation, Version = consumer.Result.Version };
                if (kind == BulkReplayKind.Outbox && _outboxReceipts.TryGetValue(entry.RequestId, out var producer)) return entry with
                { Status = producer.Result.Status.ToString(), Generation = producer.Result.Generation, Version = producer.Result.Version };
                return entry;
            }).ToArray();
            return Task.FromResult<BulkReplayProgress?>(new(request, rows, DateTimeOffset.UtcNow));
        }
    }
}
