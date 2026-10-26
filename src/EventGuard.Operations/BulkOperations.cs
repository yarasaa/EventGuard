namespace EventGuard.Operations;
public sealed partial class OperationsReplayer
{
    private IBulkReplayStore Bulk() => _store as IBulkReplayStore ?? throw new NotSupportedException("Bulk replay is unavailable.");
    public Task<BulkReplayPreparation> RunBulkAsync(BulkReplayRequest request, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CommandAsync(request.RequestId, token => Bulk().RunBulkAsync(request, token), ct);
    }
    public Task<BulkReplayProgress?> FindBulkAsync(WorkScope scope, Guid requestId, string actor, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(scope); ValidateLookup(requestId, requestId, actor);
        return ReceiptAsync(token => Bulk().FindBulkAsync(scope, requestId, actor, token), ct);
    }
    public Task<BulkReplayProgress?> FindBulkAsync(ProducerScope scope, Guid requestId, string actor, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(scope); ValidateLookup(requestId, requestId, actor);
        return ReceiptAsync(token => Bulk().FindBulkAsync(scope, requestId, actor, token), ct);
    }
}
