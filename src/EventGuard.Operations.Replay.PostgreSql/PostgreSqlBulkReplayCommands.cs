namespace EventGuard.Operations.Replay.PostgreSql;
public sealed partial class PostgreSqlReplayStore
{
    public Task<BulkReplayPreparation> PrepareBulkAsync(BulkReplayRequest request, CancellationToken cancellationToken = default) => _bulk.PrepareBulkAsync(request, cancellationToken);
    public Task<BulkReplayPreparation> RunBulkAsync(BulkReplayRequest request, CancellationToken cancellationToken = default) => _bulk.RunBulkAsync(request, cancellationToken);
    public Task<BulkReplayProgress?> FindBulkAsync(WorkScope scope, Guid requestId, string actor, CancellationToken cancellationToken = default) => _bulk.FindBulkAsync(scope, requestId, actor, cancellationToken);
    public Task<BulkReplayProgress?> FindBulkAsync(ProducerScope scope, Guid requestId, string actor, CancellationToken cancellationToken = default) => _bulk.FindBulkAsync(scope, requestId, actor, cancellationToken);
}
