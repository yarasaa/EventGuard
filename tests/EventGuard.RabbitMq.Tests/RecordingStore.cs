namespace EventGuard.RabbitMq.Tests;

internal sealed class RecordingStore(IWorkStore inner) : IWorkStore
{
    public List<AcceptanceStatus> Outcomes { get; } = [];
    public async Task<AcceptanceResult> AcceptAsync(WorkSubmission submission, CancellationToken cancellationToken = default)
    {
        var result = await inner.AcceptAsync(submission, cancellationToken);
        Outcomes.Add(result.Status);
        return result;
    }
    public Task<WorkItem?> FindAsync(WorkScope scope, Guid workId, CancellationToken cancellationToken = default) => inner.FindAsync(scope, workId, cancellationToken);
    public Task<WorkClaim?> ClaimNextAsync(WorkScope scope, Guid requestId, TimeSpan duration, CancellationToken cancellationToken = default) => inner.ClaimNextAsync(scope, requestId, duration, cancellationToken);
    public Task<ReplayResult> ReplayAsync(ReplayRequest request, CancellationToken cancellationToken = default) => inner.ReplayAsync(request, cancellationToken);
}
