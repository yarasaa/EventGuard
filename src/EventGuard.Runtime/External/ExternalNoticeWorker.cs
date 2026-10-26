namespace EventGuard.Runtime;

/// <summary>Applies persisted reports and sweeps report deadlines; never calls the sending service.</summary>
public sealed class ExternalNoticeWorker(IExternalObservationStore store, ProducerScope scope, string binding,
    string bindingVersion, ExternalActionPolicy? policy = null)
{
    public Task<ExternalNoticeReceipt?> RunOnceAsync(CancellationToken cancellationToken = default) =>
        store.ApplyNextNoticeAsync(scope, binding, bindingVersion, policy ?? new(), cancellationToken);
    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        var sweepAt = DateTimeOffset.MinValue;
        while (!cancellationToken.IsCancellationRequested)
        {
            if (DateTimeOffset.UtcNow >= sweepAt)
            {
                await store.ExpireObservationsAsync(scope, binding, bindingVersion, cancellationToken: cancellationToken);
                sweepAt = DateTimeOffset.UtcNow.AddSeconds(1);
            }
            if (await RunOnceAsync(cancellationToken) is null) await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
        }
    }
}
