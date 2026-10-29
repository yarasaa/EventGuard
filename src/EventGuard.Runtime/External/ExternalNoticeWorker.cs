namespace EventGuard.Runtime;

/// <summary>Applies persisted reports and sweeps report deadlines; never calls the sending service.</summary>
public sealed class ExternalNoticeWorker
{
    private readonly IExternalObservationStore store;
    private readonly ProducerScope scope;
    private readonly string binding;
    private readonly string bindingVersion;
    private readonly ExternalActionPolicy policy;
    private readonly PollingWorkerOptions _polling;

    public ExternalNoticeWorker(IExternalObservationStore store, ProducerScope scope, string binding,
        string bindingVersion, ExternalActionPolicy? policy = null)
        : this(store, scope, binding, bindingVersion, policy, new()) { }

    public ExternalNoticeWorker(IExternalObservationStore store, ProducerScope scope, string binding,
        string bindingVersion, ExternalActionPolicy? policy, PollingWorkerOptions polling)
    {
        this.store = store ?? throw new ArgumentNullException(nameof(store));
        this.scope = scope ?? throw new ArgumentNullException(nameof(scope));
        _ = new ProducerScope(binding, bindingVersion);
        this.binding = binding; this.bindingVersion = bindingVersion;
        this.policy = policy ?? new();
        _polling = polling ?? throw new ArgumentNullException(nameof(polling));
    }

    public Task<ExternalNoticeReceipt?> RunOnceAsync(CancellationToken cancellationToken = default) =>
        store.ApplyNextNoticeAsync(scope, binding, bindingVersion, policy, cancellationToken);
    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        var sweepAt = DateTimeOffset.MinValue;
        while (!cancellationToken.IsCancellationRequested)
        {
            if (DateTimeOffset.UtcNow >= sweepAt)
            {
                await store.ExpireObservationsAsync(scope, binding, bindingVersion, _polling.MaximumItems, cancellationToken);
                sweepAt = DateTimeOffset.UtcNow.Add(_polling.SweepInterval);
            }
            if (await RunOnceAsync(cancellationToken) is null) await Task.Delay(_polling.PollInterval, cancellationToken);
        }
    }
}
