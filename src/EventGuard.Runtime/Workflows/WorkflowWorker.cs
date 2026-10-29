namespace EventGuard.Runtime;

/// <summary>Database transactions serialize advances; job handlers run through the ordinary work pump.</summary>
public sealed class WorkflowWorker
{
    private readonly IWorkflowStore _store;
    private readonly WorkflowDefinition _definition;
    private readonly TimeSpan _interval;
    private readonly int _maximumItems;
    public WorkflowWorker(IWorkflowStore store, WorkflowDefinition definition, TimeSpan? pollInterval = null)
        : this(store, definition, pollInterval, 100) { }

    public WorkflowWorker(IWorkflowStore store, WorkflowDefinition definition, TimeSpan? pollInterval, int maximumItems)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _definition = definition ?? throw new ArgumentNullException(nameof(definition));
        _interval = pollInterval ?? TimeSpan.FromSeconds(1);
        if (_interval < TimeSpan.FromMilliseconds(10) || _interval > TimeSpan.FromMinutes(5)) throw new ArgumentOutOfRangeException(nameof(pollInterval));
        if (maximumItems is < 1 or > 1000) throw new ArgumentOutOfRangeException(nameof(maximumItems));
        _maximumItems = maximumItems;
    }
    public Task<WorkflowProgress> RunOnceAsync(Guid id, CancellationToken cancellationToken = default) => _store.AdvanceWorkflowAsync(_definition, id, cancellationToken);
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            foreach (var id in await _store.ListWorkflowsAsync(_definition, _maximumItems, cancellationToken)) await RunOnceAsync(id, cancellationToken);
            await Task.Delay(_interval, cancellationToken);
        }
    }
}
