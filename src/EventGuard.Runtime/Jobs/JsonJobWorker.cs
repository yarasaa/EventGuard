namespace EventGuard.Runtime;

/// <summary>A version-bound transactional job runner. External calls need a separate durable action protocol.</summary>
public sealed class JsonJobWorker<T, TSession> where T : notnull
{
    private readonly IJobStore _jobs;
    private readonly JobDefinition _definition;
    private readonly WorkPump<TSession> _pump;

    public JsonJobWorker(ITransactionalJobStore<TSession> store, JobDefinition definition,
        JsonMessageContract<T> contract, Func<T, WorkItem, TSession, CancellationToken, ValueTask> handler,
        RetryPolicy? retryPolicy = null, WorkPumpOptions? options = null)
    {
        _jobs = store ?? throw new ArgumentNullException(nameof(store));
        _definition = definition ?? throw new ArgumentNullException(nameof(definition));
        ArgumentNullException.ThrowIfNull(contract); ArgumentNullException.ThrowIfNull(handler);
        if (definition.ContractName != contract.Name || definition.ContractVersion != contract.Version)
            throw new ArgumentException("The parameter contract does not match the job definition.", nameof(contract));
        definition.RequireCapabilities([JobCapabilities.TransactionalHandler, JobCapabilities.TransactionalItems]);
        _pump = new(store, definition.WorkScope, (work, bytes, session, ct) =>
            handler(contract.Decode(bytes), work, session, ct), retryPolicy, options);
    }

    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        await _jobs.ValidateAsync(_definition, cancellationToken);
        await _pump.RunAsync(cancellationToken);
    }
}
