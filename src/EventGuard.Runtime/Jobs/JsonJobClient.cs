namespace EventGuard.Runtime;

/// <summary>Application-facing command client. Authorization belongs to the calling host.</summary>
public sealed class JsonJobClient<T> where T : notnull
{
    private readonly IJobStore _store;
    private readonly JsonMessageContract<T> _contract;
    public JobDefinition Definition { get; }

    public JsonJobClient(IJobStore store, JobDefinition definition, JsonMessageContract<T> contract)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        Definition = definition ?? throw new ArgumentNullException(nameof(definition));
        _contract = contract ?? throw new ArgumentNullException(nameof(contract));
        if (definition.ContractName != contract.Name || definition.ContractVersion != contract.Version)
            throw new ArgumentException("The parameter contract does not match the job definition.", nameof(contract));
    }

    public Task<JobSubmissionResult> SubmitAsync(Guid requestId, string businessKey, string actor, T parameters,
        DateTimeOffset? notBefore = null, DateTimeOffset? expiresAt = null, string? correlationId = null,
        CancellationToken cancellationToken = default) => _store.SubmitAsync(
            new(Definition, requestId, businessKey, actor, _contract.Encode(parameters), notBefore, expiresAt, correlationId), cancellationToken);

    public Task<JobOperation?> FindAsync(Guid operationId, CancellationToken cancellationToken = default) =>
        _store.FindAsync(Definition.Scope, operationId, cancellationToken);
    public Task<JobSubmissionResult?> FindRequestAsync(Guid requestId, CancellationToken cancellationToken = default) =>
        _store.FindRequestAsync(Definition.Scope, requestId, cancellationToken);
}
