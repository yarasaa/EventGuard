using EventGuard.Runtime;

namespace EventGuard.Hosting;

public sealed class JsonConsumerOptions<TMessage> where TMessage : notnull
{
    public WorkScope Scope { get; }
    public IdentityPolicy Policy { get; }
    public JsonMessageContract<TMessage> Contract { get; }
    public Func<TMessage, (string BusinessKey, Guid MessageId)> Identity { get; }
    public WorkPumpOptions Worker { get; }
    public RetryPolicy Retry { get; }

    public JsonConsumerOptions(WorkScope scope, IdentityPolicy policy, JsonMessageContract<TMessage> contract,
        Func<TMessage, (string BusinessKey, Guid MessageId)> identity,
        WorkPumpOptions? worker = null, RetryPolicy? retry = null)
    {
        Scope = scope ?? throw new ArgumentNullException(nameof(scope));
        Policy = policy ?? throw new ArgumentNullException(nameof(policy));
        Contract = contract ?? throw new ArgumentNullException(nameof(contract));
        Identity = identity ?? throw new ArgumentNullException(nameof(identity));
        if (scope.EventType != contract.Name) throw new ArgumentException("Scope.EventType must match Contract.Name.", nameof(contract));
        Worker = worker ?? new(new StoreStartupOptions(StoreCapabilities.ConsumerTransactions));
        Retry = retry ?? new();
    }
}
