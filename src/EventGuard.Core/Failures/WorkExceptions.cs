namespace EventGuard;

public sealed class PolicyMismatchException(string scopeId) : InvalidOperationException(
    $"Scope {scopeId} is already bound to a different identity policy. Drain-and-switch is not supported.");

public sealed class CommitOutcomeUnknownException(Guid operationId, Exception innerException) : Exception(
    "Commit outcome could not be confirmed. Resolve persisted state before another execution.", innerException)
{
    public Guid OperationId { get; } = operationId;
}

public sealed class PermanentWorkException(string reasonCode) : Exception(reasonCode);
