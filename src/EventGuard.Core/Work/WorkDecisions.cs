namespace EventGuard;

public enum ExecutionAdmission { Execute, Expire, LeaseLost }

// An observation from durable state, not proof that a native lock is held.
public sealed record WorkLease(Guid Token, DateTimeOffset Until);

public sealed class WorkChange
{
    public WorkItem Before { get; }
    public WorkItem After { get; }

    internal WorkChange(WorkItem before, WorkItem after) { Before = before; After = after; }
}

public sealed class ClaimDecision
{
    public WorkChange Change { get; }
    public WorkLease Lease { get; }
    public Guid? AbandonedAttempt { get; }

    internal ClaimDecision(WorkChange change, WorkLease lease, Guid? abandonedAttempt)
    { Change = change; Lease = lease; AbandonedAttempt = abandonedAttempt; }
}

public sealed class ExecutionDecision
{
    public WorkChange Change { get; }
    public ExecutionStatus Status { get; }
    public string AttemptStatus { get; }

    internal ExecutionDecision(WorkChange change, ExecutionStatus status, string attemptStatus)
    { Change = change; Status = status; AttemptStatus = attemptStatus; }
}

public sealed class ReplayDecision
{
    public ReplayResult Result { get; }
    public WorkChange? Change { get; }

    internal ReplayDecision(ReplayResult result, WorkChange? change) { Result = result; Change = change; }
}
