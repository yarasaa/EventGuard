namespace EventGuard.Operations;

// Process observations, never an ownership or recovery authority. No payload or business identifiers.
public enum WorkerCondition { Running, NoRunningLoops, ProgressOverdue, ScrapeFailed, Stale, Missing, NotInstrumented }
public enum FleetAvailability { Disabled, Available, Unavailable }
public sealed record WorkerObservation(string Group, string Instance, WorkerCondition Condition,
    DateTimeOffset? SampledAt = null, long? RunningLoops = null, long? ActiveCycles = null, DateTimeOffset? LastCycleAt = null, WorkerRates? Rates = null);
public sealed record WorkerRates(double CompletedPerSecond, double PublishedPerSecond, double RetryPerSecond,
    double DeadLetterPerSecond, double UnconfirmedPerSecond, int WindowSeconds = 60);
public sealed record WorkerFleetSnapshot(FleetAvailability Availability, DateTimeOffset CheckedAt, IReadOnlyList<WorkerObservation> Workers);
public interface IWorkerMonitor
{
    Task<WorkerFleetSnapshot> ReadAsync(CancellationToken cancellationToken = default);
}
