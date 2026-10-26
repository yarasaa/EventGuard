namespace EventGuard.Operations;

public enum WorkerHistoryWindow { Hour = 1, SixHours = 6, Day = 24 }
public sealed record WorkerHistoryPoint(DateTimeOffset At, double? PerSecond);
public sealed record WorkerHistorySeries(string Outcome, IReadOnlyList<WorkerHistoryPoint> Points);
public sealed record WorkerHistorySnapshot(FleetAvailability Availability, string Group, DateTimeOffset From, DateTimeOffset Until,
    int StepSeconds, IReadOnlyList<WorkerHistorySeries> Series);
public interface IWorkerHistoryMonitor
{
    Task<WorkerHistorySnapshot> ReadHistoryAsync(string group, WorkerHistoryWindow window, CancellationToken ct = default);
}

/// <summary>Configured expected counts survive restarts through host configuration; observations never define the expected fleet.</summary>
public sealed record WorkerInventory(string Group, int? Expected, int? Observed, int? Running, int? Missing)
{
    public static WorkerInventory From(WorkerFleetSnapshot snapshot, string group, int? expected)
    {
        if (expected is < 0 or > 200) throw new ArgumentOutOfRangeException(nameof(expected));
        if (snapshot.Availability != FleetAvailability.Available) return new(group, expected, null, null, null);
        var rows = snapshot.Workers.Where(r => r.Group == group && r.Instance.Length > 0).ToArray();
        var observed = rows.Count(r => r.Condition is WorkerCondition.Running or WorkerCondition.NoRunningLoops or WorkerCondition.ProgressOverdue or WorkerCondition.NotInstrumented);
        var running = rows.Count(r => r.Condition == WorkerCondition.Running);
        return new(group, expected, observed, running, expected.HasValue ? Math.Max(0, expected.Value - running) : null);
    }
}
