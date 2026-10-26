using EventGuard.Operations;
using System.Security.Claims;

namespace EventGuard.Dashboard;

public sealed class DisabledWorkerMonitor : IWorkerMonitor
{
    public Task<WorkerFleetSnapshot> ReadAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult(new WorkerFleetSnapshot(FleetAvailability.Disabled, DateTimeOffset.UtcNow, []));
}

public sealed record WorkerView(WorkerFleetSnapshot Snapshot, IReadOnlyList<WorkerObservation> Rows, int Total, int Page);

public sealed class DashboardWorkers(DashboardOptions options, IWorkerMonitor monitor)
{
    public async Task<WorkerView> ReadAsync(ClaimsPrincipal user, string? group, string? state, int page, CancellationToken ct)
    {
        if (!MonitoringEndpoints.CanMonitor(options, user)) throw new ScopeNotAvailableException();
        WorkerCondition? condition = null;
        if (!string.IsNullOrEmpty(group) && !options.Workers.Groups.Any(g => g.Key == group)) throw new InvalidQueryException();
        if (!string.IsNullOrEmpty(state))
        {
            if (!Enum.TryParse<WorkerCondition>(state, out var parsed) || !Enum.IsDefined(parsed) || parsed.ToString() != state)
                throw new InvalidQueryException();
            condition = parsed;
        }
        if (page is < 1 or > 9) throw new InvalidQueryException();
        var snapshot = await monitor.ReadAsync(ct);
        var filtered = snapshot.Workers.Where(r => (string.IsNullOrEmpty(group) || r.Group == group) && (condition is null || r.Condition == condition)).ToArray();
        // A changing live inventory can shrink; show the nearest existing page with working back navigation.
        page = Math.Min(page, Math.Max(1, (filtered.Length + 24) / 25));
        return new(snapshot, filtered.Skip((page - 1) * 25).Take(25).ToArray(), filtered.Length, page);
    }

    public static string State(WorkerCondition state) => state switch
    {
        WorkerCondition.Running => "Running loops", WorkerCondition.NoRunningLoops => "No running loops",
        WorkerCondition.ProgressOverdue => "Progress overdue", WorkerCondition.ScrapeFailed => "Scrape failed",
        WorkerCondition.Stale => "Stale data", WorkerCondition.Missing => "No recent target", _ => "Instrumentation missing"
    };
}
