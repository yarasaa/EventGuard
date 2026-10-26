using System.Globalization;
using System.Security.Claims;
using System.Text;
using EventGuard.Operations;
namespace EventGuard.Dashboard;

public sealed record HistoryChart(string Label, string Path, string Maximum, int Available, int Total);
public sealed record WorkerHistoryView(WorkerHistorySnapshot Snapshot, string Window, IReadOnlyList<HistoryChart> Charts);
public sealed class DashboardWorkerHistory(DashboardOptions options, IWorkerMonitor monitor)
{
    public async Task<WorkerHistoryView> ReadAsync(ClaimsPrincipal user, string? group, string? window, CancellationToken ct)
    {
        if (!MonitoringEndpoints.CanMonitor(options, user)) throw new ScopeNotAvailableException();
        var period = window switch { null or "" or "1h" => WorkerHistoryWindow.Hour, "6h" => WorkerHistoryWindow.SixHours, "24h" => WorkerHistoryWindow.Day, _ => throw new InvalidQueryException() };
        var selected = string.IsNullOrEmpty(group) ? options.Workers.Groups.FirstOrDefault() : options.Workers.Groups.FirstOrDefault(g => g.Key == group);
        if (selected is null && !string.IsNullOrEmpty(group)) throw new InvalidQueryException();
        var snapshot = selected is null || monitor is not IWorkerHistoryMonitor history
            ? new WorkerHistorySnapshot(FleetAvailability.Disabled, selected?.Key ?? "", DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, 0, [])
            : await history.ReadHistoryAsync(selected.Key, period, ct);
        return new(snapshot, window is null or "" ? "1h" : window, snapshot.Series.Select(Chart).ToArray());
    }
    private static HistoryChart Chart(WorkerHistorySeries series)
    {
        var label = series.Outcome switch { "completed" => "Consumer completions", "published" => "Publication settlements", "retry_scheduled" => "Scheduled retries", "dead_letter" => "Dead letters", "unconfirmed" => "Unconfirmed outcomes", _ => throw new InvalidDataException() };
        if (series.Points.Count is < 1 or > 301 || series.Points.Any(p => p.PerSecond is { } v && (!double.IsFinite(v) || v is < 0 or > 1e15))) throw new InvalidDataException();
        var maximum = series.Points.Max(p => p.PerSecond) ?? 0; var scale = maximum > 0 ? maximum : 1;
        var path = new StringBuilder(); var joined = false; var count = 0;
        for (var i = 0; i < series.Points.Count; i++)
        {
            if (series.Points[i].PerSecond is not { } value) { joined = false; continue; }
            var x = 8 + 464d * i / Math.Max(1, series.Points.Count - 1); var y = 132 - 120 * value / scale;
            path.Append(joined ? " L " : " M ").Append(x.ToString("0.###", CultureInfo.InvariantCulture)).Append(' ').Append(y.ToString("0.###", CultureInfo.InvariantCulture));
            if (!joined) path.Append(" l 0.01 0"); // Keep isolated observations visible without bridging a missing interval.
            joined = true; count++;
        }
        return new(label, path.ToString(), maximum.ToString("0.##", CultureInfo.InvariantCulture), count, series.Points.Count);
    }
}
