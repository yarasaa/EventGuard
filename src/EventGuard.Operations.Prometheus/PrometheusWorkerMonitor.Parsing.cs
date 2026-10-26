using System.Globalization;
using System.Text.Json;

namespace EventGuard.Operations.Prometheus;

public sealed partial class PrometheusWorkerMonitor
{
    private const string Prefix = "eventguard_worker_", ScrapeTimestamp = "eventguard_scrape_timestamp_seconds";
    private static readonly string[] OutcomeNames = ["completed", "published", "retry_scheduled", "dead_letter", "unconfirmed"];
    private static readonly string[] MetricNames = ["up", ScrapeTimestamp, Prefix + "running_loops", Prefix + "active_cycles",
        Prefix + "last_cycle_timestamp_seconds", Prefix + "started_timestamp_seconds", Prefix + "observed_timestamp_seconds", Prefix + "telemetry_created_timestamp_seconds",
        ..OutcomeNames.Select(n => Prefix + n + "_total"), ..OutcomeNames.Select(n => Prefix + n + "_per_second")];

    private IReadOnlyList<WorkerObservation> Parse(JsonElement root, DateTimeOffset now)
    {
        if (root.GetProperty("status").GetString() != "success"
            || (root.TryGetProperty("warnings", out var warnings) && warnings.GetArrayLength() > 0)
            || (root.TryGetProperty("infos", out var infos) && infos.GetArrayLength() > 0)) throw new InvalidDataException();
        var data = root.GetProperty("data");
        if (data.GetProperty("resultType").GetString() != "vector") throw new InvalidDataException();
        var series = data.GetProperty("result");
        if (series.GetArrayLength() > MaximumSeries) throw new InvalidDataException();
        var samples = new Dictionary<(string Job, string Instance, string Component, string Name), double>();
        foreach (var item in series.EnumerateArray())
        {
            var metric = item.GetProperty("metric");
            if (metric.EnumerateObject().Count() > 32) throw new InvalidDataException();
            var job = Label(metric, "job"); var instance = Label(metric, "instance"); var name = Label(metric, "__name__");
            if (Label(metric, "eventguard_store") != _storeId || !_groups.Any(g => g.Job == job) || !MetricNames.Contains(name)) throw new InvalidDataException();
            var component = name is "up" or ScrapeTimestamp ? "" : Label(metric, "component");
            if (component is not ("" or "consumer" or "outbox")) throw new InvalidDataException();
            var point = item.GetProperty("value");
            if (point.GetArrayLength() != 2 || Math.Abs(point[0].GetDouble() - now.ToUnixTimeMilliseconds() / 1000d) > 5
                || !double.TryParse(point[1].GetString(), NumberStyles.Float, CultureInfo.InvariantCulture, out var value) || !double.IsFinite(value)
                || !samples.TryAdd((job, instance, component, name), value)) throw new InvalidDataException();
        }
        var rows = new List<WorkerObservation>();
        foreach (var group in _groups)
        foreach (var instance in samples.Keys.Where(k => k.Job == group.Job).Select(k => k.Instance).Distinct(StringComparer.Ordinal))
        {
            double? Get(string name) => samples.TryGetValue((group.Job, instance, name is "up" or ScrapeTimestamp ? "" : group.Component, name), out var value) ? value : null;
            var up = Get("up"); var scraped = Get(ScrapeTimestamp);
            if (up is null || scraped is null) { rows.Add(new(group.Key, instance, WorkerCondition.Missing)); continue; }
            if (up is not (0 or 1)) throw new InvalidDataException();
            var sampled = Timestamp(scraped.Value);
            if (!Fresh(sampled, now)) { rows.Add(new(group.Key, instance, WorkerCondition.Stale, sampled)); continue; }
            if (up == 0) { rows.Add(new(group.Key, instance, WorkerCondition.ScrapeFailed, sampled)); continue; }
            var observed = Get(Prefix + "observed_timestamp_seconds"); var loops = Get(Prefix + "running_loops");
            var active = Get(Prefix + "active_cycles"); var last = Get(Prefix + "last_cycle_timestamp_seconds"); var started = Get(Prefix + "started_timestamp_seconds");
            if (observed is null || loops is null || active is null || last is null || started is null)
            { rows.Add(new(group.Key, instance, WorkerCondition.NotInstrumented, sampled)); continue; }
            if (!Fresh(Timestamp(observed.Value), now)) { rows.Add(new(group.Key, instance, WorkerCondition.Stale, sampled)); continue; }
            var running = Count(loops.Value); var inflight = Count(active.Value);
            var lastCycle = last == 0 ? (DateTimeOffset?)null : Timestamp(last.Value);
            var began = started == 0 ? (DateTimeOffset?)null : Timestamp(started.Value);
            if (lastCycle > now.AddSeconds(5) || began > now.AddSeconds(5) || (running > 0 && began is null)) throw new InvalidDataException();
            var condition = running == 0 ? WorkerCondition.NoRunningLoops
                : now - (lastCycle > began ? lastCycle!.Value : began!.Value) > TimeSpan.FromSeconds(_progressSeconds) ? WorkerCondition.ProgressOverdue : WorkerCondition.Running;
            WorkerRates? rates = null;
            var created = Get(Prefix + "telemetry_created_timestamp_seconds");
            if (created is > 0 and var epoch && Timestamp(epoch) <= now.AddSeconds(-60))
            {
                var values = OutcomeNames.Select(n => Get(Prefix + n + "_per_second")).ToArray();
                var totals = OutcomeNames.Select(n => Get(Prefix + n + "_total")).ToArray();
                if (values.All(v => v.HasValue) && totals.All(v => v.HasValue))
                {
                    if (values.Any(v => v < 0) || totals.Any(v => v < 0 || v != Math.Truncate(v!.Value))) throw new InvalidDataException();
                    rates = new(values[0]!.Value, values[1]!.Value, values[2]!.Value, values[3]!.Value, values[4]!.Value);
                }
            }
            rows.Add(new(group.Key, instance, condition, sampled, running, inflight, lastCycle, rates));
        }
        if (rows.Count > MaximumRows) throw new InvalidDataException();
        return rows;
    }
    private bool Fresh(DateTimeOffset sample, DateTimeOffset now) => sample <= now.AddSeconds(5) && sample >= now.AddSeconds(-_staleSeconds);
    private static DateTimeOffset Timestamp(double value)
    {
        if (!double.IsFinite(value) || value < 0 || value > 253402300799) throw new InvalidDataException();
        return DateTimeOffset.FromUnixTimeMilliseconds(checked((long)(value * 1000)));
    }
    private static long Count(double value)
    {
        if (value is < 0 or > 1_000_000 || value != Math.Truncate(value)) throw new InvalidDataException();
        return (long)value;
    }
    private static string Label(JsonElement metric, string name)
    {
        var value = metric.GetProperty(name).GetString();
        if (value is null || value.Length is < 1 or > 200 || value.Any(char.IsControl)) throw new InvalidDataException();
        return value;
    }
}
