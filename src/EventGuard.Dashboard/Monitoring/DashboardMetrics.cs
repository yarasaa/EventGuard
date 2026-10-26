using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using Prometheus;

namespace EventGuard.Dashboard;

// Instance-owned registry: no default process collectors, route labels, exception text or exemplars.
public sealed class DashboardMetrics
{
    public CollectorRegistry Registry { get; } = Metrics.NewCustomRegistry();
    private readonly Counter _requests;
    private readonly Histogram _duration;
    private readonly Gauge _metadata;
    private readonly Gauge _replay;
    private readonly Gauge _checked;
    private readonly byte[] _tokenHash;
    private long _count;
    public long Requests => Interlocked.Read(ref _count);
    public DateTimeOffset StartedAt { get; } = DateTimeOffset.UtcNow;
    public DashboardMetrics(DashboardOptions options)
    {
        _tokenHash = SHA256.HashData(Encoding.UTF8.GetBytes(options.MetricsToken));
        var factory = Metrics.WithCustomRegistry(Registry);
        _requests = factory.CreateCounter("eventguard_dashboard_http_requests_total", "HTTP responses from this dashboard process; excludes probes and scrapes.", new CounterConfiguration { LabelNames = ["status_class"], ExemplarBehavior = ExemplarBehavior.NoExemplars() });
        _duration = factory.CreateHistogram("eventguard_dashboard_http_duration_seconds", "Dashboard request duration, excluding probes and scrapes.", new HistogramConfiguration { Buckets = [.005, .025, .1, .5, 1, 3, 10], ExemplarBehavior = ExemplarBehavior.NoExemplars() });
        _metadata = factory.CreateGauge("eventguard_dashboard_metadata_ready", "Last bounded metadata/profile probe: 1 ready, 0 unavailable.");
        _replay = factory.CreateGauge("eventguard_dashboard_replay_ready", "Last command profile probe: 1 ready, 0 unavailable, -1 disabled.");
        _checked = factory.CreateGauge("eventguard_dashboard_dependency_checked_timestamp_seconds", "UTC time of the last completed dependency probe; not worker heartbeat.");
        factory.CreateGauge("eventguard_dashboard_started_timestamp_seconds", "UTC time this dashboard instance started.").Set(StartedAt.ToUnixTimeSeconds());
    }
    public void Record(int status, long started)
    {
        Interlocked.Increment(ref _count);
        var category = (status / 100) switch { 1 => "1xx", 2 => "2xx", 3 => "3xx", 4 => "4xx", 5 => "5xx", _ => "other" };
        try { _requests.WithLabels(category).Inc(); _duration.Observe(Stopwatch.GetElapsedTime(started).TotalSeconds); } catch { }
    }
    public void Update(DashboardHealthSnapshot health)
    {
        _metadata.Set(health.MetadataReady ? 1 : 0); _replay.Set(health.ReplayReady is null ? -1 : health.ReplayReady.Value ? 1 : 0);
        _checked.Set(health.CheckedAt.ToUnixTimeMilliseconds() / 1000d);
    }
    public bool Authorized(string? header)
    {
        if (header is null || !header.StartsWith("Bearer ", StringComparison.Ordinal) || header.Length is < 39 or > 135) return false;
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(header[7..]));
        return CryptographicOperations.FixedTimeEquals(hash, _tokenHash);
    }
}
