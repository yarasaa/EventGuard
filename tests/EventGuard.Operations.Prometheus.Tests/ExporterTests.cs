using EventGuard.Runtime;
using Prometheus;
using System.Text;
namespace EventGuard.Operations.Prometheus.Tests;
public sealed class ExporterTests
{
    [Fact]
    public async Task Isolated_exporter_contains_the_observable_worker_instruments_and_outcome_totals()
    {
        using var telemetry = new RuntimeTelemetry();
        var registry = Metrics.NewCustomRegistry();
        using var bridge = MeterAdapter.StartListening(new MeterAdapterOptions
        {
            Registry = registry, MetricFactory = Metrics.WithCustomRegistry(registry),
            InstrumentFilterPredicate = instrument => ReferenceEquals(instrument.Meter.Scope, telemetry)
                && instrument.Name.StartsWith("eventguard_worker_", StringComparison.Ordinal)
        });
        using var text = new MemoryStream(); await registry.CollectAndExportAsTextAsync(text);
        var body = Encoding.UTF8.GetString(text.ToArray());
        foreach (var name in new[] { "running_loops", "active_cycles", "last_cycle_timestamp_seconds", "started_timestamp_seconds", "observed_timestamp_seconds", "telemetry_created_timestamp_seconds", "completed_total", "published_total", "retry_scheduled_total", "dead_letter_total", "unconfirmed_total" })
            Assert.Contains("eventguard_worker_" + name + "{component=\"consumer\"}", body);
        Assert.DoesNotContain("tenant", body); Assert.DoesNotContain("eventguard_runtime_cycles", body);
    }
}
