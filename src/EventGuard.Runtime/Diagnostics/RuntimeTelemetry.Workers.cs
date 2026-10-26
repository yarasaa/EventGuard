using System.Diagnostics.Metrics;

namespace EventGuard.Runtime;

public sealed partial class RuntimeTelemetry
{
    private sealed class Activity
    {
        public long Loops, Active, LastCycle, Started;
        public long Completed, Published, Retried, DeadLetter, Unconfirmed;
    }
    private readonly Activity _consumer = new();
    private readonly Activity _outbox = new();
    private Activity For(string component) => component == "consumer" ? _consumer : _outbox;

    private void AddWorkerInstruments()
    {
        Gauge("eventguard_worker_running_loops", a => Interlocked.Read(ref a.Loops));
        Gauge("eventguard_worker_active_cycles", a => Interlocked.Read(ref a.Active));
        Gauge("eventguard_worker_last_cycle_timestamp_seconds", a => Interlocked.Read(ref a.LastCycle) / 1000d);
        Gauge("eventguard_worker_started_timestamp_seconds", a => Interlocked.Read(ref a.Started) / 1000d);
        Gauge("eventguard_worker_observed_timestamp_seconds", _ => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000d);
    }
    private void Gauge(string name, Func<Activity, double> value) => _meter.CreateObservableGauge(name,
        () => new[] { new Measurement<double>(value(_consumer), new KeyValuePair<string, object?>("component", "consumer")),
            new Measurement<double>(value(_outbox), new KeyValuePair<string, object?>("component", "outbox")) });

    internal void StartLoop(string component)
    {
        var state = For(component);
        Interlocked.CompareExchange(ref state.Started, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), 0);
        Interlocked.Increment(ref state.Loops);
    }
    internal void StopLoop(string component) => Interlocked.Decrement(ref For(component).Loops);
}
