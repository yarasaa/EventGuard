using System.Diagnostics.Metrics;

namespace EventGuard.Runtime;

public sealed partial class RuntimeTelemetry
{
    private readonly long _created = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    private void AddOutcomeInstruments()
    {
        Gauge("eventguard_worker_telemetry_created_timestamp_seconds", _ => _created / 1000d);
        OutcomeCounter("completed", a => Interlocked.Read(ref a.Completed));
        OutcomeCounter("published", a => Interlocked.Read(ref a.Published));
        OutcomeCounter("retry_scheduled", a => Interlocked.Read(ref a.Retried));
        OutcomeCounter("dead_letter", a => Interlocked.Read(ref a.DeadLetter));
        OutcomeCounter("unconfirmed", a => Interlocked.Read(ref a.Unconfirmed));
    }
    private void OutcomeCounter(string name, Func<Activity, long> value) => _meter.CreateObservableCounter(
        "eventguard_worker_" + name + "_total", () => new[]
        {
            new Measurement<long>(value(_consumer), new KeyValuePair<string, object?>("component", "consumer")),
            new Measurement<long>(value(_outbox), new KeyValuePair<string, object?>("component", "outbox"))
        });
    private static void RecordOutcome(Activity state, string outcome)
    {
        switch (outcome)
        {
            case "completed": Interlocked.Increment(ref state.Completed); break;
            case "published": Interlocked.Increment(ref state.Published); break;
            case "retry_scheduled": Interlocked.Increment(ref state.Retried); break;
            case "dead_letter": Interlocked.Increment(ref state.DeadLetter); break;
            case "unconfirmed": Interlocked.Increment(ref state.Unconfirmed); break;
        }
    }
}
