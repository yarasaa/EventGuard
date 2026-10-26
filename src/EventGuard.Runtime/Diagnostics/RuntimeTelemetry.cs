using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace EventGuard.Runtime;

/// <summary>Optional, caller-owned .NET instruments. No identities, payloads, networking or durable state.</summary>
public sealed partial class RuntimeTelemetry : IDisposable
{
    public const string MeterName = "EventGuard.Runtime";
    private readonly Meter _meter;
    private readonly Counter<long> _cycles;
    private readonly Histogram<double> _duration;
    private readonly UpDownCounter<long> _active;
    public RuntimeTelemetry()
    {
        _meter = new(new MeterOptions(MeterName) { Scope = this });
        _cycles = _meter.CreateCounter<long>("eventguard.runtime.cycles", "{cycle}", "Observed runtime cycle outcomes; not a durable ledger or backlog.");
        _duration = _meter.CreateHistogram<double>("eventguard.runtime.cycle.duration", "s", "Time in claim and execution/publication settlement; excludes idle backoff.");
        _active = _meter.CreateUpDownCounter<long>("eventguard.runtime.active", "{cycle}", "Runtime cycles currently waiting or executing in this process.");
        AddWorkerInstruments();
        AddOutcomeInstruments();
    }
    internal long Begin(string component)
    {
        Interlocked.Increment(ref For(component).Active);
        try { _active.Add(1, new KeyValuePair<string, object?>("component", component)); } catch { /* Observer faults must not alter processing. */ }
        return Stopwatch.GetTimestamp();
    }
    internal void End(string component, string outcome, long started)
    {
        var state = For(component);
        Interlocked.Decrement(ref state.Active);
        Interlocked.Exchange(ref state.LastCycle, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        RecordOutcome(state, outcome);
        // Instrument callbacks are trusted in-process code. No exporter/network call is made by this library.
        try { _active.Add(-1, new KeyValuePair<string, object?>("component", component)); } catch { }
        try { _cycles.Add(1, new("component", component), new("outcome", outcome)); } catch { }
        try { _duration.Record(Stopwatch.GetElapsedTime(started).TotalSeconds, new KeyValuePair<string, object?>("component", component)); } catch { }
    }
    internal static string WorkOutcome(ExecutionStatus status) => status switch
    {
        ExecutionStatus.Completed => "completed", ExecutionStatus.RetryScheduled => "retry_scheduled",
        ExecutionStatus.DeadLetter => "dead_letter", ExecutionStatus.Expired => "expired", ExecutionStatus.LeaseLost => "lease_lost", _ => "unconfirmed"
    };
    internal static string OutboxOutcome(OutboxSettlement result) => result.Status != OutboxSettlementStatus.Recorded ? "lease_lost" : result.Record.State switch
    {
        OutboxState.Published => "published", OutboxState.RetryScheduled => "retry_scheduled", OutboxState.DeadLetter => "dead_letter", _ => "unconfirmed"
    };
    public void Dispose() => _meter.Dispose();
}
