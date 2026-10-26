using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using EventGuard.Runtime;

namespace EventGuard.PostgreSql.Tests;

public sealed class TelemetryTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    [Fact]
    public async Task Publication_metric_follows_real_settlement_and_contains_no_message_identifiers()
    {
        using var telemetry = new RuntimeTelemetry(); using var listener = new MeterListener();
        var observed = new ConcurrentBag<(string Name, long Value, string[] Tags)>();
        listener.InstrumentPublished = (instrument, sink) => { if (ReferenceEquals(instrument.Meter.Scope, telemetry)) sink.EnableMeasurementEvents(instrument); };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => observed.Add((instrument.Name, value, tags.ToArray().Select(t => t.Key + "=" + t.Value).ToArray())));
        listener.Start();
        var scope = new ProducerScope(Guid.NewGuid().ToString("N"), "sensitive-producer");
        var message = new OutgoingMessage(scope, Guid.NewGuid(), Guid.NewGuid(), new("test", "sensitive-binding", "v1", "sensitive-address"), "sensitive-payload"u8);
        await db.EnqueueAsync(message);
        var result = await new OutboxDispatcher(db.Outbox, scope, [new TestTransport()], telemetry: telemetry).DispatchNextAsync();
        Assert.Equal(OutboxState.Published, result!.Record.State);
        listener.RecordObservableInstruments();
        Assert.Equal(1, Assert.Single(observed, m => m.Name == "eventguard_worker_published_total" && m.Tags.Contains("component=outbox")).Value);
        var cycle = Assert.Single(observed, m => m.Name == "eventguard.runtime.cycles");
        Assert.Equal(1, cycle.Value); Assert.Equal(new[] { "component=outbox", "outcome=published" }, cycle.Tags);
        Assert.Equal(0, observed.Where(m => m.Name == "eventguard.runtime.active").Sum(m => m.Value));
        Assert.DoesNotContain("sensitive", string.Join(";", observed.SelectMany(m => m.Tags)));
    }
    [Fact]
    public async Task Worker_gauges_track_live_processing_and_return_to_zero_after_shutdown()
    {
        using var telemetry = new RuntimeTelemetry(); using var listener = new MeterListener();
        var gauges = new ConcurrentDictionary<string, double>();
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        listener.InstrumentPublished = (instrument, sink) => { if (ReferenceEquals(instrument.Meter.Scope, telemetry)) sink.EnableMeasurementEvents(instrument); };
        listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) =>
        { if (tags.ToArray().Any(t => t.Key == "component" && Equals(t.Value, "consumer"))) gauges[instrument.Name] = value; });
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            var labels = tags.ToArray();
            if (!labels.Any(t => t.Key == "component" && Equals(t.Value, "consumer"))) return;
            gauges[instrument.Name] = value;
            // Stop at the confirmed cycle boundary; canceling a later idle native claim
            // can legitimately increment unconfirmed and is not this gauge test's scenario.
            if (instrument.Name == "eventguard.runtime.cycles" && labels.Any(t => t.Key == "outcome" && Equals(t.Value, "completed")))
                stop.Cancel();
        });
        listener.Start();
        var scope = new WorkScope(Guid.NewGuid().ToString("N"), "worker-gauges", "native", "test");
        byte[] payload = "{}"u8.ToArray();
        var accepted = await db.Store.AcceptAsync(new(new(scope, new("key/v1", "bytes/v1"), Guid.NewGuid().ToString("N")), Guid.NewGuid(), payload, ContentFingerprint.FromBytes(payload)));
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pump = new WorkPump<PostgreSqlSession>(db.Store, scope, async (work, bytes, session, ct) =>
        { entered.TrySetResult(); await release.Task.WaitAsync(ct); await db.Effect(work, bytes, session, ct); }, options: new(maximumConcurrency: 1), telemetry: telemetry);
        var run = pump.RunAsync(stop.Token);
        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(5)); listener.RecordObservableInstruments();
            Assert.Equal(1d, gauges["eventguard_worker_running_loops"]); Assert.Equal(1d, gauges["eventguard_worker_active_cycles"]);
            Assert.True(gauges["eventguard_worker_observed_timestamp_seconds"] > 0);
            release.TrySetResult();
            await run.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(1, await db.EffectCount(accepted.Work!.Id));
        }
        finally { release.TrySetResult(); stop.Cancel(); await run; }
        listener.RecordObservableInstruments();
        Assert.Equal(0d, gauges["eventguard_worker_running_loops"]); Assert.Equal(0d, gauges["eventguard_worker_active_cycles"]);
        Assert.True(gauges["eventguard_worker_last_cycle_timestamp_seconds"] > 0);
        Assert.Equal(1d, gauges["eventguard_worker_completed_total"]);
        Assert.Equal(0d, gauges["eventguard_worker_retry_scheduled_total"]);
        Assert.Equal(0d, gauges["eventguard_worker_unconfirmed_total"]);
    }
    private sealed class TestTransport : IOutboundTransport
    {
        public string Transport => "test";
        public string Connection => "sensitive-binding";
        public string ConfigurationVersion => "v1";
        public Task<PublishOutcome> PublishAsync(OutgoingMessage message, CancellationToken ct) => Task.FromResult(new PublishOutcome(PublishStatus.Accepted, "test-accepted"));
    }
}
