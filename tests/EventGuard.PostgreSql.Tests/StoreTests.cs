using Npgsql;
using EventGuard.Runtime;

namespace EventGuard.PostgreSql.Tests;

public sealed class StoreTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    [Fact]
    public async Task Completion_after_expiry_keeps_the_transaction_that_already_started()
    {
        var scope = DatabaseFixture.Scope();
        var expiry = DateTimeOffset.UtcNow.AddSeconds(2);
        var accepted = await db.Store.AcceptAsync(DatabaseFixture.Submission(scope, expiresAt: expiry));
        var claim = (await db.Claim(scope))!;
        var result = await db.Store.ExecuteAsync(claim, async (work, payload, session, ct) =>
        {
            var delay = expiry - DateTimeOffset.UtcNow + TimeSpan.FromMilliseconds(50);
            if (delay > TimeSpan.Zero) await Task.Delay(delay, ct);
            await db.Effect(work, payload, session, ct);
        }, new());
        Assert.Equal(ExecutionStatus.Completed, result.Status);
        Assert.Equal(1, await db.EffectCount(accepted.Work.Id));
    }

    [Fact]
    public async Task Unexpected_version_change_inside_handler_rolls_back_instead_of_bypassing_transition_guard()
    {
        var scope = DatabaseFixture.Scope();
        var accepted = await db.Store.AcceptAsync(DatabaseFixture.Submission(scope));
        var claim = (await db.Claim(scope))!;
        var result = await db.Store.ExecuteAsync(claim, async (work, payload, session, ct) =>
        {
            await db.Effect(work, payload, session, ct);
            await using var command = session.CreateCommand($"UPDATE {db.Schema}.works SET version=version+10 WHERE id=@id");
            command.Parameters.AddWithValue("id", work.Id);
            await command.ExecuteNonQueryAsync(ct);
        }, new(classify: _ => FailureKind.Permanent));
        Assert.Equal(ExecutionStatus.DeadLetter, result.Status);
        Assert.Equal(claim.Work.Version + 1, result.Work.Version);
        Assert.Equal(0, await db.EffectCount(accepted.Work.Id));
    }


    [Fact]
    public async Task Conflict_preserves_original_payload_and_saves_conflicting_input()
    {
        var scope = DatabaseFixture.Scope();
        var original = DatabaseFixture.Submission(scope, payload: [1]);
        var accepted = await db.Store.AcceptAsync(original);
        var conflict = await db.Store.AcceptAsync(DatabaseFixture.Submission(scope, payload: [2]));
        Assert.Equal(AcceptanceStatus.Conflict, conflict.Status);
        Assert.Equal(accepted.Work.Id, conflict.Work.Id);
        Assert.Equal(new byte[] { 1 }, (await db.Claim(scope))!.Payload);
        await using var command = db.DataSource.CreateCommand($"SELECT payload FROM {db.Schema}.conflicts WHERE work_id=@id");
        command.Parameters.AddWithValue("id", accepted.Work.Id);
        Assert.Equal(new byte[] { 2 }, (byte[])(await command.ExecuteScalarAsync())!);
    }

    [Fact]
    public async Task Pump_bounds_parallelism_and_commits_each_business_effect_once()
    {
        var scope = DatabaseFixture.Scope();
        await Task.WhenAll(Enumerable.Range(0, 30).Select(i => db.Store.AcceptAsync(DatabaseFixture.Submission(scope, "item-" + i))));
        using var telemetry = new RuntimeTelemetry();
        using var listener = new System.Diagnostics.Metrics.MeterListener();
        listener.InstrumentPublished = (instrument, sink) => { if (ReferenceEquals(instrument.Meter.Scope, telemetry)) sink.EnableMeasurementEvents(instrument); };
        int observerCalls = 0;
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => { Interlocked.Increment(ref observerCalls); throw new InvalidOperationException("simulated exporter failure"); });
        listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) => throw new InvalidOperationException("simulated exporter failure"));
        listener.Start();
        int active = 0, maximum = 0;
        var pump = new WorkPump<PostgreSqlSession>(db.Store, scope, async (w, p, s, ct) =>
        {
            var concurrent = Interlocked.Increment(ref active);
            int observed;
            do { observed = Volatile.Read(ref maximum); }
            while (concurrent > observed && Interlocked.CompareExchange(ref maximum, concurrent, observed) != observed);
            try
            {
                await Task.Delay(10, ct);
                await db.Effect(w, p, s, ct);
            }
            finally { Interlocked.Decrement(ref active); }
        }, options: new(maximumConcurrency: 4, idleDelay: TimeSpan.FromMilliseconds(20)), telemetry: telemetry);
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var running = pump.RunAsync(stop.Token);
        try
        {
            while (true)
            {
                stop.Token.ThrowIfCancellationRequested();
                if (running.IsFaulted) await running;
                await using var command = db.DataSource.CreateCommand($"SELECT count(*) FROM {db.Schema}.works WHERE scope_id=@scope AND state=3");
                command.Parameters.AddWithValue("scope", scope.Id);
                if ((long)(await command.ExecuteScalarAsync(stop.Token))! == 30) break;
                await Task.Delay(20, stop.Token);
            }
        }
        finally { stop.Cancel(); await running; }
        Assert.True(observerCalls > 0);
        Assert.InRange(maximum, 1, 4);
        Assert.Equal(0, active);
        await using var effects = db.DataSource.CreateCommand($"SELECT count(*),count(DISTINCT work_id) FROM {db.Schema}.business_effects JOIN {db.Schema}.works ON works.id=work_id WHERE scope_id=@scope");
        effects.Parameters.AddWithValue("scope", scope.Id);
        await using var reader = await effects.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(30L, reader.GetInt64(0));
        Assert.Equal(30L, reader.GetInt64(1));
    }

}
