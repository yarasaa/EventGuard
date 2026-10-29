namespace EventGuard.Core.Tests;

public sealed class ExternalBatchContractsTests
{
    private static readonly DateTimeOffset Now = DateTimeOffset.Parse("2026-01-01T00:00:00Z");

    [Fact]
    public void Schedule_uses_acceptance_age_after_restart_and_wakes_at_deadline()
    {
        var steps = new[] { new ExternalObservationStep(TimeSpan.Zero, TimeSpan.FromMinutes(2)),
            new ExternalObservationStep(TimeSpan.FromMinutes(10), TimeSpan.FromMinutes(5)),
            new ExternalObservationStep(TimeSpan.FromHours(1), TimeSpan.FromMinutes(30)) };
        var policy = new ExternalActionPolicy(new ExternalObservationSchedule(steps));
        steps[0] = new(TimeSpan.Zero, TimeSpan.FromHours(2));
        var record = new ExternalActionRecord(new(new("t", "p"), Guid.NewGuid(), "s", "v1", [], TimeSpan.FromHours(24)),
            ExternalActionState.Sending, ExternalObservationState.None, 1, 1, Now, null, null, null, []);
        var accepted = ExternalActionLifecycle.Settle(record, ExternalPhase.Submit, new(ExternalResultKind.Accepted, "ok"), policy, Now);
        Assert.Equal(Now.AddMinutes(2), accepted.NextAttemptAt);
        var pending = new ExternalResult(ExternalResultKind.Pending, "waiting");
        Assert.Equal(Now.AddMinutes(15), ExternalActionLifecycle.Settle(accepted, ExternalPhase.Observe, pending, policy, Now.AddMinutes(10)).NextAttemptAt);
        Assert.Equal(Now.AddMinutes(90), ExternalActionLifecycle.Settle(accepted, ExternalPhase.Observe, pending, policy, Now.AddHours(1)).NextAttemptAt);
        Assert.Equal(Now.AddHours(24), ExternalActionLifecycle.Settle(accepted, ExternalPhase.Observe, pending, policy, Now.AddMinutes(1430)).NextAttemptAt);
        var timedOut = ExternalActionLifecycle.Settle(accepted, ExternalPhase.Observe, pending, policy, Now.AddHours(24));
        Assert.Equal(ExternalObservationState.TimedOut, timedOut.Observation);
        Assert.Equal(ExternalActionState.Accepted, timedOut.State);
    }

    [Fact]
    public void Fixed_interval_also_respects_a_shorter_deadline()
    {
        var record = new ExternalActionRecord(new(new("t", "p"), Guid.NewGuid(), "s", "v1", [], TimeSpan.FromSeconds(5)),
            ExternalActionState.Sending, ExternalObservationState.None, 1, 1, Now, null, null, null, []);
        Assert.Equal(Now.AddSeconds(5), ExternalActionLifecycle.Settle(record, ExternalPhase.Submit,
            new(ExternalResultKind.Accepted, "ok"), new(), Now).NextAttemptAt);
    }

    [Fact]
    public void Schedule_rejects_missing_zero_duplicate_ages_and_unbounded_inputs()
    {
        Assert.Throws<ArgumentException>(() => new ExternalObservationSchedule([]));
        Assert.Throws<ArgumentException>(() => new ExternalObservationSchedule([new(TimeSpan.FromMinutes(1), TimeSpan.FromSeconds(1))]));
        Assert.Throws<ArgumentException>(() => new ExternalObservationSchedule([new(TimeSpan.Zero, TimeSpan.Zero)]));
        Assert.Throws<ArgumentException>(() => new ExternalObservationSchedule([new(TimeSpan.Zero, TimeSpan.FromSeconds(1)), new(TimeSpan.Zero, TimeSpan.FromSeconds(2))]));
        Assert.Throws<ArgumentException>(() => new ExternalObservationSchedule(Enumerable.Range(0, 65).Select(i => new ExternalObservationStep(TimeSpan.FromMinutes(i), TimeSpan.FromSeconds(1)))));
    }

    [Fact]
    public void Batch_identity_is_order_independent_and_scope_and_phase_are_isolated()
    {
        ExternalActionClaim Claim(string tenant = "t", ExternalPhase phase = ExternalPhase.Submit) => new(
            new(new(new(tenant, "p"), Guid.NewGuid(), "s", "v1", []), ExternalActionState.Sending,
                ExternalObservationState.None, 1, 1, Now, null, null, null, []), phase, Guid.NewGuid(), Now.AddMinutes(1));
        var a = Claim(); var b = Claim();
        Assert.Equal(new ExternalBatchRequest([a,b]).AttemptId, new ExternalBatchRequest([b,a]).AttemptId);
        Assert.NotEqual(new ExternalBatchRequest([a]).AttemptId, new ExternalBatchRequest([a with { Token = Guid.NewGuid() }]).AttemptId);
        Assert.Throws<ArgumentException>(() => new ExternalBatchRequest([a,a]));
        Assert.Throws<ArgumentException>(() => new ExternalBatchRequest([a,Claim("other")]));
        Assert.Throws<ArgumentException>(() => new ExternalBatchRequest([a,Claim(phase: ExternalPhase.Observe)]));
        Assert.Throws<ArgumentException>(() => new ExternalBatchRequest([]));
        Assert.Throws<ArgumentException>(() => new ExternalBatchRequest(Enumerable.Range(0, 101).Select(_ => Claim())));
    }

    [Fact]
    public void Explicit_batch_limit_accepts_large_batches_and_stops_unbounded_inputs()
    {
        var read = 0;
        IEnumerable<ExternalActionClaim> Claims()
        {
            while (true)
            {
                read++;
                yield return new(new(new(new("t", "p"), Guid.NewGuid(), "s", "v1", []),
                    ExternalActionState.Sending, ExternalObservationState.None, 1, 1, Now, null, null, null, []),
                    ExternalPhase.Submit, Guid.NewGuid(), Now.AddMinutes(1));
            }
        }
        Assert.Equal(1000, new ExternalBatchRequest(Claims().Take(1000), 1000).Items.Count);
        read = 0;
        Assert.Throws<ArgumentException>(() => new ExternalBatchRequest(Claims(), 250));
        Assert.Equal(251, read);
        read = 0;
        Assert.Throws<ArgumentOutOfRangeException>(() => new ExternalBatchRequest(Claims(), 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new ExternalBatchRequest(Claims(), -1));
        Assert.Equal(0, read);
        Assert.Single(new ExternalBatchRequest(Claims().Take(1), int.MaxValue).Items);
    }

    [Fact]
    public void Json_helpers_preserve_user_model_and_stable_identity()
    {
        var a = JsonExternalActions.Create(new("t", "p"), Guid.NewGuid(), "s", "v1", new Model("item-7", 12));
        Assert.Equal(new Model("item-7", 12), a.ReadJson<Model>());
        Assert.Throws<System.Text.Json.JsonException>(() => new ExternalAction(a.Scope, a.Id, a.Binding, a.BindingVersion, "null"u8).ReadJson<Model>());
    }

    public sealed record Model(string Key, int Amount);
}
