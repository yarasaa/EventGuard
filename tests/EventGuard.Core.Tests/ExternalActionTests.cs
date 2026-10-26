namespace EventGuard.Core.Tests;

public sealed class ExternalActionTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static ExternalActionRecord Record() => new(new(new("t", "p"), Guid.NewGuid(), "service", "v1", [1], TimeSpan.FromMinutes(1)),
        ExternalActionState.Sending, ExternalObservationState.None, 1, 1, Now, null, null, null, []);

    [Fact]
    public void Intent_and_result_freeze_payload_and_semantic_binding()
    {
        byte[] data = [1]; var a = new ExternalAction(new("t", "p"), Guid.NewGuid(), "service", "v1", data);
        var result = new ExternalResult(ExternalResultKind.Accepted, "ok", data: data);
        data[0] = 9; a.CopyPayload()[0] = 7; result.CopyData()[0] = 8;
        Assert.Equal(new byte[] { 1 }, a.CopyPayload()); Assert.Equal(new byte[] { 1 }, result.CopyData());
        Assert.NotEqual(a.Fingerprint, new ExternalAction(a.Scope, a.Id, a.Binding, "v2", [1]).Fingerprint);
        Assert.Throws<ArgumentException>(() => new ExternalAction(a.Scope, Guid.Empty, "s", "v1", []));
        Assert.Throws<ArgumentException>(() => new ExternalResult(ExternalResultKind.Unknown, "sensitive free text"));
    }

    [Fact]
    public void Unknown_does_not_retry_or_infer_acceptance()
    {
        var after = ExternalActionLifecycle.Settle(Record(), ExternalPhase.Submit, new(ExternalResultKind.Unknown, "timeout"), new(), Now);
        Assert.Equal(ExternalActionState.Unknown, after.State); Assert.Equal(ExternalObservationState.None, after.Observation);
        var pending = ExternalActionLifecycle.Settle(after, ExternalPhase.Reconcile, new(ExternalResultKind.Unknown, "not-proven"), new(), Now);
        Assert.Equal(ExternalActionState.Unknown, pending.State);
        Assert.Throws<InvalidOperationException>(() => ExternalActionLifecycle.Settle(after, ExternalPhase.Submit, new(ExternalResultKind.Accepted, "ok"), new(), Now));
    }

    [Theory]
    [InlineData(ExternalResultKind.Accepted)]
    [InlineData(ExternalResultKind.Rejected)]
    [InlineData(ExternalResultKind.NotAcceptedRetryable)]
    public void Observation_cannot_change_submission_acceptance(ExternalResultKind kind)
    {
        var accepted = ExternalActionLifecycle.Settle(Record(), ExternalPhase.Submit, new(ExternalResultKind.Accepted, "ok"), new(), Now);
        Assert.Throws<InvalidOperationException>(() => ExternalActionLifecycle.Settle(accepted, ExternalPhase.Observe, new(kind, "invalid"), new(), Now));
    }

    [Theory]
    [InlineData(ExternalResultKind.Pending)]
    [InlineData(ExternalResultKind.Succeeded)]
    [InlineData(ExternalResultKind.Failed)]
    public void Submission_requires_acceptance_evidence(ExternalResultKind kind) =>
        Assert.Throws<InvalidOperationException>(() => ExternalActionLifecycle.Settle(Record(), ExternalPhase.Submit, new(kind, "invalid"), new(), Now));

    [Fact]
    public void Final_observation_cannot_regress_and_timeout_is_not_delivery_failure()
    {
        var accepted = ExternalActionLifecycle.Settle(Record(), ExternalPhase.Submit, new(ExternalResultKind.Accepted, "ok"), new(), Now);
        var succeeded = ExternalActionLifecycle.Settle(accepted, ExternalPhase.Observe, new(ExternalResultKind.Succeeded, "done"), new(), Now);
        Assert.Throws<InvalidOperationException>(() => ExternalActionLifecycle.Settle(succeeded, ExternalPhase.Observe, new(ExternalResultKind.Pending, "late"), new(), Now));
        var timeout = ExternalActionLifecycle.Settle(accepted, ExternalPhase.Observe, new(ExternalResultKind.Unknown, "timeout"), new(), Now.AddMinutes(2));
        Assert.Equal(ExternalObservationState.TimedOut, timeout.Observation); Assert.Equal(ExternalActionState.Accepted, timeout.State);
    }
}
