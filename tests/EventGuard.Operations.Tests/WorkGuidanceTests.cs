namespace EventGuard.Operations.Tests;

public sealed class WorkGuidanceTests
{
    private static readonly DateTimeOffset Now = DateTimeOffset.Parse("2026-09-28T12:00:00Z");
    private static WorkSummary Work(WorkState state) => new(Guid.NewGuid(), state, 1, 1, 0,
        Now.AddHours(-1), Now, null, null, null);

    [Fact]
    public void Lease_recovery_remains_visible_while_unknown_application_reasons_remain_masked()
    {
        Assert.Equal("lease-recovered", MetadataCodes.Reason("lease-recovered"));
        Assert.Contains("previous lease expired", WorkGuidance.AttemptExplanation(MetadataCodes.Reason("lease-recovered")));
        Assert.Equal("unrecognized", MetadataCodes.Reason("secret-application-error"));
        Assert.Null(WorkGuidance.AttemptExplanation("secret-application-error"));
    }

    [Theory]
    [InlineData(WorkState.Pending)]
    [InlineData(WorkState.RetryScheduled)]
    public void Waiting_guidance_distinguishes_schedule_due_and_expiry_without_diagnosing_worker_health(WorkState state)
    {
        var work = Work(state);
        Assert.Equal("scheduled", WorkGuidance.Explain(work with { NextAttemptAt = Now.AddSeconds(1) }, Now).Code);
        var ready = WorkGuidance.Explain(work, Now);
        Assert.Equal("ready-to-process", ready.Code);
        Assert.Contains("does not identify the cause", ready.NextStep);
        Assert.Equal("expiry-awaiting-sweep", WorkGuidance.Explain(work with { ExpiresAt = Now }, Now).Code);
    }

    [Fact]
    public void Terminal_and_processing_states_are_not_reported_as_waiting_even_with_old_timestamps()
    {
        foreach (var state in new[] { WorkState.Completed, WorkState.Processing, WorkState.DeadLetter, WorkState.Expired })
        {
            var result = WorkGuidance.Explain(Work(state) with { ExpiresAt = Now.AddMinutes(-1) }, Now);
            Assert.NotEqual("expiry-awaiting-sweep", result.Code);
            Assert.NotEqual("ready-to-process", result.Code);
        }
        Assert.Equal("attempt-budget-exhausted", WorkGuidance.Explain(Work(WorkState.DeadLetter) with { ReasonCode = "attempt-budget-exhausted" }, Now).Code);
        Assert.Contains("cannot determine", WorkGuidance.Explain(Work(WorkState.Processing), Now).NextStep);
    }
}
