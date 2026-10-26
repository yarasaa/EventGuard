namespace EventGuard.Core.Tests;

public sealed class WorkLifecycleTests
{
    private static readonly DateTimeOffset Now = new(2026, 9, 14, 12, 0, 0, TimeSpan.Zero);
    private static readonly WorkScope Scope = new("tenant", "files", "source", "file.created");
    private static readonly Guid Token = Guid.Parse("7b6bd95e-77bd-4306-9550-cfd7e6278f10");

    [Theory]
    [InlineData(WorkState.Completed)]
    [InlineData(WorkState.DeadLetter)]
    [InlineData(WorkState.Expired)]
    public void Terminal_work_cannot_be_claimed_even_if_dates_are_in_the_past(WorkState state)
    {
        var work = Item(state) with { ExpiresAt = Now.AddMinutes(-1) };
        Assert.Null(WorkLifecycle.Claim(work, null, Token, TimeSpan.FromSeconds(30), Now));
    }

    [Fact]
    public void Claim_preserves_identity_and_only_advances_the_attempt_counters()
    {
        var before = Item(WorkState.RetryScheduled);
        var decision = WorkLifecycle.Claim(before, null, Token, TimeSpan.FromSeconds(30), Now)!;
        Assert.Equal(before with { State = WorkState.Processing, Version = 8, AttemptCount = 5, GenerationAttempt = 3 }, decision.Change.After);
        Assert.Same(before, decision.Change.Before);
        Assert.Equal(new WorkLease(Token, Now.AddSeconds(30)), decision.Lease);
        Assert.Null(decision.AbandonedAttempt);
        Assert.Equal(WorkState.RetryScheduled, before.State);
    }

    [Fact]
    public void Scheduling_and_recovery_use_the_supplied_store_clock_at_the_exact_boundary()
    {
        var scheduled = Item(WorkState.Pending) with { NextAttemptAt = Now.AddSeconds(1) };
        Assert.Null(WorkLifecycle.Claim(scheduled, null, Token, TimeSpan.FromSeconds(30), Now));
        Assert.NotNull(WorkLifecycle.Claim(scheduled, null, Token, TimeSpan.FromSeconds(30), Now.AddSeconds(1)));
        var processing = Item(WorkState.Processing);
        var lease = new WorkLease(Guid.NewGuid(), Now.AddSeconds(1));
        Assert.Null(WorkLifecycle.Claim(processing, lease, Token, TimeSpan.FromSeconds(30), Now));
        Assert.Equal(lease.Token, WorkLifecycle.Claim(processing, lease, Token, TimeSpan.FromSeconds(30), Now.AddSeconds(1))!.AbandonedAttempt);
    }

    [Fact]
    public void Expired_retry_can_be_claimed_for_cleanup_without_waiting_for_next_attempt()
    {
        var work = Item(WorkState.RetryScheduled) with { NextAttemptAt = Now.AddHours(1), ExpiresAt = Now };
        var claimed = WorkLifecycle.Claim(work, null, Token, TimeSpan.FromSeconds(30), Now)!;
        Assert.Equal(ExecutionAdmission.Expire, WorkLifecycle.AdmitExecution(claimed.Change.After, claimed.Lease, Token, Now));
        var expired = WorkLifecycle.Expire(claimed.Change.After, claimed.Lease, Token, Now);
        Assert.Equal(WorkState.Expired, expired.Change.After.State);
        Assert.Equal(ExecutionStatus.Expired, expired.Status);
    }

    [Fact]
    public void Missing_or_stale_ownership_cannot_authorize_handler_or_failure_transition()
    {
        var work = Item(WorkState.Processing);
        var current = new WorkLease(Guid.NewGuid(), Now.AddMinutes(1));
        Assert.Equal(ExecutionAdmission.LeaseLost, WorkLifecycle.AdmitExecution(work, current, Token, Now));
        Assert.Equal(ExecutionAdmission.LeaseLost, WorkLifecycle.AdmitExecution(work, null, Token, Now));
        Assert.Null(WorkLifecycle.Fail(work, current, Token, new(), FailureKind.Permanent, Now, 0.5));
        Assert.Throws<InvalidOperationException>(() => WorkLifecycle.Complete(work, current, Token, Now));
    }

    [Fact]
    public void Expired_lease_blocks_start_but_does_not_revoke_completion_under_an_unbroken_lock()
    {
        var work = Item(WorkState.Processing) with { ExpiresAt = Now.AddSeconds(1) };
        var lease = new WorkLease(Token, Now.AddSeconds(1));
        Assert.Equal(ExecutionAdmission.Execute, WorkLifecycle.AdmitExecution(work, lease, Token, Now));
        Assert.Equal(ExecutionAdmission.LeaseLost, WorkLifecycle.AdmitExecution(work, lease, Token, Now.AddSeconds(1)));
        var decision = WorkLifecycle.Complete(work, lease, Token, Now.AddMinutes(1));
        Assert.Equal(ExecutionStatus.Completed, decision.Status);
        Assert.Equal(work with { State = WorkState.Completed, Version = 8, NextAttemptAt = Now.AddMinutes(1), LastReason = null }, decision.Change.After);
    }

    [Theory]
    [InlineData(FailureKind.Transient, 3, WorkState.RetryScheduled, "transient-failure")]
    [InlineData(FailureKind.Transient, 2, WorkState.DeadLetter, "attempt-budget-exhausted")]
    [InlineData(FailureKind.Permanent, 3, WorkState.DeadLetter, "permanent-failure")]
    [InlineData(FailureKind.Cancelled, 1, WorkState.RetryScheduled, "cancelled")]
    public void Failure_uses_generation_budget_and_preserves_lifetime_history(FailureKind kind, int budget, WorkState expected, string reason)
    {
        var work = Item(WorkState.Processing);
        var result = WorkLifecycle.Fail(work, new(Token, Now.AddMinutes(1)), Token,
            new(budget, jitterRatio: 0), kind, Now, 0.5)!;
        Assert.Equal(expected, result.Change.After.State);
        Assert.Equal(reason, result.Change.After.LastReason);
        Assert.Equal(work.AttemptCount, result.Change.After.AttemptCount);
        Assert.Equal(work.GenerationAttempt, result.Change.After.GenerationAttempt);
        Assert.Equal(work.ReplayGeneration, result.Change.After.ReplayGeneration);
        Assert.Equal("Failed", result.AttemptStatus);
        Assert.Equal(expected == WorkState.DeadLetter ? ExecutionStatus.DeadLetter : ExecutionStatus.RetryScheduled, result.Status);
    }

    [Theory]
    [InlineData(WorkState.Pending)]
    [InlineData(WorkState.Processing)]
    [InlineData(WorkState.RetryScheduled)]
    [InlineData(WorkState.Completed)]
    [InlineData(WorkState.Expired)]
    public void Replay_cannot_bypass_non_failed_state(WorkState state)
    {
        var work = Item(state);
        var decision = WorkLifecycle.Replay(Request(work), work, Now);
        Assert.Equal(ReplayStatus.NotFailed, decision.Result.Status);
        Assert.Null(decision.Change);
    }

    [Fact]
    public void Replay_resets_only_generation_attempt_and_keeps_identity_and_expiry()
    {
        var work = Item(WorkState.DeadLetter) with { ExpiresAt = Now.AddHours(1) };
        var decision = WorkLifecycle.Replay(Request(work), work, Now);
        Assert.Equal(work with
        {
            State = WorkState.Pending,
            Version = 8,
            ReplayGeneration = 2,
            GenerationAttempt = 0,
            NextAttemptAt = Now,
            LastReason = "replay-requested"
        }, decision.Change!.After);
        Assert.Equal(new ReplayResult(ReplayStatus.Accepted, work.Id, 2, 8), decision.Result);
    }

    [Fact]
    public void Replay_checks_scope_then_state_version_and_expiry_without_leaking_foreign_metadata()
    {
        var work = Item(WorkState.DeadLetter);
        var request = Request(work);
        Assert.Equal(ReplayStatus.VersionConflict, WorkLifecycle.Replay(request, work with { Version = 8 }, Now).Result.Status);
        Assert.Equal(ReplayStatus.PayloadExpired, WorkLifecycle.Replay(request, work with { ExpiresAt = Now }, Now).Result.Status);
        var foreign = WorkLifecycle.Replay(request, work with { ScopeId = "foreign", Version = 999 }, Now);
        Assert.Equal(new ReplayResult(ReplayStatus.NotFound, work.Id, 0, 0), foreign.Result);
        Assert.Null(foreign.Change);
    }

    [Fact]
    public void Repeated_replay_returns_the_recorded_outcome_including_a_previous_rejection()
    {
        var work = Item(WorkState.Completed);
        var request = Request(work);
        var receipt = WorkLifecycle.Replay(request, work, Now).Result;
        var repeated = WorkLifecycle.RepeatReplay(request, request.Fingerprint, receipt);
        Assert.Equal(receipt with { RepeatedRequest = true }, repeated);
        var altered = new ReplayRequest(Scope, request.RequestId, work.Id, work.Version, "operator", "changed reason");
        Assert.Equal(ReplayStatus.RequestConflict, WorkLifecycle.RepeatReplay(altered, request.Fingerprint, receipt).Status);
    }

    [Fact]
    public void Counter_overflow_cannot_wrap_into_a_reusable_version_or_generation()
    {
        Assert.Throws<OverflowException>(() => WorkLifecycle.Claim(Item(WorkState.Pending) with { Version = long.MaxValue }, null, Token, TimeSpan.FromSeconds(1), Now));
        var work = Item(WorkState.DeadLetter) with { ReplayGeneration = int.MaxValue };
        Assert.Throws<OverflowException>(() => WorkLifecycle.Replay(Request(work), work, Now));
    }

    [Fact]
    public void Corrupt_state_and_ownership_are_rejected_instead_of_treated_as_ready_work()
    {
        Assert.Throws<InvalidOperationException>(() => WorkLifecycle.Claim(Item(WorkState.Processing), null, Token, TimeSpan.FromSeconds(1), Now));
        Assert.Throws<InvalidOperationException>(() => WorkLifecycle.Claim(Item((WorkState)100), null, Token, TimeSpan.FromSeconds(1), Now));
        Assert.Throws<InvalidOperationException>(() => WorkLifecycle.Claim(Item(WorkState.Pending), new(Token, Now), Token, TimeSpan.FromSeconds(1), Now));
    }

    private static ReplayRequest Request(WorkItem work) => new(Scope, Guid.NewGuid(), work.Id, work.Version, "operator", "configuration fixed");
    private static WorkItem Item(WorkState state) => new(Guid.NewGuid(), "identity", Scope.Id, Guid.NewGuid(),
        ContentFingerprint.FromBytes([1]), state, 7, 4, 2, 1, Now.AddSeconds(-1), null, "previous-failure", "correlation");

    [Fact]
    public void Removed_payload_cannot_be_replayed_even_if_expiry_is_in_the_future()
    {
        var work = Item(WorkState.DeadLetter) with { ExpiresAt = Now.AddDays(30) };
        var request = new ReplayRequest(Scope, Guid.NewGuid(), work.Id, work.Version, "operator", "test");
        Assert.Equal(ReplayStatus.Accepted, WorkLifecycle.Replay(request, work, Now).Result.Status);
        var refused = WorkLifecycle.Replay(request, work, Now, payloadAvailable: false);
        Assert.Equal(ReplayStatus.PayloadExpired, refused.Result.Status); Assert.Null(refused.Change);
    }
}
