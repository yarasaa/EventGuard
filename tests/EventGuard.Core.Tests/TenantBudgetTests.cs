namespace EventGuard.Core.Tests;
public sealed class TenantBudgetTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 12, 0, 10, TimeSpan.Zero);
    [Fact]
    public void Retry_is_charged_to_both_shared_and_retry_budgets_but_recovery_is_separate()
    {
        var state = new TenantBudgetSnapshot(new("t", maximumClaims: 2, maximumRetries: 1, maximumRecoveries: 1), 0, TenantBudgets.Window(Now, 60), 0, 0, 0);
        state = TenantBudgets.Admit(state, TenantBudgetKind.RetryClaim, Now).After;
        Assert.False(TenantBudgets.Admit(state, TenantBudgetKind.RetryClaim, Now).Allowed);
        Assert.True(TenantBudgets.Admit(state, TenantBudgetKind.Claim, Now).Allowed);
        state = TenantBudgets.Admit(state, TenantBudgetKind.Claim, Now).After;
        Assert.False(TenantBudgets.Admit(state, TenantBudgetKind.Claim, Now).Allowed);
        state = TenantBudgets.Admit(state, TenantBudgetKind.Recovery, Now).After;
        Assert.Equal(2, state.Claims); Assert.Equal(1, state.Retries); Assert.Equal(1, state.Recoveries);
        Assert.False(TenantBudgets.Admit(state, TenantBudgetKind.Recovery, Now).Allowed);
    }
    [Fact]
    public void Forward_window_resets_but_clock_rollback_does_not_create_allowance()
    {
        var state = new TenantBudgetSnapshot(new("t", maximumClaims: 1), 0, TenantBudgets.Window(Now, 60), 1, 0, 0);
        Assert.False(TenantBudgets.Admit(state, TenantBudgetKind.Claim, Now.AddMinutes(-1)).Allowed);
        var next = TenantBudgets.Admit(state, TenantBudgetKind.Claim, Now.AddMinutes(1));
        Assert.True(next.Allowed); Assert.Equal(1, next.After.Claims); Assert.True(next.After.WindowStartedAt > state.WindowStartedAt);
    }
    [Fact]
    public void Configuration_updates_preserve_spending_and_receipts_cannot_be_reused_for_different_limits()
    {
        var first = new TenantBudgetRequest(Guid.NewGuid(), new("t", maximumClaims: 5), -1, "operator", "create");
        var applied = TenantBudgets.Configure(first, null, Now);
        var state = TenantBudgets.Admit(applied.After!, TenantBudgetKind.Claim, Now).After;
        var update = new TenantBudgetRequest(Guid.NewGuid(), new("t", maximumClaims: 1), 0, "operator", "tighten");
        var after = TenantBudgets.Configure(update, state, Now).After!;
        Assert.Equal(1, after.Version); Assert.Equal(1, after.Claims); Assert.False(TenantBudgets.Admit(after, TenantBudgetKind.Claim, Now).Allowed);
        Assert.Equal(TenantBudgetStatus.VersionConflict, TenantBudgets.Configure(first, after, Now).Result.Status);
        Assert.True(TenantBudgets.Repeat(first, first.Fingerprint, applied.Result).RepeatedRequest);
        Assert.Equal(TenantBudgetStatus.RequestConflict, TenantBudgets.Repeat(update, first.Fingerprint, applied.Result).Status);
    }
    [Fact]
    public void Zero_limit_pauses_admission_and_invalid_persisted_counters_fail_closed()
    {
        var state = new TenantBudgetSnapshot(new("t", maximumClaims: 0), 0, Now, 0, 0, 0);
        Assert.False(TenantBudgets.Admit(state, TenantBudgetKind.Claim, Now).Allowed);
        Assert.Throws<InvalidOperationException>(() => TenantBudgets.Admit(state with { Retries = 1 }, TenantBudgetKind.Claim, Now));
        Assert.Throws<ArgumentException>(() => new TenantBudgetPolicy("t", maximumClaims: -1));
    }
}
