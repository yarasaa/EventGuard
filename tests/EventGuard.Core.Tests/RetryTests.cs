namespace EventGuard.Core.Tests;

public sealed class RetryTests
{
    [Fact]
    public void Backoff_is_bounded_and_budget_includes_the_first_attempt()
    {
        var now = DateTimeOffset.UtcNow;
        var policy = new RetryPolicy(5, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(3), 0);
        Assert.Equal(now.AddSeconds(1), policy.Decide(FailureKind.Transient, 1, now, 0.5).NextAttemptAt);
        Assert.Equal(now.AddSeconds(2), policy.Decide(FailureKind.Transient, 2, now, 0.5).NextAttemptAt);
        Assert.Equal(now.AddSeconds(3), policy.Decide(FailureKind.Transient, 4, now, 0.5).NextAttemptAt);
        Assert.True(policy.Decide(FailureKind.Transient, 5, now, 0.5).IsTerminal);
    }

    [Fact]
    public void Cancellation_does_not_dead_letter_and_permanent_failure_does()
    {
        var policy = new RetryPolicy(1);
        Assert.False(policy.Decide(FailureKind.Cancelled, 1, DateTimeOffset.UtcNow, 0.5).IsTerminal);
        Assert.True(policy.Decide(FailureKind.Permanent, 1, DateTimeOffset.UtcNow, 0.5).IsTerminal);
    }

    [Fact]
    public void Jitter_cannot_create_zero_delay_busy_loop()
    {
        var now = DateTimeOffset.UtcNow;
        Assert.True(new RetryPolicy(jitterRatio: 1).Decide(FailureKind.Transient, 1, now, 0).NextAttemptAt > now);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(1.1)]
    [InlineData(double.NaN)]
    public void Invalid_jitter_is_rejected(double jitter) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => new RetryPolicy(jitterRatio: jitter));
}
