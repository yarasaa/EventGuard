namespace EventGuard;

public enum FailureKind { Transient, Permanent, Cancelled }
public sealed record RetryDecision(bool IsTerminal, DateTimeOffset NextAttemptAt, string ReasonCode);

public sealed class RetryPolicy
{
    public int MaximumAttempts { get; }
    public TimeSpan InitialDelay { get; }
    public TimeSpan MaximumDelay { get; }
    public double JitterRatio { get; }
    public Func<Exception, FailureKind> Classify { get; }

    public RetryPolicy(int maximumAttempts = 5, TimeSpan? initialDelay = null, TimeSpan? maximumDelay = null,
        double jitterRatio = 0.2, Func<Exception, FailureKind>? classify = null)
    {
        InitialDelay = initialDelay ?? TimeSpan.FromSeconds(1);
        MaximumDelay = maximumDelay ?? TimeSpan.FromMinutes(1);
        if (maximumAttempts < 1) throw new ArgumentOutOfRangeException(nameof(maximumAttempts));
        if (InitialDelay <= TimeSpan.Zero || MaximumDelay < InitialDelay || MaximumDelay > TimeSpan.FromDays(30))
            throw new ArgumentOutOfRangeException(nameof(initialDelay));
        if (!double.IsFinite(jitterRatio) || jitterRatio < 0 || jitterRatio > 1)
            throw new ArgumentOutOfRangeException(nameof(jitterRatio));
        MaximumAttempts = maximumAttempts;
        JitterRatio = jitterRatio;
        Classify = classify ?? (e => e switch
        {
            PermanentWorkException => FailureKind.Permanent,
            IngressMessageRejectedException => FailureKind.Permanent,
            OutboxConflictException => FailureKind.Permanent,
            OperationCanceledException => FailureKind.Cancelled,
            _ => FailureKind.Transient
        });
    }

    public RetryDecision Decide(FailureKind kind, int generationAttempt, DateTimeOffset now, double randomSample)
    {
        if (!Enum.IsDefined(kind)) throw new ArgumentOutOfRangeException(nameof(kind));
        if (generationAttempt < 1) throw new ArgumentOutOfRangeException(nameof(generationAttempt));
        if (!double.IsFinite(randomSample) || randomSample < 0 || randomSample > 1)
            throw new ArgumentOutOfRangeException(nameof(randomSample));
        if (kind == FailureKind.Permanent) return new(true, now, "permanent-failure");
        // Shutdown cancellation consumes an attempt history entry, but not a terminal failure decision.
        if (kind != FailureKind.Cancelled && generationAttempt >= MaximumAttempts)
            return new(true, now, "attempt-budget-exhausted");
        var exponential = Math.Min(MaximumDelay.TotalMilliseconds,
            InitialDelay.TotalMilliseconds * Math.Pow(2, Math.Min(generationAttempt - 1, 30)));
        var delay = Math.Min(MaximumDelay.TotalMilliseconds,
            exponential * (1 - JitterRatio + randomSample * JitterRatio));
        return new(false, now.AddMilliseconds(Math.Max(1, delay)), kind == FailureKind.Cancelled ? "cancelled" : "transient-failure");
    }
}
