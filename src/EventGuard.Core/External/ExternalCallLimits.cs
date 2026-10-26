using System.Globalization;

namespace EventGuard;

/// <summary>One shared endpoint/account budget in one processing store, independent of worker count.</summary>
public sealed class ExternalCallLimit
{
    public string Namespace { get; }
    public string Endpoint { get; }
    public int MaximumCalls { get; }
    public TimeSpan Window { get; }
    public int MaximumConcurrency { get; }
    public int FailureThreshold { get; }
    public TimeSpan BreakDuration { get; }
    public string Id { get; }
    public string Fingerprint { get; }
    public ExternalCallLimit(string @namespace, string endpoint, int maximumCalls, TimeSpan window,
        int maximumConcurrency = 8, int failureThreshold = 5, TimeSpan? breakDuration = null)
    {
        Namespace = IdentityRules.Required(@namespace, nameof(@namespace)); Endpoint = IdentityRules.Required(endpoint, nameof(endpoint));
        if (maximumCalls < 1 || maximumConcurrency < 1 || failureThreshold is < 1 or > 1000) throw new ArgumentOutOfRangeException(nameof(maximumCalls));
        BreakDuration = breakDuration ?? TimeSpan.FromSeconds(30);
        if (window < TimeSpan.FromMilliseconds(10) || window > TimeSpan.FromDays(1) || BreakDuration < TimeSpan.FromMilliseconds(10) || BreakDuration > TimeSpan.FromDays(1))
            throw new ArgumentOutOfRangeException(nameof(window));
        MaximumCalls = maximumCalls; MaximumConcurrency = maximumConcurrency; FailureThreshold = failureThreshold;
        Window = TimeSpan.FromMilliseconds((long)window.TotalMilliseconds); BreakDuration = TimeSpan.FromMilliseconds((long)BreakDuration.TotalMilliseconds);
        Id = StableHash.Of("external-limit/v1", Namespace, Endpoint);
        Fingerprint = StableHash.Of("external-limit-content/v1", Id, maximumCalls.ToString(CultureInfo.InvariantCulture), Window.Ticks.ToString(CultureInfo.InvariantCulture),
            maximumConcurrency.ToString(CultureInfo.InvariantCulture), failureThreshold.ToString(CultureInfo.InvariantCulture), BreakDuration.Ticks.ToString(CultureInfo.InvariantCulture));
    }
}
public enum ExternalCallOutcome { Unused, Healthy, Unhealthy }
public sealed record ExternalCallPermit(ExternalCallLimit Limit, Guid Id, DateTimeOffset ExpiresAt);
public sealed record ExternalCallLimitStatus(int Used, int Active, int ConsecutiveFailures, DateTimeOffset WindowStartedAt, DateTimeOffset? OpenUntil);
public interface IExternalCallLimiter
{
    Task<ExternalCallPermit?> TryAcquireCallAsync(ExternalCallLimit limit, Guid requestId, TimeSpan duration, CancellationToken cancellationToken = default);
    Task CompleteCallAsync(ExternalCallPermit permit, ExternalCallOutcome outcome, CancellationToken cancellationToken = default);
    Task<ExternalCallLimitStatus?> ReadCallLimitAsync(ExternalCallLimit limit, CancellationToken cancellationToken = default);
}
