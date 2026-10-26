using System.Globalization;

namespace EventGuard;

public enum RetentionTarget { ConsumerPayload, PublishedOutboxPayload, ConflictPayload, ReleasedIngressData, PublishedOutboxQuarantineData }
public enum RetentionStatus { Completed, RequestConflict }
public sealed record RetentionResult(RetentionStatus Status, IReadOnlyList<Guid> PurgedIds, bool RepeatedRequest = false);
public sealed record RetentionCandidate(Guid Id, bool AlreadyPurged, WorkState? WorkState, OutboxState? OutboxState,
    DateTimeOffset? TerminalAt, DateTimeOffset? DataCreatedAt = null);

/// <summary>A bounded, auditable data-removal command. Identity and durable request tombstones are never removed.</summary>
public sealed class RetentionRequest
{
    public Guid RequestId { get; }
    public string ScopeId { get; }
    public RetentionTarget Target { get; }
    public DateTimeOffset Before { get; }
    public TimeSpan MinimumAge { get; }
    public int BatchSize { get; }
    public string Actor { get; }
    public string Reason { get; }
    public string Fingerprint { get; }
    public RetentionRequest(WorkScope scope, Guid requestId, RetentionTarget target, DateTimeOffset before, string actor, string reason,
        int batchSize = 100, TimeSpan? minimumAge = null)
        : this((scope ?? throw new ArgumentNullException(nameof(scope))).Id, false, requestId, target, before, actor, reason, batchSize, minimumAge) { }
    public RetentionRequest(ProducerScope scope, Guid requestId, RetentionTarget target, DateTimeOffset before, string actor, string reason,
        int batchSize = 100, TimeSpan? minimumAge = null)
        : this((scope ?? throw new ArgumentNullException(nameof(scope))).Id, true, requestId, target, before, actor, reason, batchSize, minimumAge) { }
    private RetentionRequest(string scopeId, bool producer, Guid requestId, RetentionTarget target, DateTimeOffset before, string actor, string reason,
        int batchSize, TimeSpan? minimumAge)
    {
        if (requestId == Guid.Empty || !Enum.IsDefined(target) || batchSize is < 1 or > 1000)
            throw new ArgumentException("Specify a stable request ID, known target and 1–1000 records.");
        if (producer != (target is RetentionTarget.PublishedOutboxPayload or RetentionTarget.PublishedOutboxQuarantineData))
            throw new ArgumentException("Retention target and scope type must match.", nameof(target));
        MinimumAge = minimumAge ?? TimeSpan.FromDays(7);
        if (MinimumAge < TimeSpan.FromHours(1) || MinimumAge > TimeSpan.FromDays(3650)) throw new ArgumentOutOfRangeException(nameof(minimumAge));
        ScopeId = scopeId; RequestId = requestId; Target = target; Before = before.ToUniversalTime(); BatchSize = batchSize;
        Actor = IdentityRules.Required(actor, nameof(actor)); Reason = IdentityRules.Required(reason, nameof(reason));
        Fingerprint = StableHash.Of("retention/v1", scopeId, target.ToString(), Before.ToString("O", CultureInfo.InvariantCulture),
            MinimumAge.Ticks.ToString(CultureInfo.InvariantCulture), batchSize.ToString(CultureInfo.InvariantCulture), Actor, Reason);
    }
}

public interface IRetentionStore
{
    Task<RetentionResult> RetainAsync(RetentionRequest request, CancellationToken cancellationToken = default);
}

public sealed class PayloadUnavailableException(string kind, Guid id) : InvalidOperationException("Payload was removed by an explicit retention operation. Metadata remains available.")
{
    public string Kind { get; } = kind;
    public Guid Id { get; } = id;
}

public static class Retention
{
    public static void ValidateWindow(RetentionRequest request, DateTimeOffset databaseNow)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.Before > databaseNow - request.MinimumAge)
            throw new ArgumentException("Retention cutoff is newer than the configured minimum age.", nameof(request));
    }
    public static bool CanPurge(RetentionRequest request, RetentionCandidate candidate, DateTimeOffset databaseNow)
    {
        ValidateWindow(request, databaseNow);
        if (candidate.AlreadyPurged || candidate.TerminalAt is null || candidate.TerminalAt > request.Before
            || candidate.DataCreatedAt > request.Before) return false;
        return request.Target switch
        {
            RetentionTarget.ConsumerPayload or RetentionTarget.ConflictPayload => candidate.WorkState is WorkState.Completed or WorkState.Expired or WorkState.DeadLetter,
            RetentionTarget.PublishedOutboxPayload or RetentionTarget.PublishedOutboxQuarantineData => candidate.OutboxState == OutboxState.Published,
            RetentionTarget.ReleasedIngressData => true, // TerminalAt is the locked row's actual release time, never creation time.
            _ => false
        };
    }
    public static RetentionResult Repeat(RetentionRequest request, string fingerprint, IReadOnlyList<Guid> purgedIds) =>
        request.Fingerprint == fingerprint ? new(RetentionStatus.Completed, purgedIds, true) : new(RetentionStatus.RequestConflict, []);
}
