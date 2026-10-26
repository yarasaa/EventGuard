namespace EventGuard;

public enum WorkState { Pending, Processing, RetryScheduled, Completed, DeadLetter, Expired }
public enum AcceptanceStatus { Accepted, Duplicate, Conflict }
public enum ExecutionStatus { Completed, RetryScheduled, DeadLetter, Expired, LeaseLost }
public enum ReplayStatus { Accepted, NotFound, NotFailed, VersionConflict, PayloadExpired, RequestConflict, RateLimited }

public sealed class WorkSubmission
{
    private readonly byte[] _payload;
    public WorkIdentity Identity { get; }
    public Guid MessageId { get; }
    public ContentFingerprint Fingerprint { get; }
    public DateTimeOffset? NotBefore { get; }
    public DateTimeOffset? ExpiresAt { get; }
    public string? CorrelationId { get; }
    public byte[] CopyPayload() => (byte[])_payload.Clone();

    public WorkSubmission(WorkIdentity identity, Guid messageId, ReadOnlySpan<byte> payload,
        ContentFingerprint fingerprint, DateTimeOffset? notBefore = null,
        DateTimeOffset? expiresAt = null, string? correlationId = null)
    {
        Identity = identity ?? throw new ArgumentNullException(nameof(identity));
        Fingerprint = fingerprint ?? throw new ArgumentNullException(nameof(fingerprint));
        if (messageId == Guid.Empty) throw new ArgumentException("A stable message id is required.", nameof(messageId));
        if (payload.Length > 1024 * 1024) throw new ArgumentException("Inline payload limit is 1 MiB.", nameof(payload));
        if (notBefore.HasValue && expiresAt.HasValue && expiresAt <= notBefore)
            throw new ArgumentException("Expiry must be after the earliest execution time.", nameof(expiresAt));
        if (correlationId?.Length > 256) throw new ArgumentException("Correlation id exceeds 256 characters.", nameof(correlationId));
        MessageId = messageId;
        _payload = payload.ToArray();
        NotBefore = notBefore?.ToUniversalTime();
        ExpiresAt = expiresAt?.ToUniversalTime();
        CorrelationId = correlationId;
    }
}

public sealed record WorkItem(
    Guid Id, string IdentityId, string ScopeId, Guid MessageId, ContentFingerprint Fingerprint,
    WorkState State, long Version, int AttemptCount, int GenerationAttempt,
    int ReplayGeneration, DateTimeOffset NextAttemptAt, DateTimeOffset? ExpiresAt,
    string? LastReason, string? CorrelationId);

public sealed record AcceptanceResult(AcceptanceStatus Status, WorkItem Work);
public sealed record WorkClaim(WorkItem Work, Guid Token, DateTimeOffset LeaseUntil, byte[] Payload);
public sealed record ExecutionResult(ExecutionStatus Status, WorkItem Work);
public sealed record ReplayResult(ReplayStatus Status, Guid WorkId, int Generation, long Version, bool RepeatedRequest = false);

public sealed record ReplayRequest
{
    public WorkScope Scope { get; }
    public Guid RequestId { get; }
    public Guid WorkId { get; }
    public long ExpectedVersion { get; }
    public string Actor { get; }
    public string Reason { get; }
    public string Fingerprint { get; }

    public ReplayRequest(WorkScope scope, Guid requestId, Guid workId, long expectedVersion, string actor, string reason)
    {
        Scope = scope ?? throw new ArgumentNullException(nameof(scope));
        if (requestId == Guid.Empty || workId == Guid.Empty) throw new ArgumentException("Stable request and work IDs are required.");
        if (expectedVersion < 0) throw new ArgumentOutOfRangeException(nameof(expectedVersion));
        Actor = IdentityRules.Required(actor, nameof(actor));
        Reason = IdentityRules.Required(reason, nameof(reason));
        RequestId = requestId;
        WorkId = workId;
        ExpectedVersion = expectedVersion;
        Fingerprint = StableHash.Of("replay/v1", scope.Id, workId.ToString("D"),
            expectedVersion.ToString(System.Globalization.CultureInfo.InvariantCulture), Actor, Reason);
    }
}
