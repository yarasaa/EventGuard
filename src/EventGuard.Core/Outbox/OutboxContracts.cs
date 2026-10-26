namespace EventGuard;

public enum ProducerStatus { Committed, AlreadyCommitted, RequestConflict }
public enum OutboxEnqueueStatus { Enqueued, AlreadyEnqueued }
public enum OutboxState { Pending, Publishing, RetryScheduled, Published, DeadLetter, Quarantined }
public enum PublishStatus { Accepted, NotSentTransient, NotSentPermanent, Unknown }
public enum OutboxSettlementStatus { Recorded, LeaseLost }

public sealed class ProducerRequest
{
    public ProducerScope Scope { get; }
    public Guid RequestId { get; }
    public ContentFingerprint Fingerprint { get; }

    public ProducerRequest(ProducerScope scope, Guid requestId, ContentFingerprint fingerprint)
    {
        Scope = scope ?? throw new ArgumentNullException(nameof(scope));
        if (requestId == Guid.Empty) throw new ArgumentException("A stable producer request ID is required.", nameof(requestId));
        RequestId = requestId;
        Fingerprint = fingerprint ?? throw new ArgumentNullException(nameof(fingerprint));
    }
}

public sealed record ProducerResult(ProducerStatus Status, Guid RequestId);
public sealed record OutboxRecord(OutgoingMessage Message, OutboxState State, long Version, int AttemptCount,
    DateTimeOffset NextAttemptAt, bool DeliveryUncertain, string? LastReason, Guid? OriginWorkId, Guid? OriginRequestId)
{
    public int GenerationAttempt { get; init; } = AttemptCount;
    public int ReplayGeneration { get; init; }
}
public sealed record OutboxClaim(OutboxRecord Record, Guid Token, DateTimeOffset LeaseUntil);
public sealed record OutboxSettlement(OutboxSettlementStatus Status, OutboxRecord Record);

// Accepted means the configured transport's documented broker acceptance, not consumer completion.
public sealed class PublishOutcome
{
    public PublishStatus Status { get; }
    public string ReasonCode { get; }

    public PublishOutcome(PublishStatus status, string reasonCode)
    {
        if (!Enum.IsDefined(status)) throw new ArgumentOutOfRangeException(nameof(status));
        if (string.IsNullOrEmpty(reasonCode) || reasonCode.Length > 128
            || reasonCode.Any(c => !(char.IsAsciiLetterOrDigit(c) || c is '-' or '_' or '.')))
            throw new ArgumentException("Use a short non-sensitive ASCII reason code.", nameof(reasonCode));
        Status = status;
        ReasonCode = reasonCode;
    }
}

public interface IOutboxWriter
{
    Task<OutboxEnqueueStatus> EnqueueAsync(OutgoingMessage message, CancellationToken cancellationToken = default);
}

public interface ITransactionalProducer<TSession>
{
    Task<ProducerResult> ProduceAsync(ProducerRequest request,
        Func<TSession, CancellationToken, ValueTask> handler, CancellationToken cancellationToken = default);
}

public interface IOutboxStore
{
    Task<OutboxRecord?> FindAsync(ProducerScope scope, Guid intentId, CancellationToken cancellationToken = default);
    Task<OutboxClaim?> ClaimNextAsync(ProducerScope scope, Guid requestId, TimeSpan leaseDuration,
        CancellationToken cancellationToken = default);
    Task<OutboxSettlement> SettleAsync(OutboxClaim claim, PublishOutcome outcome, RetryPolicy retryPolicy,
        CancellationToken cancellationToken = default);
}

public interface IOutboundTransport
{
    string Transport { get; }
    string Connection { get; }
    string ConfigurationVersion { get; }
    Task<PublishOutcome> PublishAsync(OutgoingMessage message, CancellationToken cancellationToken);
}

public sealed class OutboxConflictException(Guid intentId) : InvalidOperationException(
    $"Outbox intent {intentId} already exists with different immutable content or origin.");
