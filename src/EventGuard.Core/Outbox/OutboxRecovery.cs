using System.Globalization;

namespace EventGuard;

// Metadata is readable even when the envelope cannot be decoded. It contains no payload or headers.
public sealed record OutboxMetadata(Guid IntentId, string ScopeId, OutboxState State, long Version,
    int AttemptCount, int GenerationAttempt, int ReplayGeneration, bool DeliveryUncertain,
    string? LastReason, Guid? QuarantineId);

public enum OutboxReplayStatus { Accepted, NotFound, NotFailed, VersionConflict, DeliveryUncertain, Quarantined, RequestConflict, RateLimited }
public sealed record OutboxReplayResult(OutboxReplayStatus Status, Guid IntentId, int Generation, long Version, bool RepeatedRequest = false);
public sealed record OutboxReplayDecision(OutboxReplayResult Result, OutboxMetadata? After);

public sealed class OutboxReplayRequest
{
    public ProducerScope Scope { get; }
    public Guid RequestId { get; }
    public Guid IntentId { get; }
    public long ExpectedVersion { get; }
    public string Actor { get; }
    public string Reason { get; }
    public bool AcknowledgePossibleDuplicate { get; }
    public string Fingerprint { get; }

    public OutboxReplayRequest(ProducerScope scope, Guid requestId, Guid intentId, long expectedVersion,
        string actor, string reason, bool acknowledgePossibleDuplicate = false)
    {
        Scope = scope ?? throw new ArgumentNullException(nameof(scope));
        if (requestId == Guid.Empty || intentId == Guid.Empty) throw new ArgumentException("Stable replay request and intent IDs are required.");
        if (expectedVersion < 0) throw new ArgumentOutOfRangeException(nameof(expectedVersion));
        RequestId = requestId;
        IntentId = intentId;
        ExpectedVersion = expectedVersion;
        Actor = IdentityRules.Required(actor, nameof(actor));
        Reason = IdentityRules.Required(reason, nameof(reason));
        AcknowledgePossibleDuplicate = acknowledgePossibleDuplicate;
        Fingerprint = StableHash.Of("outbox-replay/v1", scope.Id, intentId.ToString("D"),
            expectedVersion.ToString(CultureInfo.InvariantCulture), Actor, Reason, acknowledgePossibleDuplicate ? "1" : "0");
    }
}

public interface IOutboxRecoveryStore
{
    Task<OutboxMetadata?> FindMetadataAsync(ProducerScope scope, Guid intentId, CancellationToken cancellationToken = default);
    Task<OutboxReplayResult> ReplayAsync(OutboxReplayRequest request, CancellationToken cancellationToken = default);
}

public sealed class OutboxEnvelopeException(Guid intentId, string reasonCode) : InvalidOperationException(
    $"Outbox intent {intentId} cannot be decoded. Read recovery metadata for its status.")
{
    public Guid IntentId { get; } = intentId;
    public string ReasonCode { get; } = reasonCode;
}

public static partial class OutboxRecovery
{
    public static OutboxReplayDecision Replay(OutboxReplayRequest request, OutboxMetadata? current)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (current is not null && (current.ScopeId != request.Scope.Id || current.IntentId != request.IntentId)) current = null;
        if (current is not null) Validate(current);
        var status = current switch
        {
            null => OutboxReplayStatus.NotFound,
            { State: OutboxState.Quarantined } => OutboxReplayStatus.Quarantined,
            { State: not OutboxState.DeadLetter } => OutboxReplayStatus.NotFailed,
            _ when current.Version != request.ExpectedVersion => OutboxReplayStatus.VersionConflict,
            _ when current.DeliveryUncertain && !request.AcknowledgePossibleDuplicate => OutboxReplayStatus.DeliveryUncertain,
            _ => OutboxReplayStatus.Accepted
        };
        if (status != OutboxReplayStatus.Accepted)
            return new(new(status, request.IntentId, current?.ReplayGeneration ?? 0, current?.Version ?? 0), null);
        var after = current! with
        {
            State = OutboxState.Pending,
            Version = checked(current.Version + 1),
            GenerationAttempt = 0,
            ReplayGeneration = checked(current.ReplayGeneration + 1),
            LastReason = "outbox-replay-requested"
        };
        return new(new(status, request.IntentId, after.ReplayGeneration, after.Version), after);
    }

    public static OutboxReplayResult RepeatReplay(OutboxReplayRequest request, string fingerprint, OutboxReplayResult result)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(result);
        if (request.Fingerprint != fingerprint) return new(OutboxReplayStatus.RequestConflict, request.IntentId, 0, 0);
        if (result.IntentId != request.IntentId) throw new InvalidOperationException("Outbox replay receipt is inconsistent.");
        return result with { RepeatedRequest = true };
    }

    // Provider locks and snapshots the raw row before persisting this state change.
    public static OutboxMetadata Quarantine(OutboxMetadata before, Guid quarantineId, string reasonCode)
    {
        Validate(before);
        if (quarantineId == Guid.Empty) throw new ArgumentException("A quarantine ID is required.", nameof(quarantineId));
        _ = new PublishOutcome(PublishStatus.NotSentPermanent, reasonCode);
        if (before.State is OutboxState.Published or OutboxState.Quarantined)
            throw new InvalidOperationException("Terminal publication cannot be quarantined again.");
        return before with
        {
            State = OutboxState.Quarantined,
            Version = checked(before.Version + 1),
            DeliveryUncertain = before.DeliveryUncertain || before.State == OutboxState.Publishing,
            LastReason = reasonCode,
            QuarantineId = quarantineId
        };
    }

    private static void Validate(OutboxMetadata metadata)
    {
        ArgumentNullException.ThrowIfNull(metadata);
        if (metadata.IntentId == Guid.Empty || string.IsNullOrEmpty(metadata.ScopeId) || !Enum.IsDefined(metadata.State)
            || metadata.Version < 0 || metadata.AttemptCount < 0 || metadata.GenerationAttempt < 0
            || metadata.GenerationAttempt > metadata.AttemptCount || metadata.ReplayGeneration < 0
            || (metadata.State == OutboxState.Quarantined) != (metadata.QuarantineId is not null))
            throw new InvalidOperationException("Invalid outbox recovery metadata.");
    }
}
