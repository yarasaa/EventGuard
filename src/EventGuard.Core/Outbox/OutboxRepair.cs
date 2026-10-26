using System.Globalization;

namespace EventGuard;

public enum OutboxRepairStatus { Accepted, NotFound, NotQuarantined, VersionConflict, EnvelopeMismatch, DeliveryUncertain, RequestConflict, RateLimited }
public sealed record OutboxRepairResult(OutboxRepairStatus Status, Guid IntentId, int Generation, long Version, bool RepeatedRequest = false);
public sealed record OutboxRepairDecision(OutboxRepairResult Result, OutboxMetadata? After);

/// <summary>Restores the exact original immutable envelope; it cannot authorize a new payload, route or identity.</summary>
public sealed class OutboxRepairRequest
{
    public Guid RequestId { get; }
    public Guid QuarantineId { get; }
    public OutgoingMessage Message { get; }
    public long ExpectedVersion { get; }
    public string Actor { get; }
    public string Reason { get; }
    public bool AcknowledgePossibleDuplicate { get; }
    public string Fingerprint { get; }
    public OutboxRepairRequest(Guid requestId, Guid quarantineId, OutgoingMessage message, long expectedVersion,
        string actor, string reason, bool acknowledgePossibleDuplicate = false)
    {
        Message = message ?? throw new ArgumentNullException(nameof(message));
        if (requestId == Guid.Empty || quarantineId == Guid.Empty || expectedVersion < 0)
            throw new ArgumentException("Stable request/quarantine IDs and a nonnegative version are required.");
        RequestId = requestId; QuarantineId = quarantineId; ExpectedVersion = expectedVersion;
        Actor = IdentityRules.Required(actor, nameof(actor)); Reason = IdentityRules.Required(reason, nameof(reason));
        AcknowledgePossibleDuplicate = acknowledgePossibleDuplicate;
        Fingerprint = StableHash.Of("outbox-repair/v1", quarantineId.ToString("D"), message.Fingerprint,
            expectedVersion.ToString(CultureInfo.InvariantCulture), Actor, Reason, acknowledgePossibleDuplicate ? "1" : "0");
    }
}

public interface IOutboxRepairStore
{
    Task<OutboxRepairResult> RepairAsync(OutboxRepairRequest request, CancellationToken cancellationToken = default);
}

public static partial class OutboxRecovery
{
    public static string OriginFingerprint(OutgoingMessage message, Guid? work, Guid? request) =>
        StableHash.Of("outbox-origin/v1", message.Fingerprint, work?.ToString("D") ?? "", request?.ToString("D") ?? "");

    public static OutboxRepairDecision Repair(OutboxRepairRequest request, OutboxMetadata? current,
        string? originalFingerprint, Guid? originWorkId, Guid? originRequestId)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (current is not null && (current.ScopeId != request.Message.Scope.Id || current.IntentId != request.Message.IntentId)) current = null;
        if (current is not null) Validate(current);
        var status = current switch
        {
            null => OutboxRepairStatus.NotFound,
            { State: not OutboxState.Quarantined } => OutboxRepairStatus.NotQuarantined,
            _ when current.Version != request.ExpectedVersion || current.QuarantineId != request.QuarantineId => OutboxRepairStatus.VersionConflict,
            _ when originalFingerprint != OriginFingerprint(request.Message, originWorkId, originRequestId) => OutboxRepairStatus.EnvelopeMismatch,
            _ when current.DeliveryUncertain && !request.AcknowledgePossibleDuplicate => OutboxRepairStatus.DeliveryUncertain,
            _ => OutboxRepairStatus.Accepted
        };
        if (status != OutboxRepairStatus.Accepted)
            return new(new(status, request.Message.IntentId, current?.ReplayGeneration ?? 0, current?.Version ?? 0), null);
        var after = current! with { State = OutboxState.Pending, Version = checked(current.Version + 1),
            GenerationAttempt = 0, ReplayGeneration = checked(current.ReplayGeneration + 1),
            QuarantineId = null, LastReason = "outbox-envelope-restored" };
        return new(new(status, after.IntentId, after.ReplayGeneration, after.Version), after);
    }

    public static OutboxRepairResult RepeatRepair(OutboxRepairRequest request, string fingerprint, OutboxRepairResult previous) =>
        request.Fingerprint == fingerprint ? previous with { RepeatedRequest = true }
            : new(OutboxRepairStatus.RequestConflict, request.Message.IntentId, 0, 0);
}
