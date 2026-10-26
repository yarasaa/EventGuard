namespace EventGuard;

public enum ExternalActionState { Ready, Sending, Accepted, Rejected, Unknown }
public enum ExternalObservationState { None, Pending, Succeeded, Failed, TimedOut }
public enum ExternalPhase { Submit, Reconcile, Observe }
public enum ExternalResultKind { Accepted, NotAcceptedRetryable, Rejected, Unknown, Pending, Succeeded, Failed }

public sealed class ExternalAction
{
    private readonly byte[] _payload;
    public ProducerScope Scope { get; }
    public Guid Id { get; }
    public string Binding { get; }
    public string BindingVersion { get; }
    public TimeSpan? ObservationTimeout { get; }
    public string Fingerprint { get; }
    public byte[] CopyPayload() => (byte[])_payload.Clone();

    public ExternalAction(ProducerScope scope, Guid id, string binding, string bindingVersion,
        ReadOnlySpan<byte> payload, TimeSpan? observationTimeout = null)
    {
        Scope = scope ?? throw new ArgumentNullException(nameof(scope));
        if (id == Guid.Empty) throw new ArgumentException("A stable external action ID is required.", nameof(id));
        if (payload.Length > 1024 * 1024) throw new ArgumentException("External action exceeds 1 MiB.", nameof(payload));
        if (observationTimeout is { } duration && (duration < TimeSpan.FromSeconds(1) || duration > TimeSpan.FromDays(365)))
            throw new ArgumentOutOfRangeException(nameof(observationTimeout));
        Binding = IdentityRules.Required(binding, nameof(binding)); BindingVersion = IdentityRules.Required(bindingVersion, nameof(bindingVersion));
        Id = id; _payload = payload.ToArray();
        ObservationTimeout = observationTimeout is { } time ? TimeSpan.FromMilliseconds((long)time.TotalMilliseconds) : null;
        Fingerprint = StableHash.Of("external/action/v1", scope.Id, id.ToString("D"), Binding, BindingVersion,
            ContentFingerprint.FromBytes(_payload).Value,
            ObservationTimeout?.Ticks.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "");
    }
}

/// <summary>Adapter evidence, not an inference from HTTP status alone. References and result bytes may be sensitive.</summary>
public sealed class ExternalResult
{
    private readonly byte[] _data;
    public ExternalResultKind Kind { get; }
    public string Reason { get; }
    public string? Reference { get; }
    public byte[] CopyData() => (byte[])_data.Clone();
    public ExternalResult(ExternalResultKind kind, string reason, string? reference = null, ReadOnlySpan<byte> data = default)
    {
        if (!Enum.IsDefined(kind)) throw new ArgumentOutOfRangeException(nameof(kind));
        if (string.IsNullOrEmpty(reason) || reason.Length > 128 || reason.Any(c => !(char.IsAsciiLetterOrDigit(c) || c is '-' or '_' or '.')))
            throw new ArgumentException("Use a short non-sensitive reason code.", nameof(reason));
        if (reference is not null) _ = IdentityRules.Required(reference, nameof(reference));
        if (data.Length > 64 * 1024) throw new ArgumentException("Result exceeds 64 KiB.", nameof(data));
        Kind = kind; Reason = reason; Reference = reference; _data = data.ToArray();
    }
}

public sealed record ExternalActionRecord(ExternalAction Action, ExternalActionState State,
    ExternalObservationState Observation, long Version, int SubmissionAttempts, DateTimeOffset NextAttemptAt,
    DateTimeOffset? ObservationDeadline, string? ProviderReference, string? LastReason, byte[] ResultData);
public sealed record ExternalActionClaim(ExternalActionRecord Record, ExternalPhase Phase, Guid Token, DateTimeOffset LeaseUntil);
public sealed record ExternalAttemptEvidence(Guid ClaimId, ExternalPhase Phase, DateTimeOffset StartedAt, DateTimeOffset? FinishedAt, ExternalResult? Result, bool Applied);
public sealed record ExternalSettlement(bool Recorded, ExternalActionRecord Record);

public sealed class ExternalActionConflictException(Guid id)
    : InvalidOperationException($"External action {id} already has different immutable content or origin.");

public interface IExternalActionWriter
{
    Task<OutboxEnqueueStatus> EnqueueAsync(ExternalAction action, CancellationToken cancellationToken = default);
}
public interface IExternalActionSession { IExternalActionWriter ExternalActions { get; } }

public interface IExternalActionStore
{
    Task ValidateExternalActionsAsync(CancellationToken cancellationToken = default);
    Task<IReadOnlyList<ExternalAttemptEvidence>> ReadActionEvidenceAsync(ProducerScope scope, Guid actionId,
        int limit = 100, CancellationToken cancellationToken = default);
    Task<ExternalActionRecord?> FindActionAsync(ProducerScope scope, Guid id, CancellationToken cancellationToken = default);
    Task<ExternalActionClaim?> ClaimActionAsync(ProducerScope scope, string binding, string bindingVersion,
        ExternalPhase phase, Guid requestId, TimeSpan leaseDuration, CancellationToken cancellationToken = default);
    Task<ExternalSettlement> SettleActionAsync(ExternalActionClaim claim, ExternalResult result,
        ExternalActionPolicy policy, CancellationToken cancellationToken = default);
}

public interface IExternalServiceAdapter
{
    string Binding { get; }
    string BindingVersion { get; }
    bool CanReconcile { get; }
    bool CanObserve { get; }
    // The stable Action.Id must be retained as the provider request key when supported.
    Task<ExternalResult> SubmitAsync(ExternalAction action, CancellationToken cancellationToken);
    Task<ExternalResult> ReconcileAsync(ExternalActionRecord action, CancellationToken cancellationToken);
    Task<ExternalResult> ObserveAsync(ExternalActionRecord action, CancellationToken cancellationToken);
}
