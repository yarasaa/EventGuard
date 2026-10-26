using System.Globalization;

namespace EventGuard;

/// <summary>Only this explicit permanent mapper rejection is eligible for opt-in quarantine.</summary>
public sealed class IngressMessageRejectedException : InvalidOperationException
{
    public string ReasonCode { get; }
    public IngressMessageRejectedException(string reasonCode) : base("Ingress mapping rejected the message.")
    {
        if (string.IsNullOrEmpty(reasonCode) || reasonCode.Length > 64
            || reasonCode.Any(c => !char.IsAsciiLetterOrDigit(c) && c != '-'))
            throw new ArgumentException("Use a bounded, non-sensitive reason code.", nameof(reasonCode));
        ReasonCode = reasonCode;
    }
}

/// <summary>Explicit permission to settle permanently rejected deliveries after durable quarantine.</summary>
public sealed class IngressQuarantineOptions
{
    public string SourceBinding { get; }
    public IngressQuarantineOptions(string sourceBinding) => SourceBinding = IdentityRules.Required(sourceBinding, nameof(sourceBinding));
}

/// <summary>Immutable bounded raw input. Metadata uses the named provider codec; never log it.</summary>
public sealed class IngressSnapshot
{
    private readonly byte[] _body;
    private readonly byte[] _metadata;
    public string SourceBinding { get; }
    public string SourcePosition { get; }
    public string Codec { get; }
    public bool IsNullBody { get; }
    public string Fingerprint { get; }
    public byte[] CopyBody() => (byte[])_body.Clone();
    public byte[] CopyMetadata() => (byte[])_metadata.Clone();
    public IngressSnapshot(string sourceBinding, string sourcePosition, string codec, ReadOnlySpan<byte> body,
        ReadOnlySpan<byte> metadata, bool isNullBody = false)
    {
        SourceBinding = IdentityRules.Required(sourceBinding, nameof(sourceBinding));
        SourcePosition = IdentityRules.Required(sourcePosition, nameof(sourcePosition));
        Codec = IdentityRules.Required(codec, nameof(codec));
        if (body.Length > 1024 * 1024 || metadata.Length > 64 * 1024)
            throw new ArgumentException("Quarantine limits are 1 MiB body and 64 KiB metadata; input is never truncated.");
        if (isNullBody && !body.IsEmpty) throw new ArgumentException("A null body cannot have bytes.", nameof(body));
        _body = body.ToArray(); _metadata = metadata.ToArray(); IsNullBody = isNullBody;
        Fingerprint = StableHash.Of("ingress-snapshot/v1", SourceBinding, SourcePosition, Codec,
            isNullBody ? "null" : "bytes", ContentFingerprint.FromBytes(body).Value, ContentFingerprint.FromBytes(metadata).Value);
    }
    public string Identity(WorkScope scope) => StableHash.Of("ingress-quarantine/v1", scope.Id, SourceBinding, SourcePosition);
}

public enum IngressQuarantineStatus { Recorded, Duplicate, Conflict }
public enum IngressReleaseStatus { Accepted, Duplicate, NotFound, AlreadyReleased, VersionConflict, WorkConflict, RequestConflict, RateLimited }
public sealed record IngressQuarantineItem(Guid Id, string ScopeId, string IdentityId, string Fingerprint,
    string KeyVersion, string FingerprintVersion, string ReasonCode, long Version, Guid? ReleasedWorkId)
{
    public bool DataRetained { get; init; } = true;
}
public sealed record IngressQuarantineResult(IngressQuarantineStatus Status, IngressQuarantineItem Item);
public sealed record IngressReleaseResult(IngressReleaseStatus Status, Guid QuarantineId, Guid? WorkId, long Version, bool RepeatedRequest = false);

public sealed class IngressReleaseRequest
{
    public Guid RequestId { get; }
    public Guid QuarantineId { get; }
    public long ExpectedVersion { get; }
    public WorkSubmission Submission { get; }
    public string Actor { get; }
    public string Reason { get; }
    public string Fingerprint { get; }
    public IngressReleaseRequest(Guid requestId, Guid quarantineId, long expectedVersion,
        WorkSubmission submission, string actor, string reason)
    {
        if (requestId == Guid.Empty || quarantineId == Guid.Empty || expectedVersion < 0)
            throw new ArgumentException("Stable request/quarantine IDs and a nonnegative version are required.");
        Submission = submission ?? throw new ArgumentNullException(nameof(submission));
        RequestId = requestId; QuarantineId = quarantineId; ExpectedVersion = expectedVersion;
        Actor = IdentityRules.Required(actor, nameof(actor)); Reason = IdentityRules.Required(reason, nameof(reason));
        // Include raw replacement bytes as well as the application's canonical fingerprint.
        Fingerprint = StableHash.Of("ingress-release/v1", quarantineId.ToString("D"), expectedVersion.ToString(CultureInfo.InvariantCulture),
            submission.Identity.Id, submission.Identity.Policy.KeyVersion, submission.Identity.Policy.FingerprintVersion,
            submission.MessageId.ToString("D"), submission.Fingerprint.Value, ContentFingerprint.FromBytes(submission.CopyPayload()).Value,
            submission.NotBefore?.ToString("O", CultureInfo.InvariantCulture) ?? "", submission.ExpiresAt?.ToString("O", CultureInfo.InvariantCulture) ?? "",
            submission.CorrelationId ?? "", Actor, Reason);
    }
}

public interface IIngressQuarantineStore
{
    Task<IngressQuarantineResult> QuarantineAsync(WorkScope scope, IdentityPolicy policy, IngressSnapshot snapshot,
        string reasonCode, CancellationToken cancellationToken = default);
    Task<IngressQuarantineItem?> FindQuarantineAsync(WorkScope scope, Guid id, CancellationToken cancellationToken = default);
    // Raw access is deliberately separate from metadata and must be explicitly authorized by the host.
    Task<IngressSnapshot?> ReadQuarantineDataAsync(WorkScope scope, Guid id, CancellationToken cancellationToken = default);
    Task<IngressReleaseResult> ReleaseAsync(IngressReleaseRequest request, CancellationToken cancellationToken = default);
}

public static class IngressRecovery
{
    public static IngressQuarantineStatus Accept(WorkScope scope, IdentityPolicy policy, IngressSnapshot snapshot, IngressQuarantineItem? current)
    {
        if (current is null) return IngressQuarantineStatus.Recorded;
        if (current.ScopeId != scope.Id || current.IdentityId != snapshot.Identity(scope))
            throw new InvalidOperationException("Quarantine identity is inconsistent.");
        if (current.KeyVersion != policy.KeyVersion || current.FingerprintVersion != policy.FingerprintVersion)
            throw new PolicyMismatchException(scope.Id);
        return current.Fingerprint == snapshot.Fingerprint ? IngressQuarantineStatus.Duplicate : IngressQuarantineStatus.Conflict;
    }
    public static IngressReleaseStatus? AdmitRelease(IngressReleaseRequest request, IngressQuarantineItem? current)
    {
        if (current is null || current.ScopeId != request.Submission.Identity.Scope.Id || current.Id != request.QuarantineId)
            return IngressReleaseStatus.NotFound;
        if (current.ReleasedWorkId.HasValue) return IngressReleaseStatus.AlreadyReleased;
        if (current.Version != request.ExpectedVersion) return IngressReleaseStatus.VersionConflict;
        if (current.KeyVersion != request.Submission.Identity.Policy.KeyVersion
            || current.FingerprintVersion != request.Submission.Identity.Policy.FingerprintVersion)
            throw new PolicyMismatchException(current.ScopeId);
        return null;
    }
    public static IngressReleaseResult Repeat(IngressReleaseRequest request, string fingerprint, IngressReleaseResult previous) =>
        request.Fingerprint == fingerprint ? previous with { RepeatedRequest = true }
            : new(IngressReleaseStatus.RequestConflict, request.QuarantineId, null, 0);
}
