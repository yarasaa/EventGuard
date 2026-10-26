using System.Globalization;

namespace EventGuard;

public enum JobSubmissionStatus { Accepted, AlreadyAccepted, RequestConflict, BusinessConflict }
public enum JobRegistrationStatus { Registered, AlreadyRegistered }

public sealed class JobDefinitionConflictException(string definitionId)
    : InvalidOperationException($"Job definition {definitionId} is already registered with different semantics. Use a new version.");

public sealed class JobSubmission
{
    private readonly byte[] _parameters;
    public JobDefinition Definition { get; }
    public Guid RequestId { get; }
    public string BusinessKey { get; }
    public string Actor { get; }
    public DateTimeOffset? NotBefore { get; }
    public DateTimeOffset? ExpiresAt { get; }
    public string? CorrelationId { get; }
    public string BusinessIdentity { get; }
    public string OperationFingerprint { get; }
    public string RequestFingerprint { get; }
    public byte[] CopyParameters() => (byte[])_parameters.Clone();

    public JobSubmission(JobDefinition definition, Guid requestId, string businessKey, string actor,
        ReadOnlySpan<byte> parameters, DateTimeOffset? notBefore = null, DateTimeOffset? expiresAt = null,
        string? correlationId = null)
    {
        Definition = definition ?? throw new ArgumentNullException(nameof(definition));
        if (requestId == Guid.Empty) throw new ArgumentException("A stable request ID is required.", nameof(requestId));
        if (parameters.Length > 1024 * 1024) throw new ArgumentException("Parameters exceed 1 MiB.", nameof(parameters));
        BusinessKey = IdentityRules.Required(businessKey, nameof(businessKey));
        Actor = IdentityRules.Required(actor, nameof(actor));
        if (correlationId?.Length > 256) throw new ArgumentException("Correlation ID exceeds 256 characters.", nameof(correlationId));
        NotBefore = Normalize(notBefore); ExpiresAt = Normalize(expiresAt);
        if (NotBefore.HasValue && ExpiresAt.HasValue && ExpiresAt <= NotBefore)
            throw new ArgumentException("Expiry must be after the scheduled time.", nameof(expiresAt));
        RequestId = requestId; CorrelationId = correlationId; _parameters = parameters.ToArray();
        // Definition version and transport never create a new business intent implicitly.
        BusinessIdentity = StableHash.Of("jobs/business/v1", definition.Scope.Id, definition.Name, BusinessKey);
        OperationFingerprint = StableHash.Of("jobs/operation/v1", BusinessIdentity, definition.Fingerprint,
            ContentFingerprint.FromBytes(_parameters).Value, Time(NotBefore), Time(ExpiresAt), correlationId ?? "");
        RequestFingerprint = StableHash.Of("jobs/request/v1", OperationFingerprint, Actor);
    }

    public WorkSubmission ToWorkSubmission() => new(new(Definition.WorkScope, Definition.IdentityPolicy, BusinessIdentity),
        RequestId, _parameters, ContentFingerprint.Parse(OperationFingerprint), NotBefore, ExpiresAt, CorrelationId);

    private static DateTimeOffset? Normalize(DateTimeOffset? value) => value is { } date
        ? new DateTimeOffset(date.UtcTicks - date.UtcTicks % 10, TimeSpan.Zero) : null;
    private static string Time(DateTimeOffset? value) => value?.UtcTicks.ToString(CultureInfo.InvariantCulture) ?? "";
}

public sealed record JobOperation(Guid Id, JobScope Scope, string DefinitionName, int DefinitionVersion,
    string DefinitionFingerprint, string BusinessKey, string OperationFingerprint, string WorkScopeId,
    WorkState State, long WorkVersion, int AttemptCount, DateTimeOffset NextAttemptAt, string? LastReason,
    DateTimeOffset CreatedAt);

/// <summary>Conflict responses never authorize execution. Repeating a saved refusal returns that refusal.</summary>
public sealed record JobSubmissionResult(JobSubmissionStatus Status, Guid RequestId, Guid OperationId, bool RepeatedRequest = false);

public interface IJobStore
{
    Task<JobRegistrationStatus> RegisterAsync(JobDefinition definition, CancellationToken cancellationToken = default);
    Task ValidateAsync(JobDefinition definition, CancellationToken cancellationToken = default);
    Task<JobSubmissionResult> SubmitAsync(JobSubmission submission, CancellationToken cancellationToken = default);
    Task<JobSubmissionResult?> FindRequestAsync(JobScope scope, Guid requestId, CancellationToken cancellationToken = default);
    Task<JobOperation?> FindAsync(JobScope scope, Guid operationId, CancellationToken cancellationToken = default);
}

/// <summary>Jobs and their executable work must share the same native database/schema.</summary>
public interface ITransactionalJobStore<TSession> : IJobStore, ITransactionalWorkStore<TSession> { }
