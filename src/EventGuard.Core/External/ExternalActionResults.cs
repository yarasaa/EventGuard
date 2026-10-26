namespace EventGuard;

/// <summary>An immutable, opt-in destination for an action's durable status projections.</summary>
public sealed class ExternalResultTarget
{
    public ProducerScope Scope { get; }
    public string Name { get; }
    public string HandlerVersion { get; }
    public string Id { get; }
    public WorkScope WorkScope { get; }
    public IdentityPolicy IdentityPolicy { get; } = new("external-result-key/v1", "external-result-content/v1");

    public ExternalResultTarget(ProducerScope scope, string name, string handlerVersion)
    {
        Scope = scope ?? throw new ArgumentNullException(nameof(scope));
        Name = IdentityRules.Required(name, nameof(name));
        HandlerVersion = IdentityRules.Required(handlerVersion, nameof(handlerVersion));
        Id = StableHash.Of("external-result-target/v1", scope.Id, Name, HandlerVersion);
        WorkScope = new(scope.Tenant, "external-result-" + Id, "eventguard.external-results/v1", "action-status");
    }
}

/// <summary>An immutable state snapshot. Acceptance and later fulfillment are separate outcomes.</summary>
public sealed class ExternalActionUpdate
{
    private readonly byte[] _stateData;
    public ExternalAction Action { get; }
    public long Version { get; }
    public ExternalActionState State { get; }
    public ExternalObservationState Observation { get; }
    public ExternalPhase Phase { get; }
    public string? ProviderReference { get; }
    public string? Reason { get; }
    public ExternalResult? Response { get; }
    public string Fingerprint { get; }
    public byte[] CopyStateData() => (byte[])_stateData.Clone();
    public ExternalActionUpdate(ExternalAction action, long version, ExternalActionState state,
        ExternalObservationState observation, ExternalPhase phase, string? providerReference,
        string? reason, ReadOnlySpan<byte> stateData, ExternalResult? response)
    {
        Action = action ?? throw new ArgumentNullException(nameof(action));
        if (version < 1 || !Enum.IsDefined(state) || !Enum.IsDefined(observation) || !Enum.IsDefined(phase))
            throw new ArgumentException("Invalid external result snapshot.");
        if (stateData.Length > 64 * 1024) throw new ArgumentException("State result exceeds 64 KiB.", nameof(stateData));
        if (providerReference is not null) _ = IdentityRules.Required(providerReference, nameof(providerReference));
        if (reason is not null) _ = new ExternalResult(ExternalResultKind.Unknown, reason);
        Version = version; State = state; Observation = observation; Phase = phase;
        ProviderReference = providerReference; Reason = reason; _stateData = stateData.ToArray(); Response = response;
        var culture = System.Globalization.CultureInfo.InvariantCulture;
        Fingerprint = StableHash.Of("external-result-snapshot/v1", action.Fingerprint, version.ToString(culture),
            ((int)state).ToString(culture), ((int)observation).ToString(culture), ((int)phase).ToString(culture),
            providerReference ?? "", reason ?? "", ContentFingerprint.FromBytes(_stateData).Value,
            response is null ? "" : ((int)response.Kind).ToString(culture), response?.Reason ?? "", response?.Reference ?? "",
            response is null ? "" : ContentFingerprint.FromBytes(response.CopyData()).Value);
    }
}

// Optional capability: existing third-party IExternalActionWriter implementations remain source/API compatible.
public interface IExternalActionResultWriter : IExternalActionWriter
{
    Task<OutboxEnqueueStatus> EnqueueAsync(ExternalAction action, ExternalResultTarget target,
        CancellationToken cancellationToken = default);
}

public static class ExternalActionResultExtensions
{
    public static Task<OutboxEnqueueStatus> EnqueueAsync(this IExternalActionWriter writer, ExternalAction action,
        ExternalResultTarget target, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(writer); ArgumentNullException.ThrowIfNull(action); ArgumentNullException.ThrowIfNull(target);
        if (writer is not IExternalActionResultWriter capable)
            throw new NotSupportedException("This session cannot atomically route external action results.");
        return capable.EnqueueAsync(action, target, cancellationToken);
    }
}

public enum ExternalResultApplyStatus { Applied, AlreadyApplied }
public interface IExternalResultWriter
{
    /// <summary>Apply the owning result work under the session transaction. Older versions are skipped.</summary>
    Task<ExternalResultApplyStatus> ApplyAsync(ExternalResultTarget target,
        Func<ExternalActionUpdate, CancellationToken, ValueTask> handler, CancellationToken cancellationToken = default);
}
public interface IExternalResultSession { IExternalResultWriter ExternalResults { get; } }
