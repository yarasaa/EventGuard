using System.Globalization;

namespace EventGuard;

/// <summary>The adapter receives authenticated reports through the application's own endpoint.</summary>
public interface ICallbackExternalServiceAdapter : IExternalServiceAdapter { }
public enum ExternalNoticeStatus { Received, Applied, Ignored, Conflict }
public sealed record ExternalNoticeReceipt(Guid NoticeId, Guid ActionId, ExternalNoticeStatus Status, string Reason, long ActionVersion);
public sealed class ExternalObservationNotice
{
    public ProducerScope Scope { get; }
    public Guid Id { get; }
    public Guid ActionId { get; }
    public string Binding { get; }
    public string BindingVersion { get; }
    public long? ProviderRevision { get; }
    public ExternalResult Result { get; }
    public string ResultFingerprint { get; }
    public string Fingerprint { get; }
    public ExternalObservationNotice(ProducerScope scope, Guid id, Guid actionId, string binding, string bindingVersion,
        ExternalResult result, long? providerRevision = null)
    {
        Scope = scope ?? throw new ArgumentNullException(nameof(scope)); Result = result ?? throw new ArgumentNullException(nameof(result));
        if (id == Guid.Empty || actionId == Guid.Empty || providerRevision is < 1) throw new ArgumentException("Stable notice/action identities and a positive provider revision are required.");
        if (result.Kind is not (ExternalResultKind.Pending or ExternalResultKind.Unknown or ExternalResultKind.Succeeded or ExternalResultKind.Failed))
            throw new ArgumentException("A report describes fulfillment, not submission acceptance.", nameof(result));
        Id = id; ActionId = actionId; Binding = IdentityRules.Required(binding, nameof(binding)); BindingVersion = IdentityRules.Required(bindingVersion, nameof(bindingVersion));
        ProviderRevision = providerRevision;
        ResultFingerprint = StableHash.Of("external-notice-result/v1", ((int)result.Kind).ToString(CultureInfo.InvariantCulture), result.Reason,
            result.Reference ?? "", ContentFingerprint.FromBytes(result.CopyData()).Value);
        Fingerprint = StableHash.Of("external-notice/v1", scope.Id, id.ToString("D"), actionId.ToString("D"), Binding, BindingVersion,
            providerRevision?.ToString(CultureInfo.InvariantCulture) ?? "", ResultFingerprint);
    }
}
public interface IExternalObservationStore
{
    /// <summary>Only call after authenticating the sender, authorizing the scope and mapping the provider contract.</summary>
    Task<ExternalNoticeReceipt> RecordNoticeAsync(ExternalObservationNotice notice, CancellationToken cancellationToken = default);
    Task<ExternalNoticeReceipt?> FindNoticeAsync(ProducerScope scope, Guid noticeId, CancellationToken cancellationToken = default);
    Task<ExternalNoticeReceipt?> ApplyNextNoticeAsync(ProducerScope scope, string binding, string bindingVersion,
        ExternalActionPolicy policy, CancellationToken cancellationToken = default);
    Task<int> ExpireObservationsAsync(ProducerScope scope, string binding, string bindingVersion, int limit = 100, CancellationToken cancellationToken = default);
}
