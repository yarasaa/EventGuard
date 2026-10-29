using System.Globalization;

namespace EventGuard;

/// <summary>One bounded storage page, independent of the remote service's batch size.</summary>
public sealed class ExternalBatchClaimRequest
{
    public const int MaximumItemsPerPage = 1000;
    public const int MaximumBytesPerPage = 64 * 1024 * 1024;
    public ProducerScope Scope { get; }
    public string Binding { get; }
    public string BindingVersion { get; }
    public ExternalPhase Phase { get; }
    public Guid RequestId { get; }
    public TimeSpan LeaseDuration { get; }
    public int MaximumItems { get; }
    public int MaximumPayloadBytes { get; }
    public string Fingerprint { get; }

    public ExternalBatchClaimRequest(ProducerScope scope, string binding, string bindingVersion, ExternalPhase phase,
        Guid requestId, TimeSpan leaseDuration, int maximumItems, int maximumPayloadBytes)
    {
        Scope = scope ?? throw new ArgumentNullException(nameof(scope));
        _ = new ProducerScope(binding, bindingVersion);
        if (!Enum.IsDefined(phase) || requestId == Guid.Empty) throw new ArgumentException("A valid phase and request ID are required.");
        if (leaseDuration < TimeSpan.FromMilliseconds(1) || leaseDuration > TimeSpan.FromHours(1))
            throw new ArgumentOutOfRangeException(nameof(leaseDuration));
        if (maximumItems is < 1 or > MaximumItemsPerPage) throw new ArgumentOutOfRangeException(nameof(maximumItems));
        if (maximumPayloadBytes is < 1 or > MaximumBytesPerPage) throw new ArgumentOutOfRangeException(nameof(maximumPayloadBytes));
        Binding = binding; BindingVersion = bindingVersion; Phase = phase; RequestId = requestId;
        LeaseDuration = leaseDuration; MaximumItems = maximumItems; MaximumPayloadBytes = maximumPayloadBytes;
        Fingerprint = StableHash.Of("external-batch-claim/v1", scope.Id, binding, bindingVersion,
            ((int)phase).ToString(CultureInfo.InvariantCulture), leaseDuration.Ticks.ToString(CultureInfo.InvariantCulture),
            maximumItems.ToString(CultureInfo.InvariantCulture), maximumPayloadBytes.ToString(CultureInfo.InvariantCulture));
    }
}

public sealed record ExternalBatchSettlementItem(ExternalActionClaim Claim, ExternalResult Result);

/// <summary>Immutable, bounded evidence for one atomic storage settlement, with independently fenced item outcomes.</summary>
public sealed class ExternalBatchSettlementRequest
{
    public IReadOnlyList<ExternalBatchSettlementItem> Items { get; }
    public ExternalActionPolicy Policy { get; }

    public ExternalBatchSettlementRequest(IEnumerable<ExternalBatchSettlementItem> items, ExternalActionPolicy policy)
    {
        ArgumentNullException.ThrowIfNull(items);
        Policy = policy ?? throw new ArgumentNullException(nameof(policy));
        var copy = new List<ExternalBatchSettlementItem>();
        long payloadBytes = 0;
        foreach (var item in items)
        {
            if (copy.Count == ExternalBatchClaimRequest.MaximumItemsPerPage)
                throw new ArgumentException("The storage settlement page is too large.", nameof(items));
            if (item?.Claim?.Record?.Action is null || item.Result is null)
                throw new ArgumentException("A settlement requires non-null claims and results.", nameof(items));
            payloadBytes += item.Claim.Record.Action.PayloadLength;
            if (payloadBytes > ExternalBatchClaimRequest.MaximumBytesPerPage)
                throw new ArgumentException("The storage settlement page exceeds its payload byte limit.", nameof(items));
            copy.Add(item);
        }
        // Reuse the remote batch's scope, phase, identity and token validation.
        _ = new ExternalBatchRequest(copy.Select(i => i.Claim), ExternalBatchClaimRequest.MaximumItemsPerPage);
        if (copy.Select(i => i.Claim.Token).Distinct().Count() != copy.Count)
            throw new ArgumentException("Claim tokens must be distinct.", nameof(items));
        Items = copy.AsReadOnly();
    }
}

/// <summary>
/// Optional native storage capability. A request that acquires or recovers actions is durable; retrying its ID never adds members.
/// Empty polls are not recorded and do not consume their request ID.
/// A settlement commits evidence and application result work atomically. Repeating identical evidence is idempotent.
/// Implementations accept pages up to the limits in ExternalBatchClaimRequest; workers may combine several pages.
/// </summary>
public interface IExternalActionBatchStore : IExternalActionStore
{
    Task ValidateExternalBatchAsync(CancellationToken cancellationToken = default);
    Task<IReadOnlyList<ExternalActionClaim>> ClaimActionsAsync(ExternalBatchClaimRequest request, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<ExternalSettlement>> SettleActionsAsync(ExternalBatchSettlementRequest request, CancellationToken cancellationToken = default);
}
