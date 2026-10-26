using System.Collections.ObjectModel;
using System.Globalization;

namespace EventGuard;

public enum SnapshotConsistency { SessionBound, ResumableView }
public enum SnapshotDelivery { LocalJob, Outbox, Cdc }
public enum SnapshotState { Pending, Capturing, Sealed, Batched, Cancelled }

/// <summary>A registered source and target; all semantic choices are frozen for an operation.</summary>
public sealed class SnapshotDefinition
{
    public JobDefinition Target { get; }
    public string Name { get; }
    public string Source { get; }
    public string SourceVersion { get; }
    public SnapshotConsistency Consistency { get; }
    public SnapshotDelivery Delivery { get; }
    public PublishRoute? Route { get; }
    public int BatchSize { get; }
    public string Id { get; }
    public string Fingerprint { get; }
    public ProducerScope OutputScope { get; }
    public SnapshotDefinition(JobDefinition target, string name, string source, string sourceVersion,
        SnapshotConsistency consistency = SnapshotConsistency.SessionBound, int batchSize = 100,
        SnapshotDelivery delivery = SnapshotDelivery.LocalJob, PublishRoute? route = null)
    {
        Target = target ?? throw new ArgumentNullException(nameof(target));
        Name = IdentityRules.Required(name, nameof(name)); Source = IdentityRules.Required(source, nameof(source));
        SourceVersion = IdentityRules.Required(sourceVersion, nameof(sourceVersion));
        if (!Enum.IsDefined(consistency) || !Enum.IsDefined(delivery)) throw new ArgumentException("Invalid snapshot policy.");
        if (batchSize is < 1 or > 1000) throw new ArgumentOutOfRangeException(nameof(batchSize));
        if ((delivery == SnapshotDelivery.LocalJob) != (route is null)) throw new ArgumentException("Broker delivery requires a logical route; local jobs do not use one.", nameof(route));
        Consistency = consistency; Delivery = delivery; BatchSize = batchSize; Route = route;
        Id = StableHash.Of("snapshot-definition/v1", target.Scope.Id, Name);
        OutputScope = new(target.Scope.Tenant, "snapshot-" + Id);
        Fingerprint = StableHash.Of("snapshot-definition-content/v1", Id, target.Fingerprint, Source, SourceVersion,
            ((int)consistency).ToString(CultureInfo.InvariantCulture), batchSize.ToString(CultureInfo.InvariantCulture),
            ((int)delivery).ToString(CultureInfo.InvariantCulture), route?.Transport ?? "", route?.Connection ?? "",
            route?.ConfigurationVersion ?? "", route?.Address ?? "", route?.RoutingKey ?? "");
    }
}

public sealed class SnapshotRequest
{
    private readonly byte[] _parameters;
    public SnapshotDefinition Definition { get; }
    public Guid RequestId { get; }
    public Guid Id { get; }
    public string BusinessKey { get; }
    public string Actor { get; }
    public string Fingerprint { get; }
    public byte[] CopyParameters() => (byte[])_parameters.Clone();
    public SnapshotRequest(SnapshotDefinition definition, Guid requestId, string businessKey, string actor, ReadOnlySpan<byte> parameters)
    {
        Definition = definition ?? throw new ArgumentNullException(nameof(definition));
        if (requestId == Guid.Empty) throw new ArgumentException("Stable request ID required.", nameof(requestId));
        if (parameters.Length > 1024 * 1024) throw new ArgumentException("Filter parameters exceed 1 MiB.", nameof(parameters));
        RequestId = requestId; BusinessKey = IdentityRules.Required(businessKey, nameof(businessKey));
        Actor = IdentityRules.Required(actor, nameof(actor)); _parameters = parameters.ToArray();
        Id = new Guid(Convert.FromHexString(StableHash.Of("snapshot-operation/v1", definition.Id, BusinessKey)).AsSpan(0, 16));
        Fingerprint = StableHash.Of("snapshot-request/v1", Id.ToString("D"), definition.Fingerprint,
            ContentFingerprint.FromBytes(_parameters).Value, Actor);
    }
}

public sealed class SnapshotItem
{
    private readonly byte[] _payload;
    public string Key { get; }
    public string KeyHash { get; }
    public string Fingerprint { get; }
    public byte[] CopyPayload() => (byte[])_payload.Clone();
    public SnapshotItem(string key, ReadOnlySpan<byte> payload)
    {
        Key = IdentityRules.Required(key, nameof(key));
        if (payload.Length > 1024 * 1024) throw new ArgumentException("Snapshot item exceeds 1 MiB.", nameof(payload));
        KeyHash = StableHash.Of("snapshot-item-key/v1", Key); _payload = payload.ToArray();
        Fingerprint = ContentFingerprint.FromBytes(_payload).Value;
    }
}

public sealed class SnapshotPage
{
    private readonly byte[] _nextCursor;
    public IReadOnlyList<SnapshotItem> Items { get; }
    public bool IsLast { get; }
    public string Fingerprint { get; }
    public byte[] CopyNextCursor() => (byte[])_nextCursor.Clone();
    public SnapshotPage(IEnumerable<SnapshotItem> items, ReadOnlySpan<byte> nextCursor, bool isLast)
    {
        ArgumentNullException.ThrowIfNull(items);
        var copy = items.Take(1001).ToArray();
        if (copy.Length > 1000 || copy.Any(i => i is null) || copy.Sum(i => (long)i.CopyPayload().Length) > 4 * 1024 * 1024)
            throw new ArgumentException("A source page is limited to 1000 items and 4 MiB.", nameof(items));
        if (nextCursor.Length > 64 * 1024) throw new ArgumentException("Cursor exceeds 64 KiB.", nameof(nextCursor));
        Items = new ReadOnlyCollection<SnapshotItem>(copy); _nextCursor = nextCursor.ToArray(); IsLast = isLast;
        Fingerprint = StableHash.Of(["snapshot-page/v1", ContentFingerprint.FromBytes(_nextCursor).Value,
            isLast ? "last" : "more", .. copy.SelectMany(i => new[] { i.KeyHash, i.Fingerprint })]);
    }
}

/// <summary>Source SQL and filter validation belong to an application-owned, versioned adapter.</summary>
public interface ISnapshotSource
{
    string Name { get; }
    string Version { get; }
    SnapshotConsistency Consistency { get; }
    Task<ISnapshotReader> OpenAsync(ReadOnlyMemory<byte> parameters, string? resumeView, CancellationToken cancellationToken = default);
}
public interface ISnapshotReader : IAsyncDisposable
{
    string View { get; }
    Task<SnapshotPage> ReadAsync(ReadOnlyMemory<byte> cursor, int maximumItems, CancellationToken cancellationToken = default);
}

public sealed record SnapshotProgress(SnapshotRequest Request, SnapshotState State, long Generation, long Page,
    long ItemCount, long BatchedItems, long BatchCount, string? View, byte[] Cursor, bool Paused,
    Guid? Owner, DateTimeOffset? LeaseUntil, long Version);
public sealed record SnapshotLease(SnapshotRequest Request, Guid Token, TimeSpan Duration);
public sealed record SnapshotBatchItem(long Ordinal, SnapshotItem Item);
public sealed record SnapshotBatchCandidate(SnapshotProgress Progress, IReadOnlyList<SnapshotBatchItem> Items);
public sealed class SnapshotConflictException() : InvalidOperationException("Snapshot identity already has different immutable content.");
public sealed class SnapshotLeaseLostException() : InvalidOperationException("Snapshot lease was lost or expired.");

public interface ISnapshotStore
{
    Task InitializeSnapshotsAsync(CancellationToken cancellationToken = default);
    Task<SnapshotProgress> CreateSnapshotAsync(SnapshotRequest request, CancellationToken cancellationToken = default);
    Task<SnapshotProgress?> FindSnapshotAsync(SnapshotDefinition definition, Guid id, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<SnapshotProgress>> ListSnapshotsAsync(SnapshotDefinition definition, int limit = 100, CancellationToken cancellationToken = default);
    Task<SnapshotLease?> ClaimSnapshotAsync(SnapshotRequest request, Guid owner, TimeSpan duration, CancellationToken cancellationToken = default);
    Task RenewSnapshotAsync(SnapshotLease lease, CancellationToken cancellationToken = default);
    Task<SnapshotProgress> BeginSnapshotAsync(SnapshotLease lease, string view, CancellationToken cancellationToken = default);
    Task<SnapshotProgress> AppendSnapshotAsync(SnapshotLease lease, long expectedGeneration, long expectedPage, SnapshotPage page, CancellationToken cancellationToken = default);
    Task<SnapshotBatchCandidate> ReadSnapshotBatchAsync(SnapshotLease lease, CancellationToken cancellationToken = default);
    Task<SnapshotProgress> PublishSnapshotBatchAsync(SnapshotLease lease, SnapshotBatchCandidate candidate,
        CancellationToken cancellationToken = default);
    Task<SnapshotProgress> ControlSnapshotAsync(SnapshotDefinition definition, Guid id, long expectedVersion,
        bool paused, bool cancel, string actor, string reason, CancellationToken cancellationToken = default);
}
