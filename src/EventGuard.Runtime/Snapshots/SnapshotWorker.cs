using System.Text.Json;

namespace EventGuard.Runtime;

/// <summary>Captures a consistent view before exposing any batch to an executor.</summary>
public sealed class SnapshotWorker<T> where T : notnull
{
    private readonly ISnapshotStore _store;
    private readonly SnapshotDefinition _definition;
    private readonly ISnapshotSource _source;
    private readonly JsonMessageContract<T> _items;
    private readonly TimeSpan _leaseDuration;
    private readonly PollingWorkerOptions _polling;
    public SnapshotWorker(ISnapshotStore store, SnapshotDefinition definition, ISnapshotSource source,
        JsonMessageContract<T> items, TimeSpan? leaseDuration = null)
        : this(store, definition, source, items, leaseDuration, new()) { }

    public SnapshotWorker(ISnapshotStore store, SnapshotDefinition definition, ISnapshotSource source,
        JsonMessageContract<T> items, TimeSpan? leaseDuration, PollingWorkerOptions polling)
    {
        _polling = polling ?? throw new ArgumentNullException(nameof(polling));
        _store = store ?? throw new ArgumentNullException(nameof(store)); _definition = definition ?? throw new ArgumentNullException(nameof(definition));
        _source = source ?? throw new ArgumentNullException(nameof(source)); _items = items ?? throw new ArgumentNullException(nameof(items));
        if (source.Name != definition.Source || source.Version != definition.SourceVersion || source.Consistency != definition.Consistency)
            throw new InvalidOperationException("Snapshot source binding does not match the saved definition.");
        _leaseDuration = leaseDuration ?? TimeSpan.FromMinutes(1);
        if (_leaseDuration < TimeSpan.FromSeconds(1) || _leaseDuration > TimeSpan.FromHours(1)) throw new ArgumentOutOfRangeException(nameof(leaseDuration));
    }

    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var rows = await _store.ListSnapshotsAsync(_definition, _polling.MaximumItems, cancellationToken);
            foreach (var row in rows) await RunOnceAsync(row.Request, cancellationToken);
            await Task.Delay(_polling.PollInterval, cancellationToken);
        }
    }

    public async Task<bool> RunOnceAsync(SnapshotRequest request, CancellationToken cancellationToken = default)
    {
        if (request.Definition.Fingerprint != _definition.Fingerprint) throw new SnapshotConflictException();
        var lease = await _store.ClaimSnapshotAsync(request, Guid.NewGuid(), _leaseDuration, cancellationToken);
        if (lease is null) return false;
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var renewing = Renew();
        try
        {
            var progress = (await _store.FindSnapshotAsync(_definition, request.Id, stop.Token))!;
            if (progress.State is SnapshotState.Pending or SnapshotState.Capturing)
            {
                await using var reader = await _source.OpenAsync(request.CopyParameters(),
                    _definition.Consistency == SnapshotConsistency.ResumableView ? progress.View : null, stop.Token);
                progress = await _store.BeginSnapshotAsync(lease, reader.View, stop.Token);
                while (progress.State == SnapshotState.Capturing)
                {
                    var page = await reader.ReadAsync(progress.Cursor, _definition.BatchSize, stop.Token);
                    foreach (var item in page.Items) _ = _items.Decode(item.CopyPayload());
                    progress = await _store.AppendSnapshotAsync(lease, progress.Generation, progress.Page, page, stop.Token);
                }
            }
            while (progress.State == SnapshotState.Sealed)
            {
                var candidate = await _store.ReadSnapshotBatchAsync(lease, stop.Token);
                if (candidate.Items.Count == 0)
                {
                    progress = await _store.PublishSnapshotBatchAsync(lease, candidate, stop.Token);
                    break;
                }
                // Bound encoded bytes too: JSON escaping and wrapper overhead count toward the work limit.
                var count = candidate.Items.Count;
                while (true)
                {
                    try { _ = SnapshotBatchCodec.Create(candidate.Progress, candidate.Items.Take(count).ToArray()); break; }
                    catch (ArgumentException) when (count > 1) { count = Math.Max(1, count / 2); }
                }
                candidate = candidate with { Items = candidate.Items.Take(count).ToArray() };
                progress = await _store.PublishSnapshotBatchAsync(lease, candidate, stop.Token);
            }
            return progress.State == SnapshotState.Batched;
        }
        catch (SnapshotLeaseLostException) { return false; }
        finally
        {
            stop.Cancel();
            try { await renewing; }
            catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
            catch (SnapshotLeaseLostException) { }
        }
        async Task Renew()
        {
            try
            {
                while (!stop.IsCancellationRequested)
                {
                    await Task.Delay(_leaseDuration / 3, stop.Token);
                    await _store.RenewSnapshotAsync(lease, stop.Token);
                }
            }
            catch { stop.Cancel(); throw; }
        }
    }
}

/// <summary>At-least-once broker/CDC normalization. Acknowledge transport only after this returns successfully.</summary>
public sealed class SnapshotBatchReceiver(IJobStore jobs, JobDefinition definition)
{
    public async Task<JobSubmissionResult> AcceptAsync(ReadOnlyMemory<byte> envelope, CancellationToken cancellationToken = default)
    {
        if (envelope.Length > 2 * 1024 * 1024) throw new IngressMessageRejectedException("snapshot-envelope-too-large");
        SnapshotBatchEnvelope value;
        try { value = JsonSerializer.Deserialize<SnapshotBatchEnvelope>(envelope.Span) ?? throw new IngressMessageRejectedException("invalid-snapshot-envelope"); }
        catch (JsonException) { throw new IngressMessageRejectedException("invalid-snapshot-envelope"); }
        if (value.SnapshotId == Guid.Empty || value.Sequence < 1 || value.Payload is null || value.Payload.Length > 1024 * 1024)
            throw new IngressMessageRejectedException("invalid-snapshot-envelope");
        var key = "snapshot:" + value.SnapshotId.ToString("D") + ":" + value.Sequence.ToString(System.Globalization.CultureInfo.InvariantCulture);
        var expectedId = new Guid(Convert.FromHexString(StableHash.Of("snapshot-batch/v1", definition.Scope.Id, key)).AsSpan(0, 16));
        if (value.BusinessKey != key || value.BatchId != expectedId) throw new IngressMessageRejectedException("invalid-snapshot-identity");
        if (value.DefinitionId != definition.Id || value.DefinitionFingerprint != definition.Fingerprint || value.ScopeId != definition.Scope.Id)
            throw new IngressMessageRejectedException("snapshot-binding-mismatch");
        var result = await jobs.SubmitAsync(new(definition, value.BatchId, value.BusinessKey, value.Actor, value.Payload), cancellationToken);
        if (result.Status is JobSubmissionStatus.RequestConflict or JobSubmissionStatus.BusinessConflict)
            throw new IngressMessageRejectedException("snapshot-batch-conflict");
        return result;
    }
}
