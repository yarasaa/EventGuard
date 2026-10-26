namespace EventGuard;

/// <summary>Durable batch export and bounded staging cleanup; identity and delivery receipts are retained.</summary>
public interface ISnapshotMaintenance
{
    Task<IReadOnlyList<SnapshotBatchEnvelope>> ReadSnapshotBatchesAsync(SnapshotDefinition definition, Guid snapshotId,
        long afterSequence = 0, int limit = 25, CancellationToken cancellationToken = default);
    /// <summary>Removes only obsolete generations or fully batched/cancelled staging. Never removes ready batch envelopes.</summary>
    Task<int> PruneSnapshotStagingAsync(SnapshotDefinition definition, Guid snapshotId, DateTimeOffset olderThan,
        int limit = 1000, CancellationToken cancellationToken = default);
}
