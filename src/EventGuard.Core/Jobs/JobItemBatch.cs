namespace EventGuard;

public sealed class JobItemBatch
{
    public IReadOnlyList<JobItemSubmission> Items { get; }
    public JobItemBatch(IEnumerable<JobItemSubmission> items)
    {
        ArgumentNullException.ThrowIfNull(items);
        var copy = new List<JobItemSubmission>();
        foreach (var item in items)
        {
            if (item is null || copy.Count == 1000) throw new ArgumentException("Use 1–1000 non-null job items.", nameof(items));
            copy.Add(item);
        }
        if (copy.Count == 0 || copy.Any(i => i.Definition.Fingerprint != copy[0].Definition.Fingerprint)
            || copy.Select(i => i.Id).Distinct().Count() != copy.Count)
            throw new ArgumentException("Use distinct item identities from one job definition.", nameof(items));
        Items = Array.AsReadOnly(copy.OrderBy(i => i.Id, StringComparer.Ordinal).ToArray());
    }
}

public interface IJobItemBatchWriter : IJobItemWriter
{
    /// <summary>Calls the handler once with only unapplied items. Receipts and all application writes roll back together.</summary>
    Task<int> ApplyBatchAsync(JobItemBatch batch, Func<IReadOnlyList<JobItemSubmission>, CancellationToken, ValueTask> handler,
        CancellationToken cancellationToken = default);
}
