namespace EventGuard.Runtime;

public sealed record JobBatchItem<T>(string Key, T Value) where T : notnull;
public sealed record JobBatch<T>(int ContractVersion, IReadOnlyList<JobBatchItem<T>> Items) where T : notnull;
public sealed record JobBatchResult(int Applied, int AlreadyApplied);

/// <summary>Bounded atomic SQL batches. Each item has durable identity independent of its batch.</summary>
public static class JsonJobBatches
{
    public static JsonMessageContract<JobBatch<T>> Contract<T>(string name, int version,
        JsonMessageContract<T> itemContract, int maximumItems = 100) where T : notnull
    {
        ArgumentNullException.ThrowIfNull(itemContract);
        if (maximumItems is < 1 or > 1000) throw new ArgumentOutOfRangeException(nameof(maximumItems));
        return new(name, version, b => b.ContractVersion, b =>
        {
            ValidateItems(b, maximumItems);
            foreach (var item in b.Items) _ = itemContract.Encode(item.Value);
        });
    }

    public static async ValueTask<JobBatchResult> ExecuteAsync<T, TSession>(JobDefinition definition, JobBatch<T> batch,
        JsonMessageContract<T> itemContract, TSession session, Func<T, TSession, CancellationToken, ValueTask> handler,
        CancellationToken cancellationToken = default) where T : notnull where TSession : IJobItemSession
    {
        ArgumentNullException.ThrowIfNull(definition); ArgumentNullException.ThrowIfNull(itemContract);
        ArgumentNullException.ThrowIfNull(session); ArgumentNullException.ThrowIfNull(handler);
        ValidateItems(batch, 1000);
        // Freeze and validate the entire batch before calling application code. Sorting durable
        // item identities prevents reversed batches from acquiring crossed receipt locks.
        var items = new List<(JobItemSubmission Submission, byte[] Bytes)>();
        var totalBytes = 0;
        foreach (var item in batch.Items)
        {
            var bytes = itemContract.Encode(item.Value);
            totalBytes = checked(totalBytes + bytes.Length);
            if (totalBytes > 1024 * 1024) throw new IngressMessageRejectedException("job-batch-too-large");
            items.Add((new(definition, item.Key, new(itemContract.Name, itemContract.Version), bytes), bytes));
        }
        items.Sort((a, b) => StringComparer.Ordinal.Compare(a.Submission.Id, b.Submission.Id));
        var applied = 0;
        foreach (var item in items)
        {
            var result = await session.JobItems.ApplyAsync(item.Submission,
                ct => handler(itemContract.Decode(item.Bytes), session, ct), cancellationToken);
            if (result == JobItemStatus.Applied) applied++;
        }
        return new(applied, items.Count - applied);
    }

    private static void ValidateItems<T>(JobBatch<T> batch, int maximumItems) where T : notnull
    {
        if (batch?.Items is null || batch.Items.Count is < 1 || batch.Items.Count > maximumItems
            || batch.Items.Any(i => i is null || string.IsNullOrWhiteSpace(i.Key) || i.Key.Length > 1024 || i.Value is null)
            || batch.Items.Select(i => i.Key).Distinct(StringComparer.Ordinal).Count() != batch.Items.Count)
            throw new IngressMessageRejectedException("invalid-job-batch");
    }
}
