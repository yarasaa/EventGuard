namespace EventGuard;

public sealed record ExternalActionIntent(ExternalAction Action, ExternalResultTarget? ResultTarget = null);
public sealed record ExternalActionEnqueueResult(Guid ActionId, OutboxEnqueueStatus Status);

public sealed class ExternalActionEnqueueBatch
{
    public IReadOnlyList<ExternalActionIntent> Items { get; }
    public ExternalActionEnqueueBatch(IEnumerable<ExternalActionIntent> items)
    {
        ArgumentNullException.ThrowIfNull(items);
        var copy = new List<ExternalActionIntent>(); long bytes = 0;
        foreach (var item in items)
        {
            if (item?.Action is null || copy.Count == 1000) throw new ArgumentException("Use 1–1000 non-null action intents.", nameof(items));
            bytes += item.Action.PayloadLength;
            if (bytes > 64 * 1024 * 1024) throw new ArgumentException("Action intent page exceeds 64 MiB.", nameof(items));
            if (item.ResultTarget is { } target && target.Scope != item.Action.Scope) throw new ArgumentException("Result target scope differs.", nameof(items));
            copy.Add(item);
        }
        if (copy.Count == 0 || copy.Any(i => i.Action.Scope != copy[0].Action.Scope)
            || copy.Select(i => i.Action.Id).Distinct().Count() != copy.Count)
            throw new ArgumentException("Use distinct actions in one producer scope.", nameof(items));
        Items = copy.AsReadOnly();
    }
}

public interface IExternalActionBatchWriter : IExternalActionWriter
{
    /// <summary>Atomic with the owning producer/job transaction. A conflicting item rejects the whole page.</summary>
    Task<IReadOnlyList<ExternalActionEnqueueResult>> EnqueueBatchAsync(ExternalActionEnqueueBatch batch, CancellationToken cancellationToken = default);
}

public static class ExternalActionBatchExtensions
{
    public static Task<IReadOnlyList<ExternalActionEnqueueResult>> EnqueueBatchAsync(this IExternalActionWriter writer,
        ExternalActionEnqueueBatch batch, CancellationToken cancellationToken = default) =>
        writer is IExternalActionBatchWriter native ? native.EnqueueBatchAsync(batch, cancellationToken)
            : throw new NotSupportedException("This session does not support native action preparation.");
}
