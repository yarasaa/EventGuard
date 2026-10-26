namespace EventGuard;

public enum JobItemStatus { Applied, AlreadyApplied }

/// <summary>Item identity survives rebatching. Content and definition changes require a new explicit business intent.</summary>
public sealed class JobItemSubmission
{
    public JobDefinition Definition { get; }
    public string Key { get; }
    public string Id { get; }
    public string Fingerprint { get; }

    public JobItemSubmission(JobDefinition definition, string key, MessageContractVersion contract, ReadOnlySpan<byte> content)
    {
        Definition = definition ?? throw new ArgumentNullException(nameof(definition));
        ArgumentNullException.ThrowIfNull(contract);
        Key = IdentityRules.Required(key, nameof(key));
        if (content.Length > 1024 * 1024) throw new ArgumentException("Item content exceeds 1 MiB.", nameof(content));
        Id = StableHash.Of("jobs/item-identity/v1", definition.Scope.Id, definition.Name, Key);
        Fingerprint = StableHash.Of("jobs/item-content/v1", definition.Fingerprint, contract.Name,
            contract.Version.ToString(System.Globalization.CultureInfo.InvariantCulture), ContentFingerprint.FromBytes(content).Value);
    }
}

public sealed class JobItemConflictException(string itemId)
    : InvalidOperationException($"Job item {itemId} already has a different definition or content.");

public interface IJobItemWriter
{
    /// <summary>The receipt and handler SQL/outbox writes share the owning job transaction. No external calls.</summary>
    Task<JobItemStatus> ApplyAsync(JobItemSubmission item, Func<CancellationToken, ValueTask> handler,
        CancellationToken cancellationToken = default);
}

public interface IJobItemSession
{
    IJobItemWriter JobItems { get; }
}
