using System.Text.Json;

namespace EventGuard;

/// <summary>A bounded remote call. Action IDs, not positions or this attempt ID, identify logical operations.</summary>
public sealed class ExternalBatchRequest
{
    public string AttemptId { get; }
    public ExternalPhase Phase { get; }
    public IReadOnlyList<ExternalActionRecord> Items { get; }

    public ExternalBatchRequest(IEnumerable<ExternalActionClaim> claims) : this(claims, 100) { }

    public ExternalBatchRequest(IEnumerable<ExternalActionClaim> claims, int maximumBatchSize)
    {
        ArgumentNullException.ThrowIfNull(claims);
        if (maximumBatchSize < 1) throw new ArgumentOutOfRangeException(nameof(maximumBatchSize));
        var bounded = new List<ExternalActionClaim>();
        foreach (var claim in claims)
        {
            if (bounded.Count == maximumBatchSize)
                throw new ArgumentException("The remote batch exceeds its configured item limit.", nameof(claims));
            if (claim is null) throw new ArgumentException("A remote batch cannot contain null claims.", nameof(claims));
            bounded.Add(claim);
        }
        var copy = bounded.ToArray();
        if (copy.Length == 0)
            throw new ArgumentException("A remote batch requires at least one claim.", nameof(claims));
        var first = copy[0];
        if (!Enum.IsDefined(first.Phase) || copy.Any(c => c.Phase != first.Phase
                || c.Record.Action.Scope != first.Record.Action.Scope
                || c.Record.Action.Binding != first.Record.Action.Binding
                || c.Record.Action.BindingVersion != first.Record.Action.BindingVersion)
            || copy.Select(c => c.Record.Action.Id).Distinct().Count() != copy.Length
            || copy.Any(c => c.Token == Guid.Empty))
            throw new ArgumentException("A batch requires distinct actions in one scope, binding, version and phase.", nameof(claims));
        Phase = first.Phase;
        Items = Array.AsReadOnly(copy.Select(c => c.Record).ToArray());
        AttemptId = StableHash.Of(["external-batch/v1", first.Record.Action.Scope.Id, Phase.ToString(),
            .. copy.OrderBy(c => c.Record.Action.Id).Select(c => c.Record.Action.Id.ToString("D") + ":" + c.Token.ToString("D"))]);
    }
}

public sealed record ExternalBatchItemResult(Guid ActionId, ExternalResult Result);

/// <summary>Implement one real provider call per execution. Preserve action IDs across every provider operation.</summary>
public interface IExternalBatchServiceAdapter
{
    string Binding { get; }
    string BindingVersion { get; }
    bool CanReconcile { get; }
    bool CanObserve { get; }
    bool CanReceiveCallbacks => false;
    Task<IReadOnlyList<ExternalBatchItemResult>> ExecuteAsync(ExternalBatchRequest request, CancellationToken cancellationToken);
}

/// <summary>JSON convenience only: validation, authentication and business identity remain application-owned.</summary>
public static class JsonExternalActions
{
    private static readonly JsonSerializerOptions DefaultOptions = new(JsonSerializerDefaults.Web);

    public static ExternalAction Create<T>(ProducerScope scope, Guid id, string binding, string bindingVersion,
        T payload, TimeSpan? observationTimeout = null, JsonSerializerOptions? jsonOptions = null) where T : notnull
    {
        ArgumentNullException.ThrowIfNull(payload);
        return new(scope, id, binding, bindingVersion,
            JsonSerializer.SerializeToUtf8Bytes(payload, jsonOptions ?? DefaultOptions), observationTimeout);
    }

    public static T ReadJson<T>(this ExternalAction action, JsonSerializerOptions? jsonOptions = null) where T : notnull
    {
        ArgumentNullException.ThrowIfNull(action);
        return JsonSerializer.Deserialize<T>(action.CopyPayload(), jsonOptions ?? DefaultOptions)
            ?? throw new JsonException("External action JSON must contain a non-null value.");
    }
}
