namespace EventGuard.Operations;

/// <summary>Optional bounded dependency validation. It does not assert worker/broker health or future commit success.</summary>
public interface IStoreHealthCheck
{
    Task CheckHealthAsync(bool consumer, bool outbox, CancellationToken ct = default);
}
