namespace EventGuard.Hosting;

/// <summary>Resolved in a fresh async DI scope for each attempt; use the supplied native transaction.</summary>
public interface IEventGuardHandler<in TMessage, in TSession> where TMessage : notnull
{
    ValueTask HandleAsync(TMessage message, WorkItem work, TSession session, CancellationToken cancellationToken);
}

public interface IEventGuardConsumer<TMessage> where TMessage : notnull
{
    WorkSubmission Map(ReadOnlyMemory<byte> originalJson);
    Task<AcceptanceResult> SubmitAsync(TMessage message, CancellationToken cancellationToken = default);
    Task<AcceptanceResult> SubmitJsonAsync(ReadOnlyMemory<byte> originalJson, CancellationToken cancellationToken = default);
    Task<WorkItem?> FindAsync(Guid workId, CancellationToken cancellationToken = default);
}
