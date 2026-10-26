namespace EventGuard.Store.Conformance;

// Test-only adapter seam. Each provider writes effects through its real transaction/session.
public interface IWorkStoreFixture<TSession>
{
    ITransactionalWorkStore<TSession> Store { get; }
    ITransactionalWorkStore<TSession> RestartStore();
    ValueTask Effect(WorkItem work, ReadOnlyMemory<byte> payload, TSession session, CancellationToken cancellationToken);
    Task<long> EffectCount(Guid workId);

    Task<WorkClaim?> Claim(WorkScope scope, TimeSpan? lease = null) =>
        Store.ClaimNextAsync(scope, Guid.NewGuid(), lease ?? TimeSpan.FromSeconds(10));
}
