namespace EventGuard.Store.Conformance;

public interface IOutboxStoreFixture
{
    IOutboxStore Outbox { get; }
    IOutboxRecoveryStore Recovery { get; }
    IOutboxRecoveryStore RestartRecovery();
    Task EnqueueAsync(OutgoingMessage message);
}
