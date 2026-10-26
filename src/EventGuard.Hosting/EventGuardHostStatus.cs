namespace EventGuard.Hosting;

/// <summary>Lets the application choose its exit code or health response after a supervised loop fails.</summary>
public sealed class EventGuardHostStatus
{
    private EventGuardWorkerFailure? _failure;
    public EventGuardWorkerFailure? Failure => Volatile.Read(ref _failure);
    internal void Fail(string name, Exception error) =>
        Interlocked.CompareExchange(ref _failure, new(name, error.GetType().Name), null);
}

// Exception messages/inner exceptions are deliberately excluded from the public status.
public sealed record EventGuardWorkerFailure(string WorkerName, string ErrorType);
