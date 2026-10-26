using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EventGuard.Hosting;

internal sealed class EventGuardHostedService(IServiceProvider services, IEnumerable<WorkerRegistration> registrations,
    IHostApplicationLifetime lifetime, ILogger<EventGuardHostedService> logger, EventGuardHostStatus status) : BackgroundService
{
    private readonly WorkerRegistration[] _workers = registrations.ToArray();

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        if (_workers.Length == 0) throw new InvalidOperationException("AddEventGuard requires at least one consumer or worker.");
        // Validate every dependency before any EventGuard worker starts claiming or acknowledging input.
        foreach (var worker in _workers)
        {
            try { await worker.Validate(services, cancellationToken); }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
            catch (Exception error)
            {
                // Provider exception messages can contain SQL, payload or connection details.
                throw new InvalidOperationException($"EventGuard worker '{worker.Name}' failed startup validation ({error.GetType().Name}). Check DI registrations, schema migration, permissions and required capabilities.");
            }
        }
        await base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var shutdown = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        await Task.WhenAll(_workers.Select(SuperviseAsync));

        async Task SuperviseAsync(WorkerRegistration worker)
        {
            try
            {
                shutdown.Token.ThrowIfCancellationRequested();
                await worker.Run(services, shutdown.Token);
                if (!shutdown.IsCancellationRequested)
                    throw new InvalidOperationException("A worker returned before shutdown.");
            }
            catch (OperationCanceledException) when (shutdown.IsCancellationRequested) { }
            catch (Exception error)
            {
                status.Fail(worker.Name, error);
                logger.LogCritical("EventGuard worker {Worker} stopped unexpectedly ({ErrorType}). Stopping the host.", worker.Name, error.GetType().Name);
                shutdown.Cancel();
                lifetime.StopApplication();
                throw new InvalidOperationException($"EventGuard worker '{worker.Name}' failed ({error.GetType().Name}).");
            }
        }
    }
}
