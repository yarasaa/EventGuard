using System.Diagnostics;

namespace EventGuard.Runtime;

internal sealed class ExternalPermitMaintenance(IExternalCallLimiter? limiter, ExternalCallLimit? limit, WorkPumpOptions options)
{
    private long _next;
    public async Task RunAsync(CancellationToken ct)
    {
        if (limiter is not IExternalCallPermitMaintenance maintenance || options.ExternalPermitRetention is not { } retention) return;
        var now = Stopwatch.GetTimestamp();
        var prior = Interlocked.Read(ref _next);
        if (now < prior || Interlocked.CompareExchange(ref _next,
                now + (long)(options.ExternalPermitCleanupInterval.TotalSeconds * Stopwatch.Frequency), prior) != prior) return;
        try
        {
            // Drain a retention backlog one bounded page per poll; a fixed interval after every full
            // page would cap reclamation below a busy service's receipt creation rate.
            if (await maintenance.PruneCallPermitsAsync(limit!, retention, 1000, ct) == 1000)
                Interlocked.Exchange(ref _next, 0);
        }
        catch { Interlocked.Exchange(ref _next, 0); throw; }
    }
}
