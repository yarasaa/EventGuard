using EventGuard.Operations;

namespace EventGuard.Dashboard;

public sealed record DashboardHealthSnapshot(bool MetadataReady, bool? ReplayReady, DateTimeOffset CheckedAt)
{
    public bool Ready => MetadataReady && ReplayReady is not false;
}

// Single-flight, five-second cache. Caller cancellation never cancels another caller's shared probe.
public sealed class DashboardHealth(DashboardOptions options, OperationsReader reader, OperationsReplayer replayer,
    TimeProvider time, IHostApplicationLifetime lifetime)
{
    public bool IsReady(DashboardHealthSnapshot snapshot) => !lifetime.ApplicationStopping.IsCancellationRequested && snapshot.Ready;
    private readonly object _gate = new();
    private Task<DashboardHealthSnapshot>? _probe;
    private DashboardHealthSnapshot? _last;
    private long _checked;

    public Task<DashboardHealthSnapshot> ReadAsync(CancellationToken ct = default)
    {
        Task<DashboardHealthSnapshot> pending;
        lock (_gate)
        {
            if (_last is not null && time.GetElapsedTime(_checked) < TimeSpan.FromSeconds(5)) return Task.FromResult(_last);
            if (_probe is not { IsCompleted: false }) _probe = ProbeAsync();
            pending = _probe;
        }
        return pending.WaitAsync(ct);
    }

    private async Task<DashboardHealthSnapshot> ProbeAsync()
    {
        // Both Operations services keep their admission slot until their provider call has really ended.
        async Task<bool> Check(Func<Task> action)
        {
            try { await action(); return true; }
            catch { return false; }
        }
        var metadata = Check(async () =>
        {
            await reader.CheckHealthAsync(options.Scopes.Length > 0, options.OutboxScopes.Length > 0, lifetime.ApplicationStopping);
            if (options.IngressMonitoringEnabled) await reader.CheckIngressHealthAsync(lifetime.ApplicationStopping);
            if (options.AuditEnabled) await reader.CheckAuditHealthAsync(options.Scopes.Any(s => s.AllowAudit), options.OutboxScopes.Any(s => s.AllowAudit), lifetime.ApplicationStopping);
        });
        var commandsEnabled = options.ReplayEnabled || options.OutboxReplayEnabled || options.RecoveryEnabled;
        var replay = commandsEnabled ? Check(async () =>
        {
            if (options.ReplayEnabled || options.OutboxReplayEnabled) await replayer.CheckHealthAsync(options.ReplayEnabled, options.OutboxReplayEnabled, lifetime.ApplicationStopping);
            if (options.RecoveryEnabled) await replayer.CheckRecoveryHealthAsync(lifetime.ApplicationStopping);
        }) : Task.FromResult(true);
        await Task.WhenAll(metadata, replay);
        var result = new DashboardHealthSnapshot(metadata.Result, commandsEnabled ? replay.Result : null, time.GetUtcNow());
        lock (_gate) { _last = result; _checked = time.GetTimestamp(); }
        return result;
    }
}
