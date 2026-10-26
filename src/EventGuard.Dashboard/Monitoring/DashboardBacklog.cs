using EventGuard.Operations;
using System.Security.Claims;

namespace EventGuard.Dashboard;

public sealed record BacklogTarget(string Key, string Label, BacklogKind Kind, string ScopeKey);
public sealed record BacklogView(BacklogTarget Target, bool Available, ReadSnapshot<BacklogSummary>? Snapshot);

public sealed class DashboardBacklog(DashboardOptions options, DashboardQueries works, DashboardOutboxQueries outbox,
    OperationsReader reader, TimeProvider time, IHostApplicationLifetime lifetime)
{
    private sealed class Entry { public Task<BacklogView>? Pending; public BacklogView? Last; public long Checked; }
    // At most the configured 100 consumer + 100 producer entries; request text cannot expand this cache.
    private readonly Dictionary<string, Entry> _cache = new(StringComparer.Ordinal);
    private readonly object _gate = new();
    public IReadOnlyList<BacklogTarget> Allowed(ClaimsPrincipal user) => !options.BacklogEnabled ? [] :
        works.AllowedScopes(user).Select(s => new BacklogTarget("consumer:" + s.Key, s.Label, BacklogKind.Consumer, s.Key))
            .Concat(outbox.AllowedScopes(user).Select(s => new BacklogTarget("outbox:" + s.Key, s.Label, BacklogKind.Outbox, s.Key))).ToArray();

    public Task<BacklogView> ReadAsync(ClaimsPrincipal user, string? key, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var target = Allowed(user).FirstOrDefault(t => key is null || t.Key == key) ?? throw new ScopeNotAvailableException();
        var query = target.Kind == BacklogKind.Consumer ? BacklogQuery.Consumer(works.Resolve(user, target.ScopeKey).ToWorkScope())
            : BacklogQuery.Outbox(outbox.Resolve(user, target.ScopeKey).ToProducerScope());
        Task<BacklogView> pending;
        lock (_gate)
        {
            if (!_cache.TryGetValue(target.Key, out var entry)) _cache.Add(target.Key, entry = new());
            if (entry.Last is not null && time.GetElapsedTime(entry.Checked) < TimeSpan.FromSeconds(15)) return Task.FromResult(entry.Last);
            if (entry.Pending is not { IsCompleted: false }) entry.Pending = ProbeAsync(entry, target, query);
            pending = entry.Pending;
        }
        return pending.WaitAsync(ct);
    }
    private async Task<BacklogView> ProbeAsync(Entry entry, BacklogTarget target, BacklogQuery query)
    {
        BacklogView result;
        try { result = new(target, true, await reader.ReadBacklogAsync(query, lifetime.ApplicationStopping)); }
        catch { result = new(target, false, null); }
        lock (_gate) { entry.Last = result; entry.Checked = time.GetTimestamp(); }
        return result;
    }
}
