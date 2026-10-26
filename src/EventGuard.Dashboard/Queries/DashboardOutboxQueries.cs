using System.Security.Claims;
using System.Text.Json;
using EventGuard.Operations;
using Microsoft.AspNetCore.DataProtection;

namespace EventGuard.Dashboard;

public sealed record DashboardOutboxPage(OutboxScopeRegistration Scope, OutboxState? State, ReadSnapshot<OutboxPage> Snapshot,
    string? NextCursor, string? PreviousCursor, string CurrentCursor, OutboxFilters Filters, bool Quarantine);

public sealed class DashboardOutboxQueries(DashboardOptions options, OperationsReader reader, IDataProtectionProvider protection)
{
    private readonly IDataProtector _cursor = protection.CreateProtector("EventGuard.Operations.Outbox.Cursor.v1", options.StoreId, options.Provider, options.Schema);

    public IReadOnlyList<OutboxScopeRegistration> AllowedScopes(ClaimsPrincipal user) => options.OutboxScopes
        .Where(s => user.Identity?.IsAuthenticated == true && user.HasClaim("eventguard_outbox", options.StoreId + "/" + s.Key)).ToArray();

    public OutboxScopeRegistration Resolve(ClaimsPrincipal user, string? key) =>
        AllowedScopes(user).FirstOrDefault(s => key is null || s.Key == key) ?? throw new ScopeNotAvailableException();

    public async Task<DashboardOutboxPage> ListAsync(ClaimsPrincipal user, string? key, string? state, string? cursor, CancellationToken ct, OutboxFilters? filters = null, bool quarantine = false)
    {
        var scope = Resolve(user, key);
        filters ??= new();
        OutboxState? filter = null;
        if (!string.IsNullOrEmpty(state))
        {
            if (!Enum.TryParse<OutboxState>(state, false, out var parsed) || !Enum.IsDefined(parsed)) throw new InvalidQueryException();
            filter = parsed;
        }
        if (quarantine)
        {
            if (filter.HasValue && filter != OutboxState.Quarantined) throw new InvalidQueryException();
            filter = OutboxState.Quarantined;
        }
        var saved = cursor is null
            ? new PageCursor(scope.ToProducerScope().Id, filter, DateTimeOffset.UtcNow, null, false, DateTimeOffset.UtcNow.AddMinutes(15), filters, quarantine)
            : Decode(scope, cursor);
        if (saved.State != filter || saved.Filters != filters || saved.Quarantine != quarantine) throw new InvalidQueryException();
        var snapshot = await reader.ListOutboxAsync(Query(scope, saved), ct);
        string Encode(OutboxPosition? position, bool reverse) => _cursor.Protect(JsonSerializer.Serialize(saved with { Position = position, Reverse = reverse }));
        return new(scope, filter, snapshot,
            snapshot.Value.Next is { } next ? Encode(next, false) : null,
            snapshot.Value.Previous is { } previous ? Encode(previous, true) : null,
            Encode(saved.Position, saved.Reverse), filters, quarantine);
    }

    // Return destinations are fixed local routes, never caller-supplied URLs.
    public string ListLocation(ClaimsPrincipal user, string key, string? returnTo)
    {
        var scope = Resolve(user, key);
        if (returnTo is null) return OutboxFilterInput.Location(scope.Key, null, new());
        var saved = Decode(scope, returnTo);
        _ = Query(scope, saved);
        return OutboxFilterInput.Location(scope.Key, saved.State, saved.Filters, returnTo, saved.Quarantine);
    }

    private PageCursor Decode(OutboxScopeRegistration scope, string cursor)
    {
        try
        {
            if (cursor.Length > 2048) throw new InvalidQueryException();
            var saved = JsonSerializer.Deserialize<PageCursor>(_cursor.Unprotect(cursor))!;
            if (saved.ScopeId != scope.ToProducerScope().Id || saved.ExpiresAt <= DateTimeOffset.UtcNow
                || saved.Reverse && saved.Position is null) throw new InvalidQueryException();
            return saved;
        }
        catch (Exception) { throw new InvalidQueryException(); }
    }

    private static OutboxQuery Query(OutboxScopeRegistration scope, PageCursor saved)
    {
        try { return new(scope.ToProducerScope(), saved.Cutoff, saved.State, 25, saved.Reverse ? null : saved.Position, saved.Reverse ? saved.Position : null, saved.Filters); }
        catch (ArgumentException) { throw new InvalidQueryException(); }
    }

    public Task<ReadSnapshot<OutboxDetail?>> FindAsync(ClaimsPrincipal user, string key, Guid id, CancellationToken ct)
    {
        var scope = Resolve(user, key);
        if (id == Guid.Empty) throw new InvalidQueryException();
        return reader.FindOutboxAsync(scope.ToProducerScope(), id, ct);
    }

    private sealed record PageCursor(string ScopeId, OutboxState? State, DateTimeOffset Cutoff,
        OutboxPosition? Position, bool Reverse, DateTimeOffset ExpiresAt, OutboxFilters Filters, bool Quarantine);
}
