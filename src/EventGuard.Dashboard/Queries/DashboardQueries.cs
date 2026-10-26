using System.Security.Claims;
using System.Text.Json;
using EventGuard.Operations;
using Microsoft.AspNetCore.DataProtection;

namespace EventGuard.Dashboard;

public sealed record DashboardPage(ScopeRegistration Scope, WorkState? State, ReadSnapshot<WorkPage> Snapshot,
    string? NextCursor, string? PreviousCursor, string CurrentCursor, WorkFilters Filters);

public sealed class DashboardQueries(DashboardOptions options, OperationsReader reader, IDataProtectionProvider protection)
{
    private readonly IDataProtector _cursor = protection.CreateProtector("EventGuard.Operations.Cursor.v2", options.StoreId, options.Provider, options.Schema);

    public IReadOnlyList<ScopeRegistration> AllowedScopes(ClaimsPrincipal user) => options.Scopes
        .Where(s => user.Identity?.IsAuthenticated == true && user.HasClaim("eventguard_scope", options.StoreId + "/" + s.Key)).ToArray();

    public ScopeRegistration Resolve(ClaimsPrincipal user, string? key) =>
        AllowedScopes(user).FirstOrDefault(s => key is null || s.Key == key) ?? throw new ScopeNotAvailableException();

    public bool CanReadData(ClaimsPrincipal user, ScopeRegistration scope) => scope.DataFields.Length > 0
        && user.Identity?.IsAuthenticated == true
        && user.HasClaim("eventguard_scope", options.StoreId + "/" + scope.Key)
        && user.HasClaim("eventguard_data", options.StoreId + "/" + scope.Key);

    public async Task<DashboardPage> ListAsync(ClaimsPrincipal user, string? key, string? state, string? cursor, CancellationToken ct, WorkFilters? filters = null)
    {
        var scope = Resolve(user, key);
        filters ??= new();
        WorkState? filter = null;
        if (!string.IsNullOrEmpty(state))
        {
            if (!Enum.TryParse<WorkState>(state, false, out var parsed) || !Enum.IsDefined(parsed)) throw new InvalidQueryException();
            filter = parsed;
        }
        var saved = cursor is null
            ? new PageCursor(scope.ToWorkScope().Id, filter, DateTimeOffset.UtcNow, null, false, DateTimeOffset.UtcNow.AddMinutes(15), filters)
            : Decode(scope, cursor);
        if (saved.State != filter || saved.Filters != filters) throw new InvalidQueryException();
        var snapshot = await reader.ListAsync(Query(scope, saved), ct);
        string Encode(WorkPosition? position, bool reverse) => _cursor.Protect(JsonSerializer.Serialize(saved with { Position = position, Reverse = reverse }));
        return new(scope, filter, snapshot,
            snapshot.Value.Next is { } next ? Encode(next, false) : null,
            snapshot.Value.Previous is { } previous ? Encode(previous, true) : null,
            Encode(saved.Position, saved.Reverse), filters);
    }

    // Return destinations are fixed local routes, never caller-supplied URLs.
    public string ListLocation(ClaimsPrincipal user, string key, string? returnTo)
    {
        var scope = Resolve(user, key);
        var route = "/?scope=" + Uri.EscapeDataString(scope.Key);
        if (returnTo is null) return route;
        var saved = Decode(scope, returnTo);
        _ = Query(scope, saved);
        var values = FilterInput.Routes(scope.Key, saved.State, saved.Filters);
        values["cursor"] = returnTo;
        return "/" + QueryString.Create(values.Select(p => new KeyValuePair<string, string?>(p.Key, p.Value)));
    }

    private PageCursor Decode(ScopeRegistration scope, string cursor)
    {
        try
        {
            if (cursor.Length > 2048) throw new InvalidQueryException();
            var saved = JsonSerializer.Deserialize<PageCursor>(_cursor.Unprotect(cursor))!;
            if (saved.ScopeId != scope.ToWorkScope().Id || saved.ExpiresAt <= DateTimeOffset.UtcNow
                || saved.Reverse && saved.Position is null) throw new InvalidQueryException();
            return saved;
        }
        catch (Exception) { throw new InvalidQueryException(); }
    }

    private static WorkQuery Query(ScopeRegistration scope, PageCursor saved)
    {
        try { return new(scope.ToWorkScope(), saved.Cutoff, saved.State, 25, saved.Reverse ? null : saved.Position, saved.Reverse ? saved.Position : null, saved.Filters); }
        catch (ArgumentException) { throw new InvalidQueryException(); }
    }

    public Task<ReadSnapshot<WorkDetail?>> FindAsync(ClaimsPrincipal user, string key, Guid id, CancellationToken ct)
    {
        var scope = Resolve(user, key);
        if (id == Guid.Empty) throw new InvalidQueryException();
        return reader.FindAsync(scope.ToWorkScope(), id, ct);
    }

    public Task<ReadSnapshot<WorkData?>> ReadDataAsync(ClaimsPrincipal user, string key, Guid id, CancellationToken ct)
    {
        var scope = Resolve(user, key);
        if (!CanReadData(user, scope)) throw new ScopeNotAvailableException();
        if (id == Guid.Empty) throw new InvalidQueryException();
        return reader.ReadDataAsync(scope.ToWorkScope(), id, scope.DataSelection(), ct);
    }

    private sealed record PageCursor(string ScopeId, WorkState? State, DateTimeOffset Cutoff,
        WorkPosition? Position, bool Reverse, DateTimeOffset ExpiresAt, WorkFilters Filters);
}

public sealed class ScopeNotAvailableException : Exception;
public sealed class InvalidQueryException : Exception;
