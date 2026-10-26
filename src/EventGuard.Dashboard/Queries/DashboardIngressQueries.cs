using System.Security.Claims;
using System.Text.Json;
using EventGuard.Operations;
using Microsoft.AspNetCore.DataProtection;
namespace EventGuard.Dashboard;

public sealed record DashboardIngressPage(ScopeRegistration Scope, bool? Released, Guid? Id,
    ReadSnapshot<IngressPage> Snapshot, string? NextCursor, string? PreviousCursor);
public sealed class DashboardIngressQueries(DashboardOptions options, DashboardQueries scopes,
    OperationsReader reader, IDataProtectionProvider protection)
{
    private readonly IDataProtector _cursor = protection.CreateProtector("EventGuard.Ingress.Cursor.v1", options.StoreId, options.Provider, options.Schema);
    public ScopeRegistration Resolve(ClaimsPrincipal user, string? key)
    {
        if (!options.IngressMonitoringEnabled) throw new ScopeNotAvailableException();
        return scopes.Resolve(user, key);
    }
    public async Task<DashboardIngressPage> ListAsync(ClaimsPrincipal user, string? key, string? state, string? id, string? cursor, CancellationToken ct)
    {
        var scope = Resolve(user, key); // Authorize before parsing IDs, cursors or querying storage.
        bool? released = state switch { null or "" => null, "Held" => false, "Released" => true, _ => throw new InvalidQueryException() };
        Guid? lookup = string.IsNullOrEmpty(id) ? null : Guid.TryParse(id, out var value) && value != Guid.Empty ? value : throw new InvalidQueryException();
        Cursor saved;
        try
        {
            if (cursor?.Length > 2048) throw new InvalidQueryException();
            saved = cursor is null ? new(scope.ToWorkScope().Id, released, lookup, DateTimeOffset.UtcNow, null, false, DateTimeOffset.UtcNow.AddMinutes(15))
                : JsonSerializer.Deserialize<Cursor>(_cursor.Unprotect(cursor))!;
            if (saved.ScopeId != scope.ToWorkScope().Id || saved.Released != released || saved.Id != lookup || saved.ExpiresAt <= DateTimeOffset.UtcNow
                || saved.Reverse && saved.Position is null) throw new InvalidQueryException();
        }
        catch (Exception) { throw new InvalidQueryException(); }
        IngressQuery query;
        try { query = new(scope.ToWorkScope(), saved.Cutoff, released, 25, saved.Reverse ? null : saved.Position, saved.Reverse ? saved.Position : null, lookup); }
        catch (ArgumentException) { throw new InvalidQueryException(); }
        var result = await reader.ListIngressAsync(query, ct);
        string Encode(WorkPosition position, bool reverse) => _cursor.Protect(JsonSerializer.Serialize(saved with { Position = position, Reverse = reverse }));
        return new(scope, released, lookup, result, result.Value.Next is { } next ? Encode(next, false) : null,
            result.Value.Previous is { } previous ? Encode(previous, true) : null);
    }
    public Task<ReadSnapshot<IngressSummary?>> FindAsync(ClaimsPrincipal user, string key, Guid id, CancellationToken ct)
    {
        var scope = Resolve(user, key);
        if (id == Guid.Empty) throw new InvalidQueryException();
        return reader.FindIngressAsync(scope.ToWorkScope(), id, ct);
    }
    private sealed record Cursor(string ScopeId, bool? Released, Guid? Id, DateTimeOffset Cutoff, WorkPosition? Position, bool Reverse, DateTimeOffset ExpiresAt);
}
