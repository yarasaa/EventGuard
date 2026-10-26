using System.Security.Claims;
using System.Text.Json;
using EventGuard.Operations;
using Microsoft.AspNetCore.DataProtection;
namespace EventGuard.Dashboard;

public sealed record ModuleView(ModuleRegistration Registration, ReadSnapshot<ModulePage> Snapshot, int? State, Guid? Id, string? Next, string? Previous);
public sealed class DashboardModules(DashboardOptions options, OperationsReader reader, IDataProtectionProvider protection)
{
    private readonly IDataProtector _cursor = protection.CreateProtector("EventGuard.Modules.v1", options.StoreId, options.Provider, options.Schema);
    private sealed record Cursor(string Key, string ScopeId, ExecutionModule Module, int? State, Guid? Id, Guid? Position, bool Reverse, DateTimeOffset Expires);
    public IReadOnlyList<ModuleRegistration> Allowed(ClaimsPrincipal user) => options.Modules.Where(s => user.Identity?.IsAuthenticated == true
        && user.HasClaim("eventguard_module", options.StoreId + "/" + s.Key)).ToArray();
    public async Task<ModuleView> ReadAsync(ClaimsPrincipal user, string? key, int? state, string? id, string? cursor, CancellationToken ct)
    {
        var registration = Allowed(user).FirstOrDefault(s => key is null || s.Key == key) ?? throw new ScopeNotAvailableException();
        Guid? exact = null;
        if (!string.IsNullOrEmpty(id)) { if (!Guid.TryParse(id, out var parsed) || parsed == Guid.Empty) throw new InvalidQueryException(); exact = parsed; }
        var saved = new Cursor(registration.Key, registration.ScopeId, registration.Module, state, exact, null, false, DateTimeOffset.UtcNow.AddMinutes(15));
        if (cursor is not null)
        {
            try
            {
                if (cursor.Length > 2048) throw new InvalidQueryException();
                var decoded = JsonSerializer.Deserialize<Cursor>(_cursor.Unprotect(cursor))!;
                if (decoded.Key != saved.Key || decoded.ScopeId != saved.ScopeId || decoded.Module != saved.Module || decoded.State != state || decoded.Id != exact || decoded.Expires <= DateTimeOffset.UtcNow) throw new InvalidQueryException();
                saved = decoded;
            }
            catch { throw new InvalidQueryException(); }
        }
        ModuleQuery query;
        try { query = new(registration.Module, registration.ScopeId, state, exact, saved.Position, saved.Reverse); }
        catch (ArgumentException) { throw new InvalidQueryException(); }
        var snapshot = await reader.ReadModuleAsync(query, ct);
        string? Encode(Guid? position, bool reverse) => position is null ? null : _cursor.Protect(JsonSerializer.Serialize(saved with { Position = position, Reverse = reverse }));
        return new(registration, snapshot, state, exact, Encode(snapshot.Value.Next, false), Encode(snapshot.Value.Previous, true));
    }
}
