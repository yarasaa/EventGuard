using System.Globalization;
using System.Security.Claims;
using System.Text.Json;
using EventGuard.Operations;
using Microsoft.AspNetCore.DataProtection;
namespace EventGuard.Dashboard;

public sealed record AuditTarget(string Kind, string Key, string Label);
public sealed record DashboardAuditPage(AuditTarget Target, string From, string Until, ReadSnapshot<AuditPage> Snapshot,
    string CurrentCursor, string? NextCursor, string? PreviousCursor);
public sealed class DashboardAudit(DashboardOptions options, DashboardQueries consumers, DashboardOutboxQueries producers,
    OperationsReader reader, IDataProtectionProvider protection)
{
    private readonly IDataProtector _cursors = protection.CreateProtector("EventGuard.Audit.Cursor.v1", options.StoreId, options.Provider, options.Schema);
    public IReadOnlyList<AuditTarget> Allowed(ClaimsPrincipal user)
    {
        if (!options.AuditEnabled || user.Identity?.IsAuthenticated != true) return [];
        return consumers.AllowedScopes(user).Where(s => s.AllowAudit && user.HasClaim("eventguard_audit", options.StoreId + "/" + s.Key))
            .Select(s => new AuditTarget("consumer", s.Key, s.Label)).Concat(producers.AllowedScopes(user)
                .Where(s => s.AllowAudit && user.HasClaim("eventguard_outbox_audit", options.StoreId + "/" + s.Key))
                .Select(s => new AuditTarget("outbox", s.Key, s.Label))).ToArray();
    }
    public async Task<DashboardAuditPage> ListAsync(ClaimsPrincipal user, string kind, string key, string? from, string? until, string? cursor, CancellationToken ct)
    {
        var target = Allowed(user).FirstOrDefault(s => s.Kind == kind && s.Key == key) ?? throw new ScopeNotAvailableException();
        var scope = kind == "consumer" ? consumers.Resolve(user, key).ToWorkScope().Id : producers.Resolve(user, key).ToProducerScope().Id;
        Cursor saved;
        try
        {
            if (cursor?.Length > 2048) throw new InvalidQueryException();
            var now = DateTimeOffset.FromUnixTimeSeconds(DateTimeOffset.UtcNow.ToUnixTimeSeconds());
            var end = Parse(until, now); var start = Parse(from, end.AddDays(-1));
            saved = string.IsNullOrEmpty(cursor) ? new(scope, kind, start, end, null, false, now.AddMinutes(15))
                : JsonSerializer.Deserialize<Cursor>(_cursors.Unprotect(cursor))!;
            if (saved.Scope != scope || saved.Kind != kind || saved.From != start || saved.Until != end || saved.Expires <= now
                || saved.Reverse && saved.Position is null || end > now.AddSeconds(1)) throw new InvalidQueryException();
        }
        catch { throw new InvalidQueryException(); }
        AuditQuery query;
        try
        {
            query = kind == "consumer" ? new(consumers.Resolve(user, key).ToWorkScope(), saved.From, saved.Until, 100, saved.Reverse ? null : saved.Position, saved.Reverse ? saved.Position : null)
                : new(producers.Resolve(user, key).ToProducerScope(), saved.From, saved.Until, 100, saved.Reverse ? null : saved.Position, saved.Reverse ? saved.Position : null);
        }
        catch (ArgumentException) { throw new InvalidQueryException(); }
        var result = await reader.ListAuditAsync(query, ct);
        string Encode(Cursor state) => _cursors.Protect(JsonSerializer.Serialize(state));
        return new(target, Format(saved.From), Format(saved.Until), result, Encode(saved),
            result.Value.Next is { } next ? Encode(saved with { Position = next, Reverse = false }) : null,
            result.Value.Previous is { } previous ? Encode(saved with { Position = previous, Reverse = true }) : null);
    }
    private const string DateFormat = "yyyy-MM-dd'T'HH:mm:ss";
    private static string Format(DateTimeOffset date) => date.UtcDateTime.ToString(DateFormat, CultureInfo.InvariantCulture);
    private static DateTimeOffset Parse(string? value, DateTimeOffset fallback) => string.IsNullOrEmpty(value) ? fallback :
        DateTimeOffset.TryParseExact(value, [DateFormat, "yyyy-MM-dd'T'HH:mm"], CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsed)
            ? parsed : throw new InvalidQueryException();
    private sealed record Cursor(string Scope, string Kind, DateTimeOffset From, DateTimeOffset Until, AuditPosition? Position, bool Reverse, DateTimeOffset Expires);
}
