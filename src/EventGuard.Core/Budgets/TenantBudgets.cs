using System.Globalization;
namespace EventGuard;

public enum TenantBudgetKind { Claim, RetryClaim, Recovery }
public enum TenantBudgetStatus { Applied, VersionConflict, RequestConflict }
public sealed record TenantBudgetPolicy
{
    public string Tenant { get; }
    public string Id { get; }
    public int WindowSeconds { get; }
    public long? MaximumClaims { get; }
    public long? MaximumRetries { get; }
    public long? MaximumRecoveries { get; }
    public TenantBudgetPolicy(string tenant, int windowSeconds = 60, long? maximumClaims = null,
        long? maximumRetries = null, long? maximumRecoveries = null)
    {
        Tenant = IdentityRules.Required(tenant, nameof(tenant)); Id = StableHash.Of("tenant-budget/v1", Tenant);
        if (windowSeconds is < 1 or > 86400 || maximumClaims is < 0 or > 1_000_000_000
            || maximumRetries is < 0 or > 1_000_000_000 || maximumRecoveries is < 0 or > 1_000_000_000)
            throw new ArgumentException("Use a 1–86400 second window and limits between zero and one billion, or null for unlimited.");
        WindowSeconds = windowSeconds; MaximumClaims = maximumClaims; MaximumRetries = maximumRetries; MaximumRecoveries = maximumRecoveries;
    }
}
public sealed record TenantBudgetSnapshot(TenantBudgetPolicy Policy, long Version, DateTimeOffset WindowStartedAt,
    long Claims, long Retries, long Recoveries);
public sealed record TenantBudgetDecision(bool Allowed, TenantBudgetSnapshot After);
public sealed record TenantBudgetResult(TenantBudgetStatus Status, long Version, bool RepeatedRequest = false);
public sealed record TenantBudgetConfiguration(TenantBudgetResult Result, TenantBudgetSnapshot? After);
public sealed class TenantBudgetRequest
{
    public Guid RequestId { get; }
    public TenantBudgetPolicy Policy { get; }
    // -1 means create only; existing policies start at version zero.
    public long ExpectedVersion { get; }
    public string Actor { get; }
    public string Reason { get; }
    public string Fingerprint { get; }
    public TenantBudgetRequest(Guid requestId, TenantBudgetPolicy policy, long expectedVersion, string actor, string reason)
    {
        Policy = policy ?? throw new ArgumentNullException(nameof(policy));
        if (requestId == Guid.Empty || expectedVersion < -1) throw new ArgumentException("Specify a stable request ID and expected policy version.");
        RequestId = requestId; ExpectedVersion = expectedVersion;
        Actor = IdentityRules.Required(actor, nameof(actor)); Reason = IdentityRules.Required(reason, nameof(reason));
        static string Number(long? value) => value?.ToString(CultureInfo.InvariantCulture) ?? "unlimited";
        Fingerprint = StableHash.Of("tenant-budget-request/v1", policy.Id, Number(expectedVersion), Number(policy.WindowSeconds),
            Number(policy.MaximumClaims), Number(policy.MaximumRetries), Number(policy.MaximumRecoveries), Actor, Reason);
    }
}
public interface ITenantBudgetStore
{
    Task<TenantBudgetResult> ConfigureBudgetAsync(TenantBudgetRequest request, CancellationToken cancellationToken = default);
    Task<TenantBudgetSnapshot?> FindBudgetAsync(string tenant, CancellationToken cancellationToken = default);
}

/// <summary>Store-wide fixed-window admission. Native providers lock and persist the counter with the admitted transition.</summary>
public static class TenantBudgets
{
    public static DateTimeOffset Window(DateTimeOffset now, int seconds)
    {
        if (seconds is < 1 or > 86400 || now < DateTimeOffset.UnixEpoch) throw new ArgumentOutOfRangeException(nameof(seconds));
        return DateTimeOffset.FromUnixTimeSeconds(now.ToUnixTimeSeconds() / seconds * seconds);
    }
    private static TenantBudgetSnapshot Normalize(TenantBudgetSnapshot current, DateTimeOffset now)
    {
        if (current.Version < 0 || current.Claims < 0 || current.Retries < 0 || current.Recoveries < 0 || current.Retries > current.Claims)
            throw new InvalidOperationException("Invalid durable tenant counters.");
        var window = Window(now, current.Policy.WindowSeconds);
        // A backward clock adjustment must not create a second allowance in an already-used window.
        return window > current.WindowStartedAt ? current with { WindowStartedAt = window, Claims = 0, Retries = 0, Recoveries = 0 } : current;
    }
    public static TenantBudgetDecision Admit(TenantBudgetSnapshot current, TenantBudgetKind kind, DateTimeOffset databaseNow)
    {
        ArgumentNullException.ThrowIfNull(current);
        if (!Enum.IsDefined(kind)) throw new ArgumentOutOfRangeException(nameof(kind));
        var normalized = Normalize(current, databaseNow); var policy = normalized.Policy;
        var allowed = kind == TenantBudgetKind.Recovery ? policy.MaximumRecoveries is null || normalized.Recoveries < policy.MaximumRecoveries
            : (policy.MaximumClaims is null || normalized.Claims < policy.MaximumClaims)
                && (kind != TenantBudgetKind.RetryClaim || policy.MaximumRetries is null || normalized.Retries < policy.MaximumRetries);
        if (!allowed) return new(false, normalized);
        return new(true, kind == TenantBudgetKind.Recovery ? normalized with { Recoveries = checked(normalized.Recoveries + 1) }
            : normalized with { Claims = checked(normalized.Claims + 1), Retries = checked(normalized.Retries + (kind == TenantBudgetKind.RetryClaim ? 1 : 0)) });
    }
    public static TenantBudgetConfiguration Configure(TenantBudgetRequest request, TenantBudgetSnapshot? current, DateTimeOffset databaseNow)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (current is not null && (current.Policy.Id != request.Policy.Id || current.Policy.Tenant != request.Policy.Tenant))
            throw new InvalidOperationException("Tenant budget identity mismatch.");
        if (request.ExpectedVersion != (current?.Version ?? -1)) return new(new(TenantBudgetStatus.VersionConflict, current?.Version ?? -1), null);
        var after = current is null ? new TenantBudgetSnapshot(request.Policy, 0, Window(databaseNow, request.Policy.WindowSeconds), 0, 0, 0)
            : Normalize(current with { Policy = request.Policy, Version = checked(current.Version + 1) }, databaseNow);
        return new(new(TenantBudgetStatus.Applied, after.Version), after);
    }
    public static TenantBudgetResult Repeat(TenantBudgetRequest request, string fingerprint, TenantBudgetResult previous) =>
        fingerprint == request.Fingerprint ? previous with { RepeatedRequest = true } : new(TenantBudgetStatus.RequestConflict, previous.Version);
    public static ReplayDecision AdmitReplay(ReplayDecision decision, bool allowed) => allowed || decision.Change is null ? decision
        : new(decision.Result with { Status = ReplayStatus.RateLimited, Version = decision.Change.Before.Version, Generation = decision.Change.Before.ReplayGeneration }, null);
    public static OutboxReplayDecision AdmitReplay(OutboxReplayDecision decision, bool allowed) => allowed || decision.After is null ? decision
        : new(decision.Result with { Status = OutboxReplayStatus.RateLimited, Version = decision.Result.Version - 1, Generation = decision.Result.Generation - 1 }, null);
    public static OutboxRepairDecision AdmitRepair(OutboxRepairDecision decision, bool allowed) => allowed || decision.After is null ? decision
        : new(decision.Result with { Status = OutboxRepairStatus.RateLimited, Version = decision.Result.Version - 1, Generation = decision.Result.Generation - 1 }, null);
}
