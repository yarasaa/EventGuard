using System.Security.Claims;
using System.Text.Json;
using EventGuard.Operations;
using Microsoft.AspNetCore.DataProtection;

namespace EventGuard.Dashboard;

public sealed record ReplayPreview(WorkSummary Work, string? Ticket, string Explanation);
public sealed record ReplayReview(Guid RequestId, Guid WorkId, long ExpectedVersion, string Reason, string Ticket);

public sealed class DashboardReplays(DashboardOptions options, DashboardQueries queries,
    OperationsReplayer commands, IDataProtectionProvider protection)
{
    private readonly IDataProtector _drafts = protection.CreateProtector("EventGuard.Replay.Draft.v1", options.StoreId, options.Provider, options.Schema);
    private readonly IDataProtector _commands = protection.CreateProtector("EventGuard.Replay.Command.v1", options.StoreId, options.Provider, options.Schema);

    public bool CanReplay(ClaimsPrincipal user, ScopeRegistration scope) => options.ReplayEnabled && scope.AllowReplay
        && user.Identity?.IsAuthenticated == true
        && user.HasClaim("eventguard_scope", options.StoreId + "/" + scope.Key)
        && user.HasClaim("eventguard_replay", options.StoreId + "/" + scope.Key)
        && OperatorIdentity.Actor(user) is not null;

    public ScopeRegistration Authorize(ClaimsPrincipal user, string key)
    {
        var scope = queries.Resolve(user, key);
        if (!CanReplay(user, scope)) throw new ScopeNotAvailableException();
        return scope;
    }

    public async Task<ReplayPreview?> PreviewAsync(ClaimsPrincipal user, string key, Guid id, CancellationToken ct)
    {
        var scope = Authorize(user, key);
        var detail = await queries.FindAsync(user, key, id, ct);
        if (detail.Value is null) return null;
        var work = detail.Value.Work;
        if (work.State != WorkState.DeadLetter) return new(work, null, "Only dead-letter work can be replayed. Completed and active work cannot be reopened.");
        if (work.ExpiresAt <= DateTimeOffset.UtcNow) return new(work, null, "The event processing window has expired. Replay does not extend it.");
        var draft = new Ticket(scope.ToWorkScope().Id, id, Guid.NewGuid(), work.Version, OperatorIdentity.Actor(user)!, "", DateTimeOffset.UtcNow.AddMinutes(15));
        return new(work, _drafts.Protect(JsonSerializer.Serialize(draft)), "Eligible at the time of this read. The database will check status, version and expiry again when the request is submitted.");
    }

    public ReplayReview Review(ClaimsPrincipal user, string key, Guid id, string ticket, string reason)
    {
        var scope = Authorize(user, key);
        var draft = Decode(_drafts, ticket, scope, id, user);
        try { _ = new ReplayRequest(scope.ToWorkScope(), draft.RequestId, id, draft.Version, draft.Actor, reason); }
        catch (ArgumentException) { throw new InvalidReplayException("Enter a reason of up to 1,024 UTF-8 bytes."); }
        var command = draft with { Reason = reason, ExpiresAt = DateTimeOffset.UtcNow.AddHours(24) };
        return new(command.RequestId, id, command.Version, reason, _commands.Protect(JsonSerializer.Serialize(command)));
    }

    public ReplayReview ReadCommand(ClaimsPrincipal user, string key, Guid id, string ticket)
    {
        var scope = Authorize(user, key);
        var command = Decode(_commands, ticket, scope, id, user);
        return new(command.RequestId, id, command.Version, command.Reason, ticket);
    }

    public Task<ReplayResult> SubmitAsync(ClaimsPrincipal user, string key, Guid id, string ticket, CancellationToken ct)
    {
        var scope = Authorize(user, key);
        var command = Decode(_commands, ticket, scope, id, user);
        // Never reject based on a fresh preview here: the durable receipt must win on repeated requests.
        return commands.ReplayAsync(new(scope.ToWorkScope(), command.RequestId, id, command.Version, command.Actor, command.Reason), ct);
    }

    public Task<ReplayReceipt?> FindReceiptAsync(ClaimsPrincipal user, string key, Guid id, Guid requestId, CancellationToken ct)
    {
        var scope = Authorize(user, key);
        if (id == Guid.Empty || requestId == Guid.Empty) throw new InvalidReplayException("Invalid work or request ID.");
        return commands.FindReceiptAsync(scope.ToWorkScope(), id, requestId, OperatorIdentity.Actor(user)!, ct);
    }

    private static Ticket Decode(IDataProtector protector, string value, ScopeRegistration scope, Guid id, ClaimsPrincipal user)
    {
        try
        {
            if (string.IsNullOrEmpty(value) || value.Length > 12_288) throw new InvalidReplayException("");
            var ticket = JsonSerializer.Deserialize<Ticket>(protector.Unprotect(value))!;
            if (ticket.ScopeId != scope.ToWorkScope().Id || ticket.WorkId != id || ticket.Actor != OperatorIdentity.Actor(user)
                || ticket.RequestId == Guid.Empty || ticket.WorkId == Guid.Empty || ticket.Version < 0 || ticket.ExpiresAt <= DateTimeOffset.UtcNow)
                throw new InvalidReplayException("");
            return ticket;
        }
        catch (Exception) { throw new InvalidReplayException("Invalid or expired replay form. Check the saved request status before preparing another request."); }
    }

    private sealed record Ticket(string ScopeId, Guid WorkId, Guid RequestId, long Version, string Actor, string Reason, DateTimeOffset ExpiresAt);
}

public sealed class InvalidReplayException(string message) : Exception(message);

internal sealed class DisabledReplayStore : IWorkReplayStore
{
    public Task<ReplayResult> ReplayAsync(ReplayRequest request, CancellationToken ct = default) => throw new InvalidOperationException("Replay is disabled.");
    public Task<ReplayReceipt?> FindReceiptAsync(WorkScope scope, Guid workId, Guid requestId, string actor, CancellationToken ct = default) => throw new InvalidOperationException("Replay is disabled.");
}
