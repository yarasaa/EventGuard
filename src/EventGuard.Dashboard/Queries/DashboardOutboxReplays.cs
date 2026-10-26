using System.Security.Claims;
using System.Text.Json;
using EventGuard.Operations;
using Microsoft.AspNetCore.DataProtection;

namespace EventGuard.Dashboard;

public sealed record OutboxReplayPreview(OutboxSummary Message, string? Ticket, string Explanation);
public sealed record OutboxReplayReview(Guid RequestId, Guid IntentId, long ExpectedVersion, string Reason, bool AcknowledgePossibleDuplicate, string Ticket);

public sealed class DashboardOutboxReplays(DashboardOptions options, DashboardOutboxQueries queries,
    OperationsReplayer commands, IDataProtectionProvider protection)
{
    private readonly IDataProtector _drafts = protection.CreateProtector("EventGuard.OutboxReplay.Draft.v1", options.StoreId, options.Provider, options.Schema);
    private readonly IDataProtector _commands = protection.CreateProtector("EventGuard.OutboxReplay.Command.v1", options.StoreId, options.Provider, options.Schema);

    public bool CanReplay(ClaimsPrincipal user, OutboxScopeRegistration scope) => options.OutboxReplayEnabled && scope.AllowReplay
        && user.Identity?.IsAuthenticated == true
        && user.HasClaim("eventguard_outbox", options.StoreId + "/" + scope.Key)
        && user.HasClaim("eventguard_outbox_replay", options.StoreId + "/" + scope.Key)
        && OperatorIdentity.Actor(user) is not null;

    public OutboxScopeRegistration Authorize(ClaimsPrincipal user, string key)
    {
        var scope = queries.Resolve(user, key);
        if (!CanReplay(user, scope)) throw new ScopeNotAvailableException();
        return scope;
    }

    public async Task<OutboxReplayPreview?> PreviewAsync(ClaimsPrincipal user, string key, Guid id, CancellationToken ct)
    {
        var scope = Authorize(user, key);
        var detail = await queries.FindAsync(user, key, id, ct);
        if (detail.Value is null) return null;
        var work = detail.Value.Message;
        if (work.State != OutboxState.DeadLetter) return new(work, null, "Only dead-letter publications can be replayed. Published, quarantined and active publications cannot be reopened.");
        var draft = new Ticket(scope.ToProducerScope().Id, id, Guid.NewGuid(), work.Version, OperatorIdentity.Actor(user)!, "", work.DeliveryUncertain, false, DateTimeOffset.UtcNow.AddMinutes(15));
        return new(work, _drafts.Protect(JsonSerializer.Serialize(draft)), "Eligible at the time of this read. The database will check status, version and delivery uncertainty again when the request is submitted.");
    }

    public OutboxReplayReview Review(ClaimsPrincipal user, string key, Guid id, string ticket, string reason, bool acknowledgePossibleDuplicate)
    {
        var scope = Authorize(user, key);
        var draft = Decode(_drafts, ticket, scope, id, user);
        if (draft.RequiresAcknowledgement && !acknowledgePossibleDuplicate)
            throw new InvalidReplayException("Explicitly acknowledge that another publication may create a duplicate before reviewing this request.");
        try { _ = new OutboxReplayRequest(scope.ToProducerScope(), draft.RequestId, id, draft.Version, draft.Actor, reason, acknowledgePossibleDuplicate); }
        catch (ArgumentException) { throw new InvalidReplayException("Enter a reason of up to 1,024 UTF-8 bytes."); }
        var command = draft with { Reason = reason, AcknowledgePossibleDuplicate = acknowledgePossibleDuplicate, ExpiresAt = DateTimeOffset.UtcNow.AddHours(24) };
        return new(command.RequestId, id, command.Version, reason, command.AcknowledgePossibleDuplicate, _commands.Protect(JsonSerializer.Serialize(command)));
    }

    public OutboxReplayReview ReadCommand(ClaimsPrincipal user, string key, Guid id, string ticket)
    {
        var scope = Authorize(user, key);
        var command = Decode(_commands, ticket, scope, id, user);
        return new(command.RequestId, id, command.Version, command.Reason, command.AcknowledgePossibleDuplicate, ticket);
    }

    public Task<OutboxReplayResult> SubmitAsync(ClaimsPrincipal user, string key, Guid id, string ticket, CancellationToken ct)
    {
        var scope = Authorize(user, key);
        var command = Decode(_commands, ticket, scope, id, user);
        // Never reject based on a fresh preview here: the durable receipt must win on repeated requests.
        return commands.ReplayOutboxAsync(new(scope.ToProducerScope(), command.RequestId, id, command.Version, command.Actor, command.Reason, command.AcknowledgePossibleDuplicate), ct);
    }

    public Task<OutboxReplayReceipt?> FindReceiptAsync(ClaimsPrincipal user, string key, Guid id, Guid requestId, CancellationToken ct)
    {
        var scope = Authorize(user, key);
        if (id == Guid.Empty || requestId == Guid.Empty) throw new InvalidReplayException("Invalid intent or request ID.");
        return commands.FindOutboxReceiptAsync(scope.ToProducerScope(), id, requestId, OperatorIdentity.Actor(user)!, ct);
    }

    private static Ticket Decode(IDataProtector protector, string value, OutboxScopeRegistration scope, Guid id, ClaimsPrincipal user)
    {
        try
        {
            if (string.IsNullOrEmpty(value) || value.Length > 12_288) throw new InvalidReplayException("");
            var ticket = JsonSerializer.Deserialize<Ticket>(protector.Unprotect(value))!;
            if (ticket.ScopeId != scope.ToProducerScope().Id || ticket.IntentId != id || ticket.Actor != OperatorIdentity.Actor(user)
                || ticket.RequestId == Guid.Empty || ticket.IntentId == Guid.Empty || ticket.Version < 0 || ticket.ExpiresAt <= DateTimeOffset.UtcNow)
                throw new InvalidReplayException("");
            return ticket;
        }
        catch (Exception) { throw new InvalidReplayException("Invalid or expired replay form. Check the saved request status before preparing another request."); }
    }

    private sealed record Ticket(string ScopeId, Guid IntentId, Guid RequestId, long Version, string Actor, string Reason, bool RequiresAcknowledgement, bool AcknowledgePossibleDuplicate, DateTimeOffset ExpiresAt);
}

