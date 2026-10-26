using System.Security.Claims;
using System.Text.Json;
using EventGuard.Operations;
using Microsoft.AspNetCore.DataProtection;
namespace EventGuard.Dashboard;

public sealed record RecoveryPreview(string Label, string Explanation, string? Draft, long Version, bool DeliveryUncertain);
public sealed record RecoveryReview(Guid RequestId, Guid Id, long Version, string Manifest, string Reason,
    bool AcknowledgePossibleDuplicate, string Ticket);
public sealed record RecoveryReceipt(Guid RequestId, string Status, Guid? WorkId, long Version, string Reason,
    bool AcknowledgePossibleDuplicate, DateTimeOffset CreatedAt);
public sealed class DashboardRecovery(DashboardOptions options, DashboardQueries consumers, DashboardOutboxQueries producers,
    DashboardIngressQueries ingress, OperationsReplayer commands, IDataProtectionProvider protection)
{
    private readonly IDataProtector _drafts = protection.CreateProtector("EventGuard.Recovery.Draft.v1", options.StoreId, options.Provider, options.Schema);
    private readonly IDataProtector _commands = protection.CreateProtector("EventGuard.Recovery.Command.v1", options.StoreId, options.Provider, options.Schema);
    public string Authorize(ClaimsPrincipal user, string kind, string key)
    {
        if (!options.RecoveryEnabled || OperatorIdentity.Actor(user) is null) throw new ScopeNotAvailableException();
        if (kind == "ingress")
        {
            var scope = ingress.Resolve(user, key);
            if (!scope.AllowRecovery || !user.HasClaim("eventguard_recovery", options.StoreId + "/" + key)) throw new ScopeNotAvailableException();
            return scope.Label;
        }
        if (kind == "outbox")
        {
            var scope = producers.Resolve(user, key);
            if (!scope.AllowRecovery || !user.HasClaim("eventguard_outbox_recovery", options.StoreId + "/" + key)) throw new ScopeNotAvailableException();
            return scope.Label;
        }
        throw new ScopeNotAvailableException();
    }
    public bool CanRecover(ClaimsPrincipal user, string kind, string key)
    {
        try { _ = Authorize(user, kind, key); return true; } catch (ScopeNotAvailableException) { return false; }
    }
    public async Task<RecoveryPreview?> PreviewAsync(ClaimsPrincipal user, string kind, string key, Guid id, CancellationToken ct)
    {
        var label = Authorize(user, kind, key);
        Ticket draft;
        bool uncertain = false;
        if (kind == "ingress")
        {
            var row = (await ingress.FindAsync(user, key, id, ct)).Value;
            if (row is null) return null;
            if (row.ReleasedWorkId.HasValue) return new(label, "This quarantine entry has already been released.", null, row.Version, false);
            draft = new(kind, consumers.Resolve(user, key).ToWorkScope().Id, id, id, row.Version, row.KeyVersion, row.FingerprintVersion,
                Guid.NewGuid(), OperatorIdentity.Actor(user)!, "", "", false, false, DateTimeOffset.UtcNow.AddMinutes(15));
        }
        else
        {
            var row = (await producers.FindAsync(user, key, id, ct)).Value;
            if (row is null) return null;
            if (row.Message.State != OutboxState.Quarantined || row.Quarantine is null)
                return new(label, "Only a quarantined outgoing event can be repaired.", null, row.Message.Version, false);
            uncertain = row.Message.DeliveryUncertain;
            draft = new(kind, producers.Resolve(user, key).ToProducerScope().Id, id, row.Quarantine.Id, row.Message.Version, "", "",
                Guid.NewGuid(), OperatorIdentity.Actor(user)!, "", "", uncertain, false, DateTimeOffset.UtcNow.AddMinutes(15));
        }
        return new(label, kind == "ingress" ? "Supply a corrected submission using the application's existing business-key and fingerprint policy. Normal deduplication still applies."
            : "Supply the exact original envelope from a trusted backup. Storage checks its original fingerprint; changing the event or destination is rejected.",
            _drafts.Protect(JsonSerializer.Serialize(draft)), draft.Version, uncertain);
    }
    public RecoveryReview Review(ClaimsPrincipal user, string kind, string key, Guid id, string draft, string manifest, string reason, bool acknowledgePossibleDuplicate)
    {
        Authorize(user, kind, key);
        var saved = Decode(_drafts, user, kind, key, id, draft) with
        { Manifest = manifest, Reason = reason, AcknowledgePossibleDuplicate = acknowledgePossibleDuplicate, ExpiresAt = DateTimeOffset.UtcNow.AddHours(24) };
        if (saved.RequiresAcknowledgement && !saved.AcknowledgePossibleDuplicate)
            throw new InvalidReplayException("Explicitly acknowledge possible duplicate publication before reviewing this repair.");
        Validate(user, key, saved);
        var ticket = _commands.Protect(JsonSerializer.Serialize(saved));
        if (ticket.Length > 12_288) throw new InvalidReplayException("The protected recovery request exceeds the dashboard form limit. Use a smaller manifest.");
        return ToReview(saved, ticket);
    }
    public RecoveryReview ReadCommand(ClaimsPrincipal user, string kind, string key, Guid id, string ticket)
    {
        Authorize(user, kind, key);
        return ToReview(Decode(_commands, user, kind, key, id, ticket), ticket);
    }
    public async Task<string> SubmitAsync(ClaimsPrincipal user, string kind, string key, Guid id, string ticket, CancellationToken ct)
    {
        Authorize(user, kind, key);
        var saved = Decode(_commands, user, kind, key, id, ticket);
        var manifest = Validate(user, key, saved);
        // Durable receipt lookup in the provider precedes version/state checks. Never re-preview here.
        return kind == "ingress"
            ? (await commands.ReleaseIngressAsync(IngressRequest(user, key, saved, manifest), ct)).Status.ToString()
            : (await commands.RepairOutboxAsync(OutboxRequest(user, key, saved, manifest), ct)).Status.ToString();
    }
    public async Task<RecoveryReceipt?> FindReceiptAsync(ClaimsPrincipal user, string kind, string key, Guid id, Guid requestId, CancellationToken ct)
    {
        Authorize(user, kind, key);
        if (id == Guid.Empty || requestId == Guid.Empty) throw new InvalidReplayException("Invalid record or request ID.");
        var actor = OperatorIdentity.Actor(user)!;
        if (kind == "ingress")
        {
            var receipt = await commands.FindIngressReleaseReceiptAsync(consumers.Resolve(user, key).ToWorkScope(), id, requestId, actor, ct);
            return receipt is null ? null : new(receipt.RequestId, receipt.Result.Status.ToString(), receipt.Result.WorkId,
                receipt.Result.Version, receipt.Reason, false, receipt.CreatedAt);
        }
        var outgoing = await commands.FindOutboxRepairReceiptAsync(producers.Resolve(user, key).ToProducerScope(), id, requestId, actor, ct);
        return outgoing is null ? null : new(outgoing.RequestId, outgoing.Result.Status.ToString(), null, outgoing.Result.Version,
            outgoing.Reason, outgoing.AcknowledgePossibleDuplicate, outgoing.CreatedAt);
    }
    private RecoveryManifest Validate(ClaimsPrincipal user, string key, Ticket ticket)
    {
        try
        {
            var manifest = RecoveryManifest.Parse(ticket.Manifest);
            if (ticket.Kind == "ingress") _ = IngressRequest(user, key, ticket, manifest);
            else _ = OutboxRequest(user, key, ticket, manifest);
            return manifest;
        }
        catch (Exception error) when (error is ArgumentException or JsonException or FormatException)
        { throw new InvalidReplayException("Invalid recovery manifest or reason. Check the documented fields, identity policy and size limits."); }
    }
    private IngressReleaseRequest IngressRequest(ClaimsPrincipal user, string key, Ticket ticket, RecoveryManifest manifest) =>
        new(ticket.RequestId, ticket.QuarantineId, ticket.Version,
            manifest.Submission(consumers.Resolve(user, key).ToWorkScope(), ticket.KeyVersion, ticket.FingerprintVersion), ticket.Actor, ticket.Reason);
    private OutboxRepairRequest OutboxRequest(ClaimsPrincipal user, string key, Ticket ticket, RecoveryManifest manifest) =>
        new(ticket.RequestId, ticket.QuarantineId, manifest.Outgoing(producers.Resolve(user, key).ToProducerScope(), ticket.Id),
            ticket.Version, ticket.Actor, ticket.Reason, ticket.AcknowledgePossibleDuplicate);
    private Ticket Decode(IDataProtector protector, ClaimsPrincipal user, string kind, string key, Guid id, string value)
    {
        try
        {
            if (string.IsNullOrEmpty(value) || value.Length > 12_288) throw new InvalidReplayException("");
            var saved = JsonSerializer.Deserialize<Ticket>(protector.Unprotect(value))!;
            var scopeId = kind == "ingress" ? consumers.Resolve(user, key).ToWorkScope().Id : producers.Resolve(user, key).ToProducerScope().Id;
            if (saved.Kind != kind || saved.ScopeId != scopeId || saved.Id != id || id == Guid.Empty || saved.QuarantineId == Guid.Empty
                || saved.RequestId == Guid.Empty || saved.Version < 0 || saved.Actor != OperatorIdentity.Actor(user) || saved.ExpiresAt <= DateTimeOffset.UtcNow)
                throw new InvalidReplayException("");
            return saved;
        }
        catch (Exception) { throw new InvalidReplayException("Invalid or expired recovery form. Check the saved request before preparing another request."); }
    }
    private static RecoveryReview ToReview(Ticket saved, string ticket) => new(saved.RequestId, saved.Id, saved.Version, saved.Manifest,
        saved.Reason, saved.AcknowledgePossibleDuplicate, ticket);
    private sealed record Ticket(string Kind, string ScopeId, Guid Id, Guid QuarantineId, long Version, string KeyVersion, string FingerprintVersion,
        Guid RequestId, string Actor, string Manifest, string Reason, bool RequiresAcknowledgement, bool AcknowledgePossibleDuplicate, DateTimeOffset ExpiresAt);
}
