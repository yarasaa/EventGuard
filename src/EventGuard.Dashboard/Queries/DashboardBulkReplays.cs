using System.Security.Claims;
using System.Text.Json;
using EventGuard.Operations;
using Microsoft.AspNetCore.DataProtection;
namespace EventGuard.Dashboard;

public sealed record BulkReplayReview(BulkReplayRequest Request, string Ticket);
public sealed class DashboardBulkReplays(DashboardOptions options, DashboardQueries consumers, DashboardOutboxQueries producers,
    DashboardReplays replays, DashboardOutboxReplays outboxReplays, OperationsReplayer commands, IDataProtectionProvider protection)
{
    private readonly IDataProtector _commands = protection.CreateProtector("EventGuard.BulkReplay.Command.v1", options.StoreId, options.Provider, options.Schema);
    public string Authorize(ClaimsPrincipal user, string kind, string key)
    {
        if (!options.BulkReplayEnabled) throw new ScopeNotAvailableException();
        return kind switch { "consumer" => replays.Authorize(user, key).Label, "outbox" => outboxReplays.Authorize(user, key).Label, _ => throw new ScopeNotAvailableException() };
    }
    public async Task<BulkReplayReview> ReviewAsync(ClaimsPrincipal user, string kind, string key, string ids, string reason, bool acknowledge, CancellationToken ct)
    {
        Authorize(user, kind, key);
        using var budget = CancellationTokenSource.CreateLinkedTokenSource(ct); budget.CancelAfter(TimeSpan.FromSeconds(5));
        try { return await ReviewWithinBudgetAsync(user, kind, key, ids, reason, acknowledge, budget.Token); }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested) { throw new OperationsUnavailableException("read-unavailable"); }
    }
    private async Task<BulkReplayReview> ReviewWithinBudgetAsync(ClaimsPrincipal user, string kind, string key, string ids, string reason, bool acknowledge, CancellationToken ct)
    {
        Authorize(user, kind, key);
        Guid[] selected;
        try
        {
            if (ids is null || ids.Length > 1024) throw new ArgumentException();
            selected = ids.Split([' ', ',', '\r', '\n', '\t'], StringSplitOptions.RemoveEmptyEntries).Select(Guid.Parse).ToArray();
            if (selected.Length is < 1 or > 25 || selected.Any(id => id == Guid.Empty) || selected.Distinct().Count() != selected.Length) throw new ArgumentException();
        }
        catch (Exception error) when (error is ArgumentException or FormatException)
        { throw new InvalidReplayException("Select 1–25 unique full record IDs in this scope."); }
        var items = new List<BulkReplayItem>();
        foreach (var id in selected)
        {
            if (kind == "consumer")
            {
                var record = (await consumers.FindAsync(user, key, id, ct)).Value?.Work;
                if (record is null) throw new InvalidReplayException("A selected record is not available in this scope.");
                if (record.State != WorkState.DeadLetter || record.ExpiresAt <= DateTimeOffset.UtcNow)
                    throw new InvalidReplayException("Select only eligible dead-letter records. One selected record is ineligible.");
                items.Add(new(id, record.Version));
            }
            else
            {
                var record = (await producers.FindAsync(user, key, id, ct)).Value?.Message;
                if (record is null) throw new InvalidReplayException("A selected record is not available in this scope.");
                if (record.State != OutboxState.DeadLetter) throw new InvalidReplayException("Select only dead-letter outgoing events.");
                if (record.DeliveryUncertain && !acknowledge) throw new InvalidReplayException("Explicitly acknowledge possible duplicate publication before reviewing this batch.");
                items.Add(new(id, record.Version));
            }
        }
        try
        {
            var actor = OperatorIdentity.Actor(user)!;
            BulkReplayRequest request = kind == "consumer" ? new(consumers.Resolve(user, key).ToWorkScope(), Guid.NewGuid(), items, actor, reason)
                : new(producers.Resolve(user, key).ToProducerScope(), Guid.NewGuid(), items, actor, reason, acknowledge);
            return Protect(request);
        }
        catch (ArgumentException) { throw new InvalidReplayException("Enter a reason of up to 1,024 UTF-8 bytes."); }
    }
    public BulkReplayReview ReadCommand(ClaimsPrincipal user, string kind, string key, string value)
    {
        Authorize(user, kind, key);
        try
        {
            if (string.IsNullOrEmpty(value) || value.Length > 12_288) throw new ArgumentException();
            var saved = JsonSerializer.Deserialize<Ticket>(_commands.Unprotect(value))!;
            var actor = OperatorIdentity.Actor(user)!;
            if (saved.Kind != kind || saved.Actor != actor || saved.ExpiresAt <= DateTimeOffset.UtcNow || saved.Items.Length is < 1 or > 25) throw new ArgumentException();
            BulkReplayRequest request = kind == "consumer" ? new(consumers.Resolve(user, key).ToWorkScope(), saved.RequestId, saved.Items, actor, saved.Reason)
                : new(producers.Resolve(user, key).ToProducerScope(), saved.RequestId, saved.Items, actor, saved.Reason, saved.Acknowledge);
            if (request.ScopeId != saved.ScopeId) throw new ArgumentException();
            return new(request, value);
        }
        catch (Exception) { throw new InvalidReplayException("Invalid or expired bulk replay form. Check the saved batch before preparing another request."); }
    }
    public Task<BulkReplayPreparation> SubmitAsync(ClaimsPrincipal user, string kind, string key, string ticket, CancellationToken ct) =>
        commands.RunBulkAsync(ReadCommand(user, kind, key, ticket).Request, ct);
    public Task<BulkReplayProgress?> FindAsync(ClaimsPrincipal user, string kind, string key, Guid requestId, CancellationToken ct)
    {
        Authorize(user, kind, key);
        if (requestId == Guid.Empty) throw new InvalidReplayException("A batch request ID is required.");
        return kind == "consumer" ? commands.FindBulkAsync(consumers.Resolve(user, key).ToWorkScope(), requestId, OperatorIdentity.Actor(user)!, ct)
            : commands.FindBulkAsync(producers.Resolve(user, key).ToProducerScope(), requestId, OperatorIdentity.Actor(user)!, ct);
    }
    public BulkReplayReview Resume(ClaimsPrincipal user, string kind, string key, BulkReplayProgress progress)
    {
        Authorize(user, kind, key);
        var scopeId = kind == "consumer" ? consumers.Resolve(user, key).ToWorkScope().Id : producers.Resolve(user, key).ToProducerScope().Id;
        if (progress.Request.ScopeId != scopeId || progress.Request.Actor != OperatorIdentity.Actor(user)
            || progress.Request.Kind != (kind == "consumer" ? BulkReplayKind.Consumer : BulkReplayKind.Outbox)) throw new ScopeNotAvailableException();
        if (progress.Request.Items.Count > 25) throw new InvalidReplayException("Resume this larger batch through the Operations command port.");
        return Protect(progress.Request);
    }
    private BulkReplayReview Protect(BulkReplayRequest request)
    {
        var saved = new Ticket(request.Kind == BulkReplayKind.Consumer ? "consumer" : "outbox", request.ScopeId, request.RequestId,
            request.Items.ToArray(), request.Actor, request.Reason, request.AcknowledgePossibleDuplicate, DateTimeOffset.UtcNow.AddHours(24));
        var ticket = _commands.Protect(JsonSerializer.Serialize(saved));
        if (ticket.Length > 12_288) throw new InvalidReplayException("The protected batch exceeds the dashboard form limit.");
        return new(request, ticket);
    }
    private sealed record Ticket(string Kind, string ScopeId, Guid RequestId, BulkReplayItem[] Items, string Actor, string Reason, bool Acknowledge, DateTimeOffset ExpiresAt);
}
