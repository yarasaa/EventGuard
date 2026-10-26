using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
namespace EventGuard.Dashboard.Pages;
public sealed class AuditModel(DashboardAudit audit) : PageModel
{
    public static string Operation(EventGuard.Operations.AuditKind kind) => kind switch
    {
        EventGuard.Operations.AuditKind.ConsumerReplay => "Consumer replay", EventGuard.Operations.AuditKind.IngressRelease => "Ingress release",
        EventGuard.Operations.AuditKind.OutboxReplay => "Outbox replay", EventGuard.Operations.AuditKind.OutboxRepair => "Outbox repair",
        EventGuard.Operations.AuditKind.Retention => "Retention", _ => "Bulk replay plan"
    };
    public DashboardAuditPage Data { get; private set; } = null!;
    public IReadOnlyList<AuditTarget> Targets => audit.Allowed(User);
    public async Task<IActionResult> OnGetAsync(string kind, string scope, string? from, string? until, string? cursor)
    {
        Data = await audit.ListAsync(User, kind, scope, from, until, cursor, HttpContext.RequestAborted);
        return Page();
    }
    public async Task<IActionResult> OnGetExportAsync(string kind, string scope, string? from, string? until, string? cursor)
    {
        var page = await audit.ListAsync(User, kind, scope, from, until, cursor, HttpContext.RequestAborted);
        // JSON Lines avoids spreadsheet formulas; values remain escaped data, never executable cells or HTML.
        var lines = page.Snapshot.Value.Items.Select(row => JsonSerializer.Serialize(new { Store = HttpContext.RequestServices.GetRequiredService<DashboardOptions>().StoreId,
            Scope = page.Target.Key, Kind = row.Kind.ToString(), row.RequestId, row.SubjectId, row.Actor, row.Reason, row.Outcome,
            row.AcknowledgePossibleDuplicate, row.CreatedAt }));
        return File(Encoding.UTF8.GetBytes(string.Join('\n', lines) + (page.Snapshot.Value.Items.Count > 0 ? "\n" : "")), "application/x-ndjson", "eventguard-audit.jsonl");
    }
}
