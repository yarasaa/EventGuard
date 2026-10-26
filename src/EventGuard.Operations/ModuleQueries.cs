namespace EventGuard.Operations;

public enum ExecutionModule { Snapshots, ExternalActions, ExternalNotices, Workflows, ServiceLimit }
public sealed class ModuleQuery
{
    public ExecutionModule Module { get; }
    public string ScopeId { get; }
    public int? State { get; }
    public Guid? Id { get; }
    public Guid? Position { get; }
    public bool Reverse { get; }
    public int Limit { get; }
    public ModuleQuery(ExecutionModule module, string scopeId, int? state = null, Guid? id = null, Guid? position = null, bool reverse = false, int limit = 25)
    {
        if (!Enum.IsDefined(module) || scopeId is null || scopeId.Length != 64 || scopeId.Any(c => !Uri.IsHexDigit(c)) || limit is < 1 or > 100
            || state.HasValue && !ModuleMetadata.States(module).Any(s => s.Code == state) || id == Guid.Empty || position == Guid.Empty || reverse && position is null
            || module == ExecutionModule.ServiceLimit && (id.HasValue || position.HasValue)) throw new ArgumentException("Invalid module query.");
        Module = module; ScopeId = scopeId; State = state; Id = id; Position = position; Reverse = reverse; Limit = limit;
    }
}
public sealed record ModuleRow(Guid Id, string State, long Version, long Completed, long Total, string Guidance, DateTimeOffset ScheduledAt);
public sealed record ModuleStep(string Name, string State, Guid? JobId, DateTimeOffset? DueAt, string Reason);
public sealed record ModulePage(IReadOnlyList<ModuleRow> Items, Guid? Next, Guid? Previous)
{
    public IReadOnlyList<ModuleStep> Steps { get; init; } = [];
    public static ModulePage FromWindow(List<ModuleRow> rows, ModuleQuery query)
    {
        var more = rows.Count > query.Limit; if (more) rows.RemoveAt(rows.Count - 1); if (query.Reverse) rows.Reverse();
        return new(rows.AsReadOnly(), query.Module == ExecutionModule.ServiceLimit ? null : more || query.Reverse ? rows.LastOrDefault()?.Id ?? query.Position : null,
            query.Module == ExecutionModule.ServiceLimit ? null : query.Position is not null && !query.Reverse || query.Reverse && more ? rows.FirstOrDefault()?.Id ?? query.Position : null);
    }
}
public interface IModuleReadStore { Task<ModulePage> ReadModuleAsync(ModuleQuery query, CancellationToken cancellationToken = default); }
public sealed partial class OperationsReader
{
    public Task<ReadSnapshot<ModulePage>> ReadModuleAsync(ModuleQuery query, CancellationToken ct = default) => ReadAsync(token => _store is IModuleReadStore modules
        ? modules.ReadModuleAsync(query, token) : throw new NotSupportedException("Module metadata is unavailable."), ct);
}
public static class ModuleMetadata
{
    public static IReadOnlyList<BacklogState> States(ExecutionModule module) => module switch
    {
        ExecutionModule.Snapshots => Enum.GetValues<SnapshotState>().Select(s => new BacklogState((int)s, s.ToString())).ToArray(),
        ExecutionModule.ExternalActions => Enum.GetValues<ExternalActionState>().Select(s => new BacklogState((int)s, s.ToString())).ToArray(),
        ExecutionModule.ExternalNotices => Enum.GetValues<ExternalNoticeStatus>().Select(s => new BacklogState((int)s, s.ToString())).ToArray(),
        ExecutionModule.Workflows => Enum.GetValues<WorkflowState>().Select(s => new BacklogState((int)s, s.ToString())).ToArray(),
        ExecutionModule.ServiceLimit => [new(0, "Closed"), new(1, "Open"), new(2, "Recovery probe")],
        _ => []
    };
    // Provider result text and payloads never enter metadata pages.
    public static string Guidance(ExecutionModule module, int state, int secondary, bool paused) => module switch
    {
        ExecutionModule.Snapshots when paused => "Paused by an operator. Resume through the application control API.",
        ExecutionModule.Snapshots => state switch { 0 => "Awaiting a snapshot worker with the matching source version.", 1 => "Capturing a consistent source view. No batches are released until sealed.", 2 => "Source sealed. Awaiting durable batch creation.", 3 => "All batches persisted. Check child jobs or the configured outbox/CDC transport.", _ => "Cancelled. Previously dispatched work is not undone." },
        ExecutionModule.ExternalActions => state switch { 0 => "Awaiting sender and shared service capacity.", 1 => "Submission is in progress; expired leases require reconciliation.", 2 => secondary switch { 1 => "Accepted by the service; awaiting a report, polling or deadline.", 2 => "Service reported success. Check the application's result handler.", 3 => "Service reported failure. Check the application's result handler.", 4 => "Report deadline elapsed. A late final report can still resolve it.", _ => "Service accepted the request. Delivery was not requested or proved." }, 3 => "Service rejected the request. Review the application's result handler.", _ => "Acceptance is uncertain. Reconcile with the service; do not resend blindly." },
        ExecutionModule.ExternalNotices => state switch { 0 => "Awaiting acceptance or a notice worker for this binding.", 1 => "Report applied atomically with result work.", 2 => "Report ignored because it cannot advance the current state.", _ => "Conflicting report held for investigation. No automatic overwrite." },
        ExecutionModule.Workflows => state switch { 0 => "Waiting for jobs, signals or timers. Select the record ID to inspect step progress.", 1 => "Paused. Already dispatched jobs may still finish.", 2 => "All selected steps completed.", 3 => "Cancelled. Already dispatched effects are not undone.", _ => "Workflow deadline elapsed; no new steps are dispatched." },
        ExecutionModule.ServiceLimit => state switch { 1 => "Circuit open after service failures. Wait for cooldown.", 2 => "One recovery call is probing service health.", _ => "Calls share the configured window and concurrent permit capacity." },
        _ => "Unknown module state. Check package/schema compatibility."
    };
}
