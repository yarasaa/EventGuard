using System.Collections.ObjectModel;
using System.Globalization;

namespace EventGuard;

public enum WorkflowStepKind { Job, Delay, Signal }
public enum WorkflowState { Running, Paused, Completed, Cancelled, Expired }
public enum WorkflowStepState { Pending, Waiting, Completed, Skipped }
public sealed record WorkflowDependency(string Step, string? RequiredValue = null);
public sealed class WorkflowStep
{
    public string Name { get; }
    public WorkflowStepKind Kind { get; }
    public JobDefinition? Job { get; }
    public TimeSpan Delay { get; }
    public IReadOnlyList<WorkflowDependency> Dependencies { get; }
    public string Fingerprint { get; }
    public WorkflowStep(string name, WorkflowStepKind kind, IEnumerable<WorkflowDependency>? dependencies = null, JobDefinition? job = null, TimeSpan? delay = null)
    {
        Name = IdentityRules.Required(name, nameof(name));
        if (Name.Length > 256) throw new ArgumentException("Step name exceeds 256 characters.", nameof(name));
        if (!Enum.IsDefined(kind) || (kind == WorkflowStepKind.Job) != (job is not null)) throw new ArgumentException("Job steps require a job definition; other steps do not.");
        Delay = delay ?? TimeSpan.Zero;
        if (Delay < TimeSpan.Zero || Delay > TimeSpan.FromDays(365) || kind != WorkflowStepKind.Delay && Delay != TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(delay));
        Delay = TimeSpan.FromMilliseconds((long)Delay.TotalMilliseconds);
        var edges = (dependencies ?? []).Take(65).ToArray();
        if (edges.Length > 64 || edges.Any(e => e is null) || edges.Select(e => e.Step).Distinct(StringComparer.Ordinal).Count() != edges.Length) throw new ArgumentException("Invalid dependencies.");
        foreach (var edge in edges) { _ = IdentityRules.Required(edge.Step, nameof(dependencies)); if (edge.RequiredValue is not null) _ = IdentityRules.Required(edge.RequiredValue, nameof(dependencies)); }
        Kind = kind; Job = job; Dependencies = new ReadOnlyCollection<WorkflowDependency>(edges.OrderBy(e => e.Step, StringComparer.Ordinal).ToArray());
        Fingerprint = StableHash.Of(["workflow-step/v1", Name, ((int)kind).ToString(CultureInfo.InvariantCulture), Job?.Fingerprint ?? "", Delay.Ticks.ToString(CultureInfo.InvariantCulture),
            .. Dependencies.SelectMany(e => new[] { e.Step, e.RequiredValue ?? "" })]);
    }
}
public sealed class WorkflowDefinition
{
    public JobScope Scope { get; }
    public string Name { get; }
    public int Version { get; }
    public IReadOnlyList<WorkflowStep> Steps { get; }
    public string Id { get; }
    public string Fingerprint { get; }
    public WorkflowDefinition(JobScope scope, string name, int version, IEnumerable<WorkflowStep> steps)
    {
        Scope = scope ?? throw new ArgumentNullException(nameof(scope)); Name = IdentityRules.Required(name, nameof(name));
        if (version < 1) throw new ArgumentOutOfRangeException(nameof(version)); ArgumentNullException.ThrowIfNull(steps);
        var nodes = steps.Take(65).ToArray();
        if (nodes.Length is < 1 or > 64 || nodes.Any(n => n is null) || nodes.Select(n => n.Name).Distinct(StringComparer.Ordinal).Count() != nodes.Length) throw new ArgumentException("Use 1–64 uniquely named steps.");
        var names = nodes.Select(n => n.Name).ToHashSet(StringComparer.Ordinal);
        if (nodes.Any(n => n.Dependencies.Any(d => !names.Contains(d.Step)) || n.Job is not null && n.Job.Scope.Tenant != scope.Tenant)) throw new ArgumentException("Unknown dependency or foreign tenant job.");
        var sorted = new List<WorkflowStep>();
        while (sorted.Count < nodes.Length)
        {
            var ready = nodes.Where(n => !sorted.Contains(n) && n.Dependencies.All(d => sorted.Any(done => done.Name == d.Step))).OrderBy(n => n.Name, StringComparer.Ordinal).ToArray();
            if (ready.Length == 0) throw new ArgumentException("Workflow cycles are not allowed."); sorted.AddRange(ready);
        }
        Version = version; Steps = new ReadOnlyCollection<WorkflowStep>(sorted);
        Id = StableHash.Of("workflow-definition/v1", scope.Id, Name, version.ToString(CultureInfo.InvariantCulture));
        Fingerprint = StableHash.Of(["workflow-definition-content/v1", Id, .. Steps.Select(n => n.Fingerprint)]);
    }
}
public sealed class WorkflowRequest
{
    private readonly Dictionary<string, byte[]> _inputs;
    public WorkflowDefinition Definition { get; }
    public Guid RequestId { get; }
    public Guid Id { get; }
    public string BusinessKey { get; }
    public string Actor { get; }
    public DateTimeOffset? ExpiresAt { get; }
    public string Fingerprint { get; }
    public byte[] CopyInput(string step) => (byte[])_inputs[step].Clone();
    public WorkflowRequest(WorkflowDefinition definition, Guid requestId, string businessKey, string actor,
        IReadOnlyDictionary<string, byte[]> inputs, DateTimeOffset? expiresAt = null)
    {
        Definition = definition ?? throw new ArgumentNullException(nameof(definition)); ArgumentNullException.ThrowIfNull(inputs);
        if (requestId == Guid.Empty) throw new ArgumentException("Stable request ID required.", nameof(requestId));
        RequestId = requestId; BusinessKey = IdentityRules.Required(businessKey, nameof(businessKey)); Actor = IdentityRules.Required(actor, nameof(actor));
        var jobs = definition.Steps.Where(n => n.Kind == WorkflowStepKind.Job).Select(n => n.Name).ToHashSet(StringComparer.Ordinal);
        if (!jobs.SetEquals(inputs.Keys) || inputs.Values.Any(v => v is null || v.Length > 1024 * 1024) || inputs.Values.Sum(v => (long)v.Length) > 8 * 1024 * 1024)
            throw new ArgumentException("Supply one bounded payload per job step (1 MiB each, 8 MiB total).", nameof(inputs));
        _inputs = inputs.ToDictionary(p => p.Key, p => (byte[])p.Value.Clone(), StringComparer.Ordinal);
        ExpiresAt = expiresAt is { } date ? new DateTimeOffset(date.UtcTicks - date.UtcTicks % 10, TimeSpan.Zero) : null;
        Id = new Guid(Convert.FromHexString(StableHash.Of("workflow-operation/v1", definition.Scope.Id, definition.Name, BusinessKey)).AsSpan(0, 16));
        Fingerprint = StableHash.Of(["workflow-request/v1", Id.ToString("D"), definition.Fingerprint, Actor, ExpiresAt?.UtcTicks.ToString(CultureInfo.InvariantCulture) ?? "",
            .. _inputs.OrderBy(p => p.Key, StringComparer.Ordinal).SelectMany(p => new[] { p.Key, ContentFingerprint.FromBytes(p.Value).Value })]);
    }
}
public sealed record WorkflowStepProgress(string Name, WorkflowStepState State, Guid? JobId, DateTimeOffset? DueAt, string? Value, string Reason);
public sealed record WorkflowProgress(WorkflowRequest Request, WorkflowState State, long Version, IReadOnlyList<WorkflowStepProgress> Steps);
public sealed class WorkflowSignal
{
    public JobScope Scope { get; }
    public Guid WorkflowId { get; }
    public string Step { get; }
    public Guid RequestId { get; }
    public string Value { get; }
    public string Actor { get; }
    public string Fingerprint { get; }
    public WorkflowSignal(JobScope scope, Guid workflowId, string step, Guid requestId, string value, string actor)
    {
        Scope = scope ?? throw new ArgumentNullException(nameof(scope));
        if (workflowId == Guid.Empty || requestId == Guid.Empty) throw new ArgumentException("Stable workflow and signal request IDs required.");
        WorkflowId = workflowId; RequestId = requestId; Step = IdentityRules.Required(step, nameof(step)); Value = IdentityRules.Required(value, nameof(value)); Actor = IdentityRules.Required(actor, nameof(actor));
        Fingerprint = StableHash.Of("workflow-signal/v1", scope.Id, workflowId.ToString("D"), Step, Value, Actor);
    }
}
public enum WorkflowSignalStatus { Recorded, AlreadyRecorded }
public interface IWorkflowSignalWriter { Task<WorkflowSignalStatus> SignalAsync(WorkflowSignal signal, CancellationToken cancellationToken = default); }
public interface IWorkflowSignalSession { IWorkflowSignalWriter WorkflowSignals { get; } }
public interface IWorkflowStore
{
    Task InitializeWorkflowsAsync(CancellationToken cancellationToken = default);
    Task<WorkflowProgress> CreateWorkflowAsync(WorkflowRequest request, CancellationToken cancellationToken = default);
    Task<WorkflowProgress?> FindWorkflowAsync(WorkflowDefinition definition, Guid id, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<Guid>> ListWorkflowsAsync(WorkflowDefinition definition, int limit = 100, CancellationToken cancellationToken = default);
    Task<WorkflowProgress> AdvanceWorkflowAsync(WorkflowDefinition definition, Guid id, CancellationToken cancellationToken = default);
    Task<WorkflowProgress> ControlWorkflowAsync(WorkflowDefinition definition, Guid id, long expectedVersion, bool paused, bool cancel,
        string actor, string reason, CancellationToken cancellationToken = default);
    Task<WorkflowSignalStatus> SignalWorkflowAsync(WorkflowSignal signal, CancellationToken cancellationToken = default);
}
