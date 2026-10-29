using System.Globalization;
using EventGuard.Runtime;
using Microsoft.Extensions.Configuration;

namespace EventGuard.Hosting;

/// <summary>A validated startup snapshot. Configuration reloads never mutate in-flight workers.</summary>
public sealed class EventGuardConfiguration
{
    private readonly ConfigurationNode _root;
    private readonly ConfigurationNode? _defaults;
    private readonly ConfigurationNode? _workers;
    public TimeSpan ShutdownTimeout { get; }

    public EventGuardConfiguration(IConfigurationSection section)
    {
        ArgumentNullException.ThrowIfNull(section);
        _root = ConfigurationNode.Read(section);
        _root.Keys("ShutdownTimeout", "Defaults", "Workers");
        _defaults = _root.Child("Defaults"); _workers = _root.Child("Workers");
        _defaults?.Profile();
        _workers?.Object();
        foreach (var worker in _workers?.Children.Values.AsEnumerable() ?? [])
        {
            if (!ValidName(worker.Key)) throw new EventGuardConfigurationException(worker.Path, "Use a valid worker registration name.");
            worker.Profile();
        }
        ShutdownTimeout = Value(_root.Child("ShutdownTimeout"), TimeSpan.FromSeconds(30), Duration);
        if (ShutdownTimeout <= TimeSpan.Zero || ShutdownTimeout > TimeSpan.FromMilliseconds(uint.MaxValue - 1))
            throw new EventGuardConfigurationException(_root.Path + ":ShutdownTimeout", "Use a positive supported timer duration.");
    }

    public bool IsEnabled(string name) => Value(Section(name, "Enabled"), true, bool.Parse);

    public WorkPumpOptions GetWorkerOptions(string name, WorkPumpOptions? defaults = null)
    {
        defaults ??= new();
        return Guard(name, "Worker", () => new WorkPumpOptions(defaults.Startup,
            Read(name, "Worker", "MaximumConcurrency", defaults.MaximumConcurrency, Integer),
            Read(name, "Worker", "LeaseDuration", defaults.LeaseDuration, Duration),
            Read(name, "Worker", "IdleDelay", defaults.IdleDelay, Duration),
            Read(name, "Worker", "FaultDelay", defaults.FaultDelay, Duration))
        {
            StartupTimeout = Read(name, "Worker", "StartupTimeout", defaults.StartupTimeout, Duration),
            SettlementTimeout = Read(name, "Worker", "SettlementTimeout", defaults.SettlementTimeout, Duration),
            ExternalCallTimeout = Read<TimeSpan?>(name, "Worker", "ExternalCallTimeout", defaults.ExternalCallTimeout, s => Duration(s))
        });
    }

    public RetryPolicy GetRetryPolicy(string name, RetryPolicy? defaults = null)
    {
        defaults ??= new();
        return Guard(name, "Retry", () => new RetryPolicy(
            Read(name, "Retry", "MaximumAttempts", defaults.MaximumAttempts, Integer),
            Read(name, "Retry", "InitialDelay", defaults.InitialDelay, Duration),
            Read(name, "Retry", "MaximumDelay", defaults.MaximumDelay, Duration),
            Read(name, "Retry", "JitterRatio", defaults.JitterRatio, s => double.Parse(s, CultureInfo.InvariantCulture)), defaults.Classify));
    }

    public ExternalBatchWorkerOptions GetBatchOptions(string name, ExternalBatchWorkerOptions? defaults = null)
    {
        defaults ??= new();
        return Guard(name, "Batch", () => new ExternalBatchWorkerOptions(
            Read(name, "Batch", "MaximumBatchSize", defaults.MaximumBatchSize, Integer),
            GetWorkerOptions(name, defaults.Pump),
            Read(name, "Batch", "CollectionWindow", defaults.CollectionWindow, Duration))
        {
            MaximumPayloadBytes = Read(name, "Batch", "MaximumPayloadBytes", defaults.MaximumPayloadBytes, Integer),
            RequireNativeStorage = Read(name, "Batch", "RequireNativeStorage", defaults.RequireNativeStorage, bool.Parse)
        });
    }

    public ExternalActionPolicy GetExternalPolicy(string name, ExternalActionPolicy? defaults = null)
    {
        defaults ??= new();
        return Guard(name, "External", () =>
        {
            var attempts = Read(name, "External", "MaximumSubmissionAttempts", defaults.MaximumSubmissionAttempts, Integer);
            var retry = Read(name, "External", "RetryDelay", defaults.RetryDelay, Duration);
            var interval = Setting(name, "External", "ObservationInterval");
            var schedule = Setting(name, "External", "ObservationSchedule");
            // A worker's explicit interval or schedule replaces the inherited observation mode as a unit.
            var own = Own(name)?.Child("External");
            if (own?.Child("ObservationInterval") is not null) schedule = null;
            if (own?.Child("ObservationSchedule") is not null) interval = null;
            if (schedule is not null)
            {
                var steps = schedule.Children.Values.OrderBy(n => Integer(n.Key)).Select(n =>
                    new ExternalObservationStep(Required(n.Child("After"), n.Path + ":After", Duration),
                        Required(n.Child("Interval"), n.Path + ":Interval", Duration)));
                ExternalObservationSchedule configured;
                try { configured = new(steps); }
                catch (EventGuardConfigurationException) { throw; }
                catch (ArgumentException) { throw new EventGuardConfigurationException(schedule.Path,
                    "Use 1–64 increasing steps starting at zero, with intervals from 10 ms to one day."); }
                return new ExternalActionPolicy(configured, attempts, retry);
            }
            if (interval is null && defaults.ObservationSchedule is { } inherited)
                return new ExternalActionPolicy(inherited, attempts, retry);
            return new ExternalActionPolicy(attempts, retry, Value(interval, defaults.ObservationInterval, Duration));
        });
    }

    public ExternalCallLimit? GetCallLimit(string name, ExternalCallLimit? defaults = null)
    {
        var configured = Section(name, "CallLimit") is not null;
        var enabled = Read(name, "CallLimit", "Enabled", configured || defaults is not null, bool.Parse);
        if (!enabled) return null;
        return Guard(name, "CallLimit", () => new ExternalCallLimit(
            Read(name, "CallLimit", "Namespace", defaults?.Namespace ?? "", s => s),
            Read(name, "CallLimit", "Endpoint", defaults?.Endpoint ?? "", s => s),
            Read(name, "CallLimit", "MaximumCalls", defaults?.MaximumCalls ?? 0, Integer),
            Read(name, "CallLimit", "Window", defaults?.Window ?? TimeSpan.FromSeconds(1), Duration),
            Read(name, "CallLimit", "MaximumConcurrency", defaults?.MaximumConcurrency ?? 8, Integer),
            Read(name, "CallLimit", "FailureThreshold", defaults?.FailureThreshold ?? 5, Integer),
            Read(name, "CallLimit", "BreakDuration", defaults?.BreakDuration ?? TimeSpan.FromSeconds(30), Duration)));
    }

    public IReadOnlyList<ExternalPhase>? GetPhases(string name, IEnumerable<ExternalPhase>? defaults = null)
    {
        var section = Section(name, "Phases");
        var values = section is null ? defaults?.Take(4).ToArray() : section.Children.Values.OrderBy(n => Integer(n.Key))
            .Select(n => Required(n, n.Path, s => Enum.Parse<ExternalPhase>(s, true))).ToArray();
        if (values is not null && (values.Length is < 1 or > 3 || values.Distinct().Count() != values.Length || values.Any(p => !Enum.IsDefined(p))))
            throw new EventGuardConfigurationException(Path(name, "Phases"), "Choose distinct Submit, Reconcile or Observe phases.");
        return values is null ? null : Array.AsReadOnly(values);
    }

    public PollingWorkerOptions GetPollingOptions(string name, PollingWorkerOptions? defaults = null)
    {
        defaults ??= new();
        return Guard(name, "Polling", () => new PollingWorkerOptions(
            Read(name, "Polling", "MaximumItems", defaults.MaximumItems, Integer),
            Read(name, "Polling", "PollInterval", defaults.PollInterval, Duration),
            Read(name, "Polling", "SweepInterval", defaults.SweepInterval, Duration)));
    }

    internal void ValidateRegistrations(IEnumerable<WorkerRegistration> registrations)
    {
        var names = registrations.Select(r => r.ConfigurationName ?? r.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
        foreach (var node in _workers?.Children.Values.AsEnumerable() ?? [])
            if (!names.Contains(node.Key)) throw new EventGuardConfigurationException(node.Path, "No worker is registered with this configuration name.");
    }

    private ConfigurationNode? Own(string name)
    {
        if (!ValidName(name)) throw new ArgumentException("Use a valid worker registration name.", nameof(name));
        return _workers?.Child(name);
    }
    private ConfigurationNode? Section(string name, string key) => Own(name)?.Child(key) ?? _defaults?.Child(key);
    private ConfigurationNode? Setting(string name, string group, string key) => Own(name)?.Child(group)?.Child(key) ?? _defaults?.Child(group)?.Child(key);
    private string Path(string name, string group) => (_workers?.Child(name)?.Path ?? _defaults?.Path ?? _root.Path) + ":" + group;
    private T Read<T>(string name, string group, string key, T fallback, Func<string, T> convert) => Value(Setting(name, group, key), fallback, convert);
    private static int Integer(string text) => int.Parse(text, NumberStyles.Integer, CultureInfo.InvariantCulture);
    private static TimeSpan Duration(string text) => TimeSpan.ParseExact(text, "c", CultureInfo.InvariantCulture);
    private static T Value<T>(ConfigurationNode? node, T fallback, Func<string, T> convert) => node is null ? fallback : Required(node, node.Path, convert);
    private static T Required<T>(ConfigurationNode? node, string path, Func<string, T> convert)
    {
        if (node?.Value is null || node.Children.Count > 0) throw new EventGuardConfigurationException(path, "A scalar value is required.");
        try { return convert(node.Value); }
        catch (Exception e) when (e is FormatException or OverflowException or ArgumentException)
        { throw new EventGuardConfigurationException(path, "Invalid value. Use the documented type; durations use d.hh:mm:ss.fffffff."); }
    }
    private T Guard<T>(string name, string group, Func<T> create)
    {
        try { return create(); }
        catch (EventGuardConfigurationException) { throw; }
        catch (ArgumentException e)
        {
            var field = e.ParamName;
            throw new EventGuardConfigurationException(Path(name, group) + (string.IsNullOrEmpty(field) ? "" : ":" + char.ToUpperInvariant(field[0]) + field[1..]),
                "The value or combination is outside the supported range. See the configuration reference.");
        }
    }
    internal static bool ValidName(string name) => !string.IsNullOrWhiteSpace(name) && name.Length <= 128
        && name.All(c => char.IsAsciiLetterOrDigit(c) || c is '-' or '_' or '.');
}

/// <summary>Safe to display: configuration paths and guidance, never configured values or provider exceptions.</summary>
public sealed class EventGuardConfigurationException(string path, string guidance)
    : ArgumentException($"{path}: {guidance}")
{
    public string ConfigurationPath { get; } = path;
}
