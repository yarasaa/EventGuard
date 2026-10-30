using Microsoft.Extensions.Configuration;
using System.Globalization;

namespace EventGuard.Hosting;

internal sealed record ConfigurationNode(string Key, string Path, string? Value, Dictionary<string, ConfigurationNode> Children)
{
    internal static ConfigurationNode Read(IConfigurationSection section) => new(section.Key, section.Path, section.Value,
        section.GetChildren().ToDictionary(c => c.Key, Read, StringComparer.OrdinalIgnoreCase));
    internal ConfigurationNode? Child(string key) => Children.GetValueOrDefault(key);
    internal void Object()
    {
        if (Value is not null) throw new EventGuardConfigurationException(Path, "Use a configuration object, not a scalar value.");
    }
    internal void Keys(params string[] allowed)
    {
        Object();
        var known = allowed.ToHashSet(StringComparer.OrdinalIgnoreCase);
        foreach (var child in Children.Values)
            if (!known.Contains(child.Key)) throw new EventGuardConfigurationException(child.Path, "Unknown setting. Check the configuration reference for spelling and nesting.");
    }
    internal void Profile()
    {
        Keys("Enabled", "Worker", "Retry", "Batch", "External", "CallLimit", "Phases", "Polling", "ResultBatch");
        Child("Worker")?.Keys("MaximumConcurrency", "LeaseDuration", "IdleDelay", "FaultDelay", "StartupTimeout", "SettlementTimeout", "ExternalCallTimeout", "ExternalPermitRetention", "ExternalPermitCleanupInterval");
        Child("ResultBatch")?.Keys("MaximumItems", "MaximumBytes", "ExecutionTimeout", "FailureRecordingTimeout");
        Child("Retry")?.Keys("MaximumAttempts", "InitialDelay", "MaximumDelay", "JitterRatio");
        Child("Batch")?.Keys("MaximumBatchSize", "CollectionWindow", "MaximumPayloadBytes", "RequireNativeStorage");
        Child("External")?.Keys("MaximumSubmissionAttempts", "RetryDelay", "ObservationInterval", "ObservationSchedule");
        Child("CallLimit")?.Keys("Enabled", "Namespace", "Endpoint", "MaximumCalls", "Window", "MaximumConcurrency", "FailureThreshold", "BreakDuration");
        Child("Polling")?.Keys("MaximumItems", "PollInterval", "SweepInterval");
        var external = Child("External");
        if (external?.Child("ObservationInterval") is not null && external.Child("ObservationSchedule") is not null)
            throw new EventGuardConfigurationException(external.Path, "Choose ObservationInterval or ObservationSchedule, not both.");
        if (external?.Child("ObservationSchedule") is { } schedule)
        {
            schedule.Array(64);
            foreach (var step in schedule.Children.Values) step.Keys("After", "Interval");
        }
        Child("Phases")?.Array(3);
        foreach (var node in Leaves()) node.ValidateScalar();
    }
    private IEnumerable<ConfigurationNode> Leaves()
    {
        foreach (var node in Children.Values)
        {
            if (node.Key.ToUpperInvariant() is "WORKER" or "RETRY" or "BATCH" or "EXTERNAL" or "CALLLIMIT" or "POLLING" or "RESULTBATCH" or "OBSERVATIONSCHEDULE" or "PHASES"
                || int.TryParse(node.Key, out _) && node.Children.Count > 0)
                foreach (var leaf in node.Leaves()) yield return leaf;
            else yield return node;
        }
    }
    private void ValidateScalar()
    {
        if (Value is null || Children.Count > 0) throw new EventGuardConfigurationException(Path, "A scalar value is required.");
        try
        {
            if (Key.Equals("ExternalPermitRetention", StringComparison.OrdinalIgnoreCase) && Value.Equals("disabled", StringComparison.OrdinalIgnoreCase)) return;
            if (new[] { "Enabled", "RequireNativeStorage" }.Contains(Key, StringComparer.OrdinalIgnoreCase)) _ = bool.Parse(Value);
            else if (new[] { "MaximumConcurrency", "MaximumAttempts", "MaximumBatchSize", "MaximumSubmissionAttempts", "MaximumCalls", "FailureThreshold", "MaximumItems", "MaximumPayloadBytes", "MaximumBytes" }
                .Contains(Key, StringComparer.OrdinalIgnoreCase)) _ = int.Parse(Value, CultureInfo.InvariantCulture);
            else if (Key.Equals("JitterRatio", StringComparison.OrdinalIgnoreCase)) _ = double.Parse(Value, CultureInfo.InvariantCulture);
            else if (int.TryParse(Key, out _))
            {
                if (!Enum.TryParse<ExternalPhase>(Value, true, out var phase) || !Enum.IsDefined(phase)) throw new FormatException();
            }
            else if (!new[] { "Namespace", "Endpoint" }.Contains(Key, StringComparer.OrdinalIgnoreCase))
                _ = TimeSpan.ParseExact(Value, "c", CultureInfo.InvariantCulture);
        }
        catch (Exception e) when (e is FormatException or OverflowException or ArgumentException)
        { throw new EventGuardConfigurationException(Path, "Invalid value. Use the documented type; durations use d.hh:mm:ss.fffffff."); }
    }
    private void Array(int maximum)
    {
        Object();
        if (Children.Count is < 1 || Children.Count > maximum
            || Enumerable.Range(0, Children.Count).Any(i => !Children.ContainsKey(i.ToString(System.Globalization.CultureInfo.InvariantCulture))))
            throw new EventGuardConfigurationException(Path, $"Use a nonempty array of at most {maximum} items with consecutive indices starting at zero.");
    }
}
