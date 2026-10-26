namespace EventGuardWorker.Configuration;

// Only application-authored messages belong here; never wrap native driver exceptions.
public sealed class WorkerConfigurationException(string message) : Exception(message);
