using EventGuard;
using EventGuard.Hosting;
using EventGuard.Runtime;
using EventGuardWorker.Contracts;
using Microsoft.Extensions.Configuration;

namespace EventGuardWorker.Configuration;

public sealed class WorkerSettings
{
    public string Provider { get; init; } = "";
    public string Schema { get; init; } = "";
    public string Tenant { get; init; } = "";
    public string Consumer { get; init; } = "";
    public string Source { get; init; } = "";
    public int MaximumConcurrency { get; init; } = 4;
    public int LeaseSeconds { get; init; } = 30;

    public static WorkerSettings Load(IConfiguration configuration)
    {
        WorkerSettings settings;
        try
        {
            settings = configuration.GetSection("EventGuard").Get<WorkerSettings>()
                ?? throw new WorkerConfigurationException("Configure the EventGuard section in appsettings.json.");
        }
        catch (InvalidOperationException)
        { throw new WorkerConfigurationException("EventGuard settings could not be parsed. MaximumConcurrency and LeaseSeconds must be integers."); }
        if (settings.Provider is not ("PostgreSql" or "SqlServer"))
            throw new WorkerConfigurationException("EventGuard:Provider must be PostgreSql or SqlServer.");
        if (string.IsNullOrEmpty(settings.Schema) || settings.Schema.Length > 63 || !char.IsAsciiLetter(settings.Schema[0])
            || settings.Schema.Any(c => !char.IsAsciiLetterOrDigit(c) && c != '_'))
            throw new WorkerConfigurationException("EventGuard:Schema must start with an ASCII letter and contain at most 63 letters, digits or underscores.");
        if (settings.MaximumConcurrency is < 1 or > 1024 || settings.LeaseSeconds is < 1 or > 3600)
            throw new WorkerConfigurationException("EventGuard:MaximumConcurrency must be 1–1024 and LeaseSeconds 1–3600.");
        try { _ = settings.ConsumerOptions(); }
        catch (ArgumentException)
        { throw new WorkerConfigurationException("EventGuard:Tenant, Consumer and Source must be nonempty valid identity components of at most 1024 UTF-8 bytes."); }
        return settings;
    }

    public WorkScope Scope => new(Tenant, Consumer, Source, ProductChanged.Contract.Name);
    public JsonConsumerOptions<ProductChanged> ConsumerOptions() => new(Scope,
        new("message-id/v1", "raw-json/v1"), ProductChanged.Contract,
        message => (message.MessageId.ToString("D"), message.MessageId),
        new WorkPumpOptions(MaximumConcurrency, TimeSpan.FromSeconds(LeaseSeconds)));
}
