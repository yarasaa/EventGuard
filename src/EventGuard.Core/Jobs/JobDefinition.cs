using System.Collections.ObjectModel;
using System.Globalization;

namespace EventGuard;

/// <summary>Logical application boundary. The host must authorize it; a scope is not a credential.</summary>
public sealed record JobScope
{
    public string Tenant { get; }
    public string Application { get; }
    public string Id { get; }
    public JobScope(string tenant, string application)
    {
        Tenant = IdentityRules.Required(tenant, nameof(tenant));
        Application = IdentityRules.Required(application, nameof(application));
        Id = StableHash.Of("jobs/scope/v1", Tenant, Application);
    }
}

public static class JobCapabilities
{
    public const string TransactionalHandler = "transactional-handler/v1";
    public const string TransactionalItems = "transactional-items/v1";
}

/// <summary>An immutable semantic binding. Change its version when validation, mapping or handler semantics change.</summary>
public sealed class JobDefinition
{
    private readonly byte[] _plan;
    public JobScope Scope { get; }
    public string Name { get; }
    public int Version { get; }
    public string ContractName { get; }
    public int ContractVersion { get; }
    public string HandlerVersion { get; }
    public IReadOnlyList<string> RequiredCapabilities { get; }
    public string Id { get; }
    public string Fingerprint { get; }
    public WorkScope WorkScope { get; }
    public IdentityPolicy IdentityPolicy { get; }
    public byte[] CopyPlan() => (byte[])_plan.Clone();

    public JobDefinition(JobScope scope, string name, int version, string contractName, int contractVersion,
        string handlerVersion, ReadOnlySpan<byte> plan, IEnumerable<string>? requiredCapabilities = null)
    {
        Scope = scope ?? throw new ArgumentNullException(nameof(scope));
        Name = IdentityRules.Required(name, nameof(name));
        ContractName = IdentityRules.Required(contractName, nameof(contractName));
        HandlerVersion = IdentityRules.Required(handlerVersion, nameof(handlerVersion));
        if (version < 1 || contractVersion < 1) throw new ArgumentOutOfRangeException(nameof(version));
        if (plan.Length > 64 * 1024) throw new ArgumentException("Definition plan exceeds 64 KiB.", nameof(plan));
        var capabilities = (requiredCapabilities ?? [JobCapabilities.TransactionalHandler]).Take(33).ToArray();
        if (capabilities.Length is < 1 or > 32) throw new ArgumentException("Specify 1–32 capabilities.", nameof(requiredCapabilities));
        foreach (var capability in capabilities) _ = IdentityRules.Required(capability, nameof(requiredCapabilities));
        capabilities = capabilities.Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray();
        Version = version; ContractVersion = contractVersion; _plan = plan.ToArray();
        RequiredCapabilities = new ReadOnlyCollection<string>(capabilities);
        Id = StableHash.Of("jobs/definition/v1", scope.Id, Name, version.ToString(CultureInfo.InvariantCulture));
        Fingerprint = StableHash.Of(["jobs/definition-content/v1", Id, ContractName,
            contractVersion.ToString(CultureInfo.InvariantCulture), HandlerVersion,
            ContentFingerprint.FromBytes(_plan).Value, .. capabilities]);
        WorkScope = new(scope.Tenant, "job-" + Id, "eventguard.jobs/v1", ContractName);
        IdentityPolicy = new("jobs/key/v1", Fingerprint);
    }

    public void RequireCapabilities(IEnumerable<string> available)
    {
        ArgumentNullException.ThrowIfNull(available);
        var supported = available.ToHashSet(StringComparer.Ordinal);
        if (RequiredCapabilities.Any(c => !supported.Contains(c)))
            throw new NotSupportedException("The job definition requires a capability unavailable in this execution profile.");
    }
}
