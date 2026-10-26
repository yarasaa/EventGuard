namespace EventGuard.Dashboard;

public sealed class DashboardOptions
{
    public bool DevelopmentAccess { get; set; }
    public int DevelopmentPort { get; set; } = 5187;
    public string Provider { get; set; } = "PostgreSql";
    public string ConnectionString { get; set; } = "";
    public string Schema { get; set; } = "eventguard";
    public string StoreId { get; set; } = "";
    public string EnvironmentLabel { get; set; } = "";
    public string ApplicationLabel { get; set; } = "";
    public string[] AllowedHosts { get; set; } = [];
    public string Authority { get; set; } = "";
    public string ClientId { get; set; } = "";
    public string ClientSecret { get; set; } = "";
    public string KeyRingPath { get; set; } = "";
    public string KeyProtectionCertificatePath { get; set; } = "";
    public string KeyProtectionCertificatePassword { get; set; } = "";
    public KeyDecryptionCertificate[] KeyDecryptionCertificates { get; set; } = [];
    public bool BacklogEnabled { get; set; }
    public bool MonitoringEnabled { get; set; }
    public string MetricsToken { get; set; } = "";
    public EventGuard.Operations.Prometheus.PrometheusWorkerOptions Workers { get; set; } = new();
    public bool IngressMonitoringEnabled { get; set; }
    public bool AuditEnabled { get; set; }
    public bool BulkReplayEnabled { get; set; }
    public bool RecoveryEnabled { get; set; }
    public bool ReplayEnabled { get; set; }
    public bool OutboxReplayEnabled { get; set; }
    public string ReplayConnectionString { get; set; } = "";
    public ModuleRegistration[] Modules { get; set; } = [];
    public ScopeRegistration[] Scopes { get; set; } = [];
    public OutboxScopeRegistration[] OutboxScopes { get; set; } = [];

    public void Validate(bool isDevelopment, bool hasConfiguredEndpoints)
    {
        if (Workers.Endpoint.Length > 0)
        {
            if (!MonitoringEnabled) throw new InvalidOperationException("Worker monitoring requires monitoring access.");
            Workers.Validate(StoreId, DevelopmentAccess && isDevelopment);
        }
        else if (Workers.Groups.Length > 0 || Workers.BearerToken.Length > 0)
            throw new InvalidOperationException("Configure the worker monitoring endpoint before groups or credentials.");
        if ((KeyRingPath.Length > 0 || KeyProtectionCertificatePath.Length > 0) &&
            (DevelopmentAccess || !Path.IsPathFullyQualified(KeyRingPath) || !Path.IsPathFullyQualified(KeyProtectionCertificatePath)))
            throw new InvalidOperationException("Persistent protected keys require production mode, an absolute directory and an absolute certificate path.");
        if (KeyDecryptionCertificates.Length > 8 || (KeyDecryptionCertificates.Length > 0 && KeyRingPath.Length == 0)
            || KeyDecryptionCertificates.Any(c => !Path.IsPathFullyQualified(c.Path)))
            throw new InvalidOperationException("Configure at most eight decryption certificates with absolute paths and a persistent protected key ring.");
        if (MetricsToken.Length > 0 && (!MonitoringEnabled || MetricsToken.Length is < 32 or > 128 || MetricsToken.Any(c => !char.IsAsciiLetterOrDigit(c) && c is not '-' and not '_')))
            throw new InvalidOperationException("Metrics requires monitoring and a secret token of 32–128 ASCII letters, digits, hyphens or underscores.");
        if (Provider is not ("PostgreSql" or "SqlServer") || string.IsNullOrWhiteSpace(ConnectionString))
            throw new InvalidOperationException("Configure an Operations provider and read connection.");
        if ((ReplayEnabled || OutboxReplayEnabled || RecoveryEnabled) && string.IsNullOrWhiteSpace(ReplayConnectionString))
            throw new InvalidOperationException("Replay requires a separately configured command connection.");
        if (AuditEnabled && !Scopes.Any(s => s.AllowAudit) && !OutboxScopes.Any(s => s.AllowAudit))
            throw new InvalidOperationException("Audit requires at least one explicitly allowed scope.");
        if (BulkReplayEnabled && !(ReplayEnabled || OutboxReplayEnabled))
            throw new InvalidOperationException("Bulk replay requires consumer or outbox replay to be enabled.");
        if (RecoveryEnabled && Scopes.Any(s => s.AllowRecovery) && !IngressMonitoringEnabled)
            throw new InvalidOperationException("Consumer recovery requires ingress monitoring.");
        if (!Identifier(StoreId) || string.IsNullOrWhiteSpace(EnvironmentLabel) || string.IsNullOrWhiteSpace(ApplicationLabel))
            throw new InvalidOperationException("Configure a stable store ID and environment/application labels.");
        if (Scopes.Length > 100 || (Scopes.Length == 0 && OutboxScopes.Length == 0 && Modules.Length == 0)
            || Scopes.Select(s => s.Key).Distinct(StringComparer.Ordinal).Count() != Scopes.Length)
            throw new InvalidOperationException("Configure at least one consumer or outbox scope, with at most 100 unique entries in each group.");
        foreach (var scope in Scopes)
        {
            if (!Identifier(scope.Key) || string.IsNullOrWhiteSpace(scope.Label)) throw new InvalidOperationException("Invalid scope registration.");
            _ = scope.ToWorkScope();
            if (scope.Operation.Length > 120 || scope.Description.Length > 1000)
                throw new InvalidOperationException("Operation descriptions exceed their display limits.");
            if (scope.DataFields.Length > 0) _ = scope.DataSelection();
        }
        if (OutboxScopes.Length > 100 || OutboxScopes.Select(s => s.Key).Distinct(StringComparer.Ordinal).Count() != OutboxScopes.Length)
            throw new InvalidOperationException("Configure at most 100 unique outbox scopes.");
        foreach (var scope in OutboxScopes)
        {
            if (!Identifier(scope.Key) || string.IsNullOrWhiteSpace(scope.Label)
                || scope.Operation.Length > 120 || scope.Description.Length > 1000)
                throw new InvalidOperationException("Invalid outbox scope registration.");
            _ = scope.ToProducerScope();
        }
        if (Modules.Length > 100 || Modules.Select(m => m.Key).Distinct(StringComparer.Ordinal).Count() != Modules.Length)
            throw new InvalidOperationException("Configure at most 100 distinct module registrations.");
        foreach (var module in Modules)
        {
            if (!Identifier(module.Key) || string.IsNullOrWhiteSpace(module.Label) || module.Label.Length > 120 || module.Description.Length > 1000)
                throw new InvalidOperationException("Invalid module registration.");
            _ = new EventGuard.Operations.ModuleQuery(module.Module, module.ScopeId);
        }
        if (DevelopmentAccess)
        {
            if (!isDevelopment || hasConfiguredEndpoints || DevelopmentPort is < 1024 or > 65535)
                throw new InvalidOperationException("Development access requires Development environment and the exclusive loopback listener.");
            return;
        }
        if (!Uri.TryCreate(Authority, UriKind.Absolute, out var authority) || authority.Scheme != "https" || authority.UserInfo.Length != 0
            || string.IsNullOrWhiteSpace(ClientId) || string.IsNullOrWhiteSpace(ClientSecret)
            || AllowedHosts.Length == 0 || AllowedHosts.Any(h => Uri.CheckHostName(h) == UriHostNameType.Unknown))
            throw new InvalidOperationException("Production dashboard requires HTTPS OIDC, client credentials and exact allowed host names.");
    }

    private static bool Identifier(string value) => value.Length is > 0 and <= 100 && value.All(c => char.IsAsciiLetterOrDigit(c) || c is '-' or '_');
}

public sealed class KeyDecryptionCertificate
{
    public string Path { get; set; } = "";
    public string Password { get; set; } = "";
}

public sealed class ScopeRegistration
{
    public string Key { get; set; } = "";
    public string Label { get; set; } = "";
    public string Tenant { get; set; } = "";
    public string Consumer { get; set; } = "";
    public string Source { get; set; } = "";
    public string EventType { get; set; } = "";
    public string Operation { get; set; } = "";
    public string Description { get; set; } = "";
    public bool AllowReplay { get; set; }
    public bool AllowAudit { get; set; }
    public bool AllowRecovery { get; set; }
    public DataFieldRegistration[] DataFields { get; set; } = [];
    public WorkScope ToWorkScope() => new(Tenant, Consumer, Source, EventType);
    public EventGuard.Operations.WorkDataSelection DataSelection() => new(DataFields.Select(f => new EventGuard.Operations.WorkDataField(f.Label, f.Pointer)));
}

public sealed class DataFieldRegistration
{
    public string Label { get; set; } = "";
    public string Pointer { get; set; } = "";
}

public sealed class OutboxScopeRegistration
{
    public string Key { get; set; } = "";
    public string Label { get; set; } = "";
    public string Tenant { get; set; } = "";
    public string Producer { get; set; } = "";
    public string Operation { get; set; } = "";
    public string Description { get; set; } = "";
    public bool AllowReplay { get; set; }
    public bool AllowAudit { get; set; }
    public bool AllowRecovery { get; set; }
    public ProducerScope ToProducerScope() => new(Tenant, Producer);
}

public sealed class ModuleRegistration
{
    public string Key { get; set; } = "";
    public string Label { get; set; } = "";
    public string Description { get; set; } = "";
    public EventGuard.Operations.ExecutionModule Module { get; set; }
    public string ScopeId { get; set; } = "";
}
