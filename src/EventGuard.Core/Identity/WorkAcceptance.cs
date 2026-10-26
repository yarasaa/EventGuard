namespace EventGuard;

// Raw persisted values avoid re-hashing the entire scope on every acceptance.
public readonly record struct IdentityPolicyBinding(string ScopeId, string Tenant, string Consumer,
    string Source, string EventType, string KeyVersion, string FingerprintVersion);

public static class WorkAcceptance
{
    public static void EnsurePolicyCompatible(WorkIdentity identity, IdentityPolicyBinding binding)
    {
        ArgumentNullException.ThrowIfNull(identity);
        var scope = identity.Scope;
        if (binding.ScopeId != scope.Id || binding.Tenant != scope.Tenant || binding.Consumer != scope.Consumer
            || binding.Source != scope.Source || binding.EventType != scope.EventType
            || binding.KeyVersion != identity.Policy.KeyVersion || binding.FingerprintVersion != identity.Policy.FingerprintVersion)
            throw new PolicyMismatchException(scope.Id);
    }

    public static AcceptanceStatus Decide(WorkSubmission submission, WorkItem stored, string storedBusinessKey, bool inserted)
    {
        ArgumentNullException.ThrowIfNull(submission);
        ArgumentNullException.ThrowIfNull(stored);
        var identity = submission.Identity;
        if (stored.IdentityId != identity.Id || stored.ScopeId != identity.Scope.Id || storedBusinessKey != identity.BusinessKey)
            throw new InvalidOperationException("Identity digest collision or mismatched durable row detected.");
        if (inserted)
        {
            if (stored.Fingerprint != submission.Fingerprint) throw new InvalidOperationException("Inserted payload fingerprint changed.");
            return AcceptanceStatus.Accepted;
        }
        return stored.Fingerprint == submission.Fingerprint ? AcceptanceStatus.Duplicate : AcceptanceStatus.Conflict;
    }
}
