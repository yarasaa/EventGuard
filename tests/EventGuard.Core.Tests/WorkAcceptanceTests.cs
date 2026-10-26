namespace EventGuard.Core.Tests;

public sealed class WorkAcceptanceTests
{
    [Fact]
    public void Durable_policy_binding_checks_scope_components_and_both_versions()
    {
        var identity = new WorkIdentity(new("tenant", "files", "source", "created"), new("key/v1", "fp/v1"), "file-1");
        var binding = new IdentityPolicyBinding(identity.Scope.Id, "tenant", "files", "source", "created", "key/v1", "fp/v1");
        WorkAcceptance.EnsurePolicyCompatible(identity, binding);
        Assert.Throws<PolicyMismatchException>(() => WorkAcceptance.EnsurePolicyCompatible(identity, binding with { FingerprintVersion = "fp/v2" }));
        Assert.Throws<PolicyMismatchException>(() => WorkAcceptance.EnsurePolicyCompatible(identity, binding with { KeyVersion = "key/v2" }));
        Assert.Throws<PolicyMismatchException>(() => WorkAcceptance.EnsurePolicyCompatible(identity, binding with { Tenant = "other" }));
    }

    [Fact]
    public void Different_delivery_id_is_duplicate_but_different_content_is_a_conflict()
    {
        var identity = new WorkIdentity(new("tenant", "files", "source", "created"), new("v1", "v1"), "file-1");
        var submission = new WorkSubmission(identity, Guid.NewGuid(), [1], ContentFingerprint.FromBytes([1]));
        var row = new WorkItem(Guid.NewGuid(), identity.Id, identity.Scope.Id, Guid.NewGuid(), submission.Fingerprint,
            WorkState.Completed, 3, 1, 1, 0, DateTimeOffset.UtcNow, null, null, null);
        Assert.Equal(AcceptanceStatus.Duplicate, WorkAcceptance.Decide(submission, row, "file-1", false));
        var altered = new WorkSubmission(identity, Guid.NewGuid(), [2], ContentFingerprint.FromBytes([2]));
        Assert.Equal(AcceptanceStatus.Conflict, WorkAcceptance.Decide(altered, row, "file-1", false));
        Assert.Throws<InvalidOperationException>(() => WorkAcceptance.Decide(submission, row, "different-key", false));
    }
}
