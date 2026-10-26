namespace EventGuard.Core.Tests;

public sealed class JobContractsTests
{
    private static JobDefinition Definition(int version = 1, byte[]? plan = null) =>
        new(new("tenant", "application"), "rebuild", version, "parameters", 1, "handler-1", plan ?? [1]);

    [Fact]
    public void Definition_freezes_plan_and_capabilities_and_detects_semantic_changes()
    {
        byte[] plan = [1, 2]; string[] capabilities = [JobCapabilities.TransactionalHandler];
        var definition = new JobDefinition(new("t", "a"), "job", 1, "params", 1, "h1", plan, capabilities);
        var fingerprint = definition.Fingerprint;
        plan[0] = 9; capabilities[0] = "other"; definition.CopyPlan()[0] = 8;
        Assert.Equal(new byte[] { 1, 2 }, definition.CopyPlan());
        Assert.Equal(JobCapabilities.TransactionalHandler, Assert.Single(definition.RequiredCapabilities));
        Assert.Equal(fingerprint, definition.Fingerprint);
        Assert.NotEqual(Definition(plan: [2]).Fingerprint, Definition().Fingerprint);
        Assert.Equal(Definition(plan: [2]).Id, Definition().Id);
    }

    [Fact]
    public void Business_identity_survives_definition_upgrade_and_request_id_changes()
    {
        var first = new JobSubmission(Definition(), Guid.NewGuid(), "business-1", "actor", [1]);
        var next = new JobSubmission(Definition(2), Guid.NewGuid(), "business-1", "actor", [1]);
        Assert.Equal(first.BusinessIdentity, next.BusinessIdentity);
        Assert.NotEqual(first.OperationFingerprint, next.OperationFingerprint);
        Assert.NotEqual(first.Definition.WorkScope.Id, next.Definition.WorkScope.Id);
    }

    [Fact]
    public void Same_definition_version_with_changed_handler_has_different_execution_binding()
    {
        var first = Definition();
        var second = new JobDefinition(first.Scope, first.Name, first.Version, first.ContractName, first.ContractVersion, "handler-2", [1]);
        Assert.Equal(first.Id, second.Id);
        Assert.Equal(first.WorkScope, second.WorkScope);
        Assert.NotEqual(first.IdentityPolicy, second.IdentityPolicy);
    }

    [Fact]
    public void Parameters_are_frozen_and_schedule_is_in_the_semantic_fingerprint()
    {
        byte[] bytes = [1]; var definition = Definition(); var id = Guid.NewGuid();
        var submission = new JobSubmission(definition, id, "key", "actor", bytes);
        bytes[0] = 3; submission.CopyParameters()[0] = 4;
        Assert.Equal(new byte[] { 1 }, submission.CopyParameters());
        Assert.NotEqual(submission.OperationFingerprint,
            new JobSubmission(definition, id, "key", "actor", [1], DateTimeOffset.UtcNow).OperationFingerprint);
        Assert.Equal(submission.OperationFingerprint,
            new JobSubmission(definition, Guid.NewGuid(), "key", "other-actor", [1]).OperationFingerprint);
        Assert.NotEqual(submission.RequestFingerprint,
            new JobSubmission(definition, id, "key", "other-actor", [1]).RequestFingerprint);
    }

    [Fact]
    public void Logical_application_and_tenant_both_isolate_requests_and_business_identity()
    {
        var a = new JobScope("tenant-a", "app"); var b = new JobScope("tenant-b", "app"); var c = new JobScope("tenant-a", "other");
        Assert.Equal(3, new[] { a.Id, b.Id, c.Id }.Distinct().Count());
    }

    [Fact]
    public void Unsupported_capabilities_and_invalid_limits_fail_before_admission()
    {
        var definition = new JobDefinition(new("t", "a"), "job", 1, "parameters", 1, "h1", [], ["external-send/v1"]);
        Assert.Throws<NotSupportedException>(() => definition.RequireCapabilities([JobCapabilities.TransactionalHandler]));
        Assert.Throws<ArgumentException>(() => Definition(plan: new byte[65537]));
        Assert.Throws<ArgumentOutOfRangeException>(() => Definition(0));
        Assert.Throws<ArgumentException>(() => new JobSubmission(Definition(), Guid.Empty, "key", "actor", []));
        Assert.Throws<ArgumentException>(() => new JobSubmission(Definition(), Guid.NewGuid(), "key", "actor", new byte[1048577]));
    }

    [Fact]
    public void Scheduling_precision_matches_postgresql_and_sqlserver()
    {
        var date = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var first = new JobSubmission(Definition(), Guid.NewGuid(), "key", "actor", [], date.AddTicks(1));
        var second = new JobSubmission(Definition(), Guid.NewGuid(), "key", "actor", [], date.AddTicks(9));
        Assert.Equal(first.OperationFingerprint, second.OperationFingerprint);
        Assert.Throws<ArgumentException>(() => new JobSubmission(Definition(), Guid.NewGuid(), "key", "actor", [], date, date.AddTicks(1)));
    }
}
