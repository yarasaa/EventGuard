namespace EventGuard.Store.Conformance;

public interface IJobStoreFixture<TSession> : IWorkStoreFixture<TSession>
{
    ITransactionalJobStore<TSession> Jobs { get; }
    Task InitializeJobsAsync();
}

public abstract class JobStoreContractTests<TSession>(IJobStoreFixture<TSession> fixture)
{
    protected readonly IJobStoreFixture<TSession> Fixture = fixture;
    private static JobDefinition Definition(JobScope? scope = null, int version = 1, byte[]? plan = null) =>
        new(scope ?? new("tenant", Guid.NewGuid().ToString("N")), "data-job", version, "parameters", 1, "handler-v1", plan ?? [1]);
    private static JobSubmission Request(JobDefinition definition, Guid? id = null, string key = "operation", byte[]? input = null) =>
        new(definition, id ?? Guid.NewGuid(), key, "test-actor", input ?? [1]);
    private async Task<JobDefinition> Registered()
    {
        await Fixture.InitializeJobsAsync(); var definition = Definition();
        Assert.Equal(JobRegistrationStatus.Registered, await Fixture.Jobs.RegisterAsync(definition)); return definition;
    }

    [Fact]
    public async Task Job_schema_initialization_and_registration_are_repeatable()
    {
        var definition = await Registered();
        await Fixture.InitializeJobsAsync();
        Assert.Equal(JobRegistrationStatus.AlreadyRegistered, await Fixture.Jobs.RegisterAsync(definition));
        await Fixture.Jobs.ValidateAsync(definition);
    }

    [Fact]
    public async Task Concurrent_registration_serializes_an_immutable_version()
    {
        await Fixture.InitializeJobsAsync(); var definition = Definition();
        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Fixture.Jobs.RegisterAsync(definition)));
        Assert.Single(results, r => r == JobRegistrationStatus.Registered);
        Assert.Equal(7, results.Count(r => r == JobRegistrationStatus.AlreadyRegistered));
    }

    [Fact]
    public async Task Concurrent_requests_create_one_durable_operation_and_one_effect()
    {
        var definition = await Registered(); var request = Request(definition);
        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Fixture.Jobs.SubmitAsync(request)));
        Assert.Single(results, r => r.Status == JobSubmissionStatus.Accepted);
        Assert.Single(results.Select(r => r.OperationId).Distinct());
        Assert.Equal(7, results.Count(r => r.Status == JobSubmissionStatus.AlreadyAccepted && r.RepeatedRequest));
        var claimed = await Fixture.Jobs.ClaimNextAsync(definition.WorkScope, Guid.NewGuid(), TimeSpan.FromSeconds(10));
        Assert.NotNull(claimed);
        var executed = await Fixture.Jobs.ExecuteAsync(claimed, Fixture.Effect, new());
        Assert.Equal(ExecutionStatus.Completed, executed.Status);
        var saved = await Fixture.Jobs.FindAsync(definition.Scope, results[0].OperationId);
        Assert.NotNull(saved); Assert.Equal(WorkState.Completed, saved.State); Assert.Equal(1, saved.AttemptCount);
        Assert.Equal(1, await Fixture.EffectCount(saved.Id));
        Assert.Null(await Fixture.Jobs.ClaimNextAsync(definition.WorkScope, Guid.NewGuid(), TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task Concurrent_new_request_ids_for_one_business_intent_return_one_operation()
    {
        var definition = await Registered();
        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Fixture.Jobs.SubmitAsync(Request(definition))));
        Assert.Single(results, r => r.Status == JobSubmissionStatus.Accepted);
        Assert.Single(results.Select(r => r.OperationId).Distinct());
        Assert.Equal(7, results.Count(r => r.Status == JobSubmissionStatus.AlreadyAccepted));
        foreach (var result in results) Assert.Equal(result.OperationId, (await Fixture.Jobs.FindRequestAsync(definition.Scope, result.RequestId))!.OperationId);
    }

    [Fact]
    public async Task Changed_request_content_conflicts_without_creating_a_second_business_operation()
    {
        var definition = await Registered(); var id = Guid.NewGuid();
        var first = await Fixture.Jobs.SubmitAsync(Request(definition, id));
        var conflict = await Fixture.Jobs.SubmitAsync(Request(definition, id, "different", [2]));
        Assert.Equal(JobSubmissionStatus.RequestConflict, conflict.Status); Assert.Equal(first.OperationId, conflict.OperationId);
        Assert.Equal(JobSubmissionStatus.Accepted, (await Fixture.Jobs.FindRequestAsync(definition.Scope, id))!.Status);
        Assert.Equal(JobSubmissionStatus.AlreadyAccepted, (await Fixture.Jobs.SubmitAsync(Request(definition, id))).Status);
        Assert.Equal(JobSubmissionStatus.Accepted, (await Fixture.Jobs.SubmitAsync(Request(definition, key: "different", input: [2]))).Status);
    }

    [Fact]
    public async Task Business_conflict_is_a_durable_refusal_and_never_rewrites_the_original()
    {
        var definition = await Registered(); var first = await Fixture.Jobs.SubmitAsync(Request(definition));
        var other = Request(definition, input: [2]); var refusal = await Fixture.Jobs.SubmitAsync(other);
        Assert.Equal(JobSubmissionStatus.BusinessConflict, refusal.Status); Assert.Equal(first.OperationId, refusal.OperationId);
        Assert.Equal(JobSubmissionStatus.BusinessConflict, (await Fixture.Jobs.SubmitAsync(other)).Status);
        Assert.Equal(JobSubmissionStatus.RequestConflict, (await Fixture.Jobs.SubmitAsync(Request(definition, other.RequestId))).Status);
        var claim = await Fixture.Jobs.ClaimNextAsync(definition.WorkScope, Guid.NewGuid(), TimeSpan.FromSeconds(10));
        Assert.Equal(new byte[] { 1 }, claim!.Payload);
    }

    [Fact]
    public async Task Definition_upgrade_does_not_authorize_a_second_business_effect()
    {
        var first = await Registered(); var next = Definition(first.Scope, 2);
        await Fixture.Jobs.RegisterAsync(next);
        var accepted = await Fixture.Jobs.SubmitAsync(Request(first));
        Assert.Equal(JobSubmissionStatus.RequestConflict, (await Fixture.Jobs.SubmitAsync(Request(next, accepted.RequestId))).Status);
        Assert.Equal(JobSubmissionStatus.BusinessConflict, (await Fixture.Jobs.SubmitAsync(Request(next))).Status);
        Assert.Equal(JobSubmissionStatus.Accepted, (await Fixture.Jobs.SubmitAsync(Request(next, key: "explicit-new-intent"))).Status);
        Assert.Equal(1, (await Fixture.Jobs.FindAsync(first.Scope, accepted.OperationId))!.DefinitionVersion);
    }

    [Fact]
    public async Task Unregistered_or_changed_definition_is_rejected_before_work_is_created()
    {
        await Fixture.InitializeJobsAsync(); var definition = Definition(); var request = Request(definition);
        await Assert.ThrowsAsync<InvalidOperationException>(() => Fixture.Jobs.SubmitAsync(request));
        Assert.Null(await Fixture.Jobs.FindRequestAsync(definition.Scope, request.RequestId));
        await Fixture.Jobs.RegisterAsync(definition);
        var changed = Definition(definition.Scope, plan: [2]);
        await Assert.ThrowsAsync<JobDefinitionConflictException>(() => Fixture.Jobs.RegisterAsync(changed));
        await Assert.ThrowsAsync<JobDefinitionConflictException>(() => Fixture.Jobs.ValidateAsync(changed));
        await Assert.ThrowsAsync<JobDefinitionConflictException>(() => Fixture.Jobs.SubmitAsync(Request(changed)));
        Assert.Null(await Fixture.Jobs.ClaimNextAsync(definition.WorkScope, Guid.NewGuid(), TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task Tenant_and_application_boundaries_hide_receipts_and_operations()
    {
        var definition = await Registered(); var result = await Fixture.Jobs.SubmitAsync(Request(definition));
        foreach (var scope in new[] { new JobScope("other-tenant", definition.Scope.Application), new JobScope(definition.Scope.Tenant, "other-app") })
        {
            Assert.Null(await Fixture.Jobs.FindRequestAsync(scope, result.RequestId));
            Assert.Null(await Fixture.Jobs.FindAsync(scope, result.OperationId));
            var other = Definition(scope); await Fixture.Jobs.RegisterAsync(other);
            Assert.Equal(JobSubmissionStatus.Accepted, (await Fixture.Jobs.SubmitAsync(Request(other, result.RequestId))).Status);
        }
    }

    [Fact]
    public async Task Schedule_changes_conflict_and_future_jobs_are_not_claimed()
    {
        var definition = await Registered(); var id = Guid.NewGuid(); var due = DateTimeOffset.UtcNow.AddHours(1);
        var submission = new JobSubmission(definition, id, "operation", "test", [1], due, due.AddHours(1));
        var result = await Fixture.Jobs.SubmitAsync(submission);
        Assert.Equal(submission.NotBefore, (await Fixture.Jobs.FindAsync(definition.Scope, result.OperationId))!.NextAttemptAt);
        Assert.Null(await Fixture.Jobs.ClaimNextAsync(definition.WorkScope, Guid.NewGuid(), TimeSpan.FromSeconds(10)));
        var changed = new JobSubmission(definition, id, "operation", "test", [1], due.AddMinutes(1), due.AddHours(1));
        Assert.Equal(JobSubmissionStatus.RequestConflict, (await Fixture.Jobs.SubmitAsync(changed)).Status);
    }

    [Fact]
    public async Task Restart_resolves_request_receipts_without_new_execution()
    {
        var definition = await Registered(); var request = Request(definition);
        var accepted = await Fixture.Jobs.SubmitAsync(request);
        var restarted = Assert.IsAssignableFrom<IJobStore>(Fixture.RestartStore());
        Assert.Equal(accepted.OperationId, (await restarted.FindRequestAsync(definition.Scope, request.RequestId))!.OperationId);
        Assert.Equal(JobSubmissionStatus.AlreadyAccepted, (await restarted.SubmitAsync(request)).Status);
    }

    [Fact]
    public async Task Cancelled_submission_has_no_receipt_or_work()
    {
        var definition = await Registered(); var request = Request(definition);
        using var cancelled = new CancellationTokenSource(); cancelled.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Fixture.Jobs.SubmitAsync(request, cancelled.Token));
        Assert.Null(await Fixture.Jobs.FindRequestAsync(definition.Scope, request.RequestId));
        Assert.Null(await Fixture.Jobs.ClaimNextAsync(definition.WorkScope, Guid.NewGuid(), TimeSpan.FromSeconds(10)));
    }
}
