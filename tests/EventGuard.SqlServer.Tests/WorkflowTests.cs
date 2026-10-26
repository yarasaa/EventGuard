using EventGuard.Runtime;
using Microsoft.Data.SqlClient;
namespace EventGuard.SqlServer.Tests;
public sealed class WorkflowTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    private async Task<(JobDefinition Job,JobScope Scope)> Setup()
    {
        await db.Store.InitializeWorkflowsAsync(); var scope=new JobScope("tenant",Guid.NewGuid().ToString("N"));
        var job=new JobDefinition(scope,"step",1,"input",1,"v1",[1],[JobCapabilities.TransactionalHandler]); await db.Store.RegisterAsync(job); return(job,scope);
    }
    private static WorkflowRequest Request(WorkflowDefinition def) => new(def,Guid.NewGuid(),Guid.NewGuid().ToString("N"),"operator",def.Steps.Where(s=>s.Job is not null).ToDictionary(s=>s.Name,s=>new byte[]{1}));
    private async Task Complete(JobDefinition job)
    {
        var claim=await db.Claim(job.WorkScope); Assert.NotNull(claim);
        Assert.Equal(ExecutionStatus.Completed,(await db.Store.ExecuteAsync(claim!,db.Effect,new())).Status);
    }
    [Fact]
    public async Task Fork_join_is_durable_and_concurrent_advances_do_not_duplicate_children()
    {
        var (job,scope)=await Setup(); var def=new WorkflowDefinition(scope,"fork",1,[new("a",WorkflowStepKind.Job,job:job),new("b",WorkflowStepKind.Job,job:job),new("join",WorkflowStepKind.Job,[new("a"),new("b")],job)]);
        var req=Request(def); await db.Store.CreateWorkflowAsync(req);
        await Task.WhenAll(Enumerable.Range(0,6).Select(_=>db.Store.AdvanceWorkflowAsync(def,req.Id)));
        var row=(await db.Store.FindWorkflowAsync(def,req.Id))!; Assert.Equal(2,row.Steps.Count(s=>s.JobId.HasValue));
        await Complete(job); row=await db.Store.AdvanceWorkflowAsync(def,req.Id); Assert.Null(row.Steps.Single(s=>s.Name=="join").JobId);
        await Complete(job); row=await new SqlServerWorkStore(db.DataSource,db.Schema).AdvanceWorkflowAsync(def,req.Id); Assert.NotNull(row.Steps.Single(s=>s.Name=="join").JobId);
        await Complete(job); Assert.Equal(WorkflowState.Completed,(await db.Store.AdvanceWorkflowAsync(def,req.Id)).State); Assert.Null(await db.Claim(job.WorkScope));
        Assert.Equal(WorkflowState.Completed,(await db.Store.CreateWorkflowAsync(req)).State);
    }
    [Fact]
    public async Task Early_signal_selects_branch_and_merge_accepts_completed_and_skipped_dependencies()
    {
        var (job,scope)=await Setup(); var def=new WorkflowDefinition(scope,"choice",1,[new("choice",WorkflowStepKind.Signal),new("yes",WorkflowStepKind.Job,[new("choice","yes")],job),new("no",WorkflowStepKind.Job,[new("choice","no")],job),new("merge",WorkflowStepKind.Delay,[new("yes"),new("no")])]);
        var req=Request(def); await db.Store.CreateWorkflowAsync(req); var signal=new WorkflowSignal(scope,req.Id,"choice",Guid.NewGuid(),"yes","user");
        Assert.Equal(WorkflowSignalStatus.Recorded,await db.Store.SignalWorkflowAsync(signal)); Assert.Equal(WorkflowSignalStatus.AlreadyRecorded,await db.Store.SignalWorkflowAsync(signal));
        await Assert.ThrowsAsync<InvalidOperationException>(()=>db.Store.SignalWorkflowAsync(new(scope,req.Id,"choice",Guid.NewGuid(),"no","user")));
        var row=await db.Store.AdvanceWorkflowAsync(def,req.Id); Assert.Equal(WorkflowStepState.Skipped,row.Steps.Single(s=>s.Name=="no").State);
        await Complete(job); Assert.Equal(WorkflowState.Completed,(await db.Store.AdvanceWorkflowAsync(def,req.Id)).State); Assert.Null(await db.Claim(job.WorkScope));
    }
    [Fact]
    public async Task Timer_pause_cancel_and_expiry_prevent_new_dispatch()
    {
        var (job,scope)=await Setup(); var def=new WorkflowDefinition(scope,"timer",1,[new("wait",WorkflowStepKind.Delay,delay:TimeSpan.FromHours(1)),new("send",WorkflowStepKind.Job,[new("wait")],job)]);
        var req=Request(def); await db.Store.CreateWorkflowAsync(req); var row=await db.Store.AdvanceWorkflowAsync(def,req.Id);
        var due=row.Steps[0].DueAt; Assert.Equal(due,(await db.Store.AdvanceWorkflowAsync(def,req.Id)).Steps[0].DueAt);
        row=(await db.Store.FindWorkflowAsync(def,req.Id))!; row=await db.Store.ControlWorkflowAsync(def,req.Id,row.Version,true,false,"operator","maintenance");
        await using var c=await db.DataSource.OpenConnectionAsync(); await using var cmd=new SqlCommand($"UPDATE {db.Schema}.workflow_steps SET due_at=@past WHERE workflow_id=@id AND name='wait'",c);
        cmd.Parameters.AddWithValue("past",DateTimeOffset.UtcNow.AddHours(-1)); cmd.Parameters.AddWithValue("id",req.Id); await cmd.ExecuteNonQueryAsync();
        Assert.Equal(WorkflowState.Paused,(await db.Store.AdvanceWorkflowAsync(def,req.Id)).State); Assert.Null(await db.Claim(job.WorkScope));
        row=await db.Store.ControlWorkflowAsync(def,req.Id,row.Version,false,true,"operator","cancel"); Assert.Equal(WorkflowState.Cancelled,(await db.Store.AdvanceWorkflowAsync(def,req.Id)).State);
        var another=Request(def); await db.Store.CreateWorkflowAsync(another); await db.Store.AdvanceWorkflowAsync(def,another.Id);
        cmd.Parameters["id"].Value=another.Id; await cmd.ExecuteNonQueryAsync(); await db.Store.AdvanceWorkflowAsync(def,another.Id); await Complete(job);
        Assert.Equal(WorkflowState.Completed,(await db.Store.AdvanceWorkflowAsync(def,another.Id)).State);
        var expired=new WorkflowRequest(def,Guid.NewGuid(),"expired","user",new Dictionary<string,byte[]>{{"send",[1]}},DateTimeOffset.UtcNow.AddMinutes(-1));
        await db.Store.CreateWorkflowAsync(expired); Assert.Equal(WorkflowState.Expired,(await db.Store.AdvanceWorkflowAsync(def,expired.Id)).State); Assert.Null(await db.Claim(job.WorkScope));
    }
    [Fact]
    public async Task Business_update_and_signal_share_rollback_and_commit()
    {
        var (_,scope)=await Setup(); var def=new WorkflowDefinition(scope,"signal",1,[new("result",WorkflowStepKind.Signal)]); var req=Request(def); await db.Store.CreateWorkflowAsync(req);
        var signal=new WorkflowSignal(scope,req.Id,"result",Guid.NewGuid(),"accepted","handler"); var producer=new ProducerRequest(new(scope.Tenant,"results"),Guid.NewGuid(),ContentFingerprint.FromBytes([1]));
        async ValueTask Apply(SqlServerSession session,CancellationToken ct,bool fail)
        {
            await using var cmd=session.CreateCommand($"INSERT INTO {db.Schema}.business_effects VALUES(@id,'status-updated')"); cmd.Parameters.AddWithValue("id",req.Id); await cmd.ExecuteNonQueryAsync(ct);
            await session.WorkflowSignals.SignalAsync(signal,ct); if(fail) throw new InvalidOperationException("rollback");
        }
        await Assert.ThrowsAsync<InvalidOperationException>(()=>db.Outbox.ProduceAsync(producer,(s,ct)=>Apply(s,ct,true)));
        Assert.Equal(0,await db.EffectCount(req.Id)); Assert.Equal(WorkflowState.Running,(await db.Store.AdvanceWorkflowAsync(def,req.Id)).State);
        await db.Outbox.ProduceAsync(producer,(s,ct)=>Apply(s,ct,false)); Assert.Equal(1,await db.EffectCount(req.Id)); Assert.Equal(WorkflowState.Completed,(await db.Store.AdvanceWorkflowAsync(def,req.Id)).State);
        await Assert.ThrowsAsync<InvalidOperationException>(()=>db.Store.SignalWorkflowAsync(new(new("foreign",scope.Application),req.Id,"result",Guid.NewGuid(),"accepted","user")));
    }
    [Fact]
    public async Task Child_receipt_failure_rolls_back_child_and_advance()
    {
        var (job,scope)=await Setup(); var def=new WorkflowDefinition(scope,"atomic",1,[new("job",WorkflowStepKind.Job,job:job)]); var req=Request(def); await db.Store.CreateWorkflowAsync(req);
        await using var c=await db.DataSource.OpenConnectionAsync(); await using var cmd=new SqlCommand($"ALTER TABLE {db.Schema}.workflow_steps ADD CONSTRAINT fail_advance CHECK(workflow_id <> '{req.Id:D}' OR job_id IS NULL)",c); await cmd.ExecuteNonQueryAsync();
        try { await Assert.ThrowsAnyAsync<System.Data.Common.DbException>(()=>db.Store.AdvanceWorkflowAsync(def,req.Id)); }
        finally { cmd.CommandText=$"ALTER TABLE {db.Schema}.workflow_steps DROP CONSTRAINT fail_advance"; await cmd.ExecuteNonQueryAsync(); }
        Assert.Null(await db.Claim(job.WorkScope)); Assert.Equal(0,(await db.Store.FindWorkflowAsync(def,req.Id))!.Version);
        await db.Store.AdvanceWorkflowAsync(def,req.Id); await Complete(job); Assert.Equal(WorkflowState.Completed,(await db.Store.AdvanceWorkflowAsync(def,req.Id)).State);
    }
    [Theory]
    [InlineData(0)] [InlineData(1)] [InlineData(2)]
    public async Task Lost_create_advance_or_signal_commit_reply_resolves_without_second_job(int phase)
    {
        var (job,scope)=await Setup(); var def=new WorkflowDefinition(scope,"lost",1,[new("job",WorkflowStepKind.Job,job:job),new("signal",WorkflowStepKind.Signal)]); var req=Request(def); var signal=new WorkflowSignal(scope,req.Id,"signal",Guid.NewGuid(),"done","user");
        if(phase!=0) await db.Store.CreateWorkflowAsync(req);
        var settings = new SqlConnectionStringBuilder(db.ConnectionString);
        await using var proxy = new CommitReplyDropProxy();
        settings.DataSource = $"127.0.0.1,{proxy.Port}"; settings.Pooling = false; settings.Encrypt = SqlConnectionEncryptOption.Optional;
        var source = new SqlServerDataSource(settings.ConnectionString);
        var interrupted = new SqlServerWorkStore(source, db.Schema);
        await Assert.ThrowsAsync<CommitOutcomeUnknownException>(async()=>
        {
            if(phase==0) await interrupted.CreateWorkflowAsync(req); else if(phase==1) await interrupted.AdvanceWorkflowAsync(def,req.Id); else await interrupted.SignalWorkflowAsync(signal);
        });
        await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(5)); await db.Store.CreateWorkflowAsync(req); await db.Store.SignalWorkflowAsync(signal); await db.Store.AdvanceWorkflowAsync(def,req.Id);
        await Complete(job); Assert.Null(await db.Claim(job.WorkScope)); Assert.Equal(WorkflowState.Completed,(await db.Store.AdvanceWorkflowAsync(def,req.Id)).State);
    }
    [Fact]
    public async Task Cycles_foreign_tenants_and_changed_business_intents_are_rejected()
    {
        var (job,scope)=await Setup(); Assert.Throws<ArgumentException>(()=>new WorkflowDefinition(scope,"cycle",1,[new("a",WorkflowStepKind.Delay,[new("b")]),new("b",WorkflowStepKind.Delay,[new("a")])]));
        Assert.Throws<ArgumentException>(()=>new WorkflowDefinition(new("other","app"),"foreign",1,[new("a",WorkflowStepKind.Job,job:job)]));
        var def=new WorkflowDefinition(scope,"plan",1,[new("a",WorkflowStepKind.Job,job:job)]); var req=Request(def); await db.Store.CreateWorkflowAsync(req);
        await Assert.ThrowsAsync<InvalidOperationException>(()=>db.Store.CreateWorkflowAsync(new(def,Guid.NewGuid(),req.BusinessKey,"operator",new Dictionary<string,byte[]>{{"a",[2]}})));
    }
}
