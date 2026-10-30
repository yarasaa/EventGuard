using EventGuard.Runtime;
using Npgsql;

namespace EventGuard.PostgreSql.Tests;

public sealed class NativeResultBatchTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    private static readonly RetryPolicy Retry = new(initialDelay:TimeSpan.FromMilliseconds(1), jitterRatio:0);
    private async Task<(ExternalAction[] Actions, ExternalResultTarget Target)> Seed(int count=100, int payloadBytes=1, bool observe=false)
    {
        await db.Store.InitializeExternalActionsAsync();
        var scope = new ProducerScope("result-test-"+Guid.NewGuid().ToString("N"),"sender");
        var target = new ExternalResultTarget(scope,"projection","v1");
        var actions = Enumerable.Range(0,count).Select(_ => new ExternalAction(scope,Guid.NewGuid(),"service","v1",new byte[payloadBytes],observe ? TimeSpan.FromHours(1) : null)).ToArray();
        await db.Outbox.ProduceAsync(new(scope,Guid.NewGuid(),ContentFingerprint.FromBytes([1])),async (s,ct) =>
        { await s.ExternalActions.EnqueueBatchAsync(new(actions.Select(a=>new ExternalActionIntent(a,target))),ct); });
        var claims = await db.Store.ClaimActionsAsync(new(scope,"service","v1",ExternalPhase.Submit,Guid.NewGuid(),TimeSpan.FromMinutes(1),count,64*1024*1024));
        Assert.Equal(count,claims.Count);
        await db.Store.SettleActionsAsync(new(claims.Select(c=>new ExternalBatchSettlementItem(c,new(ExternalResultKind.Accepted,"ok"))),new()));
        return (actions,target);
    }
    private async ValueTask Write(IReadOnlyList<ExternalActionUpdate> updates, PostgreSqlSession session, CancellationToken ct)
    {
        await using var command=session.CreateCommand($"INSERT INTO {db.Schema}.business_effects(work_id,note) SELECT id,'applied' FROM unnest(@ids) AS x(id)");
        command.Parameters.AddWithValue("ids",updates.Select(u=>u.Action.Id).ToArray());
        await command.ExecuteNonQueryAsync(ct);
    }
    private async Task<int> Count(ExternalResultTarget target)
    {
        await using var c=await db.DataSource.OpenConnectionAsync();
        await using var q=new NpgsqlCommand($"SELECT COUNT(*) FROM {db.Schema}.business_effects WHERE work_id IN(SELECT id FROM {db.Schema}.external_actions WHERE scope_id=@scope)",c);
        q.Parameters.AddWithValue("scope",target.Scope.Id);return Convert.ToInt32(await q.ExecuteScalarAsync());
    }
    private async Task ExecuteSql(ExternalResultTarget target,string sql)
    {
        await using var c=await db.DataSource.OpenConnectionAsync();await using var q=new NpgsqlCommand(sql,c);
        q.Parameters.AddWithValue("scope",target.WorkScope.Id);q.Parameters.AddWithValue("producer",target.Scope.Id);await q.ExecuteNonQueryAsync();
    }
    private Task<ExternalResultBatchExecution> Run(ExternalResultTarget target,int count=100,
        Func<IReadOnlyList<ExternalActionUpdate>,PostgreSqlSession,CancellationToken,ValueTask>? handler=null,int bytes=8*1024*1024) =>
        db.Store.ExecuteResultBatchAsync(target,new(count,bytes),handler??Write,Retry);

    [Fact]
    public async Task One_callback_applies_a_page_and_repeated_poll_is_empty()
    {
        var (_,t)=await Seed();var calls=0;
        var r=await Run(t,handler:async (u,s,ct)=>{calls++;Assert.Equal(100,u.Count);Assert.Throws<InvalidOperationException>(()=>s.Outbox);await Write(u,s,ct);});
        Assert.Equal(100,r.Applied);Assert.Equal(100,r.Works.Count);Assert.All(r.Works,w=>Assert.Equal(ExecutionStatus.Completed,w.Status));
        Assert.Empty((await Run(t)).Works);Assert.Equal(1,calls);Assert.Equal(100,await Count(t));
    }
    [Fact]
    public async Task Failed_callback_rolls_back_the_entire_page_then_retries_without_resending()
    {
        var (_,t)=await Seed();
        var failed=await Run(t,handler:async(u,s,ct)=>{await Write(u,s,ct);throw new IOException("injected");});
        Assert.Equal(100,failed.Works.Count);Assert.All(failed.Works,w=>Assert.Equal(ExecutionStatus.RetryScheduled,w.Status));
        Assert.Equal(0,await Count(t));
        var done=await Run(t);Assert.Equal(100,done.Applied);Assert.All(done.Works,w=>Assert.Equal(2,w.Work.AttemptCount));
        Assert.Equal(100,await Count(t));
    }
    [Fact]
    public async Task Parallel_workers_take_disjoint_pages()
    {
        var (_,t)=await Seed(200);
        var results=await Task.WhenAll(Run(t,50),Run(t,50),Run(t,50),Run(t,50));
        Assert.Equal(200,results.Sum(r=>r.Applied));Assert.Equal(200,results.SelectMany(r=>r.Works).Select(w=>w.Work.Id).Distinct().Count());
        Assert.Equal(200,await Count(t));
    }
    [Fact]
    public async Task Latest_version_is_coalesced_and_late_single_worker_cannot_regress_it()
    {
        var (actions,t)=await Seed(1,observe:true);var old=(await db.Claim(t.WorkScope))!;
        await ExecuteSql(t,$"UPDATE {db.Schema}.external_actions SET next_at=@past WHERE scope_id=@producer".Replace("@past","clock_timestamp()-interval '1 minute'"));
        var claims=await db.Store.ClaimActionsAsync(new(t.Scope,"service","v1",ExternalPhase.Observe,Guid.NewGuid(),TimeSpan.FromMinutes(1),1,1024));
        await db.Store.SettleActionsAsync(new(claims.Select(c=>new ExternalBatchSettlementItem(c,new(ExternalResultKind.Succeeded,"delivered"))),new()));
        Assert.Equal(1,(await Run(t,handler:async(u,s,ct)=>{Assert.Equal(ExternalObservationState.Succeeded,Assert.Single(u).Observation);await Write(u,s,ct);})).Applied);
        Assert.Equal(ExecutionStatus.Completed,(await db.Store.ExecuteAsync(old,async(_,_,s,ct)=>
        { Assert.Equal(ExternalResultApplyStatus.AlreadyApplied,await s.ExternalResults.ApplyAsync(t,(_,_)=>throw new Exception("stale callback"),ct)); },Retry)).Status);
        Assert.Equal(1,await Count(t));
        // A second action keeps both versions pending inside the same page.
        var (_,t2)=await Seed(1,observe:true);
        await ExecuteSql(t2,$"UPDATE {db.Schema}.external_actions SET next_at=clock_timestamp()-interval '1 minute' WHERE scope_id=@producer");
        var next=await db.Store.ClaimActionsAsync(new(t2.Scope,"service","v1",ExternalPhase.Observe,Guid.NewGuid(),TimeSpan.FromMinutes(1),1,1024));
        await db.Store.SettleActionsAsync(new(next.Select(c=>new ExternalBatchSettlementItem(c,new(ExternalResultKind.Succeeded,"done"))),new()));
        var coalesced=await Run(t2);Assert.Equal(1,coalesced.Applied);Assert.Equal(1,coalesced.Superseded);Assert.Equal(2,coalesced.Works.Count);
    }
    [Fact]
    public async Task Byte_bound_applies_before_payload_hydration()
    {
        var (_,t)=await Seed(3,600*1024);
        var result=await Run(t,bytes:1152*1024);Assert.Equal(1,result.Applied);
        Assert.Equal(2,(await Run(t)).Applied);
    }
    [Fact]
    public async Task Tenant_claim_budget_is_charged_per_work_not_per_page()
    {
        var (_,t)=await Seed(10);
        await db.Store.ConfigureBudgetAsync(new(Guid.NewGuid(),new(t.Scope.Tenant,86400,maximumClaims:3),-1,"test","limit"));
        Assert.Equal(3,(await Run(t)).Applied);Assert.Empty((await Run(t)).Works);
        Assert.Equal(3,(await db.Store.FindBudgetAsync(t.Scope.Tenant))!.Claims);
    }
    [Fact]
    public async Task Expired_lease_is_recovered_and_old_owner_is_fenced()
    {
        var (_,t)=await Seed(1);var old=(await db.Claim(t.WorkScope))!;
        await ExecuteSql(t,$"UPDATE {db.Schema}.works SET lease_until=clock_timestamp()-interval '1 minute' WHERE scope_id=@scope");
        var done=await Run(t);Assert.Equal(1,done.Applied);Assert.Equal(2,Assert.Single(done.Works).Work.AttemptCount);
        Assert.Equal(ExecutionStatus.LeaseLost,(await db.Store.ExecuteAsync(old,(_,_,_,_)=>throw new Exception("old owner"),Retry)).Status);
    }
    [Fact]
    public async Task Corrupt_snapshot_never_reaches_the_application_callback()
    {
        var (_,t)=await Seed(2);
        await ExecuteSql(t,$"UPDATE {db.Schema}.external_result_updates SET state_data=decode('01','hex') WHERE scope_id=@producer");
        var calls=0;var result=await Run(t,handler:(_,_,_)=>{calls++;return ValueTask.CompletedTask;});
        Assert.Equal(0,calls);Assert.All(result.Works,w=>Assert.Equal(ExecutionStatus.RetryScheduled,w.Status));Assert.Equal(0,await Count(t));
    }
    [Fact]
    public async Task Cancellation_rolls_back_sql_and_records_retry_using_an_independent_deadline()
    {
        var (_,t)=await Seed(2);using var stop=new CancellationTokenSource();
        var result=await db.Store.ExecuteResultBatchAsync(t,new(),async(u,s,ct)=>{await Write(u,s,ct);stop.Cancel();ct.ThrowIfCancellationRequested();},Retry,stop.Token);
        Assert.All(result.Works,w=>Assert.Equal(ExecutionStatus.RetryScheduled,w.Status));Assert.Equal(0,await Count(t));
        Assert.Equal(2,(await Run(t)).Applied);
    }

    [Fact]
    public async Task Lost_result_commit_reply_does_not_repeat_application_effects()
    {
        var (_,target)=await Seed(10);
        await using var proxy = new CommitReplyDropProxy(new NpgsqlConnectionStringBuilder(db.ConnectionString).Port);
        var settings = new NpgsqlConnectionStringBuilder(db.ConnectionString) { Host="127.0.0.1",Port=proxy.Port,SslMode=SslMode.Disable,Pooling=false };
        settings.DisableGssEncryptionForFaultProxy();
        await using var source = NpgsqlDataSource.Create(settings.ConnectionString);
        var interrupted = new PostgreSqlWorkStore(source,db.Schema);
        await Assert.ThrowsAsync<CommitOutcomeUnknownException>(()=>interrupted.ExecuteResultBatchAsync(target,new(),Write,Retry));
        await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(10,await Count(target));Assert.Empty((await Run(target)).Works);
    }

    [Fact]
    public async Task Permanent_failure_and_expiry_never_invoke_the_handler_again()
    {
        var (_,target)=await Seed(2);
        var failed=await db.Store.ExecuteResultBatchAsync(target,new(),(_,_,_)=>throw new PermanentWorkException("rejected"),Retry);
        Assert.All(failed.Works,w=>Assert.Equal(ExecutionStatus.DeadLetter,w.Status));Assert.Empty((await Run(target)).Works);
        var (_,expired)=await Seed(2);
        await ExecuteSql(expired,$"UPDATE {db.Schema}.works SET expires_at=clock_timestamp()-interval '1 minute' WHERE scope_id=@scope");
        var result=await Run(expired,handler:(_,_,_)=>throw new Exception("expired handler"));
        Assert.Equal(2,result.Works.Count);Assert.All(result.Works,w=>Assert.Equal(ExecutionStatus.Expired,w.Status));
    }
}
