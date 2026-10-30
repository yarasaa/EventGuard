using EventGuard.Runtime;
using Npgsql;

namespace EventGuard.PostgreSql.Tests;

public sealed class NativePreparationTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    private JobDefinition _definition = null!;
    private ProducerScope _scope = null!;
    private static JsonMessageContract<string> Item => new("item",1,_ => 1,_ => {});
    private static JsonMessageContract<JobBatch<string>> Contract => JsonJobBatches.Contract("batch",1,Item,1000);
    private async Task<(JobDefinition Definition, ProducerScope Scope)> Setup()
    {
        await db.Store.InitializeJobsAsync(); await db.Store.InitializeExternalActionsAsync();
        var name=Guid.NewGuid().ToString("N");
        var definition=new JobDefinition(new("tenant",name),"batch",1,"batch",1,"v1",[1],[JobCapabilities.TransactionalHandler,JobCapabilities.TransactionalItems]);
        await db.Store.RegisterAsync(definition); _definition = definition; _scope = new("tenant",name); return (definition,_scope);
    }
    private Task<JobSubmissionResult> Submit(JobDefinition d,int start,int count) =>
        new JsonJobClient<JobBatch<string>>(db.Store,d,Contract).SubmitAsync(Guid.NewGuid(),Guid.NewGuid().ToString("N"),"test",
            new(1,Enumerable.Range(start,count).Select(i=>new JobBatchItem<string>(i.ToString(),i.ToString())).ToArray()));
    private Task<ExecutionResult> Execute(JobDefinition d,ProducerScope scope,WorkClaim claim,bool fail=false) =>
        db.Store.ExecuteAsync(claim,async (_,payload,session,ct)=>
        {
            await JsonJobBatches.ExecuteBatchAsync(d,Contract.Decode(payload),Item,session,async (fresh,s,token)=>
            {
                var intents=fresh.Select(i=>new ExternalActionIntent(new(scope,
                    new Guid(Convert.FromHexString(StableHash.Of("test-item",i.Key)).AsSpan(0,16)),"service","v1",System.Text.Encoding.UTF8.GetBytes(i.Value))));
                var batch=new ExternalActionEnqueueBatch(intents);
                Assert.All(await s.ExternalActions.EnqueueBatchAsync(batch,token),r=>Assert.Equal(OutboxEnqueueStatus.Enqueued,r.Status));
                Assert.All(await s.ExternalActions.EnqueueBatchAsync(batch,token),r=>Assert.Equal(OutboxEnqueueStatus.AlreadyEnqueued,r.Status));
                if(fail) throw new IOException("injected batch failure");
            },ct);
        },new(classify:_=>FailureKind.Permanent));
    private async Task<int> Count(string table)
    {
        await using var c=await db.DataSource.OpenConnectionAsync();
        await using var command=new NpgsqlCommand($"SELECT COUNT(*) FROM {db.Schema}.{table} WHERE {(table == "job_items" ? "work_id IN(SELECT id FROM " + db.Schema + ".job_operations WHERE definition_id=@id)" : "scope_id=@id")}",c);
        command.Parameters.AddWithValue("id",table == "job_items" ? _definition.Id : _scope.Id);
        return Convert.ToInt32(await command.ExecuteScalarAsync());
    }
    [Fact]
    public async Task Overlapping_batches_prepare_each_item_once()
    {
        var (d,scope)=await Setup(); await Submit(d,0,100); await Submit(d,50,100);
        var a=(await db.Claim(d.WorkScope))!;var b=(await db.Claim(d.WorkScope))!;
        Assert.All(await Task.WhenAll(Execute(d,scope,a),Execute(d,scope,b)),r=>Assert.Equal(ExecutionStatus.Completed,r.Status));
        Assert.Equal(150,await Count("job_items")); Assert.Equal(150,await Count("external_actions"));
    }
    [Fact]
    public async Task Failed_preparation_rolls_back_receipts_and_intents_then_rebatch_succeeds()
    {
        var (d,scope)=await Setup();await Submit(d,0,100);
        Assert.Equal(ExecutionStatus.DeadLetter,(await Execute(d,scope,(await db.Claim(d.WorkScope))!,true)).Status);
        Assert.Equal(0,await Count("job_items"));Assert.Equal(0,await Count("external_actions"));
        await Submit(d,0,100);
        Assert.Equal(ExecutionStatus.Completed,(await Execute(d,scope,(await db.Claim(d.WorkScope))!)).Status);
        Assert.Equal(100,await Count("job_items"));Assert.Equal(100,await Count("external_actions"));
    }
    [Fact]
    public async Task Maximum_supported_unicode_binding_names_remain_claimable()
    {
        await db.Store.InitializeExternalActionsAsync();
        var random=new Random(712);
        string Name()=>"v"+new string(Enumerable.Range(0,341).Select(_=>(char)random.Next(0x4e00,0x9fff)).ToArray());
        var scope=new ProducerScope("unicode",Guid.NewGuid().ToString("N"));
        var action=new ExternalAction(scope,Guid.NewGuid(),Name(),Name(),[1]);
        await db.Outbox.ProduceAsync(new(scope,Guid.NewGuid(),ContentFingerprint.FromBytes([1])),async(s,ct)=>
        { await s.ExternalActions.EnqueueBatchAsync(new([new ExternalActionIntent(action)]),ct); });
        Assert.True(await db.Store.HasDueActionsAsync(scope,action.Binding,action.BindingVersion,ExternalPhase.Submit));
        Assert.Equal(action.Id,Assert.Single(await db.Store.ClaimActionsAsync(new(scope,action.Binding,action.BindingVersion,
            ExternalPhase.Submit,Guid.NewGuid(),TimeSpan.FromMinutes(1),1,1024))).Record.Action.Id);
    }
}
