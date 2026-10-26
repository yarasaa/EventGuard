using EventGuard.Operations.PostgreSql;
using Fixture=EventGuard.PostgreSql.Tests.DatabaseFixture;
namespace EventGuard.Operations.Tests;
public sealed class PostgreSqlModuleTests(Fixture db):IClassFixture<Fixture>
{
    [Fact]
    public async Task Snapshot_pages_are_bounded_scoped_bidirectional_and_do_not_read_payloads()
    {
        await db.Store.InitializeSnapshotsAsync(); var scope=new JobScope("tenant","module-pages");
        var job=new JobDefinition(scope,"job",1,"input",1,"v1",[1],[JobCapabilities.TransactionalHandler]); await db.Store.RegisterAsync(job);
        var def=new SnapshotDefinition(job,"selection","source","v1");
        for(var i=0;i<7;i++) await db.Store.CreateSnapshotAsync(new(def,Guid.NewGuid(),"key-"+i,"secret-actor","secret-parameters"u8));
        await using var reader=new PostgreSqlWorkReader(db.ConnectionString,db.Schema);
        var first=await reader.ReadModuleAsync(new(ExecutionModule.Snapshots,scope.Id,limit:3)); Assert.Equal(3,first.Items.Count); Assert.NotNull(first.Next); Assert.Null(first.Previous);
        var second=await reader.ReadModuleAsync(new(ExecutionModule.Snapshots,scope.Id,position:first.Next,limit:3)); Assert.Empty(first.Items.Select(i=>i.Id).Intersect(second.Items.Select(i=>i.Id)));
        var back=await reader.ReadModuleAsync(new(ExecutionModule.Snapshots,scope.Id,position:second.Previous,reverse:true,limit:3)); Assert.Equal(first.Items,back.Items);
        Assert.Empty((await reader.ReadModuleAsync(new(ExecutionModule.Snapshots,new JobScope("foreign","module-pages").Id))).Items);
        Assert.Empty((await reader.ReadModuleAsync(new(ExecutionModule.Snapshots,scope.Id,state:3))).Items);
        Assert.DoesNotContain("secret",System.Text.Json.JsonSerializer.Serialize(first));
        Assert.Single((await reader.ReadModuleAsync(new(ExecutionModule.Snapshots,scope.Id,id:first.Items[0].Id))).Items);
    }
    [Fact]
    public async Task External_notices_workflows_and_service_limits_have_safe_metadata()
    {
        await db.Store.InitializeExternalActionsAsync(); await db.Store.InitializeWorkflowsAsync(); var scope=new ProducerScope("tenant","module-external");
        var action=new ExternalAction(scope,Guid.NewGuid(),"service","v1","private-phone"u8,TimeSpan.FromMinutes(1));
        await db.Outbox.ProduceAsync(new(scope,action.Id,ContentFingerprint.FromBytes([1])),async(s,ct)=>{await s.ExternalActions.EnqueueAsync(action,ct);});
        await db.Store.RecordNoticeAsync(new(scope,Guid.NewGuid(),action.Id,"service","v1",new(ExternalResultKind.Succeeded,"provider-secret",data:"private-result"u8)));
        var def=new WorkflowDefinition(new("tenant","modules"),"flow",1,[new("signal",WorkflowStepKind.Signal)]); await db.Store.CreateWorkflowAsync(new(def,Guid.NewGuid(),"key","actor",new Dictionary<string,byte[]>()));
        var gate=new ExternalCallLimit("tenant","service",10,TimeSpan.FromMinutes(1),failureThreshold:1); var permit=(await db.Store.TryAcquireCallAsync(gate,Guid.NewGuid(),TimeSpan.FromMinutes(1)))!; await db.Store.CompleteCallAsync(permit,ExternalCallOutcome.Unhealthy);
        await using var reader=new PostgreSqlWorkReader(db.ConnectionString,db.Schema);
        foreach(var (module,id) in new[]{(ExecutionModule.ExternalActions,scope.Id),(ExecutionModule.ExternalNotices,scope.Id),(ExecutionModule.Workflows,def.Scope.Id),(ExecutionModule.ServiceLimit,gate.Id)})
        {
            var page=await reader.ReadModuleAsync(new(module,id)); Assert.Single(page.Items); var json=System.Text.Json.JsonSerializer.Serialize(page); Assert.DoesNotContain("private",json); Assert.DoesNotContain("provider-secret",json);
            if(module==ExecutionModule.ServiceLimit) Assert.Equal("Open",page.Items[0].State);
            if(module==ExecutionModule.Workflows)
            {
                var detail=await reader.ReadModuleAsync(new(module,id,id:page.Items[0].Id)); Assert.Equal("signal",Assert.Single(detail.Steps).Name);
                Assert.Empty((await reader.ReadModuleAsync(new(module,new JobScope("foreign","modules").Id,id:page.Items[0].Id))).Steps);
            }
        }
    }
}
