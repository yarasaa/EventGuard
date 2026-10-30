using System.Data.Common;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using EventGuard.PostgreSql;
using EventGuard.Runtime;
using EventGuard.SqlServer;

namespace EventGuard.LoadTests;

// A bounded paired comparison of old per-item preparation/projection and opt-in native pages.
// Both paths keep the same native remote claim/settlement, payload, durable receipts and SQL effects.
internal static class ExternalPipelineProbe
{
    private sealed record Item(int Ordinal, string Payload);
    internal sealed record Sample(string Provider, int Items, bool Native, int Repetition,
        double PrepareMs, double ClaimAndSettleMs, double ProjectionMs, int PreparationCallbacks, int ProjectionCallbacks, bool Verified);

    internal static async Task<int> RunAsync(string[] args)
    {
        if (args.Length != 1 || Path.Exists(args[0])) throw new ArgumentException("Use --external-pipeline NEW_OUTPUT_DIRECTORY.");
        var output = Path.GetFullPath(args[0]); Directory.CreateDirectory(output);
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        var samples = new List<Sample>(); var profiles = new Dictionary<string,object>();
        await Exercise("sqlserver", LocalDatabases.SqlServer(4));
        await Exercise("postgresql", LocalDatabases.PostgreSql(4));
        await File.WriteAllTextAsync(Path.Combine(output,"results.json"),JsonSerializer.Serialize(new
        {
            Scenario="One SQL-only job page creates one action per item; native submit claim/settle; one durable application effect per accepted action. No broker, provider latency or sustained arrival load.",
            PayloadBytes=1024,Repetitions=3,Framework=System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription,
            Profiles=profiles,Samples=samples,CapacityBenchmark=false
        },new JsonSerializerOptions {WriteIndented=true}));
        return samples.All(s=>s.Verified) ? 0 : 1;

        async Task Exercise<T>(string provider, LoadDatabase<T> database)
            where T : IJobItemSession,IExternalActionSession,IExternalResultSession
        {
            await using var db=database;var ct=deadline.Token;
            await db.InitializeAsync(ct);
            try
            {
                switch (db.Store)
                {
                    case SqlServerWorkStore sql: await sql.InitializeJobsAsync(ct);await sql.InitializeExternalActionsAsync(ct);break;
                    case PostgreSqlWorkStore pg: await pg.InitializeJobsAsync(ct);await pg.InitializeExternalActionsAsync(ct);break;
                }
                profiles[provider]=await db.ProfileAsync(ct);
                foreach(var native in new[]{false,true}) await Measure(db,provider,20,native,0,ct); // warm-up, excluded
                foreach(var count in new[]{100,250})
                for(var repetition=1;repetition<=3;repetition++)
                foreach(var native in repetition%2==0 ? new[]{true,false} : new[]{false,true})
                {
                    var result=await Measure(db,provider,count,native,repetition,ct);samples.Add(result);
                    Console.WriteLine(JsonSerializer.Serialize(result));
                    if(!result.Verified) throw new InvalidOperationException("Pipeline correctness oracle failed.");
                }
            }
            finally
            {
                using var cleanup=new CancellationTokenSource(TimeSpan.FromSeconds(30));
                await db.CleanupAsync(cleanup.Token);
            }
        }
    }

    private static async Task<Sample> Measure<T>(LoadDatabase<T> db,string provider,int count,bool native,int repetition,CancellationToken ct)
        where T : IJobItemSession,IExternalActionSession,IExternalResultSession
    {
        var jobs=(ITransactionalJobStore<T>)db.Store;var actions=(IExternalActionBatchStore)db.Store;
        var results=(IExternalResultBatchStore<T>)db.Store;
        var id=Guid.NewGuid().ToString("N");var scope=new ProducerScope("pipeline",id);
        var target=new ExternalResultTarget(scope,"projection","v1");
        var itemContract=new JsonMessageContract<Item>("item",1,_=>1,_=>{});
        var contract=JsonJobBatches.Contract("page",1,itemContract,1000);
        var definition=new JobDefinition(new("pipeline",id),"page",1,"page",1,"v1",[1],
            [JobCapabilities.TransactionalHandler,JobCapabilities.TransactionalItems]);
        await jobs.RegisterAsync(definition,ct);
        var items=Enumerable.Range(0,count).Select(i=>new JobBatchItem<Item>(i.ToString(),new(i,new string('x',1024)))).ToArray();
        await new JsonJobClient<JobBatch<Item>>(jobs,definition,contract).SubmitAsync(Guid.NewGuid(),id,"probe",new(1,items),cancellationToken:ct);
        var claim=(await db.Store.ClaimNextAsync(definition.WorkScope,Guid.NewGuid(),TimeSpan.FromMinutes(1),ct))!;
        ExternalAction Intent(Item item) => new(scope,new Guid(System.Security.Cryptography.SHA256.HashData(Encoding.UTF8.GetBytes(id+":"+item.Ordinal)).AsSpan(0,16)),
            "service","v1",Encoding.UTF8.GetBytes(item.Payload));
        var preparationCalls=0;var projectionCalls=0;
        var started=Stopwatch.GetTimestamp();
        var prepared=await db.Store.ExecuteAsync(claim,async(_,payload,session,token)=>
        {
            if(native) await JsonJobBatches.ExecuteBatchAsync(definition,contract.Decode(payload),itemContract,session,async(fresh,s,t)=>
            {
                preparationCalls++;await s.ExternalActions.EnqueueBatchAsync(new(fresh.Select(i=>new ExternalActionIntent(Intent(i.Value),target))),t);
            },token);
            else await JsonJobBatches.ExecuteAsync(definition,contract.Decode(payload),itemContract,session,async(item,s,t)=>
            {
                preparationCalls++;await s.ExternalActions.EnqueueAsync(Intent(item),target,t);
            },token);
        },new(),ct);
        if(prepared.Status!=ExecutionStatus.Completed) throw new InvalidOperationException("Preparation did not complete.");
        var prepareMs=Stopwatch.GetElapsedTime(started).TotalMilliseconds;
        started=Stopwatch.GetTimestamp();
        var claimed=await actions.ClaimActionsAsync(new(scope,"service","v1",ExternalPhase.Submit,Guid.NewGuid(),TimeSpan.FromMinutes(1),count,8*1024*1024),ct);
        if(claimed.Count!=count) throw new InvalidOperationException("Action count differs.");
        await actions.SettleActionsAsync(new(claimed.Select(c=>new ExternalBatchSettlementItem(c,new(ExternalResultKind.Accepted,"accepted"))),new()),ct);
        var remoteMs=Stopwatch.GetElapsedTime(started).TotalMilliseconds;
        started=Stopwatch.GetTimestamp();
        if(native)
        {
            var applied=await results.ExecuteResultBatchAsync(target,new(count),async(u,s,t)=>{projectionCalls++;await Write(db.Schema,u,s,t);},new(),ct);
            if(applied.Applied!=count) throw new InvalidOperationException("Projection count differs.");
        }
        else
        {
            while(await db.Store.ClaimNextAsync(target.WorkScope,Guid.NewGuid(),TimeSpan.FromMinutes(1),ct) is { } work)
            {
                var applied=await db.Store.ExecuteAsync(work,async(_,_,s,t)=>
                {
                    await s.ExternalResults.ApplyAsync(target,async(u,token)=>{projectionCalls++;await Write(db.Schema,[u],s,token);},t);
                },new(),ct);
                if(applied.Status!=ExecutionStatus.Completed) throw new InvalidOperationException("Projection did not complete.");
            }
        }
        var projectionMs=Stopwatch.GetElapsedTime(started).TotalMilliseconds;
        await using var connection=await db.OpenAsync(ct);await using var verify=connection.CreateCommand();
        verify.CommandText=$"""
            SELECT COUNT(*) FROM {db.Schema}.external_actions a
            WHERE a.scope_id=@scope AND a.state=2
            AND (SELECT COUNT(*) FROM {db.Schema}.effects e WHERE e.scope_id=a.scope_id AND e.work_id=a.id)=1
            AND EXISTS(SELECT 1 FROM {db.Schema}.external_result_positions p WHERE p.scope_id=a.scope_id AND p.action_id=a.id AND p.version=a.version)
            AND EXISTS(SELECT 1 FROM {db.Schema}.external_result_updates u JOIN {db.Schema}.works w ON w.id=u.work_id
                WHERE u.scope_id=a.scope_id AND u.action_id=a.id AND w.state=3 AND w.attempt_count=1)
            """;
        Add(verify,"scope",scope.Id);
        var verified=Convert.ToInt32(await verify.ExecuteScalarAsync(ct))==count;
        return new(provider,count,native,repetition,prepareMs,remoteMs,projectionMs,preparationCalls,projectionCalls,verified);
    }

    private static async Task Write<T>(string schema,IReadOnlyList<ExternalActionUpdate> updates,T session,CancellationToken ct)
    {
        var ids=updates.Select(u=>u.Action.Id).ToArray();
        await using DbCommand command=session switch
        {
            SqlServerSession s=>s.CreateCommand($"INSERT INTO {schema}.effects(scope_id,work_id,ordinal,fingerprint) SELECT @scope,CONVERT(uniqueidentifier,value),0,'applied' FROM OPENJSON(@ids)"),
            PostgreSqlSession s=>s.CreateCommand($"INSERT INTO {schema}.effects(scope_id,work_id,ordinal,fingerprint) SELECT @scope,id,0,'applied' FROM unnest(@ids) AS x(id)"),
            _=>throw new NotSupportedException()
        };
        Add(command,"ids",session is SqlServerSession ? JsonSerializer.Serialize(ids) : ids);
        Add(command,"scope",updates[0].Action.Scope.Id);await command.ExecuteNonQueryAsync(ct);
    }
    private static void Add(DbCommand command,string name,object value)
    {var p=command.CreateParameter();p.ParameterName=name;p.Value=value;command.Parameters.Add(p);}
}
