using Microsoft.Data.SqlClient;
using System.Data;

namespace EventGuard.SqlServer;

public sealed partial class SqlServerWorkStore : IWorkflowStore
{
    public async Task InitializeWorkflowsAsync(CancellationToken cancellationToken = default)
    {
        await InitializeJobsAsync(cancellationToken);
        await using var c = await _dataSource.OpenConnectionAsync(cancellationToken); await using var t = await c.BeginLocalAsync(cancellationToken);
        await SqlServerProtocol.LockAsync(c, t, "workflow-schema:" + _schema, cancellationToken);
        await using var exists = StoreCommand(c, t, "SELECT OBJECT_ID(@name,'U')", ("name", _schema + ".workflow_schema_version"));
        if (await exists.ExecuteScalarCheckedAsync(cancellationToken) is not int)
        {
            await using var stream = typeof(SqlServerWorkStore).Assembly.GetManifestResourceStream("EventGuard.SqlServer.Workflows.Migrations.001_workflows.sql")!;
            using var reader = new StreamReader(stream);
            await using var create = StoreCommand(c, t, (await reader.ReadToEndAsync(cancellationToken)).Replace("__SCHEMA__", _schema, StringComparison.Ordinal));
            await create.ExecuteNonQueryCheckedAsync(cancellationToken);
        }
        await using var version = StoreCommand(c, t, $"SELECT version FROM {_schema}.workflow_schema_version WHERE singleton=1");
        if (await version.ExecuteScalarCheckedAsync(cancellationToken) is not int v || v != 1) throw new InvalidOperationException("Unsupported workflow schema.");
        await SqlServerProtocol.CommitAsync(t, Guid.NewGuid(), cancellationToken);
    }
    private async Task<WorkflowProgress?> ReadWorkflowAsync(SqlConnection c, SqlTransaction? t, WorkflowDefinition definition, Guid id, CancellationToken ct)
    {
        Guid requestId; string businessKey, actor, fingerprint; DateTimeOffset? expires; WorkflowState state; long version;
        await using (var cmd = StoreCommand(c, t, $"SELECT request_id,business_key,actor,expires_at,fingerprint,definition_fingerprint,state,version FROM {_schema}.workflows WHERE scope_id=@scope AND id=@id", ("scope", definition.Scope.Id), ("id", id)))
        {
            await using var r = await cmd.ExecuteReaderCheckedAsync(ct); if (!await r.ReadCheckedAsync(ct)) return null;
            if (r.GetString(5) != definition.Fingerprint) throw new InvalidOperationException("Workflow definition differs from its frozen plan.");
            requestId=r.GetGuid(0); businessKey=r.GetString(1); actor=r.GetString(2); expires=r.IsDBNull(3)?null:r.GetFieldValue<DateTimeOffset>(3);
            fingerprint=r.GetString(4); state=(WorkflowState)r.GetInt32(6); version=r.GetInt64(7);
        }
        var inputs = new Dictionary<string,byte[]>(StringComparer.Ordinal); var steps = new List<WorkflowStepProgress>();
        await using (var cmd = StoreCommand(c, t, $"SELECT name,kind,parameters,state,job_id,due_at,value,reason FROM {_schema}.workflow_steps WHERE scope_id=@scope AND workflow_id=@id", ("scope", definition.Scope.Id), ("id", id)))
        {
            await using var r=await cmd.ExecuteReaderCheckedAsync(ct);
            while(await r.ReadCheckedAsync(ct))
            {
                if ((WorkflowStepKind)r.GetInt32(1)==WorkflowStepKind.Job) inputs.Add(r.GetString(0), r.GetFieldValue<byte[]>(2));
                steps.Add(new(r.GetString(0),(WorkflowStepState)r.GetInt32(3),r.IsDBNull(4)?null:r.GetGuid(4),r.IsDBNull(5)?null:r.GetFieldValue<DateTimeOffset>(5),r.IsDBNull(6)?null:r.GetString(6),r.GetString(7)));
            }
        }
        var request = new WorkflowRequest(definition,requestId,businessKey,actor,inputs,expires);
        if(request.Id!=id || request.Fingerprint!=fingerprint || steps.Count!=definition.Steps.Count) throw new InvalidOperationException("Workflow content is corrupt.");
        return new(request,state,version,definition.Steps.Select(s=>steps.Single(x=>x.Name==s.Name)).ToArray());
    }
    public async Task<WorkflowProgress?> FindWorkflowAsync(WorkflowDefinition definition, Guid id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(definition);
        await using var c=await _dataSource.OpenConnectionAsync(cancellationToken);
        // One transaction and the same operation lock keep the header and step projection coherent.
        await using var t=await c.BeginLocalAsync(cancellationToken);
        await SqlServerProtocol.LockAsync(c,t,"workflow:"+_schema+":"+definition.Scope.Id+":"+id,cancellationToken);
        return await ReadWorkflowAsync(c,t,definition,id,cancellationToken);
    }
    public async Task<IReadOnlyList<Guid>> ListWorkflowsAsync(WorkflowDefinition definition,int limit=100,CancellationToken cancellationToken=default)
    {
        ArgumentNullException.ThrowIfNull(definition); if(limit is <1 or >1000) throw new ArgumentOutOfRangeException(nameof(limit));
        await using var c=await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd=StoreCommand(c,null,$"SELECT TOP(@limit) id FROM {_schema}.workflows WHERE scope_id=@scope AND definition_id=@definition AND definition_fingerprint=@fingerprint AND state=0 ORDER BY updated_at,id",
            ("scope",definition.Scope.Id),("definition",definition.Id),("fingerprint",definition.Fingerprint),("limit",limit));
        var ids=new List<Guid>(); await using var r=await cmd.ExecuteReaderCheckedAsync(cancellationToken); while(await r.ReadCheckedAsync(cancellationToken)) ids.Add(r.GetGuid(0)); return ids;
    }
    public async Task<WorkflowProgress> CreateWorkflowAsync(WorkflowRequest request,CancellationToken cancellationToken=default)
    {
        ArgumentNullException.ThrowIfNull(request);
        await using var c=await _dataSource.OpenConnectionAsync(cancellationToken); await using var t=await c.BeginLocalAsync(cancellationToken);
        var scope=request.Definition.Scope.Id;
        await SqlServerProtocol.LockAsync(c,t,"workflow-request:"+_schema+":"+scope+":"+request.RequestId,cancellationToken);
        await SqlServerProtocol.LockAsync(c,t,"workflow:"+_schema+":"+scope+":"+request.Id,cancellationToken);
        await using(var prior=StoreCommand(c,t,$"SELECT fingerprint FROM {_schema}.workflow_requests WHERE scope_id=@scope AND id=@id",("scope",scope),("id",request.RequestId)))
            if(await prior.ExecuteScalarCheckedAsync(cancellationToken) is string fp && fp!=request.Fingerprint) throw new InvalidOperationException("Workflow request conflict.");
        var row=await ReadWorkflowAsync(c,t,request.Definition,request.Id,cancellationToken);
        if(row is not null && row.Request.Fingerprint!=request.Fingerprint) throw new InvalidOperationException("Workflow business intent conflict.");
        if(row is null)
        {
            foreach(var step in request.Definition.Steps.Where(s=>s.Job is not null)) await ValidateJobAsync(c,t,step.Job!,cancellationToken);
            await using(var insert=StoreCommand(c,t,$"INSERT INTO {_schema}.workflows(scope_id,id,tenant,definition_id,definition_fingerprint,request_id,business_key,actor,expires_at,fingerprint) VALUES(@scope,@id,@tenant,@definition,@def_fp,@request,@key,@actor,@expires,@fp)",
                ("scope",scope),("id",request.Id),("tenant",request.Definition.Scope.Tenant),("definition",request.Definition.Id),("def_fp",request.Definition.Fingerprint),("request",request.RequestId),("key",request.BusinessKey),("actor",request.Actor),("fp",request.Fingerprint)))
            { SqlServerProtocol.Optional(insert,"expires",SqlDbType.DateTimeOffset,request.ExpiresAt); await insert.ExecuteNonQueryCheckedAsync(cancellationToken); }
            foreach(var step in request.Definition.Steps)
            {
                await using var insert=StoreCommand(c,t,$"INSERT INTO {_schema}.workflow_steps(scope_id,workflow_id,name,kind,parameters) VALUES(@scope,@id,@name,@kind,@input)",
                    ("scope",scope),("id",request.Id),("name",step.Name),("kind",(int)step.Kind),("input",step.Kind==WorkflowStepKind.Job?request.CopyInput(step.Name):Array.Empty<byte>()));
                await insert.ExecuteNonQueryCheckedAsync(cancellationToken);
            }
        }
        await using(var receipt=StoreCommand(c,t,$"INSERT INTO {_schema}.workflow_requests(scope_id,id,workflow_id,fingerprint) SELECT @scope,@request,@id,@fp WHERE NOT EXISTS(SELECT 1 FROM {_schema}.workflow_requests WHERE scope_id=@scope AND id=@request)",("scope",scope),("request",request.RequestId),("id",request.Id),("fp",request.Fingerprint))) await receipt.ExecuteNonQueryCheckedAsync(cancellationToken);
        row=(await ReadWorkflowAsync(c,t,request.Definition,request.Id,cancellationToken))!;
        await SqlServerProtocol.CommitAsync(t,request.RequestId,cancellationToken); return row;
    }
    private async Task SaveWorkflowAsync(SqlConnection c,SqlTransaction t,WorkflowProgress row,CancellationToken ct)
    {
        await using var cmd=StoreCommand(c,t,$"UPDATE {_schema}.workflows SET state=@state,version=@version,updated_at=TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00') WHERE scope_id=@scope AND id=@id",("scope",row.Request.Definition.Scope.Id),("id",row.Request.Id),("state",(int)row.State),("version",row.Version));
        await cmd.ExecuteNonQueryCheckedAsync(ct);
    }
    private async Task SaveWorkflowStepAsync(SqlConnection c,SqlTransaction t,WorkflowRequest request,WorkflowStepProgress step,CancellationToken ct)
    {
        await using var cmd=StoreCommand(c,t,$"UPDATE {_schema}.workflow_steps SET state=@state,job_id=@job,due_at=@due,value=@value,reason=@reason WHERE scope_id=@scope AND workflow_id=@id AND name=@name",
            ("scope",request.Definition.Scope.Id),("id",request.Id),("name",step.Name),("state",(int)step.State),("reason",step.Reason));
        SqlServerProtocol.Optional(cmd,"job",SqlDbType.UniqueIdentifier,step.JobId); SqlServerProtocol.Optional(cmd,"due",SqlDbType.DateTimeOffset,step.DueAt); SqlServerProtocol.Optional(cmd,"value",SqlDbType.NVarChar,step.Value);
        await cmd.ExecuteNonQueryCheckedAsync(ct);
    }
    public async Task<WorkflowProgress> AdvanceWorkflowAsync(WorkflowDefinition definition,Guid id,CancellationToken cancellationToken=default)
    {
        ArgumentNullException.ThrowIfNull(definition);
        await using var c=await _dataSource.OpenConnectionAsync(cancellationToken); await using var t=await c.BeginLocalAsync(cancellationToken);
        await SqlServerProtocol.LockAsync(c,t,"workflow:"+_schema+":"+definition.Scope.Id+":"+id,cancellationToken);
        var row=await ReadWorkflowAsync(c,t,definition,id,cancellationToken)??throw new InvalidOperationException("Workflow not found.");
        if(row.State!=WorkflowState.Running) return row;
        var now=await SqlServerProtocol.NowAsync(c,t,cancellationToken); var states=row.Steps.ToDictionary(s=>s.Name,StringComparer.Ordinal);
        foreach(var step in definition.Steps)
        {
            var current=states[step.Name]; var next=current;
            if(current.State is WorkflowStepState.Completed or WorkflowStepState.Skipped) continue;
            if(current.State==WorkflowStepState.Waiting && current.JobId is Guid jobId)
            {
                await using var cmd=StoreCommand(c,t,$"SELECT state FROM {_schema}.works WHERE id=@id",("id",jobId));
                var work=(WorkState)(int)(await cmd.ExecuteScalarCheckedAsync(cancellationToken)??throw new InvalidOperationException("Workflow job missing."));
                next=work==WorkState.Completed?current with {State=WorkflowStepState.Completed,Value="completed",Reason="job-completed"}
                    :current with {Reason=work==WorkState.DeadLetter?"job-dead-letter":work==WorkState.Expired?"job-expired":"waiting-for-job"};
            }
            else if(row.Request.ExpiresAt is null || row.Request.ExpiresAt>now)
            {
                var deps=step.Dependencies.Select(d=>(Dependency:d,Progress:states[d.Step])).ToArray();
                if(deps.All(d=>d.Progress.State is WorkflowStepState.Completed or WorkflowStepState.Skipped))
                {
                    if(deps.Any(d=>d.Dependency.RequiredValue is not null && (d.Progress.State!=WorkflowStepState.Completed || d.Progress.Value!=d.Dependency.RequiredValue)) || deps.Length>0 && deps.All(d=>d.Progress.State==WorkflowStepState.Skipped))
                        next=current with {State=WorkflowStepState.Skipped,Reason="branch-not-selected"};
                    else if(step.Kind==WorkflowStepKind.Job)
                    {
                        var requestId=new Guid(Convert.FromHexString(StableHash.Of("workflow-child/v1",definition.Scope.Id,id.ToString("D"),step.Name)).AsSpan(0,16));
                        var submitted=await SubmitJobWithinTransactionAsync(c,t,new(step.Job!,requestId,"workflow:"+id.ToString("D")+":"+step.Name,row.Request.Actor,row.Request.CopyInput(step.Name),expiresAt:row.Request.ExpiresAt,correlationId:id.ToString("D")),cancellationToken);
                        if(submitted.Status is not (JobSubmissionStatus.Accepted or JobSubmissionStatus.AlreadyAccepted)) throw new InvalidOperationException("Workflow child conflicts with an existing intent.");
                        next=current with {State=WorkflowStepState.Waiting,JobId=submitted.OperationId,Reason="waiting-for-job"};
                    }
                    else if(step.Kind==WorkflowStepKind.Delay)
                    {
                        var due=current.DueAt??now+step.Delay;
                        next=current with {State=due<=now?WorkflowStepState.Completed:WorkflowStepState.Waiting,DueAt=due,Value=due<=now?"elapsed":null,Reason=due<=now?"timer-elapsed":"waiting-for-timer"};
                    }
                    else
                    {
                        await using var cmd=StoreCommand(c,t,$"SELECT value FROM {_schema}.workflow_signals WHERE scope_id=@scope AND workflow_id=@id AND step_name=@step",("scope",definition.Scope.Id),("id",id),("step",step.Name));
                        var value=await cmd.ExecuteScalarCheckedAsync(cancellationToken) as string;
                        next=current with {State=value is null?WorkflowStepState.Waiting:WorkflowStepState.Completed,Value=value,Reason=value is null?"waiting-for-signal":"signal-received"};
                    }
                }
            }
            if(next!=current) { await SaveWorkflowStepAsync(c,t,row.Request,next,cancellationToken); states[step.Name]=next; }
        }
        row=row with {Steps=definition.Steps.Select(s=>states[s.Name]).ToArray(),Version=checked(row.Version+1),State=states.Values.All(s=>s.State is WorkflowStepState.Completed or WorkflowStepState.Skipped)?WorkflowState.Completed:row.Request.ExpiresAt<=now?WorkflowState.Expired:WorkflowState.Running};
        await SaveWorkflowAsync(c,t,row,cancellationToken); await SqlServerProtocol.CommitAsync(t,id,cancellationToken); return row;
    }
    public async Task<WorkflowProgress> ControlWorkflowAsync(WorkflowDefinition definition,Guid id,long expectedVersion,bool paused,bool cancel,string actor,string reason,CancellationToken cancellationToken=default)
    {
        _=new ProducerScope(actor,reason);
        await using var c=await _dataSource.OpenConnectionAsync(cancellationToken); await using var t=await c.BeginLocalAsync(cancellationToken);
        await SqlServerProtocol.LockAsync(c,t,"workflow:"+_schema+":"+definition.Scope.Id+":"+id,cancellationToken);
        var row=await ReadWorkflowAsync(c,t,definition,id,cancellationToken)??throw new InvalidOperationException("Workflow not found.");
        if(row.Version!=expectedVersion || row.State is not (WorkflowState.Running or WorkflowState.Paused)) throw new InvalidOperationException("Workflow control version or state is no longer current.");
        row=row with {State=cancel?WorkflowState.Cancelled:paused?WorkflowState.Paused:WorkflowState.Running,Version=checked(row.Version+1)};
        await using(var audit=StoreCommand(c,t,$"INSERT INTO {_schema}.workflow_controls(scope_id,workflow_id,version,actor,reason,paused,cancelled) VALUES(@scope,@id,@version,@actor,@reason,@paused,@cancel)",("scope",definition.Scope.Id),("id",id),("version",row.Version),("actor",actor),("reason",reason),("paused",paused),("cancel",cancel))) await audit.ExecuteNonQueryCheckedAsync(cancellationToken);
        await SaveWorkflowAsync(c,t,row,cancellationToken); await SqlServerProtocol.CommitAsync(t,id,cancellationToken); return row;
    }
    public async Task<WorkflowSignalStatus> SignalWorkflowAsync(WorkflowSignal signal,CancellationToken cancellationToken=default)
    {
        await using var c=await _dataSource.OpenConnectionAsync(cancellationToken); await using var t=await c.BeginLocalAsync(cancellationToken);
        var result=await new SqlServerWorkflowSignalWriter(c,t,_schema,null,null).SignalAsync(signal,cancellationToken);
        await SqlServerProtocol.CommitAsync(t,signal.RequestId,cancellationToken); return result;
    }
}
