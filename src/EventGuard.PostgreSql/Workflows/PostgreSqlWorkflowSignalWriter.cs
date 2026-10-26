using Npgsql;
namespace EventGuard.PostgreSql;
internal sealed class PostgreSqlWorkflowSignalWriter(NpgsqlConnection c,NpgsqlTransaction t,string schema,Guid? originWorkId,ProducerRequest? producerRequest) : IWorkflowSignalWriter
{
    private NpgsqlCommand Command(string sql,params (string Name,object Value)[] args) { var cmd=new NpgsqlCommand(sql,c,t); foreach(var (n,v) in args) cmd.Parameters.AddWithValue(n,v); return cmd; }
    public async Task<WorkflowSignalStatus> SignalAsync(WorkflowSignal signal,CancellationToken cancellationToken=default)
    {
        ArgumentNullException.ThrowIfNull(signal); _=t.IsolationLevel;
        if(!ReferenceEquals(t.Connection,c)) throw new InvalidOperationException("Owning transaction is no longer active.");
        if(producerRequest is not null && producerRequest.Scope.Tenant!=signal.Scope.Tenant) throw new InvalidOperationException("Signal must use the producer tenant.");
        if(originWorkId is Guid work)
        {
            await using var owner=Command($"SELECT b.tenant FROM {schema}.works w JOIN {schema}.policy_bindings b ON b.scope_id=w.scope_id WHERE w.id=@id",("id",work));
            if(await owner.ExecuteScalarAsync(cancellationToken) as string!=signal.Scope.Tenant) throw new InvalidOperationException("Signal must use the work tenant.");
        }
        await PostgreSqlProtocol.LockAsync(c,t,"workflow-signal-request:"+schema+":"+signal.Scope.Id+":"+signal.RequestId,cancellationToken);
        await PostgreSqlProtocol.LockAsync(c,t,"workflow:"+schema+":"+signal.Scope.Id+":"+signal.WorkflowId,cancellationToken);
        await using(var prior=Command($"SELECT fingerprint FROM {schema}.workflow_signal_requests WHERE scope_id=@scope AND id=@id",("scope",signal.Scope.Id),("id",signal.RequestId)))
            if(await prior.ExecuteScalarAsync(cancellationToken) is string fp)
            { if(fp!=signal.Fingerprint) throw new InvalidOperationException("Signal request conflict."); return WorkflowSignalStatus.AlreadyRecorded; }
        await using(var step=Command($"SELECT s.kind FROM {schema}.workflow_steps s JOIN {schema}.workflows w ON w.scope_id=s.scope_id AND w.id=s.workflow_id WHERE s.scope_id=@scope AND s.workflow_id=@id AND s.name=@step AND w.tenant=@tenant",("scope",signal.Scope.Id),("id",signal.WorkflowId),("step",signal.Step),("tenant",signal.Scope.Tenant)))
            if(await step.ExecuteScalarAsync(cancellationToken) is not int kind || kind!=(int)WorkflowStepKind.Signal) throw new InvalidOperationException("Signal target does not exist or is not a signal step.");
        string? saved;
        await using(var prior=Command($"SELECT value FROM {schema}.workflow_signals WHERE scope_id=@scope AND workflow_id=@id AND step_name=@step",("scope",signal.Scope.Id),("id",signal.WorkflowId),("step",signal.Step))) saved=await prior.ExecuteScalarAsync(cancellationToken) as string;
        if(saved is not null && saved!=signal.Value) throw new InvalidOperationException("Signal step already has a different value.");
        if(saved is null)
        {
            await using var insert=Command($"INSERT INTO {schema}.workflow_signals(scope_id,workflow_id,step_name,value) VALUES(@scope,@id,@step,@value)",("scope",signal.Scope.Id),("id",signal.WorkflowId),("step",signal.Step),("value",signal.Value));
            await insert.ExecuteNonQueryAsync(cancellationToken);
        }
        await using(var receipt=Command($"INSERT INTO {schema}.workflow_signal_requests(scope_id,id,workflow_id,step_name,fingerprint,actor) VALUES(@scope,@request,@id,@step,@fp,@actor)",("scope",signal.Scope.Id),("request",signal.RequestId),("id",signal.WorkflowId),("step",signal.Step),("fp",signal.Fingerprint),("actor",signal.Actor))) await receipt.ExecuteNonQueryAsync(cancellationToken);
        return saved is null?WorkflowSignalStatus.Recorded:WorkflowSignalStatus.AlreadyRecorded;
    }
}
