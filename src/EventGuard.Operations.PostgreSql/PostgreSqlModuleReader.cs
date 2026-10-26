using Npgsql;
namespace EventGuard.Operations.PostgreSql;
public sealed partial class PostgreSqlWorkReader : IModuleReadStore
{
    public async Task<ModulePage> ReadModuleAsync(ModuleQuery query,CancellationToken cancellationToken=default)
    {
        ArgumentNullException.ThrowIfNull(query);
        var (table,state,version,done,total,secondary,paused,date)=query.Module switch
        {
            ExecutionModule.Snapshots=>("snapshots","state","version","batched_items","item_count","0","CASE WHEN paused THEN 1 ELSE 0 END","updated_at"),
            ExecutionModule.ExternalActions=>("external_actions","state","version","CAST(submission_attempts AS bigint)","CAST(0 AS bigint)","observation","0","next_at"),
            ExecutionModule.ExternalNotices=>("external_notices","status","action_version","CAST(0 AS bigint)","CAST(0 AS bigint)","0","0","received_at"),
            ExecutionModule.Workflows=>("workflows","state","version","CAST(0 AS bigint)","CAST(0 AS bigint)","0","0","updated_at"),
            ExecutionModule.ServiceLimit=>("external_limits","CASE WHEN open_until>clock_timestamp() THEN 1 WHEN probe IS NOT NULL THEN 2 ELSE 0 END","CAST(0 AS bigint)","CAST(used AS bigint)","CAST(failures AS bigint)","0","0","window_started_at"),
            _=>throw new ArgumentException("Unknown module.")
        };
        var limit=query.Module==ExecutionModule.ServiceLimit;
        var id=limit?"'00000000-0000-0000-0000-000000000000'::uuid":"id";
        var comparison=query.Reverse?">":"<"; var direction=query.Reverse?"ASC":"DESC";
        await using var command=_data.CreateCommand($"""
            SELECT {id},{state},{version},{done},{total},{secondary},{paused},{date} FROM {_schema}.{table}
            WHERE {(limit?"id":"scope_id")}=@scope
            {(query.State.HasValue?$"AND ({state})=@state":"")}
            {(query.Id.HasValue?"AND id=@exact":"")}
            {(query.Position.HasValue?$"AND id{comparison}@position":"")}
            ORDER BY id {direction} LIMIT @limit
            """);
        command.Parameters.AddWithValue("scope",query.ScopeId); command.Parameters.AddWithValue("limit",query.Limit+1);
        if(query.State.HasValue) command.Parameters.AddWithValue("state",query.State.Value);
        if(query.Id.HasValue) command.Parameters.AddWithValue("exact",query.Id.Value);
        if(query.Position.HasValue) command.Parameters.AddWithValue("position",query.Position.Value);
        var rows=new List<ModuleRow>(); await using var r=await command.ExecuteReaderAsync(cancellationToken);
        while(await r.ReadAsync(cancellationToken))
        {
            var code=r.GetInt32(1); rows.Add(new(r.GetGuid(0),ModuleMetadata.States(query.Module).FirstOrDefault(s=>s.Code==code)?.Name??"Unknown",r.GetInt64(2),r.GetInt64(3),r.GetInt64(4),
                ModuleMetadata.Guidance(query.Module,code,r.GetInt32(5),r.GetInt32(6)!=0),r.GetFieldValue<DateTimeOffset>(7)));
        }
        var page=ModulePage.FromWindow(rows,query);
        await r.DisposeAsync();
        if(query.Module==ExecutionModule.Workflows && query.Id is Guid workflowId && rows.Count==1)
        {
            var sql=$"SELECT name,state,job_id,due_at,reason FROM {_schema}.workflow_steps WHERE scope_id=@scope AND workflow_id=@id ORDER BY name LIMIT 64";
            await using var detail = _data.CreateCommand(sql);
            detail.Parameters.AddWithValue("scope",query.ScopeId); detail.Parameters.AddWithValue("id",workflowId);
            var steps=new List<ModuleStep>(); await using var stepReader=await detail.ExecuteReaderAsync(cancellationToken);
            while(await stepReader.ReadAsync(cancellationToken))
            {
                var code=stepReader.GetInt32(1); var reason=stepReader.GetString(4);
                var safeReason=reason is "waiting-for-dependencies" or "job-completed" or "job-dead-letter" or "job-expired" or "waiting-for-job" or "branch-not-selected" or "timer-elapsed" or "waiting-for-timer" or "waiting-for-signal" or "signal-received" ? reason : "unrecognized";
                steps.Add(new(stepReader.GetString(0),Enum.IsDefined((WorkflowStepState)code)?((WorkflowStepState)code).ToString():"Unknown",stepReader.IsDBNull(2)?null:stepReader.GetGuid(2),stepReader.IsDBNull(3)?null:stepReader.GetFieldValue<DateTimeOffset>(3),safeReason));
            }
            page=page with {Steps=steps.AsReadOnly()};
        }
        return page;
    }
}
