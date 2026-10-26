using Npgsql;
namespace EventGuard.Operations.PostgreSql;
public sealed partial class PostgreSqlWorkReader
{
    private async Task ValidateMetadataAccessAsync(bool outbox, CancellationToken ct)
    {
        
        string[] queries = outbox ? [
            $"SELECT scope_id,{OutboxColumns},quarantine_id FROM {_schema}.outbox_messages WHERE 1=0",
            $"SELECT scope_id,intent_id,number,generation,status,started_at,finished_at,reason FROM {_schema}.outbox_attempts WHERE 1=0",
            $"SELECT scope_id,intent_id,id,result_status,result_generation,result_version,acknowledge_possible_duplicate,created_at FROM {_schema}.outbox_replay_requests WHERE 1=0",
            $"SELECT scope_id,intent_id,id,observed_version,reason,detected_at FROM {_schema}.outbox_quarantine WHERE 1=0"
        ] : [
            $"SELECT scope_id,{Columns} FROM {_schema}.works WHERE 1=0",
            $"SELECT work_id,number,generation,status,started_at,finished_at,reason FROM {_schema}.attempts WHERE 1=0",
            $"SELECT scope_id,work_id,id,result_status,result_generation,result_version,created_at FROM {_schema}.replay_requests WHERE 1=0"
        ];
        foreach (var sql in queries)
        { await using var command = _data.CreateCommand(sql); await using var reader = await command.ExecuteReaderAsync(ct); }
    }
}
