using System.Data;
using Npgsql;

namespace EventGuard.PostgreSql;

public sealed partial class PostgreSqlWorkStore
{
    private async Task<List<ResultCandidate>> SelectResultCandidatesAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        ExternalResultTarget target, ExternalResultBatchOptions options, IReadOnlyList<ResultCandidate>? expected, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand { Connection = connection, Transaction = transaction };
        var columns = string.Join(",", Columns.Split(',').Select(c => "w." + c));
        command.Parameters.AddWithValue("scope", target.WorkScope.Id);
        command.Parameters.AddWithValue("now", await DatabaseNowAsync(connection, transaction, ct));
        command.Parameters.AddWithValue("maximum", options.MaximumItems);
        if (expected is not null)
        {
            using var table = ResultIds(expected);
            var input = AddResultInput(command, table, "external_result_ids");
            command.CommandText = $"""
                WITH input AS ({input}) SELECT {columns},w.lease_token,w.lease_until
                FROM input x JOIN {_schema}.works w ON w.id=x.work_id AND w.version=x.version
                WHERE w.scope_id=@scope ORDER BY w.id FOR UPDATE OF w SKIP LOCKED
                """;
            var results = new List<ResultCandidate>();
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct)) results.Add(new(ReadWork(reader), reader.IsDBNull(14) ? null
                : new(reader.GetGuid(14), reader.GetFieldValue<DateTimeOffset>(15))));
            return results;
        }
        var availability = await PostgreSqlTenantBudgetProtocol.AvailableAsync(connection, transaction, _schema, target.Scope.Tenant, ct);
        command.Parameters.AddWithValue("fresh", availability.Fresh ? 1 : 0); command.Parameters.AddWithValue("retry", availability.Retry ? 1 : 0);
        // Use one access path at a time, matching ordinary claims, to avoid crossed index locks.
        foreach (var path in new[] {
            (Index: "ix_claim_expiry(scope_id,expires_at)", Predicate: "state IN(0,2) AND expires_at<=@now", Order: "expires_at"),
            (Index: "ix_recovery(scope_id,lease_until)", Predicate: "state=1 AND lease_until<=@now AND (expires_at<=@now OR @retry=1)", Order: "lease_until"),
            (Index: "ix_ready(scope_id,next_attempt_at)", Predicate: "state IN(0,2) AND next_attempt_at<=@now AND ((@fresh=1 AND attempt_count=0) OR (@retry=1 AND attempt_count>0))", Order: "next_attempt_at") })
        {
            command.CommandText = $"""
                WITH candidates AS MATERIALIZED (
                    SELECT {Columns},lease_token,lease_until,{path.Order} AS ordering FROM {_schema}.works
                    WHERE scope_id=@scope AND {path.Predicate} ORDER BY {path.Order},id LIMIT @maximum FOR UPDATE SKIP LOCKED
                )
                SELECT {columns},w.lease_token,w.lease_until,
                    COALESCE(octet_length(a.payload),0)::bigint+COALESCE(octet_length(u.state_data),0)+COALESCE(octet_length(u.response_data),0)
                FROM candidates w LEFT JOIN {_schema}.external_result_updates u ON u.work_id=w.id
                LEFT JOIN {_schema}.external_actions a ON a.scope_id=u.scope_id AND a.id=u.action_id ORDER BY w.ordering,w.id
                """;
            var results = new List<ResultCandidate>(); long bytes = 0;
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                bytes = checked(bytes + reader.GetInt64(16));
                if (bytes > options.MaximumBytes) break; // Payloads have not been fetched yet.
                results.Add(new(ReadWork(reader), reader.IsDBNull(14) ? null
                    : new(reader.GetGuid(14), reader.GetFieldValue<DateTimeOffset>(15))));
            }
            if (results.Count > 0) return results;
        }
        return [];
    }

    private static DataTable ResultVersions(IEnumerable<ExternalActionUpdate> updates)
    {
        var table = new DataTable(); table.Columns.Add("action_id", typeof(Guid)); table.Columns.Add("version", typeof(long));
        foreach (var update in updates.GroupBy(u => u.Action.Id).Select(g => g.MaxBy(u => u.Version)!))
            table.Rows.Add(update.Action.Id, update.Version);
        return table;
    }

    private async Task<Dictionary<Guid,long>> LockResultPositionsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        ExternalResultTarget target, IReadOnlyList<ExternalActionUpdate> updates, CancellationToken ct)
    {
        var positions = new Dictionary<Guid,long>();
        if (updates.Count == 0) return positions;
        using var table = ResultVersions(updates);
        await using var command = new NpgsqlCommand { Connection = connection, Transaction = transaction };
        var input = AddResultInput(command, table, "external_result_versions");
        command.Parameters.AddWithValue("scope", target.Scope.Id); command.Parameters.AddWithValue("handler", target.WorkScope.Id);
        command.Parameters.AddWithValue("prefix", "external-result-position:" + _schema + ":" + target.Id + ":");
        command.CommandText = $"""
            WITH input AS ({input}), ordered AS MATERIALIZED(SELECT action_id FROM input ORDER BY action_id::text)
            SELECT pg_advisory_xact_lock(hashtextextended(@prefix || action_id::text,0)) FROM ordered
            """;
        await command.ExecuteNonQueryAsync(ct);
        command.CommandText = $"""
            WITH input AS ({input}) SELECT p.action_id,p.version FROM {_schema}.external_result_positions p
            JOIN input x ON x.action_id=p.action_id WHERE p.scope_id=@scope AND p.handler_scope_id=@handler
            """;
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct)) positions.Add(reader.GetGuid(0), reader.GetInt64(1));
        return positions;
    }

    private async Task AdvanceResultPositionsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        ExternalResultTarget target, IReadOnlyList<ExternalActionUpdate> updates, CancellationToken ct)
    {
        if (updates.Count == 0) return;
        using var table = ResultVersions(updates);
        await using var command = new NpgsqlCommand { Connection = connection, Transaction = transaction };
        var input = AddResultInput(command, table, "external_result_versions");
        command.Parameters.AddWithValue("scope", target.Scope.Id); command.Parameters.AddWithValue("handler", target.WorkScope.Id);
        command.CommandText = $"""
            WITH input AS ({input}) INSERT INTO {_schema}.external_result_positions(scope_id,action_id,handler_scope_id,version)
            SELECT @scope,action_id,@handler,version FROM input
            ON CONFLICT(scope_id,action_id,handler_scope_id) DO UPDATE SET version=EXCLUDED.version
            """;
        await command.ExecuteNonQueryAsync(ct);
    }

    private async Task PersistResultCompletionsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        IReadOnlyList<ResultCompletion> completed, DateTimeOffset now, CancellationToken ct)
    {
        if (completed.Count == 0) return;
        using var table = new DataTable();
        table.Columns.Add("work_id",typeof(Guid)); table.Columns.Add("expected",typeof(long));
        table.Columns.Add("state",typeof(int)); table.Columns.Add("version",typeof(long));
        table.Columns.Add("attempt_count",typeof(int)); table.Columns.Add("generation_attempt",typeof(int));
        table.Columns.Add("generation",typeof(int)); table.Columns.Add("next_at",typeof(DateTimeOffset));
        table.Columns.Add("reason",typeof(string)); table.Columns.Add("attempt_id",typeof(Guid));
        table.Columns.Add("started_at",typeof(DateTimeOffset));
        table.Columns.Add("abandoned",typeof(Guid)); table.Columns.Add("attempt_status",typeof(string));
        foreach (var c in completed)
        {
            var after = c.Decision.Change.After;
            table.Rows.Add(after.Id,c.Admission.Candidate.Work.Version,(int)after.State,after.Version,after.AttemptCount,
                after.GenerationAttempt,after.ReplayGeneration,after.NextAttemptAt,(object?)after.LastReason ?? DBNull.Value,
                c.Admission.Claim.Lease.Token,c.Admission.StartedAt,(object?)c.Admission.Claim.AbandonedAttempt ?? DBNull.Value,c.Decision.AttemptStatus);
        }
        await using var command = new NpgsqlCommand { Connection = connection, Transaction = transaction };
        var input = AddResultInput(command, table, "external_result_changes");
        command.Parameters.AddWithValue("now",now);
        command.CommandText = $"""
            WITH input AS ({input}), changed AS (
                UPDATE {_schema}.works w SET state=x.state,version=x.version,attempt_count=x.attempt_count,generation_attempt=x.generation_attempt,
                    next_attempt_at=x.next_at,last_reason=x.reason,lease_token=NULL,lease_until=NULL,
                    completed_at=CASE WHEN x.state=3 THEN @now ELSE w.completed_at END
                FROM input x WHERE w.id=x.work_id AND w.version=x.expected RETURNING w.id
            ), abandoned AS (
                UPDATE {_schema}.attempts a SET status='Abandoned',reason='lease-recovered',finished_at=@now
                FROM input x WHERE a.id=x.abandoned AND a.finished_at IS NULL
            ), inserted AS (
                INSERT INTO {_schema}.attempts(id,work_id,number,generation,status,reason,started_at,finished_at)
                SELECT attempt_id,work_id,attempt_count,generation,attempt_status,reason,started_at,@now FROM input
            ) SELECT count(*) FROM changed
            """;
        if (Convert.ToInt32(await command.ExecuteScalarAsync(ct)) != completed.Count)
            throw new InvalidOperationException("Result work changed under its retained lock.");
    }
}
