using System.Data;
using Npgsql;

namespace EventGuard.PostgreSql;

public sealed partial class PostgreSqlWorkStore
{
    private async Task PublishExternalBatchUpdatesAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        IReadOnlyList<ExternalBatchChange> changes, CancellationToken ct)
    {
        if (changes.Count == 0) return;
        using var table = new DataTable();
        table.Columns.Add("scope_id", typeof(string));
        table.Columns.Add("action_id", typeof(Guid));
        table.Columns.Add("version", typeof(long));
        table.Columns.Add("work_id", typeof(Guid));
        table.Columns.Add("handler_scope_id", typeof(string));
        table.Columns.Add("state", typeof(int));
        table.Columns.Add("observation", typeof(int));
        table.Columns.Add("phase", typeof(int));
        table.Columns.Add("provider_reference", typeof(string));
        table.Columns.Add("reason", typeof(string));
        table.Columns.Add("state_data", typeof(byte[]));
        table.Columns.Add("response_kind", typeof(int));
        table.Columns.Add("response_reason", typeof(string));
        table.Columns.Add("response_reference", typeof(string));
        table.Columns.Add("response_data", typeof(byte[]));
        table.Columns.Add("identity_id", typeof(string));
        table.Columns.Add("business_key", typeof(string));
        table.Columns.Add("message_id", typeof(Guid));
        table.Columns.Add("fingerprint", typeof(string));
        table.Columns.Add("payload", typeof(byte[]));
        table.Columns.Add("correlation_id", typeof(string));
        var identities = new Dictionary<string, WorkIdentity>();
        foreach (var change in changes)
        {
            var record = change.Record; var target = change.Target!; var response = change.Response;
            var update = new ExternalActionUpdate(record.Action, record.Version, record.State, record.Observation,
                change.Phase, record.ProviderReference, record.LastReason, record.ResultData, response);
            var key = StableHash.Of("external-result-work/v1", record.Action.Scope.Id, record.Action.Id.ToString("D"),
                record.Version.ToString(System.Globalization.CultureInfo.InvariantCulture));
            var submission = new WorkSubmission(new(target.WorkScope, target.IdentityPolicy, key),
                new Guid(Convert.FromHexString(key).AsSpan(0, 16)), System.Text.Encoding.UTF8.GetBytes(key),
                ContentFingerprint.Parse(update.Fingerprint), correlationId: record.Action.Id.ToString("D"));
            identities.TryAdd(submission.Identity.Scope.Id, submission.Identity);
            table.Rows.Add(record.Action.Scope.Id, record.Action.Id, record.Version, Guid.NewGuid(), target.WorkScope.Id,
                (int)record.State, (int)record.Observation, (int)change.Phase, Db(record.ProviderReference), Db(record.LastReason),
                record.ResultData, response is null ? DBNull.Value : (int)response.Kind, Db(response?.Reason),
                Db(response?.Reference), Db(response?.CopyData()), submission.Identity.Id, submission.Identity.BusinessKey,
                submission.MessageId, submission.Fingerprint.Value, submission.CopyPayload(), submission.CorrelationId);
        }
        await EnsureExternalBatchBindingsAsync(connection, transaction, identities, ct);
        await using var command = new NpgsqlCommand { Connection = connection, Transaction = transaction };
        var input = AddExternalTable(command, table);
        command.CommandText = $"""
            WITH input AS MATERIALIZED ({input}), inserted AS (
                INSERT INTO {_schema}.works(id,identity_id,scope_id,business_key,message_id,fingerprint,payload,state,next_attempt_at,correlation_id)
                SELECT work_id,identity_id,handler_scope_id,business_key,message_id,fingerprint,payload,0,clock_timestamp(),correlation_id FROM input
                RETURNING id)
            INSERT INTO {_schema}.external_result_updates(scope_id,action_id,version,work_id,handler_scope_id,state,observation,phase,provider_reference,reason,state_data,response_kind,response_reason,response_reference,response_data)
            SELECT scope_id,action_id,version,work_id,handler_scope_id,state,observation,phase,provider_reference,reason,state_data,response_kind,response_reason,response_reference,response_data FROM input
            """;
        await command.ExecuteNonQueryAsync(ct);
    }

    private async Task EnsureExternalBatchBindingsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        IReadOnlyDictionary<string, WorkIdentity> identities, CancellationToken ct)
    {
        using var table = new DataTable();
        table.Columns.Add("scope_id", typeof(string));
        table.Columns.Add("tenant", typeof(string));
        table.Columns.Add("consumer", typeof(string));
        table.Columns.Add("source", typeof(string));
        table.Columns.Add("event_type", typeof(string));
        table.Columns.Add("key_version", typeof(string));
        table.Columns.Add("fingerprint_version", typeof(string));
        foreach (var identity in identities.Values.OrderBy(i => i.Scope.Id, StringComparer.Ordinal))
        {
            var scope = identity.Scope;
            table.Rows.Add(scope.Id, scope.Tenant, scope.Consumer, scope.Source, scope.EventType,
                identity.Policy.KeyVersion, identity.Policy.FingerprintVersion);
        }
        await using var command = new NpgsqlCommand { Connection = connection, Transaction = transaction };
        var input = AddExternalTable(command, table);
        command.CommandText = $"INSERT INTO {_schema}.policy_bindings(scope_id,tenant,consumer,source,event_type,key_version,fingerprint_version) SELECT * FROM ({input}) x ORDER BY scope_id ON CONFLICT DO NOTHING";
        await command.ExecuteNonQueryAsync(ct);
        // Use a fresh ReadCommitted statement snapshot to see a concurrent binding insert after waiting on its unique key.
        command.CommandText = $"SELECT p.scope_id,p.tenant,p.consumer,p.source,p.event_type,p.key_version,p.fingerprint_version FROM {_schema}.policy_bindings p JOIN ({input}) x ON p.scope_id=x.scope_id ORDER BY p.scope_id FOR SHARE OF p";
        await using var reader = await command.ExecuteReaderAsync(ct);
        var count = 0;
        while (await reader.ReadAsync(ct))
        {
            var scope = reader.GetString(0);
            WorkAcceptance.EnsurePolicyCompatible(identities[scope], new(scope, reader.GetString(1), reader.GetString(2),
                reader.GetString(3), reader.GetString(4), reader.GetString(5), reader.GetString(6)));
            count++;
        }
        if (count != identities.Count) throw new InvalidOperationException("External result policy binding is missing.");
    }
}
