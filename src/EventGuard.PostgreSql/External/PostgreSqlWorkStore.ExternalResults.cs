using Npgsql;
using NpgsqlTypes;

namespace EventGuard.PostgreSql;

public sealed partial class PostgreSqlWorkStore
{
    private async Task PublishExternalUpdateAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        ExternalActionRecord record, ExternalResultTarget? target, ExternalPhase phase, ExternalResult? response, CancellationToken ct)
    {
        if (target is null) return;
        var update = new ExternalActionUpdate(record.Action, record.Version, record.State, record.Observation, phase,
            record.ProviderReference, record.LastReason, record.ResultData, response);
        var key = StableHash.Of("external-result-work/v1", record.Action.Scope.Id, record.Action.Id.ToString("D"),
            record.Version.ToString(System.Globalization.CultureInfo.InvariantCulture));
        var submission = new WorkSubmission(new(target.WorkScope, target.IdentityPolicy, key),
            new Guid(Convert.FromHexString(key).AsSpan(0, 16)), System.Text.Encoding.UTF8.GetBytes(key),
            ContentFingerprint.Parse(update.Fingerprint), correlationId: record.Action.Id.ToString("D"));
        var accepted = await AcceptWithinTransactionAsync(connection, transaction, submission, ct);
        if (accepted.Status != AcceptanceStatus.Accepted) throw new InvalidOperationException("External result work already exists outside its atomic settlement.");
        await using var command = new NpgsqlCommand($"""
            INSERT INTO {_schema}.external_result_updates(scope_id,action_id,version,work_id,handler_scope_id,state,observation,phase,
                provider_reference,reason,state_data,response_kind,response_reason,response_reference,response_data)
            VALUES(@scope,@action,@version,@work,@handler,@state,@observation,@phase,@reference,@reason,@data,@response_kind,@response_reason,@response_reference,@response_data)
            """, connection, transaction);
        command.Parameters.AddWithValue("scope", record.Action.Scope.Id); command.Parameters.AddWithValue("action", record.Action.Id);
        command.Parameters.AddWithValue("version", record.Version); command.Parameters.AddWithValue("work", accepted.Work.Id);
        command.Parameters.AddWithValue("handler", target.WorkScope.Id); command.Parameters.AddWithValue("state", (int)record.State);
        command.Parameters.AddWithValue("observation", (int)record.Observation); command.Parameters.AddWithValue("phase", (int)phase);
        command.Parameters.AddWithValue("data", record.ResultData);
        PostgreSqlProtocol.Optional(command, "reference", NpgsqlDbType.Text, record.ProviderReference);
        PostgreSqlProtocol.Optional(command, "reason", NpgsqlDbType.Text, record.LastReason);
        PostgreSqlProtocol.Optional(command, "response_kind", NpgsqlDbType.Integer, response is null ? null : (int)response.Kind);
        PostgreSqlProtocol.Optional(command, "response_reason", NpgsqlDbType.Text, response?.Reason);
        PostgreSqlProtocol.Optional(command, "response_reference", NpgsqlDbType.Text, response?.Reference);
        PostgreSqlProtocol.Optional(command, "response_data", NpgsqlDbType.Bytea, response?.CopyData());
        await command.ExecuteNonQueryAsync(ct);
    }
}
