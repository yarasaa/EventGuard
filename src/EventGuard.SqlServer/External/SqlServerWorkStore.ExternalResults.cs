using Microsoft.Data.SqlClient;
using System.Data;

namespace EventGuard.SqlServer;

public sealed partial class SqlServerWorkStore
{
    private async Task PublishExternalUpdateAsync(SqlConnection connection, SqlTransaction transaction,
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
        await using var command = new SqlCommand($"""
            INSERT INTO {_schema}.external_result_updates(scope_id,action_id,version,work_id,handler_scope_id,state,observation,phase,
                provider_reference,reason,state_data,response_kind,response_reason,response_reference,response_data)
            VALUES(@scope,@action,@version,@work,@handler,@state,@observation,@phase,@reference,@reason,@data,@response_kind,@response_reason,@response_reference,@response_data)
            """, connection, transaction);
        command.Parameters.AddValue("scope", record.Action.Scope.Id); command.Parameters.AddValue("action", record.Action.Id);
        command.Parameters.AddValue("version", record.Version); command.Parameters.AddValue("work", accepted.Work.Id);
        command.Parameters.AddValue("handler", target.WorkScope.Id); command.Parameters.AddValue("state", (int)record.State);
        command.Parameters.AddValue("observation", (int)record.Observation); command.Parameters.AddValue("phase", (int)phase);
        command.Parameters.AddValue("data", record.ResultData);
        SqlServerProtocol.Optional(command, "reference", SqlDbType.NVarChar, record.ProviderReference);
        SqlServerProtocol.Optional(command, "reason", SqlDbType.NVarChar, record.LastReason);
        SqlServerProtocol.Optional(command, "response_kind", SqlDbType.Int, response is null ? null : (int)response.Kind);
        SqlServerProtocol.Optional(command, "response_reason", SqlDbType.NVarChar, response?.Reason);
        SqlServerProtocol.Optional(command, "response_reference", SqlDbType.NVarChar, response?.Reference);
        SqlServerProtocol.Optional(command, "response_data", SqlDbType.VarBinary, response?.CopyData());
        await command.ExecuteNonQueryCheckedAsync(ct);
    }
}
