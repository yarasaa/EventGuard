using Microsoft.Data.SqlClient;

namespace EventGuard.SqlServer;

public sealed partial class SqlServerWorkStore : ITransactionalJobStore<SqlServerSession>
{
    /// <summary>Explicit optional migration; existing work/outbox schema versions are unchanged.</summary>
    public async Task InitializeJobsAsync(CancellationToken cancellationToken = default)
    {
        await InitializeAsync(cancellationToken);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginLocalAsync(cancellationToken);
        await SqlServerProtocol.LockAsync(connection, transaction, "job-schema:" + _schema, cancellationToken);
        await using (var exists = new SqlCommand("SELECT OBJECT_ID(@name,'U')", connection, transaction))
        {
            exists.Parameters.AddValue("name", _schema + ".job_schema_version");
            if (await exists.ExecuteScalarCheckedAsync(cancellationToken) is not int)
            {
                await using var stream = typeof(SqlServerWorkStore).Assembly.GetManifestResourceStream(
                    "EventGuard.SqlServer.Jobs.Migrations.001_jobs.sql") ?? throw new InvalidOperationException("Jobs migration missing.");
                using var reader = new StreamReader(stream);
                await using var create = new SqlCommand((await reader.ReadToEndAsync(cancellationToken)).Replace("__SCHEMA__", _schema, StringComparison.Ordinal), connection, transaction);
                await create.ExecuteNonQueryCheckedAsync(cancellationToken);
            }
        }
        await using (var current = new SqlCommand($"SELECT version FROM {_schema}.job_schema_version WHERE singleton=1", connection, transaction))
        {
            if (await current.ExecuteScalarCheckedAsync(cancellationToken) is int version && version == 1)
            {
                await using var stream = typeof(SqlServerWorkStore).Assembly.GetManifestResourceStream(
                    "EventGuard.SqlServer.Jobs.Migrations.002_job_items.sql") ?? throw new InvalidOperationException("Jobs migration missing.");
                using var reader = new StreamReader(stream);
                await using var migrate = new SqlCommand((await reader.ReadToEndAsync(cancellationToken)).Replace("__SCHEMA__", _schema, StringComparison.Ordinal), connection, transaction);
                await migrate.ExecuteNonQueryCheckedAsync(cancellationToken);
            }
        }
        await CheckJobSchemaAsync(connection, transaction, cancellationToken);
        await SqlServerProtocol.CommitAsync(transaction, Guid.NewGuid(), cancellationToken);
    }

    private async Task CheckJobSchemaAsync(SqlConnection connection, SqlTransaction? transaction, CancellationToken ct)
    {
        await using var command = new SqlCommand($"SELECT version FROM {_schema}.job_schema_version WHERE singleton=1", connection, transaction);
        if (await command.ExecuteScalarCheckedAsync(ct) is not int version || version != 2)
            throw new InvalidOperationException("Unsupported Jobs schema version. Run the explicit Jobs migration.");
    }

    public async Task<JobRegistrationStatus> RegisterAsync(JobDefinition definition, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(definition);
        definition.RequireCapabilities([JobCapabilities.TransactionalHandler, JobCapabilities.TransactionalItems]);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginLocalAsync(cancellationToken);
        await CheckJobSchemaAsync(connection, transaction, cancellationToken);
        await SqlServerProtocol.LockAsync(connection, transaction, "job-definition:" + _schema + ":" + definition.Id, cancellationToken);
        await using (var find = new SqlCommand($"SELECT fingerprint FROM {_schema}.job_definitions WHERE id=@id", connection, transaction))
        {
            find.Parameters.AddValue("id", definition.Id);
            if (await find.ExecuteScalarCheckedAsync(cancellationToken) is string fingerprint)
            {
                if (fingerprint != definition.Fingerprint) throw new JobDefinitionConflictException(definition.Id);
                return JobRegistrationStatus.AlreadyRegistered;
            }
        }
        await EnsureBindingAsync(connection, transaction, new(definition.WorkScope, definition.IdentityPolicy, "registration"), cancellationToken);
        await using var insert = new SqlCommand($"""
            INSERT INTO {_schema}.job_definitions(id,scope_id,tenant,application,name,version,fingerprint,
                contract_name,contract_version,handler_version,definition_plan,capabilities,work_scope_id)
            VALUES(@id,@scope,@tenant,@application,@name,@version,@fingerprint,@contract,@contract_version,@handler,@plan,@capabilities,@work_scope)
            """, connection, transaction);
        insert.Parameters.AddValue("id", definition.Id); insert.Parameters.AddValue("scope", definition.Scope.Id);
        insert.Parameters.AddValue("tenant", definition.Scope.Tenant); insert.Parameters.AddValue("application", definition.Scope.Application);
        insert.Parameters.AddValue("name", definition.Name); insert.Parameters.AddValue("version", definition.Version);
        insert.Parameters.AddValue("fingerprint", definition.Fingerprint); insert.Parameters.AddValue("contract", definition.ContractName);
        insert.Parameters.AddValue("contract_version", definition.ContractVersion); insert.Parameters.AddValue("handler", definition.HandlerVersion);
        insert.Parameters.AddValue("plan", definition.CopyPlan()); insert.Parameters.AddValue("capabilities", string.Join('\n', definition.RequiredCapabilities));
        insert.Parameters.AddValue("work_scope", definition.WorkScope.Id);
        await insert.ExecuteNonQueryCheckedAsync(cancellationToken);
        await SqlServerProtocol.CommitAsync(transaction, Guid.NewGuid(), cancellationToken);
        return JobRegistrationStatus.Registered;
    }

    public async Task ValidateAsync(JobDefinition definition, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(definition);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await CheckJobSchemaAsync(connection, null, cancellationToken);
        await ValidateJobAsync(connection, null, definition, cancellationToken);
    }

    private async Task ValidateJobAsync(SqlConnection connection, SqlTransaction? transaction, JobDefinition definition, CancellationToken ct)
    {
        definition.RequireCapabilities([JobCapabilities.TransactionalHandler, JobCapabilities.TransactionalItems]);
        await using var command = new SqlCommand($"SELECT fingerprint FROM {_schema}.job_definitions WHERE scope_id=@scope AND id=@id", connection, transaction);
        command.Parameters.AddValue("scope", definition.Scope.Id); command.Parameters.AddValue("id", definition.Id);
        var saved = await command.ExecuteScalarCheckedAsync(ct) as string;
        if (saved is null) throw new InvalidOperationException("Job definition is not registered. Register it explicitly before submission or execution.");
        if (saved != definition.Fingerprint) throw new JobDefinitionConflictException(definition.Id);
    }

    public async Task<JobSubmissionResult> SubmitAsync(JobSubmission submission, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(submission);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginLocalAsync(cancellationToken);
        var result = await SubmitJobWithinTransactionAsync(connection, transaction, submission, cancellationToken);
        await SqlServerProtocol.CommitAsync(transaction, submission.RequestId, cancellationToken);
        return result;
    }

    private async Task<JobSubmissionResult> SubmitJobWithinTransactionAsync(SqlConnection connection, SqlTransaction transaction,
        JobSubmission submission, CancellationToken cancellationToken)
    {
        var definition = submission.Definition;
        await CheckJobSchemaAsync(connection, transaction, cancellationToken);
        await SqlServerProtocol.LockAsync(connection, transaction, "job-request:" + _schema + ":" + definition.Scope.Id + ":" + submission.RequestId, cancellationToken);
        var prior = await ReadJobRequestAsync(connection, transaction, definition.Scope, submission.RequestId, cancellationToken);
        if (prior is not null)
            return new(prior.Value.Fingerprint != submission.RequestFingerprint ? JobSubmissionStatus.RequestConflict
                : prior.Value.Result.Status == JobSubmissionStatus.Accepted ? JobSubmissionStatus.AlreadyAccepted : prior.Value.Result.Status,
                submission.RequestId, prior.Value.Result.OperationId, true);
        await ValidateJobAsync(connection, transaction, definition, cancellationToken);
        await SqlServerProtocol.LockAsync(connection, transaction, "job-business:" + _schema + ":" + submission.BusinessIdentity, cancellationToken);
        Guid operationId = Guid.Empty;
        JobSubmissionStatus status = JobSubmissionStatus.Accepted;
        await using (var find = new SqlCommand($"SELECT id,fingerprint FROM {_schema}.job_operations WHERE scope_id=@scope AND business_identity=@identity", connection, transaction))
        {
            find.Parameters.AddValue("scope", definition.Scope.Id); find.Parameters.AddValue("identity", submission.BusinessIdentity);
            await using var reader = await find.ExecuteReaderCheckedAsync(cancellationToken);
            if (await reader.ReadCheckedAsync(cancellationToken))
            {
                operationId = reader.GetGuid(0);
                status = reader.GetString(1) == submission.OperationFingerprint ? JobSubmissionStatus.AlreadyAccepted : JobSubmissionStatus.BusinessConflict;
            }
        }
        if (operationId == Guid.Empty)
        {
            var accepted = await AcceptWithinTransactionAsync(connection, transaction, submission.ToWorkSubmission(), cancellationToken);
            if (accepted.Status != AcceptanceStatus.Accepted) throw new InvalidOperationException("Job execution identity exists outside the command protocol.");
            operationId = accepted.Work.Id;
            await using var insert = new SqlCommand($"""
                INSERT INTO {_schema}.job_operations(id,scope_id,definition_id,business_identity,business_key,fingerprint)
                VALUES(@id,@scope,@definition,@identity,@key,@fingerprint)
                """, connection, transaction);
            insert.Parameters.AddValue("id", operationId); insert.Parameters.AddValue("scope", definition.Scope.Id);
            insert.Parameters.AddValue("definition", definition.Id); insert.Parameters.AddValue("identity", submission.BusinessIdentity);
            insert.Parameters.AddValue("key", submission.BusinessKey); insert.Parameters.AddValue("fingerprint", submission.OperationFingerprint);
            await insert.ExecuteNonQueryCheckedAsync(cancellationToken);
        }
        await using (var receipt = new SqlCommand($"""
            INSERT INTO {_schema}.job_requests(scope_id,id,fingerprint,actor,operation_id,status) VALUES(@scope,@id,@fingerprint,@actor,@operation,@status)
            """, connection, transaction))
        {
            receipt.Parameters.AddValue("scope", definition.Scope.Id); receipt.Parameters.AddValue("id", submission.RequestId);
            receipt.Parameters.AddValue("fingerprint", submission.RequestFingerprint); receipt.Parameters.AddValue("actor", submission.Actor);
            receipt.Parameters.AddValue("operation", operationId); receipt.Parameters.AddValue("status", (int)status);
            await receipt.ExecuteNonQueryCheckedAsync(cancellationToken);
        }
        return new(status, submission.RequestId, operationId);
    }

    private async Task<(string Fingerprint, JobSubmissionResult Result)?> ReadJobRequestAsync(SqlConnection connection,
        SqlTransaction? transaction, JobScope scope, Guid requestId, CancellationToken ct)
    {
        await using var command = new SqlCommand($"SELECT fingerprint,operation_id,status FROM {_schema}.job_requests WHERE scope_id=@scope AND id=@id", connection, transaction);
        command.Parameters.AddValue("scope", scope.Id); command.Parameters.AddValue("id", requestId);
        await using var reader = await command.ExecuteReaderCheckedAsync(ct);
        if (!await reader.ReadCheckedAsync(ct)) return null;
        var status = (JobSubmissionStatus)reader.GetInt32(2);
        if (status is not (JobSubmissionStatus.Accepted or JobSubmissionStatus.AlreadyAccepted or JobSubmissionStatus.BusinessConflict))
            throw new InvalidOperationException("Invalid durable job receipt status.");
        return (reader.GetString(0), new(status, requestId, reader.GetGuid(1)));
    }

    public async Task<JobSubmissionResult?> FindRequestAsync(JobScope scope, Guid requestId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        if (requestId == Guid.Empty) throw new ArgumentException("A request ID is required.", nameof(requestId));
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        return (await ReadJobRequestAsync(connection, null, scope, requestId, cancellationToken))?.Result;
    }

    public async Task<JobOperation?> FindAsync(JobScope scope, Guid operationId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        if (operationId == Guid.Empty) throw new ArgumentException("An operation ID is required.", nameof(operationId));
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new SqlCommand($"""
            SELECT d.name,d.version,d.fingerprint,o.business_key,o.fingerprint,d.work_scope_id,
                w.state,w.version,w.attempt_count,w.next_attempt_at,w.last_reason,o.created_at
            FROM {_schema}.job_operations o JOIN {_schema}.job_definitions d ON d.id=o.definition_id
            JOIN {_schema}.works w ON w.id=o.id WHERE o.scope_id=@scope AND o.id=@id
            """, connection);
        command.Parameters.AddValue("scope", scope.Id); command.Parameters.AddValue("id", operationId);
        await using var reader = await command.ExecuteReaderCheckedAsync(cancellationToken);
        return await reader.ReadCheckedAsync(cancellationToken) ? new(operationId, scope, reader.GetString(0), reader.GetInt32(1),
            reader.GetString(2), reader.GetString(3), reader.GetString(4), reader.GetString(5), (WorkState)reader.GetInt32(6),
            reader.GetInt64(7), reader.GetInt32(8), reader.GetFieldValue<DateTimeOffset>(9), reader.IsDBNull(10) ? null : reader.GetString(10),
            reader.GetFieldValue<DateTimeOffset>(11)) : null;
    }
}
