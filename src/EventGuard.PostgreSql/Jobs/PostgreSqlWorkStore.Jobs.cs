using Npgsql;

namespace EventGuard.PostgreSql;

public sealed partial class PostgreSqlWorkStore : ITransactionalJobStore<PostgreSqlSession>
{
    /// <summary>Explicit optional migration; existing work/outbox schema versions are unchanged.</summary>
    public async Task InitializeJobsAsync(CancellationToken cancellationToken = default)
    {
        await InitializeAsync(cancellationToken);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await PostgreSqlProtocol.LockAsync(connection, transaction, "job-schema:" + _schema, cancellationToken);
        await using (var exists = new NpgsqlCommand("SELECT to_regclass(@name)::text", connection, transaction))
        {
            exists.Parameters.AddWithValue("name", _schema + ".job_schema_version");
            if (await exists.ExecuteScalarAsync(cancellationToken) is not string)
            {
                await using var stream = typeof(PostgreSqlWorkStore).Assembly.GetManifestResourceStream(
                    "EventGuard.PostgreSql.Jobs.Migrations.001_jobs.sql") ?? throw new InvalidOperationException("Jobs migration missing.");
                using var reader = new StreamReader(stream);
                await using var create = new NpgsqlCommand((await reader.ReadToEndAsync(cancellationToken)).Replace("__SCHEMA__", _schema, StringComparison.Ordinal), connection, transaction);
                await create.ExecuteNonQueryAsync(cancellationToken);
            }
        }
        await using (var current = new NpgsqlCommand($"SELECT version FROM {_schema}.job_schema_version WHERE singleton", connection, transaction))
        {
            if (await current.ExecuteScalarAsync(cancellationToken) is int version && version == 1)
            {
                await using var stream = typeof(PostgreSqlWorkStore).Assembly.GetManifestResourceStream(
                    "EventGuard.PostgreSql.Jobs.Migrations.002_job_items.sql") ?? throw new InvalidOperationException("Jobs migration missing.");
                using var reader = new StreamReader(stream);
                await using var migrate = new NpgsqlCommand((await reader.ReadToEndAsync(cancellationToken)).Replace("__SCHEMA__", _schema, StringComparison.Ordinal), connection, transaction);
                await migrate.ExecuteNonQueryAsync(cancellationToken);
            }
        }
        await CheckJobSchemaAsync(connection, transaction, cancellationToken);
        await PostgreSqlProtocol.CommitAsync(transaction, Guid.NewGuid(), cancellationToken);
    }

    private async Task CheckJobSchemaAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($"SELECT version FROM {_schema}.job_schema_version WHERE singleton", connection, transaction);
        if (await command.ExecuteScalarAsync(ct) is not int version || version != 2)
            throw new InvalidOperationException("Unsupported Jobs schema version. Run the explicit Jobs migration.");
    }

    public async Task<JobRegistrationStatus> RegisterAsync(JobDefinition definition, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(definition);
        definition.RequireCapabilities([JobCapabilities.TransactionalHandler, JobCapabilities.TransactionalItems]);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await CheckJobSchemaAsync(connection, transaction, cancellationToken);
        await PostgreSqlProtocol.LockAsync(connection, transaction, "job-definition:" + _schema + ":" + definition.Id, cancellationToken);
        await using (var find = new NpgsqlCommand($"SELECT fingerprint FROM {_schema}.job_definitions WHERE id=@id", connection, transaction))
        {
            find.Parameters.AddWithValue("id", definition.Id);
            if (await find.ExecuteScalarAsync(cancellationToken) is string fingerprint)
            {
                if (fingerprint != definition.Fingerprint) throw new JobDefinitionConflictException(definition.Id);
                return JobRegistrationStatus.AlreadyRegistered;
            }
        }
        await EnsureBindingAsync(connection, transaction, new(definition.WorkScope, definition.IdentityPolicy, "registration"), cancellationToken);
        await using var insert = new NpgsqlCommand($"""
            INSERT INTO {_schema}.job_definitions(id,scope_id,tenant,application,name,version,fingerprint,
                contract_name,contract_version,handler_version,definition_plan,capabilities,work_scope_id)
            VALUES(@id,@scope,@tenant,@application,@name,@version,@fingerprint,@contract,@contract_version,@handler,@plan,@capabilities,@work_scope)
            """, connection, transaction);
        insert.Parameters.AddWithValue("id", definition.Id); insert.Parameters.AddWithValue("scope", definition.Scope.Id);
        insert.Parameters.AddWithValue("tenant", definition.Scope.Tenant); insert.Parameters.AddWithValue("application", definition.Scope.Application);
        insert.Parameters.AddWithValue("name", definition.Name); insert.Parameters.AddWithValue("version", definition.Version);
        insert.Parameters.AddWithValue("fingerprint", definition.Fingerprint); insert.Parameters.AddWithValue("contract", definition.ContractName);
        insert.Parameters.AddWithValue("contract_version", definition.ContractVersion); insert.Parameters.AddWithValue("handler", definition.HandlerVersion);
        insert.Parameters.AddWithValue("plan", definition.CopyPlan()); insert.Parameters.AddWithValue("capabilities", string.Join('\n', definition.RequiredCapabilities));
        insert.Parameters.AddWithValue("work_scope", definition.WorkScope.Id);
        await insert.ExecuteNonQueryAsync(cancellationToken);
        await PostgreSqlProtocol.CommitAsync(transaction, Guid.NewGuid(), cancellationToken);
        return JobRegistrationStatus.Registered;
    }

    public async Task ValidateAsync(JobDefinition definition, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(definition);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await CheckJobSchemaAsync(connection, null, cancellationToken);
        await ValidateJobAsync(connection, null, definition, cancellationToken);
    }

    private async Task ValidateJobAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction, JobDefinition definition, CancellationToken ct)
    {
        definition.RequireCapabilities([JobCapabilities.TransactionalHandler, JobCapabilities.TransactionalItems]);
        await using var command = new NpgsqlCommand($"SELECT fingerprint FROM {_schema}.job_definitions WHERE scope_id=@scope AND id=@id", connection, transaction);
        command.Parameters.AddWithValue("scope", definition.Scope.Id); command.Parameters.AddWithValue("id", definition.Id);
        var saved = await command.ExecuteScalarAsync(ct) as string;
        if (saved is null) throw new InvalidOperationException("Job definition is not registered. Register it explicitly before submission or execution.");
        if (saved != definition.Fingerprint) throw new JobDefinitionConflictException(definition.Id);
    }

    public async Task<JobSubmissionResult> SubmitAsync(JobSubmission submission, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(submission);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var result = await SubmitJobWithinTransactionAsync(connection, transaction, submission, cancellationToken);
        await PostgreSqlProtocol.CommitAsync(transaction, submission.RequestId, cancellationToken);
        return result;
    }

    private async Task<JobSubmissionResult> SubmitJobWithinTransactionAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        JobSubmission submission, CancellationToken cancellationToken)
    {
        var definition = submission.Definition;
        await CheckJobSchemaAsync(connection, transaction, cancellationToken);
        await PostgreSqlProtocol.LockAsync(connection, transaction, "job-request:" + _schema + ":" + definition.Scope.Id + ":" + submission.RequestId, cancellationToken);
        var prior = await ReadJobRequestAsync(connection, transaction, definition.Scope, submission.RequestId, cancellationToken);
        if (prior is not null)
            return new(prior.Value.Fingerprint != submission.RequestFingerprint ? JobSubmissionStatus.RequestConflict
                : prior.Value.Result.Status == JobSubmissionStatus.Accepted ? JobSubmissionStatus.AlreadyAccepted : prior.Value.Result.Status,
                submission.RequestId, prior.Value.Result.OperationId, true);
        await ValidateJobAsync(connection, transaction, definition, cancellationToken);
        await PostgreSqlProtocol.LockAsync(connection, transaction, "job-business:" + _schema + ":" + submission.BusinessIdentity, cancellationToken);
        Guid operationId = Guid.Empty;
        JobSubmissionStatus status = JobSubmissionStatus.Accepted;
        await using (var find = new NpgsqlCommand($"SELECT id,fingerprint FROM {_schema}.job_operations WHERE scope_id=@scope AND business_identity=@identity", connection, transaction))
        {
            find.Parameters.AddWithValue("scope", definition.Scope.Id); find.Parameters.AddWithValue("identity", submission.BusinessIdentity);
            await using var reader = await find.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
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
            await using var insert = new NpgsqlCommand($"""
                INSERT INTO {_schema}.job_operations(id,scope_id,definition_id,business_identity,business_key,fingerprint)
                VALUES(@id,@scope,@definition,@identity,@key,@fingerprint)
                """, connection, transaction);
            insert.Parameters.AddWithValue("id", operationId); insert.Parameters.AddWithValue("scope", definition.Scope.Id);
            insert.Parameters.AddWithValue("definition", definition.Id); insert.Parameters.AddWithValue("identity", submission.BusinessIdentity);
            insert.Parameters.AddWithValue("key", submission.BusinessKey); insert.Parameters.AddWithValue("fingerprint", submission.OperationFingerprint);
            await insert.ExecuteNonQueryAsync(cancellationToken);
        }
        await using (var receipt = new NpgsqlCommand($"""
            INSERT INTO {_schema}.job_requests(scope_id,id,fingerprint,actor,operation_id,status) VALUES(@scope,@id,@fingerprint,@actor,@operation,@status)
            """, connection, transaction))
        {
            receipt.Parameters.AddWithValue("scope", definition.Scope.Id); receipt.Parameters.AddWithValue("id", submission.RequestId);
            receipt.Parameters.AddWithValue("fingerprint", submission.RequestFingerprint); receipt.Parameters.AddWithValue("actor", submission.Actor);
            receipt.Parameters.AddWithValue("operation", operationId); receipt.Parameters.AddWithValue("status", (int)status);
            await receipt.ExecuteNonQueryAsync(cancellationToken);
        }
        return new(status, submission.RequestId, operationId);
    }

    private async Task<(string Fingerprint, JobSubmissionResult Result)?> ReadJobRequestAsync(NpgsqlConnection connection,
        NpgsqlTransaction? transaction, JobScope scope, Guid requestId, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($"SELECT fingerprint,operation_id,status FROM {_schema}.job_requests WHERE scope_id=@scope AND id=@id", connection, transaction);
        command.Parameters.AddWithValue("scope", scope.Id); command.Parameters.AddWithValue("id", requestId);
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) return null;
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
        await using var command = new NpgsqlCommand($"""
            SELECT d.name,d.version,d.fingerprint,o.business_key,o.fingerprint,d.work_scope_id,
                w.state,w.version,w.attempt_count,w.next_attempt_at,w.last_reason,o.created_at
            FROM {_schema}.job_operations o JOIN {_schema}.job_definitions d ON d.id=o.definition_id
            JOIN {_schema}.works w ON w.id=o.id WHERE o.scope_id=@scope AND o.id=@id
            """, connection);
        command.Parameters.AddWithValue("scope", scope.Id); command.Parameters.AddWithValue("id", operationId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? new(operationId, scope, reader.GetString(0), reader.GetInt32(1),
            reader.GetString(2), reader.GetString(3), reader.GetString(4), reader.GetString(5), (WorkState)reader.GetInt32(6),
            reader.GetInt64(7), reader.GetInt32(8), reader.GetFieldValue<DateTimeOffset>(9), reader.IsDBNull(10) ? null : reader.GetString(10),
            reader.GetFieldValue<DateTimeOffset>(11)) : null;
    }
}
