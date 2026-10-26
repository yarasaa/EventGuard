using Npgsql;

namespace EventGuard.PostgreSql;

internal sealed class PostgreSqlJobItemWriter(NpgsqlConnection connection, NpgsqlTransaction transaction,
    string schema, Guid? originWorkId) : IJobItemWriter
{
    public async Task<JobItemStatus> ApplyAsync(JobItemSubmission item, Func<CancellationToken, ValueTask> handler,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(item); ArgumentNullException.ThrowIfNull(handler);
        _ = transaction.IsolationLevel;
        if (originWorkId is not Guid workId || !ReferenceEquals(transaction.Connection, connection))
            throw new InvalidOperationException("Job item effects require an active job transaction.");
        await using (var owner = new NpgsqlCommand($"""
            SELECT d.fingerprint FROM {schema}.job_operations o JOIN {schema}.job_definitions d ON d.id=o.definition_id
            WHERE o.id=@work AND o.scope_id=@scope AND o.definition_id=@definition
            """, connection, transaction))
        {
            owner.Parameters.AddWithValue("work", workId); owner.Parameters.AddWithValue("scope", item.Definition.Scope.Id);
            owner.Parameters.AddWithValue("definition", item.Definition.Id);
            if (await owner.ExecuteScalarAsync(cancellationToken) as string != item.Definition.Fingerprint)
                throw new InvalidOperationException("Item definition does not own this job transaction.");
        }
        await PostgreSqlProtocol.LockAsync(connection, transaction, "job-item:" + schema + ":" + item.Id, cancellationToken);
        await using (var existing = new NpgsqlCommand($"SELECT fingerprint FROM {schema}.job_items WHERE id=@id", connection, transaction))
        {
            existing.Parameters.AddWithValue("id", item.Id);
            if (await existing.ExecuteScalarAsync(cancellationToken) is string saved)
            {
                if (saved != item.Fingerprint) throw new JobItemConflictException(item.Id);
                return JobItemStatus.AlreadyApplied;
            }
        }
        var savepoint = "ji" + Guid.NewGuid().ToString("N")[..30];
        await transaction.SaveAsync(savepoint, cancellationToken);
        try
        {
            await using (var insert = new NpgsqlCommand($"INSERT INTO {schema}.job_items(id,work_id,item_key,fingerprint) VALUES(@id,@work,@key,@fingerprint)", connection, transaction))
            {
                insert.Parameters.AddWithValue("id", item.Id); insert.Parameters.AddWithValue("work", workId);
                insert.Parameters.AddWithValue("key", item.Key); insert.Parameters.AddWithValue("fingerprint", item.Fingerprint);
                await insert.ExecuteNonQueryAsync(cancellationToken);
            }
            await handler(cancellationToken);
            await transaction.ReleaseAsync(savepoint, cancellationToken);
        }
        catch
        {
            // Even when application code catches the error, a failed item cannot leave an applied receipt or partial SQL effects.
            await transaction.RollbackAsync(savepoint, CancellationToken.None);
            throw;
        }
        return JobItemStatus.Applied;
    }
}
