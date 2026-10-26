using Microsoft.Data.SqlClient;

namespace EventGuard.SqlServer;

internal sealed class SqlServerJobItemWriter(SqlConnection connection, SqlTransaction transaction,
    string schema, Guid? originWorkId) : IJobItemWriter
{
    public async Task<JobItemStatus> ApplyAsync(JobItemSubmission item, Func<CancellationToken, ValueTask> handler,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(item); ArgumentNullException.ThrowIfNull(handler);
        _ = transaction.IsolationLevel;
        if (originWorkId is not Guid workId || !ReferenceEquals(transaction.Connection, connection))
            throw new InvalidOperationException("Job item effects require an active job transaction.");
        await using (var owner = new SqlCommand($"""
            SELECT d.fingerprint FROM {schema}.job_operations o JOIN {schema}.job_definitions d ON d.id=o.definition_id
            WHERE o.id=@work AND o.scope_id=@scope AND o.definition_id=@definition
            """, connection, transaction))
        {
            owner.Parameters.AddValue("work", workId); owner.Parameters.AddValue("scope", item.Definition.Scope.Id);
            owner.Parameters.AddValue("definition", item.Definition.Id);
            if (await owner.ExecuteScalarCheckedAsync(cancellationToken) as string != item.Definition.Fingerprint)
                throw new InvalidOperationException("Item definition does not own this job transaction.");
        }
        await SqlServerProtocol.LockAsync(connection, transaction, "job-item:" + schema + ":" + item.Id, cancellationToken);
        await using (var existing = new SqlCommand($"SELECT fingerprint FROM {schema}.job_items WHERE id=@id", connection, transaction))
        {
            existing.Parameters.AddValue("id", item.Id);
            if (await existing.ExecuteScalarCheckedAsync(cancellationToken) is string saved)
            {
                if (saved != item.Fingerprint) throw new JobItemConflictException(item.Id);
                return JobItemStatus.AlreadyApplied;
            }
        }
        var savepoint = "ji" + Guid.NewGuid().ToString("N")[..30];
        await transaction.SaveAsync(savepoint, cancellationToken);
        try
        {
            await using (var insert = new SqlCommand($"INSERT INTO {schema}.job_items(id,work_id,item_key,fingerprint) VALUES(@id,@work,@key,@fingerprint)", connection, transaction))
            {
                insert.Parameters.AddValue("id", item.Id); insert.Parameters.AddValue("work", workId);
                insert.Parameters.AddValue("key", item.Key); insert.Parameters.AddValue("fingerprint", item.Fingerprint);
                await insert.ExecuteNonQueryCheckedAsync(cancellationToken);
            }
            await handler(cancellationToken);
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
