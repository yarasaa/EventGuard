using Npgsql;

namespace EventGuard.PostgreSql;

internal sealed class PostgreSqlJobItemWriter(NpgsqlConnection connection, NpgsqlTransaction transaction,
    string schema, Guid? originWorkId) : IJobItemBatchWriter
{
    public async Task<int> ApplyBatchAsync(JobItemBatch batch,
        Func<IReadOnlyList<JobItemSubmission>, CancellationToken, ValueTask> handler, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(batch); ArgumentNullException.ThrowIfNull(handler);
        _ = transaction.IsolationLevel;
        if (originWorkId is not Guid workId || !ReferenceEquals(transaction.Connection, connection))
            throw new InvalidOperationException("Job item effects require an active job transaction.");
        var definition = batch.Items[0].Definition;
        await using (var owner = new NpgsqlCommand($"SELECT d.fingerprint FROM {schema}.job_operations o JOIN {schema}.job_definitions d ON d.id=o.definition_id WHERE o.id=@work AND o.scope_id=@scope AND o.definition_id=@definition", connection, transaction))
        {
            owner.Parameters.AddWithValue("work", workId); owner.Parameters.AddWithValue("scope", definition.Scope.Id); owner.Parameters.AddWithValue("definition", definition.Id);
            if (await owner.ExecuteScalarAsync(cancellationToken) as string != definition.Fingerprint) throw new InvalidOperationException("Item definition does not own this job transaction.");
        }
        await using var command = new NpgsqlCommand { Connection = connection, Transaction = transaction };
        command.Parameters.AddWithValue("ids", batch.Items.Select(i => i.Id).ToArray());
        command.Parameters.AddWithValue("keys", batch.Items.Select(i => i.Key).ToArray());
        command.Parameters.AddWithValue("fingerprints", batch.Items.Select(i => i.Fingerprint).ToArray());
        command.Parameters.AddWithValue("work", workId); command.Parameters.AddWithValue("prefix", "job-item:" + schema + ":");
        command.CommandText = $"""
            WITH ordered AS MATERIALIZED (SELECT id FROM unnest(@ids) x(id) ORDER BY id)
            SELECT pg_advisory_xact_lock(hashtextextended(@prefix||id,0)) FROM ordered;

            """;
        await command.ExecuteNonQueryAsync(cancellationToken);
        command.CommandText = $"""
            SELECT id,fingerprint FROM {schema}.job_items WHERE id=ANY(@ids);
            """;
        var existing = new Dictionary<string,string>();
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            while (await reader.ReadAsync(cancellationToken)) existing.Add(reader.GetString(0),reader.GetString(1));
        foreach (var item in batch.Items)
            if (existing.TryGetValue(item.Id,out var fingerprint) && fingerprint != item.Fingerprint) throw new JobItemConflictException(item.Id);
        var fresh = batch.Items.Where(i => !existing.ContainsKey(i.Id)).ToArray();
        if (fresh.Length == 0) return 0;
        var savepoint = "jb" + Guid.NewGuid().ToString("N")[..30];
        await transaction.SaveAsync(savepoint,cancellationToken);
        try
        {
            command.CommandText = $"INSERT INTO {schema}.job_items(id,work_id,item_key,fingerprint) SELECT x.id,@work,x.item_key,x.fingerprint FROM unnest(@ids,@keys,@fingerprints) x(id,item_key,fingerprint) WHERE NOT EXISTS(SELECT 1 FROM {schema}.job_items i WHERE i.id=x.id)";
            await command.ExecuteNonQueryAsync(cancellationToken);
            await handler(Array.AsReadOnly(fresh),cancellationToken);
            await transaction.ReleaseAsync(savepoint,cancellationToken);
        }
        catch { await transaction.RollbackAsync(savepoint,CancellationToken.None); throw; }
        return fresh.Length;
    }

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
