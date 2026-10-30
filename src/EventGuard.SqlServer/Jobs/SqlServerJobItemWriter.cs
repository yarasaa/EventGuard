using Microsoft.Data.SqlClient;

namespace EventGuard.SqlServer;

internal sealed class SqlServerJobItemWriter(SqlConnection connection, SqlTransaction transaction,
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
        await using (var owner = new SqlCommand($"SELECT d.fingerprint FROM {schema}.job_operations o JOIN {schema}.job_definitions d ON d.id=o.definition_id WHERE o.id=@work AND o.scope_id=@scope AND o.definition_id=@definition", connection, transaction))
        {
            owner.Parameters.AddWithValue("work", workId); owner.Parameters.AddWithValue("scope", definition.Scope.Id); owner.Parameters.AddWithValue("definition", definition.Id);
            if (await owner.ExecuteScalarCheckedAsync(cancellationToken) as string != definition.Fingerprint) throw new InvalidOperationException("Item definition does not own this job transaction.");
        }
        await using var command = new SqlCommand { Connection = connection, Transaction = transaction };
        using var table = new System.Data.DataTable();
        table.Columns.Add("id", typeof(string)); table.Columns.Add("item_key", typeof(string)); table.Columns.Add("fingerprint", typeof(string));
        foreach (var item in batch.Items) table.Rows.Add(item.Id, item.Key, item.Fingerprint);
        command.Parameters.Add(new SqlParameter("rows", System.Data.SqlDbType.Structured) { TypeName = schema + ".job_item_inputs", Value = table });
        command.Parameters.AddWithValue("work", workId); command.Parameters.AddWithValue("prefix", "job-item:" + schema + ":");
        command.CommandText = $"""
            DECLARE @id nvarchar(64),@resource nvarchar(255),@result int;
            DECLARE item_keys CURSOR LOCAL FAST_FORWARD FOR SELECT id FROM @rows ORDER BY id;
            OPEN item_keys; FETCH NEXT FROM item_keys INTO @id;
            WHILE @@FETCH_STATUS=0 BEGIN
                SET @resource=@prefix+@id;
                EXEC @result=sys.sp_getapplock @Resource=@resource,@LockMode='Exclusive',@LockOwner='Transaction',@LockTimeout=15000;
                IF @result<0 THROW 51002,'Job item lock unavailable.',1;
                FETCH NEXT FROM item_keys INTO @id;
            END;
            CLOSE item_keys; DEALLOCATE item_keys;

            """;
        await command.ExecuteNonQueryCheckedAsync(cancellationToken);
        command.CommandText = $"""
            SELECT i.id,i.fingerprint FROM {schema}.job_items i JOIN @rows x ON x.id=i.id;
            """;
        var existing = new Dictionary<string,string>();
        await using (var reader = await command.ExecuteReaderCheckedAsync(cancellationToken))
            while (await reader.ReadCheckedAsync(cancellationToken)) existing.Add(reader.GetString(0),reader.GetString(1));
        foreach (var item in batch.Items)
            if (existing.TryGetValue(item.Id,out var fingerprint) && fingerprint != item.Fingerprint) throw new JobItemConflictException(item.Id);
        var fresh = batch.Items.Where(i => !existing.ContainsKey(i.Id)).ToArray();
        if (fresh.Length == 0) return 0;
        var savepoint = "jb" + Guid.NewGuid().ToString("N")[..30];
        await transaction.SaveAsync(savepoint,cancellationToken);
        try
        {
            command.CommandText = $"INSERT INTO {schema}.job_items(id,work_id,item_key,fingerprint) SELECT x.id,@work,x.item_key,x.fingerprint FROM @rows x WHERE NOT EXISTS(SELECT 1 FROM {schema}.job_items i WHERE i.id=x.id)";
            await command.ExecuteNonQueryCheckedAsync(cancellationToken);
            await handler(Array.AsReadOnly(fresh),cancellationToken);

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
