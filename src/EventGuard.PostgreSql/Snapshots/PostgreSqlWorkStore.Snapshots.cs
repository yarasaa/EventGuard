using Npgsql;
using NpgsqlTypes;

namespace EventGuard.PostgreSql;

public sealed partial class PostgreSqlWorkStore : ISnapshotStore
{
    public async Task InitializeSnapshotsAsync(CancellationToken cancellationToken = default)
    {
        await InitializeJobsAsync(cancellationToken);
        await using var c = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var t = await c.BeginTransactionAsync(cancellationToken);
        await PostgreSqlProtocol.LockAsync(c, t, "snapshot-schema:" + _schema, cancellationToken);
        await using var exists = StoreCommand(c, t, "SELECT to_regclass(@name)::text", ("name", _schema + ".snapshot_schema_version"));
        if (await exists.ExecuteScalarAsync(cancellationToken) is not string)
        {
            await using var stream = typeof(PostgreSqlWorkStore).Assembly.GetManifestResourceStream("EventGuard.PostgreSql.Snapshots.Migrations.001_snapshots.sql")!;
            using var reader = new StreamReader(stream);
            await using var create = StoreCommand(c, t, (await reader.ReadToEndAsync(cancellationToken)).Replace("__SCHEMA__", _schema, StringComparison.Ordinal));
            await create.ExecuteNonQueryAsync(cancellationToken);
        }
        await using var version = StoreCommand(c, t, $"SELECT version FROM {_schema}.snapshot_schema_version WHERE singleton");
        if (await version.ExecuteScalarAsync(cancellationToken) is not int v || v != 1) throw new InvalidOperationException("Unsupported snapshot schema.");
        await PostgreSqlProtocol.CommitAsync(t, Guid.NewGuid(), cancellationToken);
    }

    private const string SnapshotColumns = "request_id,business_key,actor,parameters,fingerprint,definition_fingerprint,state,generation,page_no,item_count,batched_items,batch_count,source_view,source_cursor,paused,owner,lease_until,version";
    private static SnapshotProgress ReadSnapshot(NpgsqlDataReader reader, SnapshotDefinition definition, Guid? expectedId = null)
    {
        var request = new SnapshotRequest(definition, reader.GetGuid(0), reader.GetString(1), reader.GetString(2), reader.GetFieldValue<byte[]>(3));
        if (request.Fingerprint != reader.GetString(4) || definition.Fingerprint != reader.GetString(5) || expectedId.HasValue && request.Id != expectedId)
            throw new SnapshotConflictException();
        return new(request, (SnapshotState)reader.GetInt32(6), reader.GetInt64(7), reader.GetInt64(8), reader.GetInt64(9), reader.GetInt64(10), reader.GetInt64(11),
            reader.IsDBNull(12) ? null : reader.GetString(12), reader.GetFieldValue<byte[]>(13), reader.GetBoolean(14),
            reader.IsDBNull(15) ? null : reader.GetGuid(15), reader.IsDBNull(16) ? null : reader.GetFieldValue<DateTimeOffset>(16), reader.GetInt64(17));
    }
    private async Task<SnapshotProgress?> ReadSnapshotAsync(NpgsqlConnection c, NpgsqlTransaction? t, SnapshotDefinition definition, Guid id, CancellationToken ct)
    {
        await using var command = StoreCommand(c, t, $"SELECT {SnapshotColumns} FROM {_schema}.snapshots WHERE scope_id=@scope AND id=@id", ("scope", definition.Target.Scope.Id), ("id", id));
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? ReadSnapshot(reader, definition, id) : null;
    }
    public async Task<SnapshotProgress?> FindSnapshotAsync(SnapshotDefinition definition, Guid id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(definition);
        await using var c = await _dataSource.OpenConnectionAsync(cancellationToken);
        return await ReadSnapshotAsync(c, null, definition, id, cancellationToken);
    }
    public async Task<IReadOnlyList<SnapshotProgress>> ListSnapshotsAsync(SnapshotDefinition definition, int limit = 100, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(definition);
        if (limit is < 1 or > 1000) throw new ArgumentOutOfRangeException(nameof(limit));
        await using var c = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = StoreCommand(c, null, $"SELECT {SnapshotColumns} FROM {_schema}.snapshots WHERE scope_id=@scope AND definition_id=@definition AND definition_fingerprint=@fingerprint AND state<3 AND paused=false AND (lease_until IS NULL OR lease_until<=clock_timestamp()) ORDER BY updated_at,id LIMIT @limit",
            ("scope", definition.Target.Scope.Id), ("definition", definition.Id), ("fingerprint", definition.Fingerprint), ("limit", limit));
        var result = new List<SnapshotProgress>(); await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken)) result.Add(ReadSnapshot(reader, definition));
        return result;
    }
    public async Task<SnapshotProgress> CreateSnapshotAsync(SnapshotRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        await using var c = await _dataSource.OpenConnectionAsync(cancellationToken); await using var t = await c.BeginTransactionAsync(cancellationToken);
        var scope = request.Definition.Target.Scope.Id;
        await PostgreSqlProtocol.LockAsync(c, t, "snapshot-request:" + _schema + ":" + scope + ":" + request.RequestId, cancellationToken);
        await PostgreSqlProtocol.LockAsync(c, t, "snapshot:" + _schema + ":" + scope + ":" + request.Id, cancellationToken);
        await ValidateJobAsync(c, t, request.Definition.Target, cancellationToken);
        await using (var prior = StoreCommand(c, t, $"SELECT fingerprint FROM {_schema}.snapshot_requests WHERE scope_id=@scope AND id=@id", ("scope", scope), ("id", request.RequestId)))
            if (await prior.ExecuteScalarAsync(cancellationToken) is string saved && saved != request.Fingerprint) throw new SnapshotConflictException();
        var existing = await ReadSnapshotAsync(c, t, request.Definition, request.Id, cancellationToken);
        if (existing is not null && existing.Request.Fingerprint != request.Fingerprint) throw new SnapshotConflictException();
        if (existing is null)
        {
            await using var insert = StoreCommand(c, t, $"""
                INSERT INTO {_schema}.snapshots(scope_id,id,definition_id,definition_fingerprint,request_id,business_key,actor,parameters,fingerprint)
                VALUES(@scope,@id,@definition,@definition_fp,@request,@key,@actor,@parameters,@fingerprint)
                """, ("scope", scope), ("id", request.Id), ("definition", request.Definition.Id), ("definition_fp", request.Definition.Fingerprint),
                ("request", request.RequestId), ("key", request.BusinessKey), ("actor", request.Actor), ("parameters", request.CopyParameters()), ("fingerprint", request.Fingerprint));
            await insert.ExecuteNonQueryAsync(cancellationToken);
        }
        await using (var receipt = StoreCommand(c, t, $"INSERT INTO {_schema}.snapshot_requests(scope_id,id,snapshot_id,fingerprint) SELECT @scope,@request,@id,@fingerprint WHERE NOT EXISTS(SELECT 1 FROM {_schema}.snapshot_requests WHERE scope_id=@scope AND id=@request)",
            ("scope", scope), ("request", request.RequestId), ("id", request.Id), ("fingerprint", request.Fingerprint)))
            await receipt.ExecuteNonQueryAsync(cancellationToken);
        var progress = (await ReadSnapshotAsync(c, t, request.Definition, request.Id, cancellationToken))!;
        await PostgreSqlProtocol.CommitAsync(t, request.RequestId, cancellationToken); return progress;
    }

    private async Task SaveSnapshotAsync(NpgsqlConnection c, NpgsqlTransaction t, SnapshotProgress p, CancellationToken ct)
    {
        await using var command = StoreCommand(c, t, $"""
            UPDATE {_schema}.snapshots SET state=@state,generation=@generation,page_no=@page,item_count=@count,batched_items=@published,batch_count=@batches,
                source_view=@view,source_cursor=@source_cursor,paused=@paused,owner=@owner,lease_until=@lease,version=@version,updated_at=clock_timestamp()
            WHERE scope_id=@scope AND id=@id
            """, ("state", (int)p.State), ("generation", p.Generation), ("page", p.Page), ("count", p.ItemCount), ("published", p.BatchedItems), ("batches", p.BatchCount),
            ("source_cursor", p.Cursor), ("paused", p.Paused), ("version", p.Version), ("scope", p.Request.Definition.Target.Scope.Id), ("id", p.Request.Id));
        PostgreSqlProtocol.Optional(command, "view", NpgsqlDbType.Text, p.View);
        PostgreSqlProtocol.Optional(command, "owner", NpgsqlDbType.Uuid, p.Owner);
        PostgreSqlProtocol.Optional(command, "lease", NpgsqlDbType.TimestampTz, p.LeaseUntil);
        await command.ExecuteNonQueryAsync(ct);
    }
    private async Task<SnapshotProgress> LockedSnapshotAsync(NpgsqlConnection c, NpgsqlTransaction t, SnapshotRequest request, CancellationToken ct)
    {
        await PostgreSqlProtocol.LockAsync(c, t, "snapshot:" + _schema + ":" + request.Definition.Target.Scope.Id + ":" + request.Id, ct);
        var row = await ReadSnapshotAsync(c, t, request.Definition, request.Id, ct) ?? throw new InvalidOperationException("Snapshot not found.");
        if (row.Request.Fingerprint != request.Fingerprint) throw new SnapshotConflictException();
        return row;
    }
    private async Task<SnapshotProgress> OwnedSnapshotAsync(NpgsqlConnection c, NpgsqlTransaction t, SnapshotLease lease, CancellationToken ct)
    {
        var row = await LockedSnapshotAsync(c, t, lease.Request, ct);
        if (row.Owner != lease.Token || row.LeaseUntil <= await PostgreSqlProtocol.NowAsync(c, t, ct) || row.Paused || row.State == SnapshotState.Cancelled)
            throw new SnapshotLeaseLostException();
        return row;
    }
    public async Task<SnapshotLease?> ClaimSnapshotAsync(SnapshotRequest request, Guid owner, TimeSpan duration, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (owner == Guid.Empty || duration < TimeSpan.FromSeconds(1) || duration > TimeSpan.FromHours(1)) throw new ArgumentException("Invalid snapshot lease.");
        await using var c = await _dataSource.OpenConnectionAsync(cancellationToken); await using var t = await c.BeginTransactionAsync(cancellationToken);
        var row = await LockedSnapshotAsync(c, t, request, cancellationToken);
        var now = await PostgreSqlProtocol.NowAsync(c, t, cancellationToken);
        if (row.Paused || row.State is SnapshotState.Cancelled or SnapshotState.Batched || row.Owner != owner && row.LeaseUntil > now) return null;
        row = row with { Owner = owner, LeaseUntil = now + duration, Version = checked(row.Version + 1) };
        await SaveSnapshotAsync(c, t, row, cancellationToken); await PostgreSqlProtocol.CommitAsync(t, owner, cancellationToken);
        return new(request, owner, duration);
    }
    public async Task RenewSnapshotAsync(SnapshotLease lease, CancellationToken cancellationToken = default)
    {
        await using var c = await _dataSource.OpenConnectionAsync(cancellationToken); await using var t = await c.BeginTransactionAsync(cancellationToken);
        var row = await OwnedSnapshotAsync(c, t, lease, cancellationToken);
        row = row with { LeaseUntil = await PostgreSqlProtocol.NowAsync(c, t, cancellationToken) + lease.Duration };
        await SaveSnapshotAsync(c, t, row, cancellationToken); await PostgreSqlProtocol.CommitAsync(t, lease.Token, cancellationToken);
    }
    public async Task<SnapshotProgress> BeginSnapshotAsync(SnapshotLease lease, string view, CancellationToken cancellationToken = default)
    {
        _ = new ProducerScope("view", view);
        await using var c = await _dataSource.OpenConnectionAsync(cancellationToken); await using var t = await c.BeginTransactionAsync(cancellationToken);
        var row = await OwnedSnapshotAsync(c, t, lease, cancellationToken);
        if (row.State is not (SnapshotState.Pending or SnapshotState.Capturing)) return row;
        if (row.State == SnapshotState.Capturing && row.View == view) return row;
        if (row.State == SnapshotState.Capturing && lease.Request.Definition.Consistency == SnapshotConsistency.ResumableView)
            throw new InvalidOperationException("Resumed source must preserve its original immutable view.");
        row = row with { State = SnapshotState.Capturing, Generation = checked(row.Generation + 1), Page = 0, ItemCount = 0, Cursor = [], View = view, Version = checked(row.Version + 1) };
        await SaveSnapshotAsync(c, t, row, cancellationToken); await PostgreSqlProtocol.CommitAsync(t, lease.Token, cancellationToken); return row;
    }
    public async Task<SnapshotProgress> AppendSnapshotAsync(SnapshotLease lease, long expectedGeneration, long expectedPage, SnapshotPage page, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(page);
        await using var c = await _dataSource.OpenConnectionAsync(cancellationToken); await using var t = await c.BeginTransactionAsync(cancellationToken);
        var row = await OwnedSnapshotAsync(c, t, lease, cancellationToken);
        if (row.Generation != expectedGeneration) throw new SnapshotLeaseLostException();
        var scope = lease.Request.Definition.Target.Scope.Id;
        await using (var prior = StoreCommand(c, t, $"SELECT fingerprint FROM {_schema}.snapshot_pages WHERE scope_id=@scope AND snapshot_id=@id AND generation=@generation AND page_no=@page",
            ("scope", scope), ("id", lease.Request.Id), ("generation", row.Generation), ("page", expectedPage)))
            if (await prior.ExecuteScalarAsync(cancellationToken) is string saved)
            { if (saved != page.Fingerprint) throw new SnapshotConflictException(); return row; }
        if (row.State != SnapshotState.Capturing || row.Page != expectedPage) throw new InvalidOperationException("Snapshot page is not current.");
        if (!page.IsLast && row.Cursor.AsSpan().SequenceEqual(page.CopyNextCursor())) throw new InvalidOperationException("A source page must advance its source_cursor.");
        var unique = page.Items.GroupBy(item => item.KeyHash, StringComparer.Ordinal).ToArray();
        if (unique.Any(group => group.Any(item => item.Key != group.First().Key || item.Fingerprint != group.First().Fingerprint))) throw new SnapshotConflictException();
        var added = await InsertSnapshotItemsAsync(c, t, row, unique.Select(group => group.First()).ToArray(), cancellationToken);
        row = row with { ItemCount = checked(row.ItemCount + added) };
        await using (var receipt = StoreCommand(c, t, $"INSERT INTO {_schema}.snapshot_pages(scope_id,snapshot_id,generation,page_no,fingerprint) VALUES(@scope,@id,@generation,@page,@fingerprint)",
            ("scope", scope), ("id", lease.Request.Id), ("generation", row.Generation), ("page", expectedPage), ("fingerprint", page.Fingerprint)))
            await receipt.ExecuteNonQueryAsync(cancellationToken);
        row = row with { Page = checked(row.Page + 1), Cursor = page.CopyNextCursor(), State = page.IsLast ? SnapshotState.Sealed : SnapshotState.Capturing, Version = checked(row.Version + 1) };
        await SaveSnapshotAsync(c, t, row, cancellationToken); await PostgreSqlProtocol.CommitAsync(t, lease.Token, cancellationToken); return row;
    }
    private async Task<IReadOnlyList<SnapshotBatchItem>> ReadSnapshotItemsAsync(NpgsqlConnection c, NpgsqlTransaction t, SnapshotProgress row, int limit, CancellationToken ct)
    {
        await using var command = StoreCommand(c, t, $"SELECT ordinal,item_key,fingerprint,payload,key_hash FROM {_schema}.snapshot_items WHERE scope_id=@scope AND snapshot_id=@id AND generation=@generation AND ordinal>@after ORDER BY ordinal LIMIT @limit",
            ("scope", row.Request.Definition.Target.Scope.Id), ("id", row.Request.Id), ("generation", row.Generation), ("after", row.BatchedItems), ("limit", limit));
        var items = new List<SnapshotBatchItem>(); await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var item = new SnapshotItem(reader.GetString(1), reader.GetFieldValue<byte[]>(3));
            if (item.Fingerprint != reader.GetString(2) || item.KeyHash != reader.GetString(4)) throw new InvalidOperationException("Snapshot item content is corrupt.");
            items.Add(new(reader.GetInt64(0), item));
        }
        return items;
    }
    public async Task<SnapshotBatchCandidate> ReadSnapshotBatchAsync(SnapshotLease lease, CancellationToken cancellationToken = default)
    {
        await using var c = await _dataSource.OpenConnectionAsync(cancellationToken); await using var t = await c.BeginTransactionAsync(cancellationToken);
        var row = await OwnedSnapshotAsync(c, t, lease, cancellationToken);
        if (row.State != SnapshotState.Sealed) throw new InvalidOperationException("Only a sealed snapshot can publish batches.");
        return new(row, await ReadSnapshotItemsAsync(c, t, row, row.Request.Definition.BatchSize, cancellationToken));
    }
    public async Task<SnapshotProgress> PublishSnapshotBatchAsync(SnapshotLease lease, SnapshotBatchCandidate candidate, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(candidate);
        await using var c = await _dataSource.OpenConnectionAsync(cancellationToken); await using var t = await c.BeginTransactionAsync(cancellationToken);
        var row = await OwnedSnapshotAsync(c, t, lease, cancellationToken);
        var def = row.Request.Definition; var scope = def.Target.Scope.Id;
        if (candidate.Progress.Request.Fingerprint != row.Request.Fingerprint || candidate.Progress.Generation != row.Generation || candidate.Items.Count > def.BatchSize)
            throw new SnapshotConflictException();
        // Repeated commit after a lost reply resolves against the immutable batch receipt.
        if (candidate.Items.Count > 0 && row.BatchCount > candidate.Progress.BatchCount)
        {
            var prior = SnapshotBatchCodec.Create(candidate.Progress, candidate.Items);
            await using var find = StoreCommand(c, t, $"SELECT fingerprint FROM {_schema}.snapshot_batches WHERE scope_id=@scope AND id=@id", ("scope", scope), ("id", prior.BatchId));
            if (await find.ExecuteScalarAsync(cancellationToken) as string != ContentFingerprint.FromBytes(SnapshotBatchCodec.Encode(prior)).Value) throw new SnapshotConflictException();
            return row;
        }
        if (row.State == SnapshotState.Batched && candidate.Items.Count == 0) return row;
        if (row.State != SnapshotState.Sealed || candidate.Progress.BatchedItems != row.BatchedItems || candidate.Progress.BatchCount != row.BatchCount)
            throw new InvalidOperationException("Batch cursor is not current.");
        if (candidate.Items.Count == 0)
        {
            if (row.ItemCount != row.BatchedItems) throw new InvalidOperationException("Snapshot still contains unpublished items.");
        }
        else
        {
            var saved = await ReadSnapshotItemsAsync(c, t, row, candidate.Items.Count, cancellationToken);
            if (saved.Count != candidate.Items.Count || saved.Where((item, index) => item.Ordinal != candidate.Items[index].Ordinal || item.Item.Key != candidate.Items[index].Item.Key || item.Item.Fingerprint != candidate.Items[index].Item.Fingerprint).Any())
                throw new SnapshotConflictException();
            var envelope = SnapshotBatchCodec.Create(row, saved); var bytes = SnapshotBatchCodec.Encode(envelope);
            if (def.Delivery == SnapshotDelivery.LocalJob)
            {
                var accepted = await SubmitJobWithinTransactionAsync(c, t, new(def.Target, envelope.BatchId, envelope.BusinessKey, envelope.Actor, envelope.Payload), cancellationToken);
                if (accepted.Status is not (JobSubmissionStatus.Accepted or JobSubmissionStatus.AlreadyAccepted)) throw new SnapshotConflictException();
            }
            else if (def.Delivery == SnapshotDelivery.Outbox)
            {
                var writer = new PostgreSqlOutboxWriter(c, t, _schema, null, null);
                await writer.EnqueueAsync(new(def.OutputScope, envelope.BatchId, envelope.BatchId, def.Route!, bytes), cancellationToken);
            }
            await using var receipt = StoreCommand(c, t, $"INSERT INTO {_schema}.snapshot_batches(scope_id,id,snapshot_id,sequence,delivery,first_ordinal,last_ordinal,payload,fingerprint) VALUES(@scope,@id,@snapshot,@sequence,@delivery,@first,@last,@payload,@fingerprint)",
                ("scope", scope), ("id", envelope.BatchId), ("snapshot", row.Request.Id), ("sequence", envelope.Sequence), ("delivery", (int)def.Delivery),
                ("first", saved[0].Ordinal), ("last", saved[^1].Ordinal), ("payload", bytes), ("fingerprint", ContentFingerprint.FromBytes(bytes).Value));
            await receipt.ExecuteNonQueryAsync(cancellationToken);
            row = row with { BatchedItems = saved[^1].Ordinal, BatchCount = envelope.Sequence };
        }
        row = row with { State = row.ItemCount == row.BatchedItems ? SnapshotState.Batched : SnapshotState.Sealed, Version = checked(row.Version + 1) };
        await SaveSnapshotAsync(c, t, row, cancellationToken); await PostgreSqlProtocol.CommitAsync(t, lease.Token, cancellationToken); return row;
    }
    public async Task<SnapshotProgress> ControlSnapshotAsync(SnapshotDefinition definition, Guid id, long expectedVersion, bool paused, bool cancel,
        string actor, string reason, CancellationToken cancellationToken = default)
    {
        _ = new ProducerScope(actor, reason);
        await using var c = await _dataSource.OpenConnectionAsync(cancellationToken); await using var t = await c.BeginTransactionAsync(cancellationToken);
        await PostgreSqlProtocol.LockAsync(c, t, "snapshot:" + _schema + ":" + definition.Target.Scope.Id + ":" + id, cancellationToken);
        var row = await ReadSnapshotAsync(c, t, definition, id, cancellationToken) ?? throw new InvalidOperationException("Snapshot not found.");
        if (row.Version != expectedVersion || row.State is SnapshotState.Batched or SnapshotState.Cancelled)
            throw new InvalidOperationException("Snapshot control version or state is no longer current.");
        row = row with { Paused = paused, State = cancel ? SnapshotState.Cancelled : row.State, Owner = null, LeaseUntil = null, Version = checked(row.Version + 1) };
        await SaveSnapshotAsync(c, t, row, cancellationToken);
        await using var audit = StoreCommand(c, t, $"INSERT INTO {_schema}.snapshot_controls(scope_id,snapshot_id,version,actor,reason,paused,cancelled) VALUES(@scope,@id,@version,@actor,@reason,@paused,@cancelled)",
            ("scope", definition.Target.Scope.Id), ("id", id), ("version", row.Version), ("actor", actor), ("reason", reason), ("paused", paused), ("cancelled", cancel));
        await audit.ExecuteNonQueryAsync(cancellationToken); await PostgreSqlProtocol.CommitAsync(t, Guid.NewGuid(), cancellationToken); return row;
    }
}
