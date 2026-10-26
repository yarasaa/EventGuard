using System.Data.Common;
using EventGuard.Store.Conformance;
using Microsoft.EntityFrameworkCore;

namespace EventGuard.EntityFrameworkCore.Tests;

public sealed class TransactionTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Ef_changes_outbox_and_completed_commit_together_and_context_does_not_own_session(bool sql)
    {
        var outgoing = Message();
        var result = await Run(sql, async (work, connection, transaction, outbox, ct) =>
        {
            await using (var context = Context(sql, connection))
            {
                await EventGuardEfCore.ExecuteAsync(context, transaction, async (ef, token) =>
                {
                    ef.Add(new Effect { WorkId = work.Id, Note = "ef-effect" });
                    await outbox.EnqueueAsync(outgoing, token);
                }, ct);
                Assert.Null(context.Database.CurrentTransaction);
            }
            Assert.Same(connection, transaction.Connection);
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "SELECT 1";
            Assert.Equal(1, await command.ExecuteScalarAsync(ct));
        });
        Assert.Equal(ExecutionStatus.Completed, result.Status);
        Assert.Equal(1, await Count(sql, result.Work.Id));
        Assert.NotNull(await Outbox(sql).FindAsync(outgoing.Scope, outgoing.IntentId));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Exception_after_explicit_save_rolls_back_ef_and_outbox_and_preserves_failure(bool sql)
    {
        var outgoing = Message();
        var result = await Run(sql, async (work, connection, transaction, outbox, ct) =>
        {
            await using var context = Context(sql, connection);
            await EventGuardEfCore.ExecuteAsync(context, transaction, async (ef, token) =>
            {
                ef.Add(new Effect { WorkId = work.Id, Note = "must-roll-back" });
                await ef.SaveChangesAsync(token);
                await outbox.EnqueueAsync(outgoing, token);
                throw new PermanentWorkException("test-failure");
            }, ct);
        });
        Assert.Equal(ExecutionStatus.DeadLetter, result.Status);
        Assert.Equal(0, await Count(sql, result.Work.Id));
        Assert.Null(await Outbox(sql).FindAsync(outgoing.Scope, outgoing.IntentId));
    }

    [Theory]
    [InlineData(false, "connection-mismatch")]
    [InlineData(true, "connection-mismatch")]
    [InlineData(false, "fresh-context-required")]
    [InlineData(true, "fresh-context-required")]
    [InlineData(false, "retry-strategy-not-supported")]
    [InlineData(true, "retry-strategy-not-supported")]
    public async Task Unsafe_configuration_is_rejected_before_the_callback_runs(bool sql, string reason)
    {
        var called = false;
        var result = await Run(sql, async (work, connection, transaction, _, ct) =>
        {
            await using DbConnection? other = reason == "connection-mismatch"
                ? sql ? new Microsoft.Data.SqlClient.SqlConnection(db.Sql.ConnectionString) : new Npgsql.NpgsqlConnection(db.Postgres.ConnectionString)
                : null;
            await using var context = Context(sql, other ?? connection, reason == "retry-strategy-not-supported");
            if (reason == "fresh-context-required") context.Add(new Effect { WorkId = work.Id, Note = "unrelated" });
            var error = await Assert.ThrowsAsync<PermanentWorkException>(async () =>
                await EventGuardEfCore.ExecuteAsync(context, transaction, (_, _) => { called = true; return ValueTask.CompletedTask; }, ct));
            Assert.Equal("ef-core-" + reason, error.Message);
            throw error;
        });
        Assert.False(called);
        Assert.Equal(ExecutionStatus.DeadLetter, result.Status);
        Assert.Equal(0, await Count(sql, result.Work.Id));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Cancellation_after_save_does_not_commit_business_changes(bool sql)
    {
        var result = await Run(sql, async (work, connection, transaction, _, ct) =>
        {
            await using var context = Context(sql, connection);
            using var cancelled = CancellationTokenSource.CreateLinkedTokenSource(ct);
            await EventGuardEfCore.ExecuteAsync(context, transaction, async (ef, token) =>
            {
                ef.Add(new Effect { WorkId = work.Id, Note = "cancelled" });
                await ef.SaveChangesAsync(token);
                cancelled.Cancel();
                token.ThrowIfCancellationRequested();
            }, cancelled.Token);
        });
        Assert.NotEqual(ExecutionStatus.Completed, result.Status);
        Assert.Equal(0, await Count(sql, result.Work.Id));
    }

    private EffectContext Context(bool sql, DbConnection connection, bool retries = false)
    {
        var options = new DbContextOptionsBuilder<EffectContext>();
        if (sql) options.UseSqlServer(connection, o => { if (retries) o.EnableRetryOnFailure(); });
        else options.UseNpgsql(connection, o => { if (retries) o.EnableRetryOnFailure(); });
        return new(options.Options, sql ? db.Sql.Schema : db.Postgres.Schema);
    }

    private async Task<ExecutionResult> Run(bool sql, Func<WorkItem, DbConnection, DbTransaction, IOutboxWriter, CancellationToken, ValueTask> handler)
    {
        var scope = StoreContractData.Scope();
        IWorkStore store = sql ? db.Sql.Store : db.Postgres.Store;
        await store.AcceptAsync(StoreContractData.Submission(scope));
        var claim = (await store.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromSeconds(30)))!;
        Exception? observed = null;
        async ValueTask Observe(WorkItem work, DbConnection connection, DbTransaction transaction, IOutboxWriter outbox, CancellationToken ct)
        {
            try { await handler(work, connection, transaction, outbox, ct); }
            catch (Exception error) { observed = error; throw; }
        }
        var result = sql
            ? await db.Sql.Store.ExecuteAsync(claim, (w, _, s, ct) => Observe(w, s.Connection, s.Transaction, s.Outbox, ct), new())
            : await db.Postgres.Store.ExecuteAsync(claim, (w, _, s, ct) => Observe(w, s.Connection, s.Transaction, s.Outbox, ct), new());
        if (observed is not null and not PermanentWorkException and not OperationCanceledException)
            throw new InvalidOperationException("Unexpected test callback failure.", observed);
        return result;
    }
    private Task<long> Count(bool sql, Guid id) => sql ? db.Sql.EffectCount(id) : db.Postgres.EffectCount(id);
    private IOutboxStore Outbox(bool sql) => sql ? db.Sql.Outbox : db.Postgres.Outbox;
    private static OutgoingMessage Message() => new(new("tenant", Guid.NewGuid().ToString("N")), Guid.NewGuid(), Guid.NewGuid(),
        new("test", "local", "v1", "events"), "{}"u8);
}

public sealed class DatabaseFixture : IAsyncLifetime
{
    public PostgreSql.Tests.DatabaseFixture Postgres { get; } = new();
    public SqlServer.Tests.DatabaseFixture Sql { get; } = new();
    public async Task InitializeAsync() { await Postgres.InitializeAsync(); await Sql.InitializeAsync(); }
    public async Task DisposeAsync() { await Sql.DisposeAsync(); await Postgres.DisposeAsync(); }
}

public sealed class EffectContext(DbContextOptions<EffectContext> options, string schema) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        var effect = modelBuilder.Entity<Effect>();
        effect.ToTable("business_effects", schema);
        effect.HasKey(e => e.WorkId);
        effect.Property(e => e.WorkId).HasColumnName("work_id").ValueGeneratedNever();
        effect.Property(e => e.Note).HasColumnName("note");
    }
}

public sealed class Effect
{
    public Guid WorkId { get; set; }
    public string Note { get; set; } = "";
}
