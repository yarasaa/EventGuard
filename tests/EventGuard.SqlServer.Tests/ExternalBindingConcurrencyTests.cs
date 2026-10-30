using System.Data;
using System.Reflection;
using Microsoft.Data.SqlClient;

namespace EventGuard.SqlServer.Tests;

public sealed class ExternalBindingConcurrencyTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    [Fact]
    public async Task Existing_result_binding_remains_concurrent_for_the_full_transaction_lifetime()
    {
        await db.Store.InitializeExternalActionsAsync();
        var target = new ExternalResultTarget(new("binding", Guid.NewGuid().ToString("N")), "status", "v1");
        var identity = new WorkIdentity(target.WorkScope, target.IdentityPolicy, "key");
        var identities = new Dictionary<string, WorkIdentity> { [identity.Scope.Id] = identity };
        var method = typeof(SqlServerWorkStore).GetMethod("EnsureExternalBatchBindingsAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        Task Ensure(SqlConnection c, SqlTransaction t, CancellationToken ct) => (Task)method.Invoke(db.Store, [c, t, identities, ct])!;
        await using var a = await db.DataSource.OpenConnectionAsync();
        await using var b = await db.DataSource.OpenConnectionAsync();
        await using (var warm = (SqlTransaction)await a.BeginTransactionAsync(IsolationLevel.RepeatableRead))
        { await Ensure(a, warm, default); await warm.CommitAsync(); }
        await using var first = (SqlTransaction)await a.BeginTransactionAsync(IsolationLevel.RepeatableRead);
        await using var second = (SqlTransaction)await b.BeginTransactionAsync(IsolationLevel.RepeatableRead);
        await Ensure(a, first, default);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await Ensure(b, second, deadline.Token); // must finish while the first transaction still holds its locks
        await first.RollbackAsync(); await second.RollbackAsync();
    }
}
