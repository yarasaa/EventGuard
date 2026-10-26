using System.Data;
using System.Data.Common;
using System.Transactions;
using Microsoft.EntityFrameworkCore;

namespace EventGuard.EntityFrameworkCore;

/// <summary>Runs EF changes inside the connection and transaction owned by an EventGuard session.</summary>
public static class EventGuardEfCore
{
    /// <summary>
    /// Use a fresh context per handler attempt, configured with the session's existing connection and
    /// without connection ownership or an EF retry strategy. The callback must not commit, roll back,
    /// replace or dispose the transaction/connection. EventGuard owns the final commit and durable retry.
    /// Dispose the context after this call, including on failure; do not reuse its change tracker.
    /// </summary>
    public static async ValueTask ExecuteAsync<TContext>(TContext context, DbTransaction transaction,
        Func<TContext, CancellationToken, ValueTask> action, CancellationToken cancellationToken = default)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(action);
        cancellationToken.ThrowIfCancellationRequested();
        _ = transaction.IsolationLevel; // Some drivers retain Connection after commit; also check transaction readiness.
        if (!context.Database.IsRelational()) Reject("ef-core-relational-provider-required");
        var connection = transaction.Connection;
        if (connection is null || connection.State != ConnectionState.Open) Reject("ef-core-open-transaction-required");
        if (!ReferenceEquals(context.Database.GetDbConnection(), connection)) Reject("ef-core-connection-mismatch");
        if (context.Database.CurrentTransaction is not null || System.Transactions.Transaction.Current is not null
            || context.Database.GetEnlistedTransaction() is not null) Reject("ef-core-transaction-already-bound");
        if (context.ChangeTracker.Entries().Any()) Reject("ef-core-fresh-context-required");
        if (context.Database.CreateExecutionStrategy().RetriesOnFailure) Reject("ef-core-retry-strategy-not-supported");

        // UseTransaction wraps an external transaction without transferring commit/disposal ownership.
        await using var binding = await context.Database.UseTransactionAsync(transaction, cancellationToken);
        await action(context, cancellationToken);
        _ = transaction.IsolationLevel;
        if (!ReferenceEquals(context.Database.CurrentTransaction, binding)
            || !ReferenceEquals(transaction.Connection, connection)) Reject("ef-core-transaction-replaced");
        await context.SaveChangesAsync(acceptAllChangesOnSuccess: false, cancellationToken);
    }

    private static void Reject(string reason) => throw new PermanentWorkException(reason);
}
