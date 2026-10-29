using System.Globalization;
using System.Reflection;
using System.Runtime.CompilerServices;
using EventGuard.Runtime;
using Microsoft.Data.SqlClient;

namespace EventGuard.SqlServer.Tests;

public sealed class CancellationClassificationTests
{
    [Theory]
    [InlineData("en-US")]
    [InlineData("fr-FR")]
    [InlineData("tr-TR")]
    public void Packet_flush_cancellation_requires_the_cancelled_token_and_actual_driver_origin(string culture)
    {
        var previous = CultureInfo.CurrentUICulture;
        try
        {
            CultureInfo.CurrentUICulture = CultureInfo.GetCultureInfo(culture);
            // Deterministically take the driver's pre-send cancellation branch without a socket.
            // This intentionally probes the pinned driver; changes to its internals require review.
            var assembly = typeof(SqlConnection).Assembly;
            var stateType = assembly.GetType("Microsoft.Data.SqlClient.TdsParserStateObject", throwOnError: true)!;
            var concrete = assembly.GetTypes().First(t => !t.IsAbstract && t.IsSubclassOf(stateType));
            var state = RuntimeHelpers.GetUninitializedObject(concrete);
            const BindingFlags flags = BindingFlags.Instance | BindingFlags.NonPublic;
            stateType.GetField("_cancelled", flags)!.SetValue(state, true);
            var packetNumber = stateType.GetField("_outputPacketNumber", flags)!;
            packetNumber.SetValue(state, Convert.ChangeType(1, packetNumber.FieldType, CultureInfo.InvariantCulture));
            var thrown = Assert.Throws<TargetInvocationException>(() => stateType.GetMethod("ExecuteFlush", flags)!.Invoke(state, null));
            var error = Assert.IsType<InvalidOperationException>(thrown.InnerException);
            using var stop = new CancellationTokenSource();
            Assert.False(SqlServerProtocol.IsCommandCancellation(error, stop.Token));
            stop.Cancel();
            Assert.True(SqlServerProtocol.IsCommandCancellation(error, stop.Token));
            Assert.False(SqlServerProtocol.IsCommandCancellation(new InvalidOperationException(error.Message), stop.Token));
            Assert.False(SqlServerProtocol.IsCommandCancellation(new CommitOutcomeUnknownException(Guid.NewGuid(), error), stop.Token));
        }
        finally { CultureInfo.CurrentUICulture = previous; }
    }
}

public sealed class CancellationTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    [Theory]
    [InlineData("nonquery")]
    [InlineData("scalar")]
    [InlineData("reader")]
    public async Task Cancelled_commands_roll_back_uncommitted_effects_and_leave_connection_usable(string kind)
    {
        await using var connection = await db.DataSource.OpenConnectionAsync();
        var id = Guid.NewGuid();
        await using (var transaction = await connection.BeginLocalAsync(CancellationToken.None))
        {
            await using var command = new SqlCommand($"INSERT INTO {db.Schema}.business_effects VALUES(@id,'cancelled')", connection, transaction);
            command.Parameters.AddWithValue("id", id);
            await command.ExecuteNonQueryCheckedAsync(CancellationToken.None);
            command.CommandText = "WAITFOR DELAY '00:00:10'; SELECT 1";
            command.Parameters.Clear();
            using var stop = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            {
                if (kind == "nonquery") await command.ExecuteNonQueryCheckedAsync(stop.Token);
                else if (kind == "scalar") await command.ExecuteScalarCheckedAsync(stop.Token);
                else { await using var reader = await command.ExecuteReaderCheckedAsync(stop.Token); }
            });
            await transaction.RollbackAsync();
        }
        Assert.Equal(0, await db.EffectCount(id));
        await using var healthy = new SqlCommand("SELECT 1", connection);
        Assert.Equal(1, await healthy.ExecuteScalarCheckedAsync(CancellationToken.None));
        healthy.CommandText = "THROW 51020, 'synthetic database failure', 1";
        var error = await Assert.ThrowsAsync<SqlException>(() => healthy.ExecuteNonQueryCheckedAsync(CancellationToken.None));
        Assert.Equal(51020, error.Number);
    }

    [Fact]
    public async Task Idle_worker_shutdown_at_different_poll_boundaries_is_not_a_host_fault()
    {
        var scope = DatabaseFixture.Scope();
        var pump = new WorkPump<SqlServerSession>(db.Store, scope,
            (_, _, _, _) => throw new InvalidOperationException("An empty queue has no handler calls."),
            options: new(maximumConcurrency: 2, idleDelay: TimeSpan.FromMilliseconds(1)));
        for (var i = 0; i < 20; i++)
        {
            using var stop = new CancellationTokenSource();
            var running = pump.RunAsync(stop.Token);
            await Task.Delay(2 + i % 5);
            stop.Cancel();
            // Startup may still be in flight and legitimately propagates cancellation to its caller.
            try { await running.WaitAsync(TimeSpan.FromSeconds(5)); }
            catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
        }
    }
}
