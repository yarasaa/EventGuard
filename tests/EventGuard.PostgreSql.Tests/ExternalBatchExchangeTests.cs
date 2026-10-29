using Npgsql;
using EventGuard.TestSupport;
using System.Diagnostics;
using Xunit.Abstractions;

namespace EventGuard.PostgreSql.Tests;

public sealed class ExternalBatchExchangeTests(DatabaseFixture db, ITestOutputHelper output) : IClassFixture<DatabaseFixture>
{
    [Theory]
    [InlineData(100, false)]
    [InlineData(250, false)]
    [InlineData(100, true)]
    [InlineData(250, true)]
    public async Task Native_storage_exchanges_are_bounded_independently_of_item_count(int count, bool projections)
    {
        await db.Store.InitializeExternalActionsAsync();
        await using var proxy = new DatabaseExchangeCounter(new NpgsqlConnectionStringBuilder(db.ConnectionString).Port, postgres: true);
        var settings = new NpgsqlConnectionStringBuilder(db.ConnectionString) { Host = "127.0.0.1", Port = proxy.Port, SslMode = SslMode.Disable, Pooling = true, MaxPoolSize = 1 };
        settings.DisableGssEncryptionForFaultProxy();
        await using var source = NpgsqlDataSource.Create(settings.ConnectionString);
        var measured = new PostgreSqlWorkStore(source, db.Schema);
        await measured.ValidateExternalActionsAsync(); // Establish the pooled transport outside the measurement.
        var samples = new List<object>();
        for (var round = 0; round < 4; round++)
        {
            var native = round % 2 == 0;
            var scope = new ProducerScope("exchange-test", Guid.NewGuid().ToString("N"));
            var target = new ExternalResultTarget(scope, "application-status", "v1");
            var actions = Enumerable.Range(0, count).Select(_ => new ExternalAction(scope, Guid.NewGuid(), "service", "v1", new byte[1024])).ToArray();
            await db.Outbox.ProduceAsync(new(scope, Guid.NewGuid(), ContentFingerprint.FromBytes([1])), async (session, ct) =>
            { foreach (var a in actions) { if (projections) await session.ExternalActions.EnqueueAsync(a, target, ct); else await session.ExternalActions.EnqueueAsync(a, ct); } });
            var exchanges = proxy.Exchanges; var started = Stopwatch.GetTimestamp();
            var claims = new List<ExternalActionClaim>();
            var result = new ExternalResult(ExternalResultKind.Accepted, "accepted", "reference", new byte[64]);
            if (native)
            {
                claims.AddRange(await measured.ClaimActionsAsync(new(scope, "service", "v1", ExternalPhase.Submit,
                    Guid.NewGuid(), TimeSpan.FromMinutes(2), count, 8 * 1024 * 1024)));
                await measured.SettleActionsAsync(new(claims.Select(c => new ExternalBatchSettlementItem(c, result)), new()));
            }
            else
            {
                for (var i = 0; i < count; i++) claims.Add((await measured.ClaimActionAsync(scope, "service", "v1", ExternalPhase.Submit,
                    Guid.NewGuid(), TimeSpan.FromMinutes(2)))!);
                foreach (var claim in claims) await measured.SettleActionAsync(claim, result, new());
            }
            var elapsed = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
            var total = proxy.Exchanges - exchanges;
            Assert.Equal(count, claims.Count);
            if (native) Assert.InRange(total, 1, 40); else Assert.True(total > count * 10);
            foreach (var a in actions) Assert.Equal(ExternalActionState.Accepted, (await db.Store.FindActionAsync(scope, a.Id))!.State);
            samples.Add(new { round, native, count, projections, protocolExchanges = total, elapsedMs = elapsed });
        }
        output.WriteLine(System.Text.Json.JsonSerializer.Serialize(new { provider = "PostgreSql", payloadBytes = 1024, resultBytes = 64,
            remoteServiceMeasured = false, samples }));
    }
}
