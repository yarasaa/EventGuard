namespace EventGuard.Core.Tests;

public sealed class ExternalBatchStorageContractsTests
{
    private static ExternalActionClaim Claim(string tenant = "t") => new(
        new(new(new(tenant, "p"), Guid.NewGuid(), "service", "v1", [1]), ExternalActionState.Sending,
            ExternalObservationState.None, 1, 1, DateTimeOffset.UtcNow, null, null, null, []),
        ExternalPhase.Submit, Guid.NewGuid(), DateTimeOffset.UtcNow.AddMinutes(1));

    [Fact]
    public void Storage_requests_bind_all_semantics_and_enforce_page_bounds()
    {
        ExternalBatchClaimRequest Request(int count = 100, int bytes = 1024, string tenant = "t") =>
            new(new(tenant, "p"), "service", "v1", ExternalPhase.Submit, Guid.NewGuid(), TimeSpan.FromMinutes(1), count, bytes);
        Assert.Equal(Request().Fingerprint, Request().Fingerprint);
        Assert.NotEqual(Request().Fingerprint, Request(101).Fingerprint);
        Assert.NotEqual(Request().Fingerprint, Request(bytes: 2048).Fingerprint);
        Assert.NotEqual(Request().Fingerprint, Request(tenant: "other").Fingerprint);
        Assert.Throws<ArgumentOutOfRangeException>(() => Request(1001));
        Assert.Throws<ArgumentOutOfRangeException>(() => Request(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => Request(bytes: 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => Request(bytes: 64 * 1024 * 1024 + 1));
    }

    [Fact]
    public void Settlement_rejects_mixed_scope_duplicate_tokens_and_unbounded_enumeration()
    {
        var a = Claim(); var b = Claim(); var result = new ExternalResult(ExternalResultKind.Accepted, "accepted");
        Assert.Throws<ArgumentException>(() => new ExternalBatchSettlementRequest([], new()));
        Assert.Throws<ArgumentException>(() => new ExternalBatchSettlementRequest([new(a, result), new(Claim("other"), result)], new()));
        Assert.Throws<ArgumentException>(() => new ExternalBatchSettlementRequest([new(a, result), new(b with { Token = a.Token }, result)], new()));
        var read = 0;
        IEnumerable<ExternalBatchSettlementItem> Infinite()
        {
            while (true) { read++; yield return new(Claim(), result); }
        }
        Assert.Throws<ArgumentException>(() => new ExternalBatchSettlementRequest(Infinite(), new()));
        Assert.Equal(1001, read);
    }
}
