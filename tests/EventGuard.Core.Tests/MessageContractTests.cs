using System.Text.Json;
namespace EventGuard.Core.Tests;

public sealed class MessageContractTests
{
    private sealed record Current(Guid Id, string Resource, long Revision);
    private static VersionedMessageContract<Current> Contract() => new("resource.changed", 2,
        bytes => { using var doc = JsonDocument.Parse(bytes); return new(doc.RootElement.GetProperty("type").GetString()!, doc.RootElement.GetProperty("version").GetInt32()); },
        new Dictionary<int, Func<ReadOnlyMemory<byte>, Current>>
        {
            [1] = bytes => { using var doc = JsonDocument.Parse(bytes); var d = doc.RootElement; return new(d.GetProperty("id").GetGuid(), d.GetProperty("path").GetString()!, d.GetProperty("revision").GetInt64()); },
            [2] = bytes => { using var doc = JsonDocument.Parse(bytes); var d = doc.RootElement; return new(d.GetProperty("id").GetGuid(), d.GetProperty("resource").GetString()!, d.GetProperty("revision").GetInt64()); }
        }, value => { if (value.Id == Guid.Empty || string.IsNullOrWhiteSpace(value.Resource) || value.Revision < 0) throw new IngressMessageRejectedException("invalid-resource"); });
    private static byte[] Body(Guid id, int version = 1, string type = "resource.changed", long revision = 18) => JsonSerializer.SerializeToUtf8Bytes(new { id, version, type, path = "a.txt", resource = "a.txt", revision });

    [Fact]
    public void Old_and_current_contracts_map_to_one_model_without_rewriting_original_input()
    {
        var contract = Contract(); var id = Guid.NewGuid(); var old = Body(id); var current = Body(id, 2);
        Assert.Equal(contract.Decode(old), contract.Decode(current));
        var scope = new WorkScope("tenant", "consumer", "any-broker", "resource.changed");
        var submission = contract.Map(scope, new("event-id/v1", "raw/v1"), old, value => (value.Id.ToString("D"), value.Id));
        old[0] = 0;
        Assert.Equal(Body(id), submission.CopyPayload());
        Assert.Equal(ContentFingerprint.FromBytes(Body(id)), submission.Fingerprint);
        Assert.Equal(new[] { 1, 2 }, contract.SupportedVersions);
    }

    [Theory]
    [InlineData(3, "resource.changed", 18, "unsupported-contract-version")]
    [InlineData(1, "unknown", 18, "wrong-contract")]
    [InlineData(1, "resource.changed", -1, "invalid-resource")]
    public void Incompatible_and_invalid_data_is_explicitly_rejected(int version, string name, long revision, string reason)
        => Assert.Equal(reason, Assert.Throws<IngressMessageRejectedException>(() => Contract().Decode(Body(Guid.NewGuid(), version, name, revision))).ReasonCode);

    [Fact]
    public void Decoder_configuration_is_frozen_and_requires_explicit_current_version()
    {
        var decoders = new Dictionary<int, Func<ReadOnlyMemory<byte>, string>> { [1] = _ => "old" };
        Assert.Throws<ArgumentException>(() => new VersionedMessageContract<string>("event", 2, _ => new("event", 1), decoders, _ => { }));
        var contract = new VersionedMessageContract<string>("event", 1, _ => new("event", 1), decoders, _ => { });
        decoders[1] = _ => "changed";
        Assert.Equal("old", contract.Decode(Array.Empty<byte>()));
    }

    [Fact]
    public void Unexpected_codec_failures_are_not_misclassified_as_permanent_data_rejection()
    {
        var contract = new VersionedMessageContract<string>("event", 1, _ => new("event", 1),
            new Dictionary<int, Func<ReadOnlyMemory<byte>, string>> { [1] = _ => throw new IOException("dependency unavailable") }, _ => { });
        Assert.Throws<IOException>(() => contract.Decode(Array.Empty<byte>()));
    }

    [Fact]
    public void A_converter_cannot_change_the_durable_bytes_during_mapping()
    {
        var id = Guid.NewGuid();
        var contract = new VersionedMessageContract<Guid>("event", 1, _ => new("event", 1),
            new Dictionary<int, Func<ReadOnlyMemory<byte>, Guid>> { [1] = bytes => { System.Runtime.InteropServices.MemoryMarshal.TryGetArray(bytes, out var segment); segment.Array![0] = 9; return id; } }, _ => { });
        var submission = contract.Map(new("tenant", "consumer", "source", "event"), new("v1", "bytes/v1"), new byte[] { 1 }, value => (value.ToString("D"), value));
        Assert.Equal(new byte[] { 1 }, submission.CopyPayload());
    }

    [Fact]
    public void Rejected_contract_is_permanent_in_the_execution_retry_policy()
        => Assert.Equal(FailureKind.Permanent, new RetryPolicy().Classify(new IngressMessageRejectedException("unsupported-contract-version")));
}
