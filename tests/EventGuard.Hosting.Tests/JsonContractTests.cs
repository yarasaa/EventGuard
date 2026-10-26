using System.Text;
using System.Text.Json;
using EventGuard.Runtime;

namespace EventGuard.Hosting.Tests;

public sealed class JsonContractTests
{
    private static readonly WorkScope Scope = new("test", "products", "catalog", "product.changed");
    private static readonly IdentityPolicy Policy = new("message-id/v1", "raw-json/v1");
    private static JsonMessageContract<Message> Contract(JsonSerializerOptions? options = null) =>
        new(Scope.EventType, 1, m => m.Version, m =>
        {
            if (m.Id == Guid.Empty || string.IsNullOrWhiteSpace(m.Name)) throw new IngressMessageRejectedException("invalid-model");
        }, options);

    [Fact]
    public void Round_trip_preserves_type_identity_and_original_wire_bytes()
    {
        var contract = Contract();
        var model = new Message(Guid.NewGuid(), 1, "Book");
        var bytes = contract.Encode(model);
        Assert.Equal(model, contract.Decode(bytes));
        var submission = contract.Map(Scope, Policy, bytes, m => (m.Id.ToString("D"), m.Id));
        var original = bytes.ToArray();
        bytes[0] = 0;
        Assert.Equal(original, submission.CopyPayload());
        Assert.Equal(ContentFingerprint.FromBytes(original), submission.Fingerprint);
        Assert.Equal(model.Id, submission.MessageId);
    }

    [Theory]
    [InlineData("null")]
    [InlineData("{")]
    [InlineData("{}")]
    [InlineData("{\"version\":99}")]
    [InlineData("[]")]
    public void Invalid_missing_or_future_contracts_are_permanent_rejections(string json)
    {
        var error = Assert.Throws<IngressMessageRejectedException>(() => Contract().Decode(Encoding.UTF8.GetBytes(json)));
        Assert.Equal(FailureKind.Permanent, new RetryPolicy().Classify(error));
    }

    [Fact]
    public void Validation_runs_on_both_typed_and_raw_input()
    {
        var bad = new Message(Guid.NewGuid(), 1, "");
        Assert.Throws<IngressMessageRejectedException>(() => Contract().Encode(bad));
        Assert.Throws<IngressMessageRejectedException>(() => Contract().Decode(JsonSerializer.SerializeToUtf8Bytes(bad, new JsonSerializerOptions(JsonSerializerDefaults.Web))));
    }

    [Fact]
    public void Payload_size_is_bounded_for_input_and_output()
    {
        Assert.Throws<IngressMessageRejectedException>(() => Contract().Decode(new byte[1024 * 1024 + 1]));
        Assert.Throws<IngressMessageRejectedException>(() => Contract().Encode(new(Guid.NewGuid(), 1, new string('x', 1024 * 1024))));
    }

    [Fact]
    public void Caller_cannot_mutate_serializer_settings_after_registration()
    {
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web);
        var contract = Contract(options);
        options.PropertyNamingPolicy = null;
        Assert.Contains("\"version\"", Encoding.UTF8.GetString(contract.Encode(new(Guid.NewGuid(), 1, "Book"))));
    }

    [Fact]
    public void Semantically_equal_but_different_wire_bytes_keep_different_fingerprints()
    {
        var contract = Contract();
        var a = contract.Encode(new(Guid.NewGuid(), 1, "Book"));
        var b = Encoding.UTF8.GetBytes(" " + Encoding.UTF8.GetString(a));
        var first = contract.Map(Scope, Policy, a, m => (m.Id.ToString("D"), m.Id));
        var second = contract.Map(Scope, Policy, b, m => (m.Id.ToString("D"), m.Id));
        Assert.Equal(first.Identity.Id, second.Identity.Id);
        Assert.NotEqual(first.Fingerprint, second.Fingerprint);
    }

    public sealed record Message(Guid Id, int Version, string Name);
}
