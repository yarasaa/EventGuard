using System.Globalization;
using System.Text;

namespace EventGuard.Core.Tests;

public sealed class IdentityTests
{
    [Fact]
    public void Length_framing_prevents_delimiter_ambiguity()
    {
        Assert.NotEqual(StableHash.Of("a|b", "c"), StableHash.Of("a", "b|c"));
        Assert.NotEqual(StableHash.Of("ab", "c"), StableHash.Of("a", "bc"));
        Assert.NotEqual(StableHash.Of(""), StableHash.Of());
    }

    [Fact]
    public void Key_equality_is_ordinal_and_culture_independent()
    {
        var previous = CultureInfo.CurrentCulture;
        try
        {
            var expected = new WorkScope("I", "files", "source", "created").Id;
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("tr-TR");
            Assert.Equal(expected, new WorkScope("I", "files", "source", "created").Id);
            Assert.NotEqual(expected, new WorkScope("ı", "files", "source", "created").Id);
        }
        finally { CultureInfo.CurrentCulture = previous; }
    }

    [Fact]
    public void Policy_change_does_not_create_a_new_work_identity()
    {
        var scope = new WorkScope("tenant", "files", "source", "created");
        Assert.Equal(new WorkIdentity(scope, new("v1", "v1"), "file-1").Id,
            new WorkIdentity(scope, new("v2", "v2"), "file-1").Id);
        Assert.NotEqual(new WorkIdentity(scope, new("v1", "v1"), "file-1").Id,
            new WorkIdentity(new("other", "files", "source", "created"), new("v1", "v1"), "file-1").Id);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void Empty_identity_is_rejected(string key) =>
        Assert.ThrowsAny<ArgumentException>(() => new WorkIdentity(new("t", "c", "s", "e"), new("v1", "v1"), key));

    [Fact]
    public void Invalid_unicode_is_rejected_instead_of_silently_replaced() =>
        Assert.Throws<EncoderFallbackException>(() => StableHash.Of("\ud800"));

    [Fact]
    public void Payload_is_snapshotted_and_fingerprint_is_explicit()
    {
        byte[] input = [1, 2, 3];
        var fingerprint = ContentFingerprint.FromBytes(input);
        var submission = new WorkSubmission(new(new("t", "c", "s", "e"), new("v1", "v1"), "k"), Guid.NewGuid(), input, fingerprint);
        input[0] = 9;
        var copy = submission.CopyPayload();
        copy[1] = 9;
        Assert.Equal(new byte[] { 1, 2, 3 }, submission.CopyPayload());
        Assert.Equal(fingerprint, ContentFingerprint.Parse(fingerprint.Value.ToLowerInvariant()));
    }

    [Fact]
    public void Schedule_cannot_expire_before_it_becomes_eligible()
    {
        var now = DateTimeOffset.UtcNow;
        Assert.Throws<ArgumentException>(() => new WorkSubmission(new(new("t", "c", "s", "e"), new("v1", "v1"), "k"),
            Guid.NewGuid(), [], ContentFingerprint.FromBytes([]), now, now));
    }

    [Fact]
    public void Replay_identity_binds_target_version_actor_and_reason()
    {
        var scope = new WorkScope("t", "c", "s", "e");
        var id = Guid.NewGuid(); var work = Guid.NewGuid();
        var request = new ReplayRequest(scope, id, work, 7, "operator", "fixed");
        Assert.Equal(request.Fingerprint, new ReplayRequest(scope, id, work, 7, "operator", "fixed").Fingerprint);
        Assert.NotEqual(request.Fingerprint, new ReplayRequest(scope, id, work, 8, "operator", "fixed").Fingerprint);
    }
}
