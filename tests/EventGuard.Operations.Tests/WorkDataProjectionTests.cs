using System.Text;

namespace EventGuard.Operations.Tests;

public sealed class WorkDataProjectionTests
{
    [Fact]
    public void Projection_handles_missing_nested_non_scalar_escaped_and_long_fields_without_exposing_neighbors()
    {
        var json = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object?>
        {
            ["a/b"] = new Dictionary<string, object?> { ["~key"] = "selected" },
            ["object"] = new { secret = "do-not-display" }, ["array"] = new[] { "do-not-display" },
            ["text"] = new string('x', 255) + "😀tail", ["nil"] = null, ["flag"] = true
        });
        var selection = new WorkDataSelection([
            new("Escaped", "/a~1b/~0key"), new("Object", "/object"), new("Array", "/array"),
            new("Missing", "/missing/value"), new("Long", "/text"), new("Null", "/nil"), new("Flag", "/flag")]);
        var data = WorkDataProjection.FromJson(json, selection);
        Assert.Equal(WorkDataStatus.Available, data.Status);
        Assert.Equal("selected", data.Fields[0].Value);
        Assert.Equal(WorkDataValueStatus.NonScalar, data.Fields[1].Status);
        Assert.Equal(WorkDataValueStatus.NonScalar, data.Fields[2].Status);
        Assert.Equal(WorkDataValueStatus.Missing, data.Fields[3].Status);
        Assert.Equal(WorkDataValueStatus.Truncated, data.Fields[4].Status);
        Assert.Equal(new string('x', 255) + "…", data.Fields[4].Value);
        Assert.Equal("null", data.Fields[5].Value);
        Assert.Equal("true", data.Fields[6].Value);
        Assert.DoesNotContain("do-not-display", System.Text.Json.JsonSerializer.Serialize(data));
    }

    [Fact]
    public void Excessive_json_depth_is_rejected_without_a_raw_fallback()
    {
        var bytes = Encoding.UTF8.GetBytes(string.Concat(Enumerable.Repeat("{\"nested\":", 40)) + "\"secret\"" + new string('}', 40));
        var data = WorkDataProjection.FromJson(bytes, new([new("Value", "/nested")]));
        Assert.Equal(WorkDataStatus.InvalidJson, data.Status);
        Assert.Empty(data.Fields);
    }

    [Theory]
    [InlineData("")]
    [InlineData("no-root-slash")]
    [InlineData("/bad~2escape")]
    [InlineData("/bad~")]
    [InlineData("/1/2/3/4/5/6/7/8/9")]
    public void Invalid_field_paths_are_rejected(string pointer) =>
        Assert.Throws<ArgumentException>(() => new WorkDataSelection([new("Label", pointer)]));

    [Fact]
    public void Both_page_directions_cannot_be_selected_together()
    {
        var point = new WorkPosition(DateTimeOffset.UtcNow, Guid.NewGuid());
        Assert.Throws<ArgumentException>(() => new WorkQuery(new("tenant", "consumer", "source", "event"), point.CreatedAt, before: point, after: point));
    }
}
