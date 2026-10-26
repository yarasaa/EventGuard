using System.Globalization;
using EventGuard.Operations;

namespace EventGuard.Dashboard;

public static class FilterInput
{
    public static WorkFilters Parse(string? from, string? until, string? workId, string? minimumAttempts, string? replayed)
    {
        Guid? id = null;
        int? attempts = null;
        bool? replay = null;
        if (!string.IsNullOrEmpty(workId))
        {
            if (!Guid.TryParseExact(workId, "D", out var parsed) || parsed == Guid.Empty) throw new InvalidQueryException();
            id = parsed;
        }
        if (!string.IsNullOrEmpty(minimumAttempts))
        {
            if (!int.TryParse(minimumAttempts, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed)) throw new InvalidQueryException();
            attempts = parsed;
        }
        if (!string.IsNullOrEmpty(replayed)) replay = replayed switch { "yes" => true, "no" => false, _ => throw new InvalidQueryException() };
        return new(Date(from), Date(until), id, attempts, replay);
    }

    private static DateTimeOffset? Date(string? value)
    {
        if (string.IsNullOrEmpty(value)) return null;
        return DateTimeOffset.TryParseExact(value, ["yyyy-MM-dd'T'HH:mm", "yyyy-MM-dd'T'HH:mm:ss"],
            CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsed)
            ? parsed : throw new InvalidQueryException();
    }

    public static Dictionary<string, string> Routes(string scope, WorkState? state, WorkFilters filters) => new()
    {
        ["scope"] = scope, ["state"] = state?.ToString() ?? "",
        ["from"] = Time(filters.CreatedFrom), ["until"] = Time(filters.CreatedUntil),
        ["workId"] = filters.WorkId?.ToString("D") ?? "", ["minimumAttempts"] = filters.MinimumAttempts?.ToString(CultureInfo.InvariantCulture) ?? "",
        ["replayed"] = filters.Replayed is { } replayed ? replayed ? "yes" : "no" : ""
    };

    public static string Time(DateTimeOffset? value) => value?.ToUniversalTime().ToString("yyyy-MM-dd'T'HH:mm:ss", CultureInfo.InvariantCulture) ?? "";
}
