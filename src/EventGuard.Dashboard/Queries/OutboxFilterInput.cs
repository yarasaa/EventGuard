using EventGuard.Operations;

namespace EventGuard.Dashboard;

public static class OutboxFilterInput
{
    public static OutboxFilters Parse(string? from, string? until, string? intentId, string? minimumAttempts, string? replayed, string? uncertain)
    {
        var common = FilterInput.Parse(from, until, intentId, minimumAttempts, replayed);
        bool? delivery = string.IsNullOrEmpty(uncertain) ? null
            : uncertain switch { "yes" => true, "no" => false, _ => throw new InvalidQueryException() };
        return new(common.CreatedFrom, common.CreatedUntil, common.WorkId, common.MinimumAttempts, common.Replayed, delivery);
    }

    public static string Location(string scope, OutboxState? state, OutboxFilters filters, string? cursor = null, bool quarantine = false)
    {
        var values = FilterInput.Routes(scope, null, new(filters.CreatedFrom, filters.CreatedUntil, null, filters.MinimumAttempts, filters.Replayed));
        values.Remove("workId");
        values["intentId"] = filters.IntentId?.ToString("D") ?? "";
        values["state"] = state?.ToString() ?? "";
        values["uncertain"] = filters.DeliveryUncertain is { } uncertain ? uncertain ? "yes" : "no" : "";
        if (cursor is not null) values["cursor"] = cursor;
        return (quarantine ? "/quarantine" : "/outbox")
            + QueryString.Create(values.Select(p => new KeyValuePair<string, string?>(p.Key, p.Value)));
    }
}
