using System.Globalization;

namespace EventGuard;

/// <summary>Timestamped request IDs allow stores to retire receipts without resurrecting old requests.</summary>
public static class ExternalCallRequestIds
{
    public static Guid New()
    {
        var value = Guid.NewGuid().ToString("N").ToCharArray();
        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString("x12", CultureInfo.InvariantCulture).CopyTo(0, value, 0, 12);
        value[12] = '7';
        return Guid.ParseExact(new string(value), "N");
    }

    public static DateTimeOffset? Timestamp(Guid id)
    {
        var value = id.ToString("N");
        if (value[12] != '7' || value[16] is not ('8' or '9' or 'a' or 'b')) return null;
        var milliseconds = long.Parse(value.AsSpan(0, 12), NumberStyles.HexNumber, CultureInfo.InvariantCulture);
        return milliseconds <= DateTimeOffset.MaxValue.ToUnixTimeMilliseconds()
            ? DateTimeOffset.FromUnixTimeMilliseconds(milliseconds) : throw new ArgumentException("Invalid permit request timestamp.", nameof(id));
    }
}

public interface IExternalActionAvailability
{
    /// <summary>Read-only hint, not a lease. Quota must still be acquired before claiming any action.</summary>
    Task<bool> HasDueActionsAsync(ProducerScope scope, string binding, string bindingVersion, ExternalPhase phase,
        CancellationToken cancellationToken = default);
}

public interface IExternalCallPermitMaintenance
{
    /// <summary>Retire expired timestamped receipts in bounded pages. Legacy random IDs remain retained.</summary>
    Task<int> PruneCallPermitsAsync(ExternalCallLimit limit, TimeSpan retention, int maximumItems = 1000,
        CancellationToken cancellationToken = default);
}

public sealed class ExternalCallPermitRetiredException(Guid id)
    : InvalidOperationException($"Call permit {id} is outside the retained evidence window.");
