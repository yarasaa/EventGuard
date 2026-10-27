namespace EventGuard;

/// <summary>A polling interval selected by elapsed time since durable acceptance.</summary>
public sealed record ExternalObservationStep(TimeSpan After, TimeSpan Interval);

public sealed class ExternalObservationSchedule
{
    public IReadOnlyList<ExternalObservationStep> Steps { get; }

    public ExternalObservationSchedule(IEnumerable<ExternalObservationStep> steps)
    {
        ArgumentNullException.ThrowIfNull(steps);
        var copy = steps.Take(65).ToArray();
        if (copy.Length is < 1 or > 64 || copy.Any(s => s is null) || copy[0].After != TimeSpan.Zero)
            throw new ArgumentException("Supply 1–64 steps beginning at elapsed time zero.", nameof(steps));
        for (var i = 0; i < copy.Length; i++)
            if (copy[i].After < TimeSpan.Zero || copy[i].After > TimeSpan.FromDays(365)
                || copy[i].Interval < TimeSpan.FromMilliseconds(10) || copy[i].Interval > TimeSpan.FromDays(1)
                || i > 0 && copy[i].After <= copy[i - 1].After)
                throw new ArgumentException("Steps must have increasing elapsed times and intervals from 10 ms to one day.", nameof(steps));
        Steps = Array.AsReadOnly(copy);
    }

    public TimeSpan IntervalAt(TimeSpan elapsed)
    {
        if (elapsed < TimeSpan.Zero) elapsed = TimeSpan.Zero;
        return Steps.Last(s => s.After <= elapsed).Interval;
    }
}
