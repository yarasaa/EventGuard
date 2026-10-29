namespace EventGuard.Runtime;

/// <summary>Bounded database polling for snapshots, workflows and persisted reports.</summary>
public sealed class PollingWorkerOptions
{
    public int MaximumItems { get; }
    public TimeSpan PollInterval { get; }
    public TimeSpan SweepInterval { get; }

    public PollingWorkerOptions(int maximumItems = 100, TimeSpan? pollInterval = null, TimeSpan? sweepInterval = null)
    {
        // Native stores bound each listing/sweep to 1,000 rows.
        if (maximumItems is < 1 or > 1000) throw new ArgumentOutOfRangeException(nameof(maximumItems));
        MaximumItems = maximumItems;
        PollInterval = Check(pollInterval ?? TimeSpan.FromMilliseconds(250), nameof(pollInterval));
        SweepInterval = Check(sweepInterval ?? TimeSpan.FromSeconds(1), nameof(sweepInterval));
    }

    private static TimeSpan Check(TimeSpan value, string name) => value > TimeSpan.Zero
        && value <= TimeSpan.FromMilliseconds(uint.MaxValue - 1) ? value : throw new ArgumentOutOfRangeException(name);
}
