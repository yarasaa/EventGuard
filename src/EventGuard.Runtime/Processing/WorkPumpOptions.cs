namespace EventGuard.Runtime;

public sealed class WorkPumpOptions
{
    public StoreStartupOptions Startup { get; }
    public int MaximumConcurrency { get; }
    public TimeSpan LeaseDuration { get; }
    public TimeSpan IdleDelay { get; }
    public TimeSpan FaultDelay { get; }

    public WorkPumpOptions(int maximumConcurrency = 4, TimeSpan? leaseDuration = null,
        TimeSpan? idleDelay = null, TimeSpan? faultDelay = null)
        : this(new StoreStartupOptions(), maximumConcurrency, leaseDuration, idleDelay, faultDelay) { }

    public WorkPumpOptions(StoreStartupOptions startup, int maximumConcurrency = 4, TimeSpan? leaseDuration = null,
        TimeSpan? idleDelay = null, TimeSpan? faultDelay = null)
    {
        Startup = startup ?? throw new ArgumentNullException(nameof(startup));
        if (maximumConcurrency < 1 || maximumConcurrency > 1024) throw new ArgumentOutOfRangeException(nameof(maximumConcurrency));
        MaximumConcurrency = maximumConcurrency;
        LeaseDuration = leaseDuration ?? TimeSpan.FromSeconds(30);
        IdleDelay = idleDelay ?? TimeSpan.FromMilliseconds(250);
        FaultDelay = faultDelay ?? TimeSpan.FromSeconds(1);
        if (LeaseDuration <= TimeSpan.Zero || LeaseDuration > TimeSpan.FromHours(1)) throw new ArgumentOutOfRangeException(nameof(leaseDuration));
        if (IdleDelay <= TimeSpan.Zero || IdleDelay > TimeSpan.FromMinutes(1)) throw new ArgumentOutOfRangeException(nameof(idleDelay));
        if (FaultDelay <= TimeSpan.Zero || FaultDelay > TimeSpan.FromMinutes(1)) throw new ArgumentOutOfRangeException(nameof(faultDelay));
    }
}
