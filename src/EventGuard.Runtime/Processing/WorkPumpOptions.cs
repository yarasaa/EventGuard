namespace EventGuard.Runtime;

public sealed class WorkPumpOptions
{
    public StoreStartupOptions Startup { get; }
    public int MaximumConcurrency { get; }
    public TimeSpan LeaseDuration { get; }
    public TimeSpan IdleDelay { get; }
    public TimeSpan FaultDelay { get; }
    private TimeSpan _startupTimeout = TimeSpan.FromSeconds(5);
    private TimeSpan _settlementTimeout = TimeSpan.FromSeconds(10);
    private TimeSpan? _externalCallTimeout;
    public TimeSpan StartupTimeout { get => _startupTimeout; init => _startupTimeout = Timeout(value, nameof(StartupTimeout)); }
    public TimeSpan SettlementTimeout { get => _settlementTimeout; init => _settlementTimeout = Timeout(value, nameof(SettlementTimeout)); }
    public TimeSpan? ExternalCallTimeout { get => _externalCallTimeout; init => _externalCallTimeout = value is { } duration ? Timeout(duration, nameof(ExternalCallTimeout)) : null; }

    private static TimeSpan Timeout(TimeSpan value, string name) => value > TimeSpan.Zero
        && value <= TimeSpan.FromMilliseconds(uint.MaxValue - 1) ? value : throw new ArgumentOutOfRangeException(name);

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
