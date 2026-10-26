using System.Diagnostics;
namespace EventGuard.LoadTests;

// Fixed memory, upper-bound quantiles in microseconds with 32 bins per power of two (<2.2% rounding).
internal sealed class LatencyHistogram
{
    private readonly long[] _bins = new long[2048];
    private long _count, _sumMicroseconds;
    public void Record(long started) => AddMilliseconds(Stopwatch.GetElapsedTime(started).TotalMilliseconds);
    public void AddMilliseconds(double milliseconds)
    {
        if (!double.IsFinite(milliseconds) || milliseconds < 0 || milliseconds > 86_400_000) throw new ArgumentOutOfRangeException(nameof(milliseconds));
        var microseconds = Math.Max(1, (long)Math.Ceiling(milliseconds * 1000));
        var bin = (int)Math.Ceiling(Math.Log2(microseconds) * 32);
        Interlocked.Increment(ref _bins[bin]); Interlocked.Increment(ref _count); Interlocked.Add(ref _sumMicroseconds, microseconds);
    }
    public Latencies Snapshot()
    {
        var count = Interlocked.Read(ref _count);
        double? Percentile(double rank)
        {
            if (count == 0) return null;
            var target = (long)Math.Ceiling(count * rank); long seen = 0;
            for (var i = 0; i < _bins.Length; i++) if ((seen += _bins[i]) >= target) return Math.Pow(2, i / 32d) / 1000d;
            throw new InvalidOperationException("Read histograms only after the measured workers stop.");
        }
        return new(count, count == 0 ? null : _sumMicroseconds / (double)count / 1000d, Percentile(.5), Percentile(.95), Percentile(.99));
    }
}
internal sealed record Latencies(long Samples, double? MeanMs, double? P50UpperMs, double? P95UpperMs, double? P99UpperMs);
