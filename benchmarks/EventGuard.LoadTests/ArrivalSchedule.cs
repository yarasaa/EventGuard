using System.Diagnostics;
using System.Threading.Channels;
namespace EventGuard.LoadTests;

// One producer owns the cursor. A full buffer never moves the planned arrival times.
internal sealed class ArrivalSchedule(int count,int rate,int initial,int maximumLagMs,long start)
{
    private long _considered, _enqueued, _bufferRejected, _scheduleMissed;
    public long Considered => Interlocked.Read(ref _considered);
    public long Enqueued => Interlocked.Read(ref _enqueued);
    public long BufferRejected => Interlocked.Read(ref _bufferRejected);
    public long ScheduleMissed => Interlocked.Read(ref _scheduleMissed);
    public LatencyHistogram DispatchDelay { get; } = new();
    public long Due(int arrivalIndex) => checked(start + (long)Math.Ceiling(arrivalIndex*(double)Stopwatch.Frequency/rate));
    public long CompletionDue(int ordinal) => ordinal < initial ? 0 : Due(ordinal-initial);

    // At most 256 slots per dispatcher tick; resample the clock/yield before the next batch.
    internal void OfferDue(long now,ChannelWriter<int> writer)
    {
        for (var batch=0; batch<256 && _considered<count && Due((int)_considered)<=now; batch++)
        {
            var index=(int)_considered;
            var lag=Stopwatch.GetElapsedTime(Due(index),now).TotalMilliseconds;
            DispatchDelay.AddMilliseconds(lag);
            if (lag>maximumLagMs) Interlocked.Increment(ref _scheduleMissed);
            else if (writer.TryWrite(initial+index)) Interlocked.Increment(ref _enqueued);
            else Interlocked.Increment(ref _bufferRejected);
            Interlocked.Increment(ref _considered);
        }
    }
    public async Task RunAsync(ChannelWriter<int> writer,CancellationToken ct)
    {
        try
        {
            while (Considered<count)
            {
                ct.ThrowIfCancellationRequested();
                OfferDue(Stopwatch.GetTimestamp(),writer);
                if (Considered<count) await WaitUntilAsync(Due((int)Considered),ct);
            }
            // The planned window is count/rate, including the final interval.
            await WaitUntilAsync(Due(count),ct);
        }
        finally { writer.TryComplete(); }
    }
    private static async Task WaitUntilAsync(long due,CancellationToken ct)
    {
        while (Stopwatch.GetTimestamp()<due)
        {
            var remaining=Stopwatch.GetElapsedTime(Stopwatch.GetTimestamp(),due).TotalMilliseconds;
            if (remaining>0) await Task.Delay(TimeSpan.FromMilliseconds(Math.Max(1,Math.Ceiling(remaining))),ct);
        }
        // A late dispatcher must let bounded admission workers run instead of spinning to catch up.
        await Task.Yield(); ct.ThrowIfCancellationRequested();
    }
}
