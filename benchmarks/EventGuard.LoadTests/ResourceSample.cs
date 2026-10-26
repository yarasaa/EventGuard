using System.Diagnostics;
namespace EventGuard.LoadTests;

internal sealed class ResourceSample : IAsyncDisposable
{
    private readonly Process _process = Process.GetCurrentProcess();
    private readonly CancellationTokenSource _stop = new();
    private readonly Task _sample;
    private readonly long _allocated = GC.GetTotalAllocatedBytes();
    private readonly int[] _collections = Enumerable.Range(0,3).Select(GC.CollectionCount).ToArray();
    private readonly TimeSpan _cpu;
    private long _peak;
    public ResourceSample() { _cpu = _process.TotalProcessorTime; Sample(); _sample = ObserveAsync(); }
    private void Sample() { _process.Refresh(); _peak = Math.Max(_peak, _process.WorkingSet64); }
    private async Task ObserveAsync()
    {
        try { while (true) { await Task.Delay(100, _stop.Token); Sample(); } }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
    }
    public async Task<ProcessResources> FinishAsync()
    {
        _stop.Cancel(); await _sample; Sample();
        return new((_process.TotalProcessorTime-_cpu).TotalSeconds, GC.GetTotalAllocatedBytes()-_allocated,
            Enumerable.Range(0,3).Select(i=>GC.CollectionCount(i)-_collections[i]).ToArray(), _peak);
    }
    public async ValueTask DisposeAsync() { _stop.Cancel(); await _sample; _stop.Dispose(); _process.Dispose(); }
}
internal sealed record ProcessResources(double ClientCpuSeconds, long ClientAllocatedBytes, int[] ClientGcCollections, long ClientSampledPeakWorkingSetBytes);
