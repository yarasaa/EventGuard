using EventGuard.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EventGuard.Hosting.Tests;

public sealed class ExternalResultBatchHostingTests
{
    [Fact]
    public async Task Hosted_result_batch_uses_named_configuration_and_stops_cleanly()
    {
        var store=new Store();var builder=Host.CreateApplicationBuilder();builder.Logging.ClearProviders();
        builder.Services.AddEventGuard(ConfigurationTests.Json("""
            {"EventGuard":{"Workers":{"projection":{"ResultBatch":{"MaximumItems":250,"MaximumBytes":16777216},
              "Worker":{"MaximumConcurrency":1,"IdleDelay":"00:00:00.005"}}}}}
            """)).AddExternalResultBatch<object>("projection",new(new("tenant","producer"),"status","v1"),
                (_,_,_)=>ValueTask.CompletedTask,storeFactory:_=>store);
        using var host=builder.Build();await host.StartAsync();
        var options=await store.Called.Task.WaitAsync(TimeSpan.FromSeconds(5));await host.StopAsync();
        Assert.Equal(250,options.MaximumItems);Assert.Equal(16777216,options.MaximumBytes);Assert.True(store.Validations>0);
    }
    private sealed class Store : IExternalResultBatchStore<object>
    {
        public TaskCompletionSource<ExternalResultBatchOptions> Called { get; }=new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int Validations;
        public Task ValidateExternalResultBatchAsync(CancellationToken ct=default) { Validations++;return Task.CompletedTask; }
        public Task<ExternalResultBatchExecution> ExecuteResultBatchAsync(ExternalResultTarget target,ExternalResultBatchOptions options,
            Func<IReadOnlyList<ExternalActionUpdate>,object,CancellationToken,ValueTask> handler,RetryPolicy retry,CancellationToken ct=default)
        { Called.TrySetResult(options);return Task.FromResult(new ExternalResultBatchExecution([],0,0)); }
    }
}
