using System.Text;
using EventGuard.Runtime;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventGuard.Hosting.Tests;

public sealed class ConfigurationTests
{
    internal static IConfigurationRoot Json(string text) => new ConfigurationBuilder()
        .AddJsonStream(new MemoryStream(Encoding.UTF8.GetBytes(text))).Build();
    private static EventGuardConfiguration Read(string json) => new(Json(json).GetSection("EventGuard"));

    [Fact]
    public void Result_pages_and_permit_retention_can_be_configured_without_changing_handlers()
    {
        var settings = Read("""
            {"EventGuard":{"Defaults":{"ResultBatch":{"MaximumItems":250,"MaximumBytes":16777216}},
              "Workers":{"projection":{"ResultBatch":{"ExecutionTimeout":"00:00:12","FailureRecordingTimeout":"00:00:03"},
              "Worker":{"ExternalPermitRetention":"disabled","ExternalPermitCleanupInterval":"00:01:00"}}}}}
            """);
        var batch = settings.GetResultBatchOptions("projection");
        Assert.Equal(250,batch.MaximumItems); Assert.Equal(16777216,batch.MaximumBytes);
        Assert.Equal(TimeSpan.FromSeconds(12),batch.ExecutionTimeout);
        Assert.Equal(TimeSpan.FromSeconds(3),batch.FailureRecordingTimeout);
        Assert.Null(settings.GetWorkerOptions("projection").ExternalPermitRetention);
        Assert.Equal(TimeSpan.FromMinutes(1),settings.GetWorkerOptions("projection").ExternalPermitCleanupInterval);
    }

    [Fact]
    public void Named_settings_override_common_defaults_and_preserve_code_capabilities()
    {
        var settings = Read("""
            {"EventGuard":{"Defaults":{"Worker":{"MaximumConcurrency":8,"LeaseDuration":"00:00:40"},
                "Batch":{"MaximumBatchSize":500,"MaximumPayloadBytes":16777216,"RequireNativeStorage":true},"Retry":{"MaximumAttempts":7}},
                "Workers":{"sender":{"Worker":{"MaximumConcurrency":2},"Batch":{"CollectionWindow":"00:00:03"}}}}}
            """);
        var startup = new StoreStartupOptions();
        var original = new WorkPumpOptions(startup, 3, idleDelay: TimeSpan.FromMilliseconds(17))
            { SettlementTimeout = TimeSpan.FromSeconds(12) };
        var batch = settings.GetBatchOptions("SENDER", new(25, original));
        Assert.Equal(500, batch.MaximumBatchSize);
        Assert.Equal(16777216, batch.MaximumPayloadBytes);
        Assert.True(batch.RequireNativeStorage);
        Assert.Equal(TimeSpan.FromSeconds(3), batch.CollectionWindow);
        Assert.Equal(2, batch.Pump.MaximumConcurrency);
        Assert.Equal(TimeSpan.FromSeconds(40), batch.Pump.LeaseDuration);
        Assert.Equal(TimeSpan.FromMilliseconds(17), batch.Pump.IdleDelay);
        Assert.Equal(TimeSpan.FromSeconds(12), batch.Pump.SettlementTimeout);
        Assert.Same(startup, batch.Pump.Startup);
        var retry = new RetryPolicy(maximumAttempts: 2);
        Assert.Same(retry.Classify, settings.GetRetryPolicy("sender", retry).Classify);
        Assert.Equal(7, settings.GetRetryPolicy("sender", retry).MaximumAttempts);
        Assert.Equal(8, settings.GetWorkerOptions("another").MaximumConcurrency);
    }

    [Fact]
    public void Environment_override_uses_double_underscore_and_configuration_is_frozen_at_registration()
    {
        var prefix = "EVENTGUARD_TEST_" + Guid.NewGuid().ToString("N") + "_";
        var variable = prefix + "EventGuard__Workers__sender__Batch__MaximumBatchSize";
        Environment.SetEnvironmentVariable(variable, "750");
        try
        {
            var root = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
                { ["EventGuard:Workers:sender:Batch:MaximumBatchSize"] = "250" }).AddEnvironmentVariables(prefix).Build();
            var services = new ServiceCollection();
            var settings = services.AddEventGuard(root).Configuration!;
            Assert.Equal(750, settings.GetBatchOptions("sender").MaximumBatchSize);
            root["EventGuard:Workers:sender:Batch:MaximumBatchSize"] = "999";
            Assert.Equal(750, settings.GetBatchOptions("sender").MaximumBatchSize);
            Assert.Same(settings, services.AddEventGuard().Configuration);
        }
        finally { Environment.SetEnvironmentVariable(variable, null); }
    }

    [Fact]
    public void Observation_mode_is_replaced_as_a_unit_and_limit_fields_inherit()
    {
        var settings = Read("""
            {"EventGuard":{"Defaults":{"External":{"ObservationSchedule":[{"After":"00:00:00","Interval":"00:02:00"},
              {"After":"01:00:00","Interval":"00:15:00"}]},"CallLimit":{"Namespace":"shared","Endpoint":"service","MaximumCalls":10}},
              "Workers":{"sender":{"External":{"ObservationInterval":"00:05:00"},"CallLimit":{"MaximumCalls":20}},
                "reports":{"External":{"ObservationSchedule":[{"After":"00:00:00","Interval":"00:10:00"}]}},
                "free":{"CallLimit":{"Enabled":false}}}}}
            """);
        Assert.Null(settings.GetExternalPolicy("sender").ObservationSchedule);
        Assert.Equal(TimeSpan.FromMinutes(5), settings.GetExternalPolicy("sender").ObservationInterval);
        Assert.Single(settings.GetExternalPolicy("reports").ObservationSchedule!.Steps);
        Assert.Equal(2, settings.GetExternalPolicy("other").ObservationSchedule!.Steps.Count);
        var limit = settings.GetCallLimit("sender")!;
        Assert.Equal("shared", limit.Namespace); Assert.Equal(20, limit.MaximumCalls);
        Assert.Null(settings.GetCallLimit("free"));
        var partial = Read("""{"EventGuard":{"Workers":{"sender":{"CallLimit":{"MaximumCalls":50}}}}}""");
        Assert.Equal("account", partial.GetCallLimit("sender", new("account", "service", 10, TimeSpan.FromSeconds(1)))!.Namespace);
    }

    [Fact]
    public void Lowercase_configuration_supports_phases_timeouts_and_polling()
    {
        var settings = Read("""
            {"eventguard":{"workers":{"reports":{"enabled":false,"phases":["Observe"],
              "worker":{"startuptimeout":"00:00:09","settlementtimeout":"00:00:12","externalcalltimeout":"00:00:02"},
              "polling":{"maximumitems":250,"pollinterval":"00:00:02","sweepinterval":"00:00:03"}}}}}
            """);
        Assert.False(settings.IsEnabled("reports"));
        Assert.Equal(ExternalPhase.Observe, Assert.Single(settings.GetPhases("reports")!));
        var worker = settings.GetWorkerOptions("reports");
        Assert.Equal(TimeSpan.FromSeconds(9), worker.StartupTimeout);
        Assert.Equal(TimeSpan.FromSeconds(12), worker.SettlementTimeout);
        Assert.Equal(TimeSpan.FromSeconds(2), worker.ExternalCallTimeout);
        var polling = settings.GetPollingOptions("reports");
        Assert.Equal(250, polling.MaximumItems);
        Assert.Equal(TimeSpan.FromSeconds(2), polling.PollInterval);
        Assert.Equal(TimeSpan.FromSeconds(3), polling.SweepInterval);
    }

    [Theory]
    [InlineData("{\"Worker\":{\"MaxConcurrency\":4}}", "MaxConcurrency")]
    [InlineData("{\"Worker\":{\"MaximumConcurrency\":\"Password=hidden-value\"}}", "MaximumConcurrency")]
    [InlineData("{\"Batch\":{\"MaximumBatchSize\":{\"Secret\":\"hidden-value\"}}}", "MaximumBatchSize")]
    [InlineData("{\"Enabled\":\"hidden-value\"}", "Enabled")]
    [InlineData("{\"External\":{\"ObservationInterval\":\"00:01:00\",\"ObservationSchedule\":[{\"After\":\"00:00:00\",\"Interval\":\"00:02:00\"}]}}", "External")]
    [InlineData("{\"Phases\":{\"1\":\"Submit\"}}", "Phases")]
    [InlineData("{\"Phases\":[\"hidden-value\"]}", "Phases")]
    public void Invalid_shape_or_type_fails_without_disclosing_configured_values(string profile, string path)
    {
        var error = Assert.Throws<EventGuardConfigurationException>(() => Read("{\"EventGuard\":{\"Workers\":{\"sender\":" + profile + "}}}"));
        Assert.Contains(path, error.ConfigurationPath);
        Assert.DoesNotContain("hidden-value", error.ToString());
        Assert.Null(error.InnerException);
    }

    [Theory]
    [InlineData("Batch", "MaximumBatchSize", "0")]
    [InlineData("Worker", "LeaseDuration", "00:00:00")]
    [InlineData("Worker", "MaximumConcurrency", "0")]
    [InlineData("Worker", "SettlementTimeout", "00:00:00")]
    [InlineData("Retry", "JitterRatio", "NaN")]
    [InlineData("External", "MaximumSubmissionAttempts", "0")]
    [InlineData("Polling", "MaximumItems", "1001")]
    public void Invalid_ranges_are_reported_at_the_setting_path(string group, string field, string value)
    {
        var settings = Read(System.Text.Json.JsonSerializer.Serialize(new { EventGuard = new { Workers = new {
            sender = new Dictionary<string, object> { [group] = new Dictionary<string, string> { [field] = value } }
        } } }));
        var error = Assert.Throws<EventGuardConfigurationException>(() =>
        {
            switch (group)
            {
                case "Retry": settings.GetRetryPolicy("sender"); break;
                case "External": settings.GetExternalPolicy("sender"); break;
                case "Polling": settings.GetPollingOptions("sender"); break;
                default: settings.GetBatchOptions("sender"); break;
            }
        });
        Assert.Contains(field, error.ConfigurationPath);
    }

    [Fact]
    public async Task Typo_in_worker_name_fails_before_any_worker_starts()
    {
        var builder = Host.CreateApplicationBuilder(); builder.Logging.ClearProviders();
        var ran = false;
        builder.Services.AddEventGuard(Json("""{"EventGuard":{"Workers":{"sendre":{"Enabled":true}}}}"""))
            .AddWorker("sender", (_, _) => { ran = true; return Task.CompletedTask; });
        using var host = builder.Build();
        var error = await Assert.ThrowsAsync<EventGuardConfigurationException>(() => host.StartAsync());
        Assert.Contains("sendre", error.ConfigurationPath); Assert.False(ran);
    }

    [Fact]
    public async Task Disabling_shared_profile_stops_consumer_and_companion_without_resolving_dependencies()
    {
        var builder = Host.CreateApplicationBuilder(); builder.Logging.ClearProviders();
        var guard = builder.Services.AddEventGuard(Json("""
            {"Processing":{"ShutdownTimeout":"00:00:07","Workers":{"sender":{"Enabled":false}}}}
            """), "Processing");
        guard.AddExternalBatchService<ExternalBatchWorkerTests.Adapter>("sender", new("test", "host"));
        guard.AddWorker("sender.ingress", "sender", (_, _) => throw new InvalidOperationException("must not run"),
            (_, _) => throw new InvalidOperationException("must not resolve"));
        using var host = builder.Build();
        await host.StartAsync(); await host.StopAsync();
        Assert.Equal(TimeSpan.FromSeconds(7), host.Services.GetRequiredService<IOptions<HostOptions>>().Value.ShutdownTimeout);
        Assert.Null(host.Services.GetRequiredService<EventGuardHostStatus>().Failure);
    }

    [Fact]
    public void Duplicate_configured_registrations_and_case_variant_worker_names_are_rejected()
    {
        var services = new ServiceCollection(); var config = Json("{}");
        services.AddEventGuard(config).AddWorker("Sender", (_, _) => Task.CompletedTask);
        Assert.Throws<InvalidOperationException>(() => services.AddEventGuard(config));
        Assert.Throws<ArgumentException>(() => services.AddEventGuard().AddWorker("sender", (_, _) => Task.CompletedTask));
        var late = new ServiceCollection();
        late.AddEventGuard().AddWorker("sender", (_, _) => Task.CompletedTask);
        Assert.Throws<InvalidOperationException>(() => late.AddEventGuard(config));
    }

    [Fact]
    public async Task Custom_validation_honors_configured_startup_timeout()
    {
        var builder = Host.CreateApplicationBuilder(); builder.Logging.ClearProviders();
        var ran = false;
        builder.Services.AddEventGuard(Json("""{"EventGuard":{"Defaults":{"Worker":{"StartupTimeout":"00:00:00.080"}}}}"""))
            .AddWorker("sender", (_, _) => { ran = true; return Task.CompletedTask; }, (_, ct) => Task.Delay(Timeout.Infinite, ct));
        using var host = builder.Build();
        await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync().WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.False(ran);
    }
}
