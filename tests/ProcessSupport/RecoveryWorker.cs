using System.Text.Json;
using EventGuard;

namespace EventGuard.ProcessSupport;

internal static class RecoveryWorker
{
    public static async Task<int> RunAsync(IIngressQuarantineStore store, WorkScope scope)
    {
        Guid Id(string name) => Guid.Parse(Environment.GetEnvironmentVariable(name)!);
        var submission = new WorkSubmission(new(scope, new("v1", "sha256-bytes/v1"), "file-1"),
            Id("EVENTGUARD_TEST_MESSAGE"), [8], ContentFingerprint.FromBytes([8]));
        var request = new IngressReleaseRequest(Id("EVENTGUARD_TEST_REQUEST"), Id("EVENTGUARD_TEST_QUARANTINE"), 0,
            submission, "operator", "fixed");
        Console.WriteLine(JsonSerializer.Serialize(new { stage = "release-start", pid = Environment.ProcessId }));
        await Console.Out.FlushAsync();
        await store.ReleaseAsync(request);
        return 0;
    }

    public static async Task<int> RepairAsync(IOutboxRepairStore store, WorkScope scope)
    {
        Guid Id(string name) => Guid.Parse(Environment.GetEnvironmentVariable(name)!);
        var message = new OutgoingMessage(new(scope.Tenant, scope.Consumer), Id("EVENTGUARD_TEST_INTENT"), Id("EVENTGUARD_TEST_MESSAGE"),
            new("test", "local", "v1", "files.created"), [1, 2]);
        var request = new OutboxRepairRequest(Id("EVENTGUARD_TEST_REQUEST"), Id("EVENTGUARD_TEST_QUARANTINE"), message,
            long.Parse(Environment.GetEnvironmentVariable("EVENTGUARD_TEST_VERSION")!, System.Globalization.CultureInfo.InvariantCulture), "operator", "fixed");
        Console.WriteLine(JsonSerializer.Serialize(new { stage = "repair-start", pid = Environment.ProcessId }));
        await Console.Out.FlushAsync(); await store.RepairAsync(request); return 0;
    }

    public static async Task<int> BulkAsync(IBulkReplayStore store, WorkScope scope)
    {
        var items = JsonSerializer.Deserialize<BulkReplayItem[]>(Environment.GetEnvironmentVariable("EVENTGUARD_TEST_ITEMS")!)!;
        var request = new BulkReplayRequest(scope, Guid.Parse(Environment.GetEnvironmentVariable("EVENTGUARD_TEST_REQUEST")!), items, "operator", "reviewed selection");
        Console.WriteLine(JsonSerializer.Serialize(new { stage = "bulk-start", pid = Environment.ProcessId }));
        await Console.Out.FlushAsync(); await store.RunBulkAsync(request); return 0;
    }

    public static async Task<int> ClaimAsync<TSession>(ITransactionalWorkStore<TSession> store, WorkScope scope)
    {
        Console.WriteLine(JsonSerializer.Serialize(new { stage = "budget-claim-start", pid = Environment.ProcessId }));
        await Console.Out.FlushAsync();
        await store.ClaimNextAsync(scope, Guid.Parse(Environment.GetEnvironmentVariable("EVENTGUARD_TEST_REQUEST")!), TimeSpan.FromMinutes(1));
        return 0;
    }

    public static async Task<int> RetainAsync(IRetentionStore store, WorkScope scope)
    {
        var request = new RetentionRequest(scope, Guid.Parse(Environment.GetEnvironmentVariable("EVENTGUARD_TEST_REQUEST")!),
            RetentionTarget.ConsumerPayload, new(2021, 1, 1, 0, 0, 0, TimeSpan.Zero), "operator", "configured retention");
        Console.WriteLine(JsonSerializer.Serialize(new { stage = "retention-start", pid = Environment.ProcessId }));
        await Console.Out.FlushAsync(); await store.RetainAsync(request); return 0;
    }
}
