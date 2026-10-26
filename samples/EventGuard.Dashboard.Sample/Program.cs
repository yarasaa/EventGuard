using EventGuard;
using EventGuard.Operations.PostgreSql;
using EventGuard.PostgreSql;
using Npgsql;

// Explicit local setup. The web host never creates schema/indexes or seeds sample records.
const string schema = "eventguard_dashboard_sample_v2";
const string connectionString = "Host=127.0.0.1;Port=55432;Database=eventguard_test;Username=eventguard;Password=eventguard-local-only;Maximum Pool Size=4";
await using var data = NpgsqlDataSource.Create(connectionString);
var store = new PostgreSqlWorkStore(data, schema);
await store.InitializeAsync();
await using (var indexes = data.CreateCommand(OperationsIndexes.CreateScript(schema) + OutboxReadIndexes.CreateScript(schema) + AuditIndexes.CreateScript(schema))) await indexes.ExecuteNonQueryAsync();
var scopes = new[]
{
    new WorkScope("demo", "document-indexer", "document-service", "document.updated"),
    new WorkScope("demo", "notification-sender", "notification-service", "notification.requested")
};
foreach (var scope in scopes)
{
    // Fixed sample identities make rerunning setup safe without duplicating the dataset.
    for (var i = 0; i < (scope.Consumer == "document-indexer" ? 32 : 6); i++)
    {
        var state = i % 6;
        var now = DateTimeOffset.UtcNow;
        var payload = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(new
        {
            reference = $"{(scope.Consumer == "document-indexer" ? "DOC" : "NTF")}-{i + 1:0000}",
            requestedAction = scope.Consumer == "document-indexer" ? "Update search index" : "Send notification",
            document = new { title = $"Example document {i + 1}", revision = 18 + i },
            notification = new { channel = "email", template = "document-updated" },
            internalToken = "demo-hidden-value"
        });
        var result = await store.AcceptAsync(new(new(scope, new("sample-key/v1", "body/v1"), "sample-" + i), Guid.NewGuid(),
            payload, ContentFingerprint.FromBytes(payload), notBefore: state == 0 ? now.AddDays(1) : null,
            expiresAt: state == 5 ? DateTimeOffset.Parse("2000-01-01T00:00:00Z") : null));
        if (result.Status != AcceptanceStatus.Accepted || state == 0) continue;
        var claim = (await store.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromHours(1)))!;
        if (state == 1) continue;
        var retry = new RetryPolicy(initialDelay: TimeSpan.FromHours(1), maximumDelay: TimeSpan.FromHours(1), jitterRatio: 0);
        await store.ExecuteAsync(claim, (_, _, _, _) => state switch
        {
            2 => throw new InvalidOperationException("Demonstration of a transient failure."),
            4 => throw new PermanentWorkException("demonstration"),
            _ => ValueTask.CompletedTask
        }, retry);
    }
}
// Raw ingress snapshots are explicit local examples, never real customer input.
foreach (var scope in scopes)
{
    await store.QuarantineAsync(scope, new("sample-key/v1", "body/v1"),
        new("dashboard-example/v1", scope.Consumer + "/unknown-contract", "sample-json/v1", "{\"schemaVersion\":99}"u8, "{}"u8), "contract-version-unsupported");
}
// These outcomes are simulations. No broker connection or external publication is made by the seed.
var outbox = new PostgreSqlOutboxStore(data, schema);
var recovery = new PostgreSqlOutboxRecoveryStore(data, schema);
foreach (var producer in new[] { "document-publisher", "notification-publisher" })
{
    var scope = new ProducerScope("demo", producer);
    for (var i = 0; i < (producer == "document-publisher" ? 32 : 6); i++)
    {
        var state = i % 7;
        Guid StableId(string kind) => new(System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes($"dashboard-outbox/v1/{producer}/{i}/{kind}"))[..16]);
        var payload = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(new { reference = $"OUT-{i + 1:0000}", requestedAction = "Publish document update", internalToken = "demo-hidden-value" });
        var message = new OutgoingMessage(scope, StableId("intent"), StableId("message"), new(i % 2 == 0 ? "kafka" : "rabbitmq", "document-events", "v1", "document.updated"), payload,
            notBefore: state == 0 ? DateTimeOffset.Parse("2050-01-01T00:00:00Z") : null);
        var produced = await outbox.ProduceAsync(new(scope, message.IntentId, ContentFingerprint.FromBytes(payload)),
            async (session, ct) => { await session.Outbox.EnqueueAsync(message, ct); });
        if (produced.Status != ProducerStatus.Committed || state == 0) continue;
        if (state == 5)
        {
            await using var corrupt = data.CreateCommand($"UPDATE {schema}.outbox_messages SET headers='null' WHERE scope_id=@scope AND id=@id");
            corrupt.Parameters.AddWithValue("scope", scope.Id); corrupt.Parameters.AddWithValue("id", message.IntentId);
            await corrupt.ExecuteNonQueryAsync();
        }
        var claim = await outbox.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromHours(1));
        if (claim is null || state == 1) continue;
        var outcome = state switch
        {
            2 => new PublishOutcome(PublishStatus.NotSentTransient, "sample-transient"),
            4 => new PublishOutcome(PublishStatus.NotSentPermanent, "sample-rejected"),
            6 => new PublishOutcome(PublishStatus.Unknown, "sample-unknown"),
            _ => new PublishOutcome(PublishStatus.Accepted, "sample-accepted")
        };
        var settled = await outbox.SettleAsync(claim, outcome, new(maximumAttempts: state == 6 ? 1 : 5,
            initialDelay: TimeSpan.FromHours(1), maximumDelay: TimeSpan.FromHours(1), jitterRatio: 0));
        if (state == 6)
        {
            await recovery.ReplayAsync(new(scope, StableId("replay"), message.IntentId, settled.Record.Version, "demo-operator", "Simulated recovery; no broker is connected.", true));
            var retry = await outbox.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromHours(1));
            if (retry is not null) await outbox.SettleAsync(retry, new(PublishStatus.Accepted, "sample-accepted"), new());
        }
    }
}
// Optional module examples use simulated outcomes only; this setup makes no external calls.
await store.InitializeSnapshotsAsync(); await store.InitializeExternalActionsAsync(); await store.InitializeWorkflowsAsync();
Guid ModuleId(string key) => new(Convert.FromHexString(StableHash.Of("dashboard-modules/v1", key)).AsSpan(0,16));
var jobScope = new JobScope("demo", "module-examples");
var batchJob = new JobDefinition(jobScope,"selection-batch",1,"demo-batch",1,"v1",[1],[JobCapabilities.TransactionalHandler]);
await store.RegisterAsync(batchJob);
var selection = new SnapshotDefinition(batchJob,"selection","demo-replica","v1");
for (var i=0;i<30;i++)
    await store.CreateSnapshotAsync(new(selection,ModuleId("selection-"+i),"selection-"+i,"demo-operator","{}"u8));
var flow = new WorkflowDefinition(jobScope,"approval",1,[new("approval",WorkflowStepKind.Signal),new("cooldown",WorkflowStepKind.Delay,[new("approval","approved")],delay:TimeSpan.FromMinutes(10))]);
var flowRequest = new WorkflowRequest(flow,ModuleId("flow"),"demo-flow","demo-operator",new Dictionary<string,byte[]>());
await store.CreateWorkflowAsync(flowRequest); await store.AdvanceWorkflowAsync(flow,flowRequest.Id);
var serviceScope = new ProducerScope("demo","demo-service");
for (var i=0;i<4;i++)
{
    var action = new ExternalAction(serviceScope,ModuleId("action-"+i),"demo-service","v1","{}"u8,TimeSpan.FromHours(1));
    await outbox.ProduceAsync(new(serviceScope,action.Id,ContentFingerprint.FromBytes([1])),async (session,ct)=>{await session.ExternalActions.EnqueueAsync(action,ct);});
}
for (var i=0;i<3;i++)
{
    var claim = await store.ClaimActionAsync(serviceScope,"demo-service","v1",ExternalPhase.Submit,ModuleId("claim-"+i),TimeSpan.FromMinutes(1));
    if(claim is null) break;
    await store.SettleActionAsync(claim,new(i==0?ExternalResultKind.Unknown:i==1?ExternalResultKind.Rejected:ExternalResultKind.Accepted,"simulated"),new());
    if(i==2) await store.RecordNoticeAsync(new(serviceScope,ModuleId("report"),claim.Record.Action.Id,"demo-service","v1",new(ExternalResultKind.Succeeded,"simulated-final"),1));
}
var limit = new ExternalCallLimit("demo","demo-service",100,TimeSpan.FromSeconds(1));
var permit = await store.TryAcquireCallAsync(limit,Guid.NewGuid(),TimeSpan.FromSeconds(10)); if(permit is not null) await store.CompleteCallAsync(permit,ExternalCallOutcome.Unused);
if(args.Length==2 && args[0]=="--module-config")
{
    Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(args[1]))!);
    await File.WriteAllTextAsync(args[1],System.Text.Json.JsonSerializer.Serialize(new { Jobs=jobScope.Id,External=serviceScope.Id,Limit=limit.Id }));
}

await using var reader = new PostgreSqlWorkReader(connectionString, schema);
await reader.ValidateAsync();
await reader.ValidateOutboxAsync();
var page = await reader.ListAsync(new(scopes[0], DateTimeOffset.UtcNow));
Console.WriteLine($"Dashboard demo ready: {page.Items.Count} items on first page; another page: {page.Next is not null}.");
Console.WriteLine("Start with: bash eng/dashboard-demo.sh");
