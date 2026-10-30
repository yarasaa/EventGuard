using EventGuard;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text.Json;
using EventGuard.LoadTests;

if (args.Length > 0 && args[0] == "--external-pipeline") return await ExternalPipelineProbe.RunAsync(args[1..]);
if (args.Length > 0 && args[0] == "--claim-plans") return await ClaimPlanProbe.RunAsync(args[1..]);
if (args.Length > 0 && args[0] == "--sparse-claim-plans") return await ClaimPlanProbe.RunAsync(args[1..]);
if (args.Length > 0 && args[0] == "--claim-due-plans") return await ClaimDuePlanProbe.RunAsync(args[1..]);
if (args.Length > 0 && args[0] == "--point-lookup-plans") return await PointLookupPlanProbe.RunAsync(args[1..]);

LoadOptions options;
try { options = LoadOptions.Parse(args); }
catch (Exception error) { Console.Error.WriteLine("Invalid load-test options: " + error.GetType().Name); return 2; }
Directory.CreateDirectory(Path.GetDirectoryName(options.Output)!);
await using (var reservation = new FileStream(options.Output,FileMode.CreateNew)) await reservation.WriteAsync("{}"u8.ToArray());
using var stopping = new CancellationTokenSource();
Console.CancelKeyPress += (_,e) => { e.Cancel=true; stopping.Cancel(); };
var cases = new List<CaseResult>();
var started = DateTimeOffset.UtcNow;
var environment = new
{
    Framework=RuntimeInformation.FrameworkDescription, OS=RuntimeInformation.OSDescription,
    ProcessArchitecture=RuntimeInformation.ProcessArchitecture.ToString(), OSArchitecture=RuntimeInformation.OSArchitecture.ToString(),
    LogicalProcessors=Environment.ProcessorCount, GCServer=System.Runtime.GCSettings.IsServerGC,
    GCTotalAvailableMemoryBytes=GC.GetGCMemoryInfo().TotalAvailableMemoryBytes,
    AssemblyVersion=typeof(WorkScope).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion,
    Binaries=new[] {typeof(LoadScenario).Assembly,typeof(WorkScope).Assembly,typeof(EventGuard.Runtime.WorkPump<>).Assembly,
        typeof(EventGuard.PostgreSql.PostgreSqlWorkStore).Assembly,typeof(EventGuard.SqlServer.SqlServerWorkStore).Assembly}
        .Select(a=>new {Name=a.GetName().Name,Sha256=Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(File.ReadAllBytes(a.Location)))}).ToArray(),
    Drivers=new[] { typeof(Npgsql.NpgsqlConnection).Assembly,typeof(Microsoft.Data.SqlClient.SqlConnection).Assembly }
        .Select(a=>new {Name=a.GetName().Name,Version=a.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion}).ToArray()
};
async Task SaveAsync()
{
    var complete=cases.Count==options.Repetitions*options.Concurrency.Length;
    var correct=complete && cases.All(c=>c.Valid);
    bool? delivered=options.Profile=="arrivals" ? complete && cases.All(c=>c.Arrivals?.OfferedLoadDelivered==true) : null;
    var json = JsonSerializer.Serialize(new { FormatVersion=2,StartedAt=started,UpdatedAt=DateTimeOffset.UtcNow,Options=options,Environment=environment,
        Scenario=options.Profile=="arrivals"
            ? "Fixed planned arrival times, bounded nonblocking offer buffer and bounded admission workers; real WorkPump runs alongside admission and drains accepted work after the offer window. Declared buffer rejections and missed schedule slots are not database admissions. Optional initial backlog is preloaded outside measured resource/run timers. No broker."
            : "Preloaded single-scope consumer backlog; closed-loop admission, no concurrent arrivals or broker; SHA-256 plus one append-only SQL effect per transaction. Optional unavailable background is prepared outside admission/drain timers; idle probes run after drain and have separate latency statistics.",
        AdmissionOutcomeOrder=Enum.GetNames<AcceptanceStatus>(),ExecutionOutcomeOrder=Enum.GetNames<ExecutionStatus>(),
        Cases=cases,Complete=complete,CorrectnessVerified=correct,OfferedLoadDelivered=delivered,Success=correct && delivered!=false
    },new JsonSerializerOptions {WriteIndented=true});
    var temporary = options.Output + ".tmp"; await File.WriteAllTextAsync(temporary,json); File.Move(temporary,options.Output,true);
}
await SaveAsync();
for (var repetition = 1; repetition <= options.Repetitions && !stopping.IsCancellationRequested; repetition++)
{
    // Rotate order to reduce always-warm/always-cold bias; this is not randomized hardware isolation.
    var order = Enumerable.Range(0,options.Concurrency.Length).Select(i=>options.Concurrency[(i+repetition-1)%options.Concurrency.Length]);
    foreach (var workers in order)
    {
        Console.WriteLine($"{options.Provider}: repetition {repetition}, workers {workers}, records {options.Count}");
        CaseResult result;
        try { result = await LoadScenario.RunAsync(options,workers,repetition,stopping.Token); }
        catch (Exception error) { result = new(options.Provider,workers,repetition,"not-created",DateTimeOffset.UtcNow) { ErrorType=error.GetType().Name }; }
        cases.Add(result); await SaveAsync();
        Console.WriteLine(result.Valid ? result.Arrivals is { } arrivals
            ? $"Verified accepted work: {arrivals.Oracle!.Completed}; offered load delivered: {arrivals.OfferedLoadDelivered}; buffer rejected: {arrivals.BufferRejected}; schedule missed: {arrivals.ScheduleMissed}; schema removed."
            : $"Verified: {result.Measured!.VerifiedCompletionsPerSecond:F1} completions/s; schema removed."
            : $"FAILED: {result.ErrorType}; fixture {result.Schema} retained for inspection.");
        if (!result.Valid) return 1;
    }
}
await SaveAsync();
Console.WriteLine("Result: " + options.Output);
if (cases.Count!=options.Repetitions*options.Concurrency.Length || cases.Any(c=>!c.Valid)) return 1;
return options.Profile=="arrivals" && cases.Any(c=>c.Arrivals?.OfferedLoadDelivered!=true) ? 3 : 0;
