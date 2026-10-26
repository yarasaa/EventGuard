using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
using EventGuard.Runtime;
namespace EventGuard.LoadTests;

internal static class LoadScenario
{
    public static Task<CaseResult> RunAsync(LoadOptions options, int workers, int repetition, CancellationToken ct) => options.Provider == "postgresql"
        ? RunAsync(options,workers,repetition,LocalDatabases.PostgreSql(options.PoolSize),ct)
        : RunAsync(options,workers,repetition,LocalDatabases.SqlServer(options.PoolSize),ct);
    private static async Task<CaseResult> RunAsync<T>(LoadOptions options, int workers, int repetition, LoadDatabase<T> database, CancellationToken ct)
    {
        await using var owned = database;
        var result = new CaseResult(options.Provider,workers,repetition,database.Schema,DateTimeOffset.UtcNow);
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(ct); deadline.CancelAfter(TimeSpan.FromSeconds(options.TimeoutSeconds));
        try
        {
            await database.InitializeAsync(deadline.Token);
            result.Stage = "profile";
            result.DatabaseProfile = await database.ProfileAsync(deadline.Token);
            if (options.WarmupCount > 0)
            {
                result.Stage = "warmup";
                result.Warmup = await ExerciseAsync(database,options,workers,options.WarmupCount,"warmup",deadline.Token);
                if (!result.Warmup.Valid) throw new InvalidOperationException("Warm-up validation failed.");
            }
            result.Stage = "measured";
            if (options.Profile == "arrivals")
            {
                result.Arrivals=await ArrivalScenario.RunAsync(database,options,workers,deadline.Token);
                if (!result.Arrivals.CorrectnessVerified) throw new InvalidOperationException("Arrival correctness validation failed.");
            }
            else
            {
                result.Measured = await ExerciseAsync(database,options,workers,options.Count,"measured",deadline.Token);
                if (!result.Measured.Valid) throw new InvalidOperationException("Measured validation failed.");
            }
            result.Stage = "cleanup";
            using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await database.CleanupAsync(cleanup.Token); result.SchemaRemoved = true; result.Stage = "complete";
        }
        catch (Exception error) { result.ErrorType = error.GetType().Name; }
        return result;
    }
    internal static async Task<PhaseResult> ExerciseAsync<T>(LoadDatabase<T> database, LoadOptions options, int workers, int count, string name, CancellationToken ct)
    {
        var scope = new WorkScope(Guid.NewGuid().ToString("N"),"load-ledger",name,"entry.created");
        var measured = new MeasuredStore<T>(database.Store,count,ct);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var baseline = new byte[options.PayloadBytes]; new Random(options.Seed).NextBytes(baseline);
        var background = name == "measured" ? options.BackgroundCount : 0;
        // Prepare through public durable APIs, before measured resource/admission timers.
        // These same-scope records must remain untouched while the ready backlog drains.
        await Parallel.ForEachAsync(Enumerable.Range(0,background),new ParallelOptions {MaxDegreeOfParallelism=options.AdmissionConcurrency,CancellationToken=ct},async (ordinal,token) =>
        {
            var data = Submission(scope,ordinal,baseline);
            var input = new WorkSubmission(new(scope,data.Identity.Policy,"background-" + ordinal.ToString(CultureInfo.InvariantCulture)),
                data.MessageId,data.CopyPayload(),data.Fingerprint,
                notBefore:options.BackgroundKind == "scheduled" ? DateTimeOffset.UtcNow.AddDays(1) : null);
            if ((await database.Store.AcceptAsync(input,token)).Status != AcceptanceStatus.Accepted)
                throw new InvalidOperationException("Fresh background admission was not accepted.");
        });
        if (options.BackgroundKind == "leased")
            for (var i=0;i<background;i++)
                if (await database.Store.ClaimNextAsync(scope,Guid.NewGuid(),TimeSpan.FromHours(1),ct) is null)
                    throw new InvalidOperationException("Background ownership could not be established.");
        await using var resources = new ResourceSample();
        var phaseStart = Stopwatch.GetTimestamp(); double admissionSeconds = 0, drainSeconds = 0;
        Task? run = null; string? failure = null; Oracle? oracle = null;
        try
        {
            await Parallel.ForEachAsync(Enumerable.Range(0,count), new ParallelOptions { MaxDegreeOfParallelism=options.AdmissionConcurrency, CancellationToken=ct }, async (ordinal, token) =>
            {
                measured.SubmissionStarted[ordinal] = Stopwatch.GetTimestamp();
                var result = await measured.AcceptAsync(Submission(scope,ordinal,baseline),token);
                if (result.Status != AcceptanceStatus.Accepted) throw new InvalidOperationException("Fresh fixture admission was not accepted.");
            });
            admissionSeconds = Stopwatch.GetElapsedTime(phaseStart).TotalSeconds;
            measured.DrainStarted = Stopwatch.GetTimestamp();
            run = new WorkPump<T>(measured,scope,database.Effect, options: new(maximumConcurrency:workers,idleDelay:TimeSpan.FromMilliseconds(10))).RunAsync(stop.Token);
            await Task.WhenAny(run,measured.Finished.Task);
            if (!measured.Finished.Task.IsCompletedSuccessfully) { await run; ct.ThrowIfCancellationRequested(); throw new InvalidOperationException("Pump ended before completion."); }
            drainSeconds = Stopwatch.GetElapsedTime(measured.DrainStarted,measured.LastCompletion).TotalSeconds;
        }
        catch (Exception error) { failure = error.GetType().Name; }
        finally
        {
            stop.Cancel();
            if (run is not null) try { await run; } catch (Exception error) { failure ??= error.GetType().Name; }
        }
        if (measured.DrainStarted == 0) admissionSeconds = Stopwatch.GetElapsedTime(phaseStart).TotalSeconds;
        else if (drainSeconds == 0) drainSeconds = Stopwatch.GetElapsedTime(measured.DrainStarted).TotalSeconds;
        var workElapsedSeconds = Stopwatch.GetElapsedTime(phaseStart).TotalSeconds;
        var client = await resources.FinishAsync();
        var idle = new LatencyHistogram();
        try
        {
            if (failure is null && name == "measured")
                for (var i=0;i<options.IdlePolls;i++)
                {
                    var start = Stopwatch.GetTimestamp();
                    var unexpected = await database.Store.ClaimNextAsync(scope,Guid.NewGuid(),TimeSpan.FromSeconds(30),ct);
                    idle.AddMilliseconds(Stopwatch.GetElapsedTime(start).TotalMilliseconds);
                    if (unexpected is not null) throw new InvalidOperationException("Idle probe claimed unavailable background work.");
                }
        }
        catch (Exception error) { failure ??= error.GetType().Name; }
        using var verification = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        try { oracle = await database.VerifyAsync(scope,count,verification.Token,background,options.BackgroundKind == "leased"); }
        catch (Exception error) { failure ??= error.GetType().Name; }
        return new(count,scope.Id,admissionSeconds,drainSeconds,workElapsedSeconds,
            measured.Acceptance,measured.ExecutionOutcomes,measured.UnconfirmedAdmission,measured.UnconfirmedClaim,measured.UnconfirmedExecution,
            measured.CancelledClaims,measured.Admission.Snapshot(),measured.Claim.Snapshot(),measured.EmptyClaim.Snapshot(),measured.Execution.Snapshot(),
            measured.SubmissionToCompletion.Snapshot(),measured.DrainToCompletion.Snapshot(),client,oracle,failure,idle.Snapshot());
    }
    internal static WorkSubmission Submission(WorkScope scope, int ordinal, byte[] baseline)
    {
        var payload = (byte[])baseline.Clone(); BinaryPrimitives.WriteInt32LittleEndian(payload,ordinal);
        var fingerprint = ContentFingerprint.FromBytes(payload);
        // Stable message identity and bytes for a given ordinal, seed and payload size. Scope isolates every case.
        var messageId = new Guid(System.Security.Cryptography.SHA256.HashData(payload).AsSpan(0,16));
        return new(new(scope,new("load-ordinal/v1","sha256/v1"),ordinal.ToString(CultureInfo.InvariantCulture)),messageId,payload,fingerprint);
    }
}
internal sealed record PhaseResult(int Expected, string ScopeId, double AdmissionSeconds, double DrainSeconds, double ClientWorkElapsedSeconds,
    long[] AdmissionOutcomes, long[] ExecutionOutcomes, long UnconfirmedAdmission, long UnconfirmedClaim, long UnconfirmedExecution, long CancelledClaimsOnStop,
    Latencies AdmissionCalls, Latencies NonemptyClaims, Latencies EmptyClaims, Latencies ExecuteCalls,
    Latencies SubmissionToConfirmedCompletion, Latencies DrainStartToConfirmedCompletion, ProcessResources ClientResources, Oracle? Oracle, string? ErrorType,Latencies IdleProbe)
{
    public bool Valid => ErrorType is null && Oracle?.Valid == true && AdmissionOutcomes[0] == Expected
        && ExecutionOutcomes[0] == Expected && UnconfirmedAdmission + UnconfirmedClaim + UnconfirmedExecution == 0;
    public double? VerifiedCompletionsPerSecond => Valid && DrainSeconds > 0 ? Oracle!.Completed/DrainSeconds : null;
    public double? AcceptedPerSecond => Valid && AdmissionSeconds > 0 ? AdmissionOutcomes[0]/AdmissionSeconds : null;
}
internal sealed record CaseResult(string Provider,int Workers,int Repetition,string Schema,DateTimeOffset StartedAt)
{
    public string Stage { get; set; } = "setup";
    public Dictionary<string,string>? DatabaseProfile { get; set; }
    public PhaseResult? Warmup { get; set; }
    public PhaseResult? Measured { get; set; }
    public ArrivalPhase? Arrivals { get; set; }
    public bool SchemaRemoved { get; set; }
    public string? ErrorType { get; set; }
    public bool Valid => ErrorType is null && (Measured?.Valid == true || Arrivals?.CorrectnessVerified == true) && SchemaRemoved;
}
