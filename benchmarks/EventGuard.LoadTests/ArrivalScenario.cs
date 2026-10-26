using System.Diagnostics;
using System.Threading.Channels;
using EventGuard.Runtime;
namespace EventGuard.LoadTests;

internal static class ArrivalScenario
{
    public static async Task<ArrivalPhase> RunAsync<T>(LoadDatabase<T> database,LoadOptions options,int workers,CancellationToken ct)
    {
        var scope=new WorkScope(Guid.NewGuid().ToString("N"),"load-ledger","arrivals","entry.created");
        var total=options.InitialBacklog+options.Count;
        var receipts=new Guid[total];
        var baseline=new byte[options.PayloadBytes]; new Random(options.Seed).NextBytes(baseline);
        using var lifetime=CancellationTokenSource.CreateLinkedTokenSource(ct);
        using var stop=CancellationTokenSource.CreateLinkedTokenSource(lifetime.Token);
        using var stopSampling=new CancellationTokenSource();
        var channel=Channel.CreateBounded<int>(new BoundedChannelOptions(options.ArrivalBuffer)
        { SingleWriter=true,SingleReader=options.AdmissionConcurrency==1,FullMode=BoundedChannelFullMode.Wait,AllowSynchronousContinuations=false });
        ArrivalSchedule? schedule=null;
        var measured=new MeasuredStore<T>(database.Store,total,lifetime.Token,index=>schedule?.CompletionDue(index) ?? 0);
        var acceptanceFromSchedule=new LatencyHistogram();
        var result=new ArrivalPhase(scope.Id,options.Count,options.InitialBacklog,options.ArrivalRate);
        var points=new List<ArrivalPoint>();
        long started=0,admissionStarted=0,offerEnded=0;
        string? failure=null;
        Task? pump=null,producer=null,sampler=null;
        Task[] admitting=[];
        ResourceSample? resources=null;
        void Capture(string phase)
        {
            var accepted=Interlocked.Read(ref measured.Acceptance[0]);
            var completed=Interlocked.Read(ref measured.ExecutionOutcomes[0]);
            var point=new ArrivalPoint(Stopwatch.GetElapsedTime(started).TotalSeconds,phase,schedule!.Considered,schedule.Enqueued,
                schedule.BufferRejected,schedule.ScheduleMissed,channel.Reader.Count,Interlocked.Read(ref admissionStarted),accepted,completed);
            lock (points)
            {
                if (points.Count<options.TimeoutSeconds+8) points.Add(point);
                else result.TimelineTruncated=true;
            }
        }
        async Task Admit(int ordinal,CancellationToken token)
        {
            measured.SubmissionStarted[ordinal]=Stopwatch.GetTimestamp();
            Interlocked.Increment(ref admissionStarted);
            var accepted=await measured.AcceptAsync(LoadScenario.Submission(scope,ordinal,baseline),token);
            if (accepted.Status!=AcceptanceStatus.Accepted) throw new InvalidOperationException("Fresh arrival was not accepted.");
            receipts[ordinal]=accepted.Work.Id;
            if (ordinal>=options.InitialBacklog) acceptanceFromSchedule.Record(schedule!.CompletionDue(ordinal));
        }
        async Task Guard(Func<Task> action)
        {
            try { await action(); }
            catch (Exception error)
            {
                Interlocked.CompareExchange(ref failure,error.GetType().Name,null);
                lifetime.Cancel(); throw;
            }
        }
        try
        {
            var preloading=Stopwatch.GetTimestamp();
            await Parallel.ForEachAsync(Enumerable.Range(0,options.InitialBacklog),new ParallelOptions
            { MaxDegreeOfParallelism=options.AdmissionConcurrency,CancellationToken=lifetime.Token },async (ordinal,token)=>await Admit(ordinal,token));
            result.InitialAdmissionSeconds=Stopwatch.GetElapsedTime(preloading).TotalSeconds;
            resources=new ResourceSample();
            started=Stopwatch.GetTimestamp(); measured.DrainStarted=started;
            schedule=new(options.Count,options.ArrivalRate,options.InitialBacklog,options.MaximumScheduleLagMs,started);
            Capture("start");
            sampler=Task.Run(async () =>
            {
                try
                {
                    while (true)
                    {
                        await Task.Delay(1000,stopSampling.Token);
                        Capture(Interlocked.Read(ref offerEnded)==0 ? "offering" : "draining");
                    }
                }
                catch (OperationCanceledException) when (stopSampling.IsCancellationRequested) { }
            },CancellationToken.None);
            pump=Guard(()=>new WorkPump<T>(measured,scope,database.Effect,options:new(maximumConcurrency:workers,
                idleDelay:TimeSpan.FromMilliseconds(10))).RunAsync(stop.Token));
            admitting=Enumerable.Range(0,options.AdmissionConcurrency).Select(_=>Guard(async () =>
            {
                await foreach (var ordinal in channel.Reader.ReadAllAsync(lifetime.Token)) await Admit(ordinal,lifetime.Token);
            })).ToArray();
            producer=Guard(async () =>
            {
                await schedule.RunAsync(channel.Writer,lifetime.Token);
                Interlocked.Exchange(ref offerEnded,Stopwatch.GetTimestamp());
                result.OfferWindowSeconds=Stopwatch.GetElapsedTime(started,offerEnded).TotalSeconds;
                result.ConfirmedAcceptedAtOfferEnd=Interlocked.Read(ref measured.Acceptance[0]);
                result.ConfirmedCompletedAtOfferEnd=Interlocked.Read(ref measured.ExecutionOutcomes[0]);
                Capture("offer-end");
            });
            var allAdmissions=Task.WhenAll(admitting.Append(producer));
            if (await Task.WhenAny(allAdmissions,pump)==pump)
            {
                await pump; lifetime.Token.ThrowIfCancellationRequested();
                throw new InvalidOperationException("Pump ended before arrival admission stopped.");
            }
            await allAdmissions;
            measured.ExpectCompletions(receipts.Count(id=>id!=Guid.Empty));
            await Task.WhenAny(measured.Finished.Task,pump);
            if (!measured.Finished.Task.IsCompletedSuccessfully)
            {
                await pump; lifetime.Token.ThrowIfCancellationRequested();
                throw new InvalidOperationException("Pump ended before accepted work completed.");
            }
            result.RunSeconds=Stopwatch.GetElapsedTime(started).TotalSeconds;
            result.CompletionTailSeconds=measured.LastCompletion>offerEnded ? Stopwatch.GetElapsedTime(offerEnded,measured.LastCompletion).TotalSeconds : 0;
        }
        catch (Exception error) { Interlocked.CompareExchange(ref failure,error.GetType().Name,null); }
        finally
        {
            if (failure is not null) lifetime.Cancel();
            stop.Cancel(); channel.Writer.TryComplete();
            foreach (var task in admitting.Concat(new[]{producer,pump}.OfType<Task>()))
                try { await task; } catch (Exception error) { Interlocked.CompareExchange(ref failure,error.GetType().Name,null); }
            stopSampling.Cancel(); if (sampler is not null) await sampler;
            if (started!=0) Capture(failure is null ? "complete" : "failed");
            if (resources is not null)
            {
                result.ClientResources=await resources.FinishAsync();
                await resources.DisposeAsync();
            }
        }
        result.Considered=schedule?.Considered ?? 0; result.Enqueued=schedule?.Enqueued ?? 0;
        result.BufferRejected=schedule?.BufferRejected ?? 0; result.ScheduleMissed=schedule?.ScheduleMissed ?? 0;
        result.AdmissionStarted=admissionStarted; result.AdmissionOutcomes=measured.Acceptance; result.ExecutionOutcomes=measured.ExecutionOutcomes;
        result.UnconfirmedAdmission=measured.UnconfirmedAdmission; result.UnconfirmedClaim=measured.UnconfirmedClaim;
        result.UnconfirmedExecution=measured.UnconfirmedExecution; result.CancelledClaims=measured.CancelledClaims;
        result.DispatchDelay=schedule?.DispatchDelay.Snapshot(); result.ScheduledToAcceptance=acceptanceFromSchedule.Snapshot();
        result.ScheduledToCompletion=measured.ScheduledToCompletion.Snapshot(); result.AdmissionCalls=measured.Admission.Snapshot();
        result.NonemptyClaims=measured.Claim.Snapshot(); result.EmptyClaims=measured.EmptyClaim.Snapshot(); result.ExecuteCalls=measured.Execution.Snapshot();
        result.SubmissionToCompletion=measured.SubmissionToCompletion.Snapshot(); result.Timeline=points.ToArray();
        using var verify=new CancellationTokenSource(TimeSpan.FromSeconds(30));
        try
        {
            result.Oracle=await database.VerifyAsync(scope,receipts.Count(id=>id!=Guid.Empty),verify.Token,ordinalLimit:total);
            result.AdmissionSet=await database.VerifyAdmissionSetAsync(scope,receipts,verify.Token);
        }
        catch (Exception error) { Interlocked.CompareExchange(ref failure,error.GetType().Name,null); }
        result.ErrorType=failure;
        return result;
    }
}
internal sealed record ArrivalPoint(double ElapsedSeconds,string Phase,long Considered,long Enqueued,long BufferRejected,long ScheduleMissed,
    int BufferDepth,long AdmissionStarted,long ConfirmedAccepted,long ConfirmedCompleted)
{
    // These are local non-atomic observations, not a durable DB snapshot or emptiness proof.
    public long ObservedUnfinished => Math.Max(0,ConfirmedAccepted-ConfirmedCompleted);
}
internal sealed record ArrivalPhase(string ScopeId,int PlannedArrivals,int InitialBacklog,int TargetRate)
{
    public double InitialAdmissionSeconds { get; set; }
    public double OfferWindowSeconds { get; set; }
    public double RunSeconds { get; set; }
    public double CompletionTailSeconds { get; set; }
    public long ConfirmedAcceptedAtOfferEnd { get; set; }
    public long ConfirmedCompletedAtOfferEnd { get; set; }
    public long Considered { get; set; }
    public long Enqueued { get; set; }
    public long BufferRejected { get; set; }
    public long ScheduleMissed { get; set; }
    public long AdmissionStarted { get; set; }
    public long[] AdmissionOutcomes { get; set; }=[];
    public long[] ExecutionOutcomes { get; set; }=[];
    public long UnconfirmedAdmission { get; set; }
    public long UnconfirmedClaim { get; set; }
    public long UnconfirmedExecution { get; set; }
    public long CancelledClaims { get; set; }
    public Latencies? DispatchDelay { get; set; }
    public Latencies? ScheduledToAcceptance { get; set; }
    public Latencies? ScheduledToCompletion { get; set; }
    public Latencies? SubmissionToCompletion { get; set; }
    public Latencies? AdmissionCalls { get; set; }
    public Latencies? NonemptyClaims { get; set; }
    public Latencies? EmptyClaims { get; set; }
    public Latencies? ExecuteCalls { get; set; }
    public ProcessResources? ClientResources { get; set; }
    public Oracle? Oracle { get; set; }
    public AdmissionSetCheck? AdmissionSet { get; set; }
    public ArrivalPoint[] Timeline { get; set; }=[];
    public bool TimelineTruncated { get; set; }
    public string? ErrorType { get; set; }
    public bool CorrectnessVerified => ErrorType is null && Oracle?.Valid==true && AdmissionSet?.Valid==true
        && Considered==PlannedArrivals && Considered==Enqueued+BufferRejected+ScheduleMissed
        && AdmissionOutcomes.Length==3 && AdmissionOutcomes[0]==InitialBacklog+Enqueued && AdmissionOutcomes.Skip(1).All(n=>n==0)
        && AdmissionStarted==AdmissionOutcomes[0] && ExecutionOutcomes.Length==5 && ExecutionOutcomes[0]==AdmissionOutcomes[0]
        && ExecutionOutcomes.Skip(1).All(n=>n==0) && UnconfirmedAdmission+UnconfirmedClaim+UnconfirmedExecution==0
        && ScheduledToAcceptance?.Samples==Enqueued && ScheduledToCompletion?.Samples==Enqueued;
    public bool OfferedLoadDelivered => CorrectnessVerified && BufferRejected==0 && ScheduleMissed==0;
    public double? VerifiedOverallCompletionsPerSecond => CorrectnessVerified && RunSeconds>0 ? Oracle!.Completed/RunSeconds : null;
    public double? ArrivalsAcceptedPerSecondDuringOfferWindow => CorrectnessVerified && OfferWindowSeconds>0
        ? Math.Max(0,ConfirmedAcceptedAtOfferEnd-InitialBacklog)/OfferWindowSeconds : null;
    public double? CompletedPerSecondDuringOfferWindow => CorrectnessVerified && OfferWindowSeconds>0 ? ConfirmedCompletedAtOfferEnd/OfferWindowSeconds : null;
}
