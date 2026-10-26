using System.Diagnostics;
using System.Threading.Channels;
using EventGuard.LoadTests;
namespace EventGuard.LoadTests.Tests;

public sealed class ArrivalTests
{
    [Fact]
    public void Full_buffer_records_unoffered_slots_without_shifting_the_schedule()
    {
        var start=Stopwatch.Frequency*10;
        var schedule=new ArrivalSchedule(5,10,7,1000,start);
        var queue=Channel.CreateBounded<int>(2);
        schedule.OfferDue(start+Stopwatch.Frequency/2,queue.Writer);
        Assert.Equal(5,schedule.Considered); Assert.Equal(2,schedule.Enqueued); Assert.Equal(3,schedule.BufferRejected);
        Assert.Equal(0,schedule.ScheduleMissed); Assert.Equal(start+Stopwatch.Frequency/2,schedule.Due(5));
        Assert.True(queue.Reader.TryRead(out var first)); Assert.Equal(7,first);
        Assert.True(queue.Reader.TryRead(out var second)); Assert.Equal(8,second);
        schedule.OfferDue(start+Stopwatch.Frequency,queue.Writer);
        Assert.False(queue.Reader.TryRead(out _)); Assert.Equal(5,schedule.DispatchDelay.Snapshot().Samples);
    }
    [Fact]
    public void Overdue_slots_are_reported_separately_from_full_buffer_rejections()
    {
        var start=Stopwatch.Frequency*10;
        var schedule=new ArrivalSchedule(4,10,0,20,start);
        var queue=Channel.CreateBounded<int>(4);
        schedule.OfferDue(start+Stopwatch.Frequency/4,queue.Writer);
        Assert.Equal(3,schedule.Considered); Assert.Equal(3,schedule.ScheduleMissed);
        Assert.Equal(0,schedule.BufferRejected); Assert.Equal(0,schedule.Enqueued);
        schedule.OfferDue(schedule.Due(3),queue.Writer);
        Assert.Equal(4,schedule.Considered); Assert.Equal(1,schedule.Enqueued);
        Assert.True(queue.Reader.TryRead(out var ordinal)); Assert.Equal(3,ordinal);
    }
    [Fact]
    public void Arrival_options_bound_memory_and_reject_silent_profile_mismatches()
    {
        foreach (var args in new[]
        {
            new[]{"--arrival-rate","100"},new[]{"--profile","unknown"},
            new[]{"--profile","arrivals","--arrival-rate","0"},new[]{"--profile","arrivals","--arrival-buffer","65537"},
            new[]{"--profile","arrivals","--initial-backlog","1000000"},new[]{"--profile","arrivals","--background-count","1"},
            new[]{"--profile","arrivals","--idle-polls","1"},new[]{"--profile","arrivals","--count","10000","--arrival-rate","1"}
        }) Assert.ThrowsAny<ArgumentException>(()=>LoadOptions.Parse(args));
    }
    [Theory]
    [InlineData("postgresql")][InlineData("sqlserver")]
    public async Task Native_arrivals_drain_initial_backlog_and_match_every_confirmed_admission(string provider)
    {
        var options=LoadOptions.Parse(["--provider",provider,"--profile","arrivals","--count","24","--arrival-rate","40",
            "--initial-backlog","8","--arrival-buffer","128","--maximum-schedule-lag-ms","5000","--concurrency","2","--warmup","8","--repetitions","1"]);
        var result=await LoadScenario.RunAsync(options,2,1,CancellationToken.None);
        Assert.True(result.Valid,System.Text.Json.JsonSerializer.Serialize(result));
        var a=result.Arrivals!;
        Assert.True(a.OfferedLoadDelivered); Assert.Equal(32,a.Oracle!.Completed); Assert.Equal(32,a.AdmissionSet!.Matched);
        Assert.Equal(24,a.ScheduledToCompletion!.Samples); Assert.Equal(24,a.ScheduledToAcceptance!.Samples);
        Assert.Equal(8,a.Timeline[0].ConfirmedAccepted); Assert.Equal(0,a.Timeline[0].ConfirmedCompleted);
        Assert.Equal(0,a.Timeline[^1].ObservedUnfinished); Assert.Equal("complete",a.Timeline[^1].Phase);
        Assert.Single(a.Timeline,p=>p.Phase=="offer-end"); Assert.True(result.SchemaRemoved);
    }
    [Theory]
    [InlineData("postgresql")][InlineData("sqlserver")]
    public async Task Native_overload_verifies_only_admitted_work_and_reports_the_unmet_offered_load(string provider)
    {
        var options=LoadOptions.Parse(["--provider",provider,"--profile","arrivals","--count","1000","--arrival-rate","1000000",
            "--arrival-buffer","1","--admission-concurrency","1","--maximum-schedule-lag-ms","10000","--concurrency","2","--warmup","0","--repetitions","1"]);
        var result=await LoadScenario.RunAsync(options,2,1,CancellationToken.None);
        Assert.True(result.Valid,System.Text.Json.JsonSerializer.Serialize(result));
        var a=result.Arrivals!;
        Assert.False(a.OfferedLoadDelivered); Assert.True(a.BufferRejected>0);
        Assert.Equal(a.PlannedArrivals,a.Enqueued+a.BufferRejected+a.ScheduleMissed);
        Assert.Equal(a.Enqueued,a.Oracle!.Completed); Assert.Equal(a.Enqueued,a.AdmissionSet!.Matched);
        Assert.All(a.Timeline,p=>Assert.InRange(p.BufferDepth,0,1)); Assert.True(result.SchemaRemoved);
    }
    [Theory]
    [InlineData("postgresql")][InlineData("sqlserver")]
    public async Task Native_admission_oracle_detects_wrong_receipts_even_with_valid_counts_and_ordinal_holes(string provider)
    {
        if (provider=="postgresql") await CheckReceipts(LocalDatabases.PostgreSql(4)); else await CheckReceipts(LocalDatabases.SqlServer(4));
    }
    private static async Task CheckReceipts<T>(LoadDatabase<T> db)
    {
        await using var owned=db; using var deadline=new CancellationTokenSource(TimeSpan.FromSeconds(20));
        await db.InitializeAsync(deadline.Token);
        try
        {
            var scope=new WorkScope("test","arrival-oracle","native","test"); var receipts=new Guid[3];
            foreach (var ordinal in new[]{0,2})
            {
                receipts[ordinal]=(await db.Store.AcceptAsync(LoadScenario.Submission(scope,ordinal,new byte[64]),deadline.Token)).Work.Id;
                var claim=(await db.Store.ClaimNextAsync(scope,Guid.NewGuid(),TimeSpan.FromSeconds(30),deadline.Token))!;
                await db.Store.ExecuteAsync(claim,db.Effect,new(),deadline.Token);
            }
            Assert.True((await db.VerifyAsync(scope,2,deadline.Token,ordinalLimit:3)).Valid);
            Assert.True((await db.VerifyAdmissionSetAsync(scope,receipts,deadline.Token)).Valid);
            receipts[2]=Guid.NewGuid();
            var mismatch=await db.VerifyAdmissionSetAsync(scope,receipts,deadline.Token);
            Assert.False(mismatch.Valid); Assert.Equal(1,mismatch.Unexpected); Assert.Equal(1,mismatch.Matched);
        }
        finally { await db.CleanupAsync(CancellationToken.None); }
    }
    [Theory]
    [InlineData("postgresql")][InlineData("sqlserver")]
    public async Task Native_arrival_cancellation_is_a_failed_experiment_with_no_successful_rate(string provider)
    {
        if (provider=="postgresql") await CheckCancellation(LocalDatabases.PostgreSql(4)); else await CheckCancellation(LocalDatabases.SqlServer(4));
    }
    private static async Task CheckCancellation<T>(LoadDatabase<T> db)
    {
        await using var owned=db; await db.InitializeAsync(CancellationToken.None);
        try
        {
            var options=LoadOptions.Parse(["--profile","arrivals","--count","50","--arrival-rate","10","--concurrency","2","--warmup","0"]);
            using var cancel=new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
            var result=await ArrivalScenario.RunAsync(db,options,2,cancel.Token).WaitAsync(TimeSpan.FromSeconds(15));
            Assert.False(result.CorrectnessVerified); Assert.False(result.OfferedLoadDelivered);
            Assert.NotNull(result.ErrorType); Assert.Null(result.VerifiedOverallCompletionsPerSecond);
            Assert.Equal("failed",result.Timeline[^1].Phase);
        }
        finally { await db.CleanupAsync(CancellationToken.None); }
    }
}
