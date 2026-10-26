using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace EventGuard.Kafka.Tests;

public sealed class CoordinatorRetryTests
{
    [Theory]
    [InlineData(ErrorCode.NotCoordinatorForGroup)]
    [InlineData(ErrorCode.GroupCoordinatorNotAvailable)]
    [InlineData(ErrorCode.GroupLoadInProgress)]
    [InlineData(ErrorCode.Local_WaitCoord)]
    [InlineData(ErrorCode.Local_TimedOut)]
    public async Task Coordinator_recovery_still_requires_the_actual_expected_offset(ErrorCode code)
    {
        var calls = 0;
        var offset = await CoordinatorRetry.CommittedAsync(_ => ++calls switch
        {
            1 => throw new KafkaException(new Error(code)),
            2 => Offset.Unset.Value,
            3 => 1,
            _ => 2
        }, expected: 2, TimeSpan.FromSeconds(5));
        Assert.Equal(2, offset);
        Assert.Equal(4, calls);
    }

    [Theory]
    [InlineData(ErrorCode.GroupAuthorizationFailed)]
    [InlineData(ErrorCode.TopicAuthorizationFailed)]
    [InlineData(ErrorCode.InvalidGroupId)]
    [InlineData(ErrorCode.Local_Transport)]
    public async Task Unrelated_or_permanent_read_errors_are_not_retried(ErrorCode code)
    {
        var calls = 0;
        var failure = new KafkaException(new Error(code));
        Assert.Same(failure, await Assert.ThrowsAsync<KafkaException>(() => CoordinatorRetry.CommittedAsync(
            _ => { calls++; throw failure; }, null, TimeSpan.FromSeconds(5))));
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task Fatal_errors_are_not_retried_even_with_a_coordinator_code()
    {
        var failure = new KafkaException(new Error(ErrorCode.NotCoordinatorForGroup, "synthetic fatal error", true));
        Assert.Same(failure, await Assert.ThrowsAsync<KafkaException>(() => CoordinatorRetry.CommittedAsync(
            _ => throw failure, null, TimeSpan.FromSeconds(5))));
    }

    [Fact]
    public async Task Missing_commit_is_returned_only_after_a_successful_observation()
    {
        var calls = 0;
        Assert.Equal(Offset.Unset.Value, await CoordinatorRetry.CommittedAsync(_ =>
        {
            if (++calls == 1) throw new KafkaException(new Error(ErrorCode.NotCoordinatorForGroup));
            return Offset.Unset.Value;
        }, null, TimeSpan.FromSeconds(5)));
        Assert.Equal(2, calls);
    }

    [Fact]
    public async Task Persistent_coordinator_failure_has_a_deadline_and_preserves_its_cause()
    {
        var failure = new KafkaException(new Error(ErrorCode.NotCoordinatorForGroup));
        var error = await Assert.ThrowsAsync<TimeoutException>(() => CoordinatorRetry.CommittedAsync(
            _ => throw failure, 2, TimeSpan.FromMilliseconds(200)));
        Assert.Same(failure, error.InnerException);
    }

    [Fact]
    public async Task Successful_reads_without_the_expected_commit_do_not_pass()
    {
        var error = await Assert.ThrowsAsync<TimeoutException>(() => CoordinatorRetry.CommittedAsync(
            _ => Offset.Unset.Value, 2, TimeSpan.FromMilliseconds(200)));
        Assert.Null(error.InnerException);
    }

    [Theory]
    [InlineData(ErrorCode.NotCoordinatorForGroup)]
    [InlineData(ErrorCode.GroupCoordinatorNotAvailable)]
    [InlineData(ErrorCode.GroupLoadInProgress)]
    [InlineData(ErrorCode.NonEmptyGroup)]
    public async Task Cleanup_waits_for_coordinator_and_group_departure(ErrorCode code)
    {
        var calls = 0;
        await CoordinatorRetry.DeleteGroupAsync(_ => ++calls == 1
            ? Task.FromException(DeleteError(code)) : Task.CompletedTask, TimeSpan.FromSeconds(5));
        Assert.Equal(2, calls);
    }

    [Fact]
    public async Task Already_deleted_group_is_a_successful_cleanup()
    {
        await CoordinatorRetry.DeleteGroupAsync(_ => Task.FromException(DeleteError(ErrorCode.GroupIdNotFound)),
            TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task A_permanent_cleanup_error_is_not_hidden_by_other_retriable_results()
    {
        var failure = DeleteError(ErrorCode.NotCoordinatorForGroup, ErrorCode.GroupAuthorizationFailed);
        Assert.Same(failure, await Assert.ThrowsAsync<DeleteGroupsException>(() => CoordinatorRetry.DeleteGroupAsync(
            _ => Task.FromException(failure), TimeSpan.FromSeconds(5))));
    }

    [Fact]
    public async Task Cleanup_does_not_wait_forever_for_an_active_group()
    {
        var failure = DeleteError(ErrorCode.NonEmptyGroup);
        var error = await Assert.ThrowsAsync<TimeoutException>(() => CoordinatorRetry.DeleteGroupAsync(
            _ => Task.FromException(failure), TimeSpan.FromMilliseconds(200)));
        Assert.Same(failure, error.InnerException);
    }

    private static DeleteGroupsException DeleteError(params ErrorCode[] codes) => new(codes.Select((code, i) =>
        new DeleteGroupReport { Group = "fixture-" + i, Error = new Error(code) }).ToList());
}
