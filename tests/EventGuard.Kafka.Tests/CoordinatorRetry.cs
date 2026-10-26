using System.Diagnostics;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace EventGuard.Kafka.Tests;

// Test observations and idempotent fixture cleanup only. Never retry application commits here.
internal static class CoordinatorRetry
{
    private static bool Temporary(Error error) => !error.IsFatal && error.Code is
        ErrorCode.NotCoordinatorForGroup or ErrorCode.GroupCoordinatorNotAvailable or
        ErrorCode.GroupLoadInProgress or ErrorCode.Local_WaitCoord or ErrorCode.Local_TimedOut;

    public static Task<long> CommittedAsync(Func<TimeSpan, long> read, long? expected, TimeSpan timeout) =>
        WaitAsync(remaining => Task.FromResult(read(remaining)),
            offset => !expected.HasValue || offset == expected.Value,
            error => error is KafkaException kafka && Temporary(kafka.Error), timeout,
            expected.HasValue ? $"committed offset {expected.Value}" : "committed offset observation");

    public static async Task DeleteGroupAsync(Func<TimeSpan, Task> delete, TimeSpan timeout)
    {
        await WaitAsync(async remaining =>
        {
            try { await delete(remaining).ConfigureAwait(false); }
            catch (DeleteGroupsException error) when (error.Results.Count > 0 && error.Results.All(r =>
                !r.Error.IsFatal && r.Error.Code is ErrorCode.GroupIdNotFound or ErrorCode.NoError))
            { /* Already absent is the expected final state of this fixture-owned group. */ }
            return true;
        }, complete => complete, error => error switch
        {
            DeleteGroupsException groups => groups.Results.Count > 0 && groups.Results.All(r =>
                !r.Error.IsFatal && (Temporary(r.Error) || r.Error.Code is
                    ErrorCode.NonEmptyGroup or ErrorCode.GroupIdNotFound or ErrorCode.NoError)),
            KafkaException kafka => Temporary(kafka.Error),
            _ => false
        }, timeout, "consumer group removal").ConfigureAwait(false);
    }

    private static async Task<T> WaitAsync<T>(Func<TimeSpan, Task<T>> operation, Func<T, bool> ready,
        Func<Exception, bool> retryable, TimeSpan timeout, string condition)
    {
        if (timeout <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(timeout));
        var clock = Stopwatch.StartNew();
        Exception? lastError = null;
        while (clock.Elapsed < timeout)
        {
            var remaining = timeout - clock.Elapsed;
            if (remaining <= TimeSpan.Zero) break;
            var requestTimeout = remaining < TimeSpan.FromSeconds(5) ? remaining : TimeSpan.FromSeconds(5);
            try
            {
                var result = await operation(requestTimeout).ConfigureAwait(false);
                if (ready(result)) return result;
                lastError = null;
            }
            catch (Exception error) when (retryable(error)) { lastError = error; }
            remaining = timeout - clock.Elapsed;
            if (remaining <= TimeSpan.Zero) break;
            await Task.Delay(remaining < TimeSpan.FromMilliseconds(50) ? remaining : TimeSpan.FromMilliseconds(50))
                .ConfigureAwait(false);
        }
        throw new TimeoutException($"Kafka fixture did not observe {condition} within {timeout}.", lastError);
    }
}
