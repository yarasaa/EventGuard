using System.Globalization;

namespace EventGuard.Dashboard;

public static class Display
{
    public static string DataStatus(EventGuard.Operations.WorkDataStatus status) => status switch
    {
        EventGuard.Operations.WorkDataStatus.Removed => "This event data was removed by the configured retention policy. Its identity and processing history are retained.",
        EventGuard.Operations.WorkDataStatus.TooLarge => "This event exceeds the 64 KiB preview limit.",
        EventGuard.Operations.WorkDataStatus.InvalidJson => "This event cannot be previewed as JSON within the supported depth limit.",
        _ => "The configured reader does not support event data previews."
    };
    public static string DataValueStatus(EventGuard.Operations.WorkDataValueStatus status) => status switch
    {
        EventGuard.Operations.WorkDataValueStatus.Missing => "Field not present",
        EventGuard.Operations.WorkDataValueStatus.NonScalar => "Object and array values are not displayed",
        EventGuard.Operations.WorkDataValueStatus.Truncated => "Value shortened to 256 characters",
        _ => ""
    };
    public static string State(WorkState state) => state switch
    {
        WorkState.Pending => "Pending", WorkState.Processing => "Processing", WorkState.RetryScheduled => "Retry scheduled",
        WorkState.Completed => "Completed", WorkState.DeadLetter => "Dead letter", WorkState.Expired => "Expired", _ => "Unknown"
    };
    public static string Time(DateTimeOffset? time) => time?.ToUniversalTime().ToString("dd MMM yyyy · HH:mm:ss", CultureInfo.GetCultureInfo("en-US")) ?? "—";
    public static string Explanation(WorkState state) => state switch
    {
        WorkState.Pending => "Waiting for a worker or its scheduled processing time.",
        WorkState.Processing => "A processing attempt is active. A lease alone does not authorize another execution.",
        WorkState.RetryScheduled => "Waiting for the next scheduled attempt.",
        WorkState.Completed => "Processing completed and the outcome was persisted.",
        WorkState.DeadLetter => "Automatic retries have stopped. An authorized operator can review eligibility for another attempt.",
        WorkState.Expired => "The processing window for this work item has expired.",
        _ => "The status is not recognized."
    };
}
