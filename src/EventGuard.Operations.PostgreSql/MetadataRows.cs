using System.Data.Common;

namespace EventGuard.Operations.PostgreSql;

internal static class MetadataRows
{
    public static WorkSummary Work(DbDataReader r) => new(r.GetGuid(0), (WorkState)r.GetInt32(1), r.GetInt64(2),
        r.GetInt32(3), r.GetInt32(4), Time(r, 5), Time(r, 6), OptionalTime(r, 7), OptionalTime(r, 8),
        MetadataCodes.Reason(r.IsDBNull(9) ? null : r.GetString(9)));
    public static AttemptSummary Attempt(DbDataReader r) => new(r.GetInt32(0), r.GetInt32(1), MetadataCodes.Attempt(r.GetString(2)),
        Time(r, 3), OptionalTime(r, 4), MetadataCodes.Reason(r.IsDBNull(5) ? null : r.GetString(5)));
    public static ReplaySummary Replay(DbDataReader r) => new(r.GetGuid(0), (ReplayStatus)r.GetInt32(1), r.GetInt32(2), r.GetInt64(3), Time(r, 4));
    private static DateTimeOffset Time(DbDataReader r, int i) => r.GetFieldValue<DateTimeOffset>(i);
    private static DateTimeOffset? OptionalTime(DbDataReader r, int i) => r.IsDBNull(i) ? null : Time(r, i);
}
