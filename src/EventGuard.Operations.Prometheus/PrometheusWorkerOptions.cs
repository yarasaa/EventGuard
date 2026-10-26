namespace EventGuard.Operations.Prometheus;

public sealed class PrometheusWorkerOptions
{
    public string Endpoint { get; set; } = "";
    public string BearerToken { get; set; } = "";
    public int StaleAfterSeconds { get; set; } = 60;
    public int ProgressAfterSeconds { get; set; } = 300;
    public WorkerGroup[] Groups { get; set; } = [];

    public void Validate(string storeId, bool allowLoopbackHttp = false)
    {
        if (!Identifier(storeId) || !Uri.TryCreate(Endpoint, UriKind.Absolute, out var uri)
            || (uri.Scheme != "https" && !(allowLoopbackHttp && uri.Scheme == "http" && uri.Host is "127.0.0.1" or "localhost"))
            || uri.UserInfo.Length != 0 || uri.Query.Length != 0 || uri.Fragment.Length != 0
            || BearerToken.Length > 4096 || BearerToken.Any(c => c < 33 || c > 126)
            || StaleAfterSeconds is < 15 or > 300 || ProgressAfterSeconds is < 30 or > 86400
            || Groups.Length is < 1 or > 16 || Groups.Select(g => g.Key).Distinct(StringComparer.Ordinal).Count() != Groups.Length
            || Groups.Select(g => (g.Job, g.Component)).Distinct().Count() != Groups.Length)
            throw new ArgumentException("Invalid worker monitoring endpoint, credential, time window or group registration.");
        foreach (var group in Groups)
            if (!Identifier(group.Key) || !Identifier(group.Job) || group.Component is not ("consumer" or "outbox")
                || group.ExpectedInstances is < 0 or > 200 || string.IsNullOrWhiteSpace(group.Label) || group.Label.Length > 120)
                throw new ArgumentException("Invalid worker monitoring group.");
    }
    private static bool Identifier(string value) => value.Length is > 0 and <= 100 && value.All(c => char.IsAsciiLetterOrDigit(c) || c is '-' or '_');
}

public sealed class WorkerGroup
{
    public int? ExpectedInstances { get; set; }
    public string Key { get; set; } = "";
    public string Label { get; set; } = "";
    public string Job { get; set; } = "";
    public string Component { get; set; } = "consumer";
}
