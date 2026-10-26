using System.Globalization;
namespace EventGuard.LoadTests;

internal sealed record LoadOptions(string Provider, int Count, int PayloadBytes, int[] Concurrency,
    int Repetitions, int AdmissionConcurrency, int PoolSize, int WarmupCount, int TimeoutSeconds, int Seed, string Output,
    int BackgroundCount, string BackgroundKind,int IdlePolls,
    string Profile,int ArrivalRate,int ArrivalBuffer,int InitialBacklog,int MaximumScheduleLagMs)
{
    public static LoadOptions Parse(string[] args)
    {
        var values = new Dictionary<string,string>(StringComparer.Ordinal);
        for (var i = 0; i < args.Length; i += 2)
            if (i + 1 == args.Length || !args[i].StartsWith("--", StringComparison.Ordinal) || !values.TryAdd(args[i][2..], args[i+1]))
                throw new ArgumentException("Use unique --name value options.");
        string Get(string key, string fallback) => values.Remove(key, out var value) ? value : fallback;
        int Number(string key, int fallback, int min, int max)
        {
            var number = int.Parse(Get(key, fallback.ToString(CultureInfo.InvariantCulture)), CultureInfo.InvariantCulture);
            return number >= min && number <= max ? number : throw new ArgumentOutOfRangeException(key);
        }
        var provider = Get("provider", "postgresql");
        if (provider is not ("postgresql" or "sqlserver")) throw new ArgumentException("Provider must be postgresql or sqlserver.");
        var count = Number("count", 2000, 1, 1_000_000);
        var payload = Number("payload-bytes", 2048, 64, 16384);
        var workers = Get("concurrency", "1,4,8").Split(',').Select(v => int.Parse(v, CultureInfo.InvariantCulture)).ToArray();
        var repeats = Number("repetitions", 3, 1, 10);
        var admission = Number("admission-concurrency", 4, 1, 64);
        var pool = Number("pool-size", 16, 1, 128);
        var warmup = Number("warmup", 100, 0, 10000);
        var timeout = Number("timeout-seconds", 600, 5, 3600);
        var seed = Number("seed", 1729, 1, int.MaxValue);
        var background = Number("background-count",0,0,100000);
        var backgroundKind = Get("background-kind","scheduled");
        var idlePolls = Number("idle-polls",0,0,1000);
        var arrivalOptions = new[] {"arrival-rate","arrival-buffer","initial-backlog","maximum-schedule-lag-ms"}.Any(values.ContainsKey);
        var profile = Get("profile","backlog");
        var rate = Number("arrival-rate",100,1,1_000_000);
        var buffer = Number("arrival-buffer",1024,1,65536);
        var initial = Number("initial-backlog",0,0,1_000_000);
        var scheduleLag = Number("maximum-schedule-lag-ms",100,1,60000);
        if (profile is not ("backlog" or "arrivals") || (profile == "backlog" && arrivalOptions)
            || (profile == "arrivals" && (background != 0 || idlePolls != 0 || initial + count > 1_000_000 || count/(double)rate >= timeout)))
            throw new ArgumentException("Arrival profile requires a bounded plan within the case deadline; unavailable background and idle probes are separate profiles.");
        if (backgroundKind is not ("scheduled" or "leased")) throw new ArgumentException("Background kind must be scheduled or leased.");
        var output = Get("output", ".artifacts/load-" + Guid.NewGuid().ToString("N") + ".json");
        if (values.Count != 0 || workers.Length is < 1 or > 8 || workers.Distinct().Count() != workers.Length
            || workers.Any(w => w < 1 || w > 64 || w > pool) || admission > pool) throw new ArgumentException("Invalid option or concurrency/pool budget.");
        if (File.Exists(output)) throw new ArgumentException("Output already exists; select a new result file.");
        return new(provider, count, payload, workers, repeats, admission, pool, warmup, timeout, seed, Path.GetFullPath(output),background,backgroundKind,idlePolls,profile,rate,buffer,initial,scheduleLag);
    }
}
