using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;
namespace EventGuard.Operations.Prometheus;

public sealed partial class PrometheusWorkerMonitor
{
    private readonly Dictionary<(string Group, WorkerHistoryWindow Window), (WorkerHistorySnapshot Value, long Checked)> _historyCache = new();
    private Task<WorkerHistorySnapshot>? _historyProbe;
    private (string Group, WorkerHistoryWindow Window) _historyKey;
    public Task<WorkerHistorySnapshot> ReadHistoryAsync(string group, WorkerHistoryWindow window, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        var registration = _groups.SingleOrDefault(g => g.Key == group) ?? throw new ArgumentException("Unknown worker group.", nameof(group));
        if (!Enum.IsDefined(window)) throw new ArgumentOutOfRangeException(nameof(window));
        var key = (group, window); Task<WorkerHistorySnapshot> pending;
        lock (_gate)
        {
            if (_historyCache.TryGetValue(key, out var cached) && _time.GetElapsedTime(cached.Checked) < TimeSpan.FromSeconds(15)) return Task.FromResult(cached.Value);
            if (_historyProbe is { IsCompleted: false })
            {
                if (_historyKey != key) return Task.FromResult(EmptyHistory(group, window, FleetAvailability.Unavailable));
                pending = _historyProbe;
            }
            else { _historyKey = key; _historyProbe = pending = ProbeHistoryAsync(registration, window); }
        }
        return pending.WaitAsync(ct);
    }
    private WorkerHistorySnapshot EmptyHistory(string group, WorkerHistoryWindow window, FleetAvailability availability)
    {
        var step = window switch { WorkerHistoryWindow.Hour => 15, WorkerHistoryWindow.SixHours => 90, _ => 300 };
        var seconds = _time.GetUtcNow().ToUnixTimeSeconds(); var until = DateTimeOffset.FromUnixTimeSeconds(seconds - seconds % step);
        return new(availability, group, until.AddHours(-(int)window), until, step, []);
    }
    private string HistoryQuery(WorkerGroup group)
    {
        var labels = $"job=\"{group.Job}\",eventguard_store=\"{_storeId}\"";
        var runtime = labels + $",component=\"{group.Component}\"";
        // Aggregate reporting targets only. Missing samples stay gaps; there is deliberately no `or vector(0)`.
        var healthy = $"(up{{{labels}}} == 1) and on(job,instance) (time() - timestamp(up{{{labels}}}) <= {_staleSeconds}) and on(job,instance) (time() - timestamp(up{{{labels}}}) >= -5)";
        var fresh = $"(time() - {Prefix}observed_timestamp_seconds{{{runtime}}} <= {_staleSeconds}) and on(job,instance,component) (time() - {Prefix}observed_timestamp_seconds{{{runtime}}} >= -5) and on(job,instance,component) (time() - {Prefix}telemetry_created_timestamp_seconds{{{runtime}}} >= 60)";
        return string.Join(" or ", OutcomeNames.Select(outcome => $"label_replace(sum(rate({Prefix}{outcome}_total{{{runtime}}}[60s]) and on(job,instance) ({healthy}) and on(job,instance,component) ({fresh})),\"outcome\",\"{outcome}\",\"__name__\",\".*\")"));
    }
    private async Task<WorkerHistorySnapshot> ProbeHistoryAsync(WorkerGroup group, WorkerHistoryWindow window)
    {
        var result = EmptyHistory(group.Key, window, FleetAvailability.Unavailable);
        try
        {
            using var budget = CancellationTokenSource.CreateLinkedTokenSource(_stopping.Token); budget.CancelAfter(TimeSpan.FromSeconds(3));
            using var request = new HttpRequestMessage(HttpMethod.Post, new Uri(_endpoint, "query_range"));
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            if (_token.Length > 0) request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _token);
            request.Content = new FormUrlEncodedContent(new Dictionary<string, string>
            {
                ["query"] = HistoryQuery(group), ["start"] = result.From.ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture),
                ["end"] = result.Until.ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture), ["step"] = result.StepSeconds.ToString(CultureInfo.InvariantCulture),
                ["timeout"] = "2s", ["limit"] = "6"
            });
            using var response = await _client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, budget.Token);
            const int limit = 256 * 1024;
            if (response.StatusCode != HttpStatusCode.OK || response.Content.Headers.ContentLength > limit) throw new InvalidDataException();
            await using var stream = await response.Content.ReadAsStreamAsync(budget.Token); using var bytes = new MemoryStream();
            var chunk = new byte[8192]; int count;
            while ((count = await stream.ReadAsync(chunk, budget.Token)) > 0)
            { if (bytes.Length + count > limit) throw new InvalidDataException(); bytes.Write(chunk, 0, count); }
            using var json = JsonDocument.Parse(bytes.GetBuffer().AsMemory(0, (int)bytes.Length), new JsonDocumentOptions { MaxDepth = 12 });
            result = result with { Availability = FleetAvailability.Available, Series = ParseHistory(json.RootElement, result) };
        }
        catch { /* Never expose backend diagnostics, credentials or a last-known healthy chart. */ }
        lock (_gate) _historyCache[(group.Key, window)] = (result, _time.GetTimestamp()); // At most 16 registered groups × 3 windows.
        return result;
    }
    private static IReadOnlyList<WorkerHistorySeries> ParseHistory(JsonElement root, WorkerHistorySnapshot frame)
    {
        if (root.GetProperty("status").GetString() != "success" || root.TryGetProperty("warnings", out var warnings) && warnings.GetArrayLength() != 0
            || root.TryGetProperty("infos", out var infos) && infos.GetArrayLength() != 0) throw new InvalidDataException();
        var data = root.GetProperty("data"); if (data.GetProperty("resultType").GetString() != "matrix") throw new InvalidDataException();
        var items = data.GetProperty("result"); if (items.GetArrayLength() > 5) throw new InvalidDataException();
        var length = checked((int)((frame.Until - frame.From).TotalSeconds / frame.StepSeconds) + 1);
        var values = OutcomeNames.ToDictionary(o => o, _ => new double?[length]); var seen = new HashSet<string>();
        foreach (var series in items.EnumerateArray())
        {
            var metric = series.GetProperty("metric");
            if (metric.EnumerateObject().Count() != 1) throw new InvalidDataException();
            var outcome = metric.GetProperty("outcome").GetString()!;
            if (!values.TryGetValue(outcome, out var points) || !seen.Add(outcome)) throw new InvalidDataException();
            var samples = series.GetProperty("values"); if (samples.GetArrayLength() > length) throw new InvalidDataException();
            var lastIndex = -1;
            foreach (var sample in samples.EnumerateArray())
            {
                if (sample.GetArrayLength() != 2) throw new InvalidDataException();
                var seconds = sample[0].GetDouble(); var index = (seconds - frame.From.ToUnixTimeSeconds()) / frame.StepSeconds;
                if (!double.IsFinite(index) || index != Math.Truncate(index) || index <= lastIndex || index < 0 || index >= length) throw new InvalidDataException();
                lastIndex = (int)index; var text = sample[1].GetString();
                if (text == "NaN") continue;
                if (!double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var value) || !double.IsFinite(value) || value is < 0 or > 1e15) throw new InvalidDataException();
                points[lastIndex] = value;
            }
        }
        return OutcomeNames.Select(o => new WorkerHistorySeries(o, Array.AsReadOnly(values[o].Select((value, index) => new WorkerHistoryPoint(frame.From.AddSeconds(index * frame.StepSeconds), value)).ToArray()))).ToArray();
    }
}
