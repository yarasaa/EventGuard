using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;

namespace EventGuard.Operations.Prometheus;

// A bounded optional read adapter. It neither contacts workers nor changes durable work state.
public sealed partial class PrometheusWorkerMonitor : IWorkerMonitor, IWorkerHistoryMonitor, IDisposable
{
    private const int MaximumSeries = 4096, MaximumBytes = 2 * 1024 * 1024, MaximumRows = 200;
    private readonly HttpClient _client;
    private readonly TimeProvider _time;
    private readonly CancellationTokenSource _stopping = new();
    private readonly WorkerGroup[] _groups;
    private readonly string _storeId, _query;
    private readonly Uri _endpoint;
    private readonly string _token;
    private readonly int _staleSeconds, _progressSeconds;
    private readonly object _gate = new();
    private Task<WorkerFleetSnapshot>? _probe;
    private WorkerFleetSnapshot? _last;
    private long _checked;
    private readonly Dictionary<(string Group, string Instance), (WorkerObservation Row, long Seen)> _known = new();

    public PrometheusWorkerMonitor(PrometheusWorkerOptions options, string storeId, bool allowLoopbackHttp = false)
        : this(options, storeId, new HttpClient(new SocketsHttpHandler
        {
            AllowAutoRedirect = false, UseCookies = false, UseProxy = false, AutomaticDecompression = DecompressionMethods.None,
            MaxConnectionsPerServer = 1, ConnectTimeout = TimeSpan.FromSeconds(2), PooledConnectionLifetime = TimeSpan.FromMinutes(5)
        }) { Timeout = Timeout.InfiniteTimeSpan }, TimeProvider.System, allowLoopbackHttp) { }

    internal PrometheusWorkerMonitor(PrometheusWorkerOptions options, string storeId, HttpClient client, TimeProvider time, bool allowLoopbackHttp = false)
    {
        options.Validate(storeId, allowLoopbackHttp);
        _client = client; _time = time; _storeId = storeId; _token = options.BearerToken;
        _endpoint = new Uri(options.Endpoint.TrimEnd('/') + "/api/v1/query");
        _staleSeconds = options.StaleAfterSeconds; _progressSeconds = options.ProgressAfterSeconds;
        _groups = options.Groups.Select(g => new WorkerGroup { Key = g.Key, Label = g.Label, Job = g.Job, Component = g.Component, ExpectedInstances = g.ExpectedInstances }).ToArray();
        var labels = "job=~\"" + string.Join("|", _groups.Select(g => g.Job).Distinct(StringComparer.Ordinal)) + "\",eventguard_store=\"" + storeId + "\"";
        var names = string.Join("|", MetricNames.Where(n => n != ScrapeTimestamp && !n.EndsWith("_per_second", StringComparison.Ordinal)));
        // Include the metric name in union matching so up cannot hide its timestamp series.
        // timestamp(up) retrieves sample freshness; the instant result timestamp is only query evaluation time.
        _query = "{__name__=~\"" + names + "\"," + labels + "} or on(__name__, job, instance, component) label_replace(timestamp(up{" + labels
            + "}),\"__name__\",\"" + ScrapeTimestamp + "\",\"job\",\".*\")";
        foreach (var outcome in OutcomeNames)
            _query += " or on(__name__, job, instance, component) label_replace(rate(" + Prefix + outcome + "_total{" + labels
                + "}[60s]),\"__name__\",\"" + Prefix + outcome + "_per_second\",\"job\",\".*\")";
    }

    public Task<WorkerFleetSnapshot> ReadAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Task<WorkerFleetSnapshot> pending;
        lock (_gate)
        {
            if (_last is not null && _time.GetElapsedTime(_checked) < TimeSpan.FromSeconds(5)) return Task.FromResult(_last);
            if (_probe is not { IsCompleted: false }) _probe = ProbeAsync();
            pending = _probe;
        }
        return pending.WaitAsync(cancellationToken);
    }

    private async Task<WorkerFleetSnapshot> ProbeAsync()
    {
        WorkerFleetSnapshot result;
        try
        {
            using var budget = CancellationTokenSource.CreateLinkedTokenSource(_stopping.Token);
            budget.CancelAfter(TimeSpan.FromSeconds(3));
            var now = _time.GetUtcNow();
            using var request = new HttpRequestMessage(HttpMethod.Post, _endpoint);
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            if (_token.Length > 0) request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _token);
            request.Content = new FormUrlEncodedContent(new Dictionary<string, string>
            {
                ["query"] = _query, ["time"] = (now.ToUnixTimeMilliseconds() / 1000d).ToString("R", CultureInfo.InvariantCulture),
                ["timeout"] = "2s", ["limit"] = (MaximumSeries + 1).ToString(CultureInfo.InvariantCulture)
            });
            using var response = await _client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, budget.Token);
            if (response.StatusCode != HttpStatusCode.OK || response.Content.Headers.ContentLength > MaximumBytes)
                throw new InvalidDataException();
            await using var body = await response.Content.ReadAsStreamAsync(budget.Token);
            using var buffer = new MemoryStream();
            var chunk = new byte[16 * 1024];
            int count;
            while ((count = await body.ReadAsync(chunk, budget.Token)) > 0)
            {
                if (buffer.Length + count > MaximumBytes) throw new InvalidDataException();
                buffer.Write(chunk, 0, count);
            }
            using var json = JsonDocument.Parse(buffer.GetBuffer().AsMemory(0, (int)buffer.Length), new JsonDocumentOptions { MaxDepth = 16 });
            var rows = Parse(json.RootElement, now);
            var stamp = _time.GetTimestamp();
            var merged = new Dictionary<(string Group, string Instance), (WorkerObservation Row, long Seen)>();
            foreach (var (key, known) in _known)
                if (_time.GetElapsedTime(known.Seen, stamp) < TimeSpan.FromMinutes(10))
                    merged[key] = (new(key.Group, key.Instance, WorkerCondition.Missing, known.Row.SampledAt), known.Seen);
            foreach (var row in rows) merged[(row.Group, row.Instance)] = (row, stamp);
            if (merged.Count > MaximumRows) throw new InvalidDataException();
            _known.Clear();
            foreach (var pair in merged) _known.Add(pair.Key, pair.Value);
            var output = merged.Values.Select(v => v.Row).ToList();
            foreach (var group in _groups)
                if (!output.Any(r => r.Group == group.Key)) output.Add(new(group.Key, "", WorkerCondition.Missing));
            result = new(FleetAvailability.Available, _time.GetUtcNow(), output.OrderBy(r => r.Group).ThenBy(r => r.Instance).ToArray());
        }
        catch
        {
            // No backend error, URL, token or last-known healthy value is sent to the browser.
            result = new(FleetAvailability.Unavailable, _time.GetUtcNow(), []);
        }
        lock (_gate) { _last = result; _checked = _time.GetTimestamp(); }
        return result;
    }

    public void Dispose() { _stopping.Cancel(); _client.Dispose(); _stopping.Dispose(); }
}
