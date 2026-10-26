using System.Diagnostics;
using System.Text.Json;

namespace EventGuard.Store.Conformance;

// Test process ownership: always drain stderr, bound waits, and kill a child left
// behind by a failed assertion before its fixture can be dropped.
public sealed class NativeWorkerProcess : IAsyncDisposable
{
    private readonly Process _process;
    private readonly Task<string> _error;
    public int Id => _process.Id;
    public bool HasExited => _process.HasExited;
    private NativeWorkerProcess(Process process) { _process = process; _error = process.StandardError.ReadToEndAsync(); }
    public static NativeWorkerProcess Start(string project, IReadOnlyDictionary<string,string> environment)
    {
        var root = new DirectoryInfo(AppContext.BaseDirectory);
        while (root is not null && !File.Exists(Path.Combine(root.FullName, "EventGuard.slnx"))) root = root.Parent;
        if (root is null) throw new InvalidOperationException("Repository root unavailable.");
        var output = new DirectoryInfo(AppContext.BaseDirectory);
        var worker = Path.Combine(root.FullName, "tests", project, "bin", output.Parent!.Name, output.Name, project + ".dll");
        if (!File.Exists(worker)) throw new InvalidOperationException("Native worker must be built for this configuration and framework.");
        var dotnet = Environment.GetEnvironmentVariable("DOTNET_ROOT");
        var executable = string.IsNullOrEmpty(dotnet) ? "dotnet" : Path.Combine(dotnet, OperatingSystem.IsWindows() ? "dotnet.exe" : "dotnet");
        var start = new ProcessStartInfo(executable)
        { UseShellExecute = false, RedirectStandardInput = true, RedirectStandardOutput = true, RedirectStandardError = true };
        start.ArgumentList.Add(worker);
        foreach (var (key, value) in environment) start.Environment[key] = value;
        return new(Process.Start(start) ?? throw new InvalidOperationException("Native worker could not start."));
    }
    public async Task SendAsync(string command, CancellationToken ct)
    { await _process.StandardInput.WriteLineAsync(command.AsMemory(), ct); await _process.StandardInput.FlushAsync(ct); }
    public async Task<JsonElement> ReadAsync(string stage, CancellationToken ct)
    {
        var line = await _process.StandardOutput.ReadLineAsync(ct);
        if (line is null) throw new InvalidOperationException("Native worker ended before signal: " + await _error.WaitAsync(ct));
        using var json = JsonDocument.Parse(line);
        Assert.Equal(stage, json.RootElement.GetProperty("stage").GetString());
        Assert.Equal(Id, json.RootElement.GetProperty("pid").GetInt32());
        return json.RootElement.Clone();
    }
    public async Task<int> ExitAsync(CancellationToken ct)
    { await _process.WaitForExitAsync(ct); return _process.ExitCode; }
    public async Task KillAsync(CancellationToken ct)
    {
        Assert.False(HasExited);
        _process.Kill(entireProcessTree: true);
        Assert.NotEqual(0, await ExitAsync(ct));
    }
    public async ValueTask DisposeAsync()
    {
        try
        {
            if (!HasExited) _process.Kill(entireProcessTree: true);
            await _process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
            await _error.WaitAsync(TimeSpan.FromSeconds(5));
        }
        finally { _process.Dispose(); }
    }
}
