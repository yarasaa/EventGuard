using System.Runtime.InteropServices;
using System.Text.Json;
using CatalogPilot.Contracts;
using CatalogPilot.Hosting;
using CatalogPilot.Infrastructure;
using CatalogPilot.Verification;

using var stop = new CancellationTokenSource();
using var termination = PosixSignalRegistration.Create(PosixSignal.SIGTERM, signal => { signal.Cancel = true; stop.Cancel(); });
Console.CancelKeyPress += (_, signal) => { signal.Cancel = true; stop.Cancel(); };
try
{
    await using var store = new PilotStore(new PilotSettings());
    var mode = args.FirstOrDefault() ?? "help";
    switch (mode)
    {
        case "init": await store.InitializeAsync(stop.Token); Console.WriteLine("Pilot schema, read indexes and quorum queues initialized."); break;
        case "api": await CatalogApi.RunAsync(store, stop.Token); break;
        case "worker": await CatalogWorker.RunAsync(store, stop.Token); break;
        case "help": Console.WriteLine("Commands: init | api | worker | inspect | inject <json> | category <name> | replay <event> <request> <version> | release <quarantine> <request> <json> <version>"); break;
        default: Console.WriteLine(JsonSerializer.Serialize(await PilotCommands.RunAsync(args, store, stop.Token), ProductContract.Json)); break;
    }
}
catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
catch (Exception error)
{
    // No input payload or connection-string logging. Keep a type/stack for local diagnostics.
    Console.Error.WriteLine($"Pilot failed: {error.GetType().Name}\n{error.StackTrace}");
    if (error is Npgsql.PostgresException postgres) Console.Error.WriteLine($"SQLSTATE: {postgres.SqlState}");
    Environment.ExitCode = 1;
}
