using CatalogPilot.Domain;
using CatalogPilot.Infrastructure;
using EventGuard;

namespace CatalogPilot.Hosting;

public static class CatalogApi
{
    public static async Task RunAsync(PilotStore store, CancellationToken ct)
    {
        await store.ValidateAsync(ct);
        var builder = WebApplication.CreateBuilder(Array.Empty<string>());
        builder.WebHost.UseUrls($"http://127.0.0.1:{store.Settings.Port}");
        builder.WebHost.ConfigureKestrel(options => options.Limits.MaxRequestBodySize = 4096);
        builder.Logging.ClearProviders(); builder.Logging.AddSimpleConsole(); builder.Logging.SetMinimumLevel(LogLevel.Warning);
        var app = builder.Build(); var queries = new PilotQueries(store);
        // Synthetic, loopback-only pilot. These diagnostic routes have no production authentication contract.
        app.MapGet("/health", () => Results.Ok(new { status = "ready", schema = store.Settings.Schema }));
        app.MapPost("/products", async (ProductChanged value, CancellationToken token) =>
        {
            try
            {
                var result = await store.SubmitAsync(value, token);
                return Results.Json(new { status = result.Status.ToString(), result.RequestId },
                    statusCode: result.Status == ProducerStatus.RequestConflict ? 409 : 202);
            }
            catch (IngressMessageRejectedException error) { return Results.BadRequest(new { reason = error.ReasonCode }); }
        });
        app.MapGet("/events/{id:guid}", (Guid id, CancellationToken token) => queries.EventAsync(id, token));
        app.MapGet("/products/{id}", async (string id, CancellationToken token) =>
            await queries.ProductAsync(id, token) is { } item ? Results.Ok(item) : Results.NotFound());
        app.MapGet("/summary", (CancellationToken token) => queries.SummaryAsync(token));
        app.MapGet("/quarantine", (CancellationToken token) => queries.QuarantineAsync(token));
        Console.WriteLine($"Pilot API listening on http://127.0.0.1:{store.Settings.Port}");
        await app.RunAsync(ct);
    }
}
