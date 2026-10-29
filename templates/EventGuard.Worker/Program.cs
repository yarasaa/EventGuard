using System.Text.Json;
using EventGuard;
using EventGuard.Hosting;
using EventGuard.PostgreSql;
using EventGuard.SqlServer;
using EventGuardWorker.Configuration;
using EventGuardWorker.Contracts;
using EventGuardWorker.Handlers;
using EventGuardWorker.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;

var command = args.FirstOrDefault() ?? "run";
if (command is not ("run" or "migrate" or "check" or "submit" or "status"))
{
    Console.Error.WriteLine("Usage: run | migrate | check | submit | status <work-id>");
    return 2;
}
if (command == "status" && (args.Length < 2 || !Guid.TryParse(args[1], out _)))
{
    Console.Error.WriteLine("status requires the work ID returned by submit.");
    return 2;
}

try
{
    var builder = Host.CreateApplicationBuilder(args.Skip(command == "status" ? 2 : 1).ToArray());
    var settings = WorkerSettings.Load(builder.Configuration);
    var connection = Environment.GetEnvironmentVariable("EVENTGUARD_CONNECTION");
    if (string.IsNullOrWhiteSpace(connection)) throw new WorkerConfigurationException("Set EVENTGUARD_CONNECTION using your secret provider or local shell.");
    builder.Services.AddSingleton(settings);
    // Processing overrides the legacy top-level concurrency/lease defaults; keep identity settings stable.
    var eventGuard = builder.Services.AddEventGuard(builder.Configuration, "EventGuard:Processing");
    if (settings.Provider == "PostgreSql")
    {
        builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(connection));
        builder.Services.AddSingleton(sp => new PostgreSqlWorkStore(sp.GetRequiredService<NpgsqlDataSource>(), settings.Schema));
        eventGuard.AddJsonConsumer<ProductChanged, PostgreSqlSession, PostgreSqlProductHandler>("products", settings.ConsumerOptions(),
            sp => sp.GetRequiredService<PostgreSqlWorkStore>());
    }
    else
    {
        builder.Services.AddSingleton(_ => new SqlServerDataSource(connection));
        builder.Services.AddSingleton(sp => new SqlServerWorkStore(sp.GetRequiredService<SqlServerDataSource>(), settings.Schema));
        eventGuard.AddJsonConsumer<ProductChanged, SqlServerSession, SqlServerProductHandler>("products", settings.ConsumerOptions(),
            sp => sp.GetRequiredService<SqlServerWorkStore>());
    }
    using var host = builder.Build();
    if (command == "run")
    {
        var status = host.Services.GetRequiredService<EventGuardHostStatus>();
        await host.RunAsync();
        return status.Failure is null ? 0 : 1;
    }

    // Commands build the container but never start hosted consumers.
    using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(2));
    ConsoleCancelEventHandler cancel = (_, e) => { e.Cancel = true; deadline.Cancel(); };
    Console.CancelKeyPress += cancel;
    try
    {
        if (command == "migrate")
        {
            await DatabaseSetup.MigrateAsync(host.Services, settings, deadline.Token);
            Console.WriteLine("SchemaReady");
        }
        else if (command == "check")
        {
            var report = await DatabaseSetup.CheckAsync(host.Services, settings, deadline.Token);
            Console.WriteLine(JsonSerializer.Serialize(report));
        }
        else
        {
            var consumer = host.Services.GetRequiredKeyedService<IEventGuardConsumer<ProductChanged>>("products");
            if (command == "submit")
            {
                // A stable ID makes repeating this example return Duplicate. New events need new stable IDs.
                var accepted = await consumer.SubmitAsync(new(Guid.Parse("b5b0288e-b5c4-4bc9-8c78-9d0016a3c101"), 1,
                    "book-001", 18, "Example book"), deadline.Token);
                Console.WriteLine($"{accepted.Status} {accepted.Work.Id:D}");
            }
            else
            {
                var work = await consumer.FindAsync(Guid.Parse(args[1]), deadline.Token);
                Console.WriteLine(work is null ? "NotFound" : $"{work.State} attempts={work.AttemptCount}");
                if (work is null) return 3;
            }
        }
    }
    finally { Console.CancelKeyPress -= cancel; }
    return 0;
}
catch (WorkerConfigurationException error)
{
    Console.Error.WriteLine(error.Message);
    return 1;
}
catch (EventGuardConfigurationException error)
{
    Console.Error.WriteLine(error.Message);
    return 1;
}
catch (Exception error)
{
    // Do not print native exceptions: they can include SQL, message data or credentials.
    Console.Error.WriteLine($"{command} failed ({error.GetType().Name}). Check configuration, connection, schema migration and permissions.");
    return 1;
}
