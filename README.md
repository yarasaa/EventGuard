# EventGuard

<img src="https://raw.githubusercontent.com/yarasaa/EventGuard/main/src/EventGuard.Dashboard/wwwroot/eventguard.png" width="112" height="112" alt="EventGuard: protected event flow" />

Reliable event processing for **.NET 8 and .NET 10**, with PostgreSQL or SQL Server,
Kafka or RabbitMQ, and an optional operations dashboard.

EventGuard records incoming work, recognizes repeated messages, commits application
writes with processing state, and provides durable retry and recovery requests. Use
it for product synchronization, document indexing, notifications, integrations, or
other event-driven applications. Your application defines its models and business rules.

**Status:** experimental `0.1.0-dev.28`. The name, API and schema are provisional.
Install the exact preview version from nuget.org, or build from source using the local feed below. The optional modules add
typed jobs, consistent source snapshots, batches, external-service result handling and durable
workflows. Your application supplies its business models, queries and service adapters.

Base schemas: **PostgreSQL 11 / SQL Server 8**. Optional schemas:
**Jobs 2 / External actions 4 / Snapshots 1 / Workflows 1**. Enable only the modules you need.
Local correctness checks have passed; production deployment and capacity qualification remain
open. See [known limitations](#known-limitations) before adopting. **License:** [MIT](LICENSE).

## Start here

| What you want to do | Read this first |
| --- | --- |
| See the complete HTTP → broker → worker → dashboard flow | [Quick start](#quick-start) |
| Add EventGuard to a new or existing .NET application | [Hosted worker quick start](#hosted-worker-quick-start), then [adapt it to your project](#adapt-it-to-your-project) |
| Configure login, scopes and the operations UI | [Configure the dashboard](#configure-the-dashboard) |

You can run a consumer without a broker or dashboard. Add the transport when you need
broker ingestion; add the outbox when your application must publish events atomically
with business writes. Start with the path you need rather than configuring every module.

## Contents

- [What it provides](#what-it-provides)
- [Quick start](#quick-start)
- [Use the packages in your application](#use-the-packages-in-your-application)
- [Hosted worker quick start](#hosted-worker-quick-start)
- [Add hosting to an existing application](#add-hosting-to-an-existing-application)
- [Your first consumer (low-level API)](#your-first-consumer)
- [Adapt it to your project](#adapt-it-to-your-project)
- [Message models and identity](#message-models-and-identity)
- [Process business data](#process-business-data)
- [Publish with the transactional outbox](#publish-with-the-transactional-outbox)
- [Connect Kafka or RabbitMQ](#connect-kafka-or-rabbitmq)
- [Retry and recovery](#retry-and-recovery)
- [Configure the dashboard](#configure-the-dashboard)
- [Monitoring and deployment](#monitoring-and-deployment)
- [Retention and tenant budgets](#retention-and-tenant-budgets)
- [Architecture and extension points](#architecture-and-extension-points)
- [Typed jobs, batches and external services](#typed-jobs-batches-and-external-services)
- [Composable execution modules](#composable-execution-modules)
- [Testing](#testing)
- [Load experiments](#load-experiments)
- [Troubleshooting](#troubleshooting)
- [Contributing and release preparation](#contributing-and-release-preparation)
- [Known limitations](#known-limitations)

## What it provides

| Area | Available behavior |
| --- | --- |
| Incoming work | Durable acceptance, scoped deduplication, changed-content conflicts, attempt history |
| Business transactions | Application SQL, completion and outgoing intents in one local database transaction |
| Producer outbox | Business SQL, stable producer request receipt and outgoing intents committed together |
| Execution | Bounded concurrency, retry policy, deadlines, native locks and lease recovery |
| Recovery | Idempotent replay, incoming quarantine/release, immutable outgoing-envelope repair |
| Operations | English dashboard, filters, previous/next pages, selected event fields, audit and bulk replay |
| Monitoring | Optional runtime telemetry, Prometheus worker observations, history and expected instance counts |
| Maintenance | Bounded payload retention and shared tenant admission budgets within one store |

The current adapter set is **Kafka/RabbitMQ × PostgreSQL/SQL Server**. The versioned
[support matrix](eng/support-matrix.json) records tested server majors, transaction boundaries
and unsupported features; the repository check verifies its framework and schema versions. Interfaces allow
additional adapters, but an unsupported broker or database is not automatically supported.
An optional EF Core bridge uses the existing business transaction. Optional static workflow coordination is described below. There is no distributed saga
engine, HTTP/webhook adapter, or external blob-storage module in this version. The pilot's
HTTP API is application code.

Business writes must use the transaction supplied by EventGuard. Independent database
connections, HTTP calls, emails and file writes are outside that transaction. An outgoing
message can be delivered more than once when a broker outcome is uncertain; receiving
applications still need deduplication. `Published` means transport acceptance, not
completion by a downstream service.

## Quick start

### 1. Build locally

From a Git checkout of this repository, use macOS/Linux, Git, Python 3.12+, Bash and Docker with
Compose. If you downloaded a source ZIP, extract it and run `git init` in its root first;
the repository check uses Git to identify source files and exclude local artifacts. Native Windows execution has not been verified; WSL2 is an option. The bootstrap
installs the pinned SDK/runtime under `.tools` and verifies official download checksums.
Network access is needed on a fresh machine. Run repository commands from its root;
open each additional pilot terminal in that same directory.
The bootstrap installs a local SDK, not a global `dotnet` command. Repository scripts
use it automatically. For commands in your own application, use a compatible installed
SDK or run `export PATH="/absolute/path/to/EventGuard/.tools/dotnet:$PATH"` after replacing
the checkout path.

```bash
python3 eng/bootstrap_dotnet.py
bash eng/docker.sh compose up -d --wait postgres rabbitmq
bash eng/build.sh
```

The pinned SDK is **10.0.401**; the .NET/ASP.NET Core runtime versions are **8.0.31** and
**10.0.12**. Build uses locked dependencies and creates sixteen packages in
`.artifacts/packages`. This command builds and packages; use [the test command](#testing)
to run the full native test suite.

Local Compose services use disposable development credentials. PostgreSQL listens on
`127.0.0.1:55432`, RabbitMQ on `127.0.0.1:5673`, and RabbitMQ management on
`http://127.0.0.1:15673`. Database and vhost are `eventguard_test`; the development
account is `eventguard` / `eventguard-local-only`. These settings are for local fixtures.

### 2. Run the product catalog pilot

The [CatalogPilot application](pilots/CatalogPilot) consumes the NuGet packages without
project references. It demonstrates a real end-to-end flow:

```text
HTTP product change → product data + outbox → RabbitMQ → durable incoming work
→ search projection + Completed + outcome outbox → RabbitMQ
```

The search projection is a PostgreSQL table; no external search engine is required.
Build the application and initialize its separate schema and queues:

```bash
bash pilots/CatalogPilot/pilot.sh build
export PILOT_SCHEMA=pilot_catalog_manual
bash pilots/CatalogPilot/pilot.sh init
bash pilots/CatalogPilot/pilot.sh api
```

In a second terminal, start the worker with the same schema:

```bash
export PILOT_SCHEMA=pilot_catalog_manual
bash pilots/CatalogPilot/pilot.sh worker
```

In a third terminal, submit a product change and inspect its result:

```bash
curl http://127.0.0.1:5194/products \
  -H 'Content-Type: application/json' \
  --data '{"eventId":"3020c292-00ec-4821-bb73-54a767c4d7ef","contractVersion":1,"productId":"book-001","revision":18,"name":"Distributed systems handbook","category":"general"}'

curl http://127.0.0.1:5194/events/3020c292-00ec-4821-bb73-54a767c4d7ef
curl http://127.0.0.1:5194/products/book-001
```

Processing is asynchronous. Repeat the **same body and event ID**: the producer returns
`AlreadyCommitted`. Change the content while retaining that ID: it returns HTTP 409.
Send a new event ID with revision 17 for this product: the projection remains at 18.
That revision condition is the pilot's application rule, not a mandatory generic field.

| Request | Expected result |
| --- | --- |
| First POST with the shown body | HTTP 202; `status` is `Committed` |
| Identical POST again | HTTP 202; `status` is `AlreadyCommitted` |
| Changed body with the same event ID | HTTP 409; `status` is `RequestConflict` |
| GET `/events/<event-id>` after processing | `work.state` is `Completed`; `effects` is 1 |
| GET `/products/book-001` after processing | The product projection has revision 18 |

Immediately after POST, `work` can be null or still pending. Poll the event status until
Completed; HTTP 202 confirms the producer commit, not the worker's completion. Use Ctrl+C
to stop the API, worker and dashboard when you finish. Restart with the same schema to
preserve work and identity history.

### 3. Inspect your pilot in the dashboard

In another terminal:

```bash
PILOT_SCHEMA=pilot_catalog_manual bash pilots/CatalogPilot/dashboard.sh
```

Open **http://127.0.0.1:5188/?scope=catalog**. It reads the actual pilot's work, attempts,
outgoing intents, quarantine and audit records. Details include the intended operation
and explicitly selected product fields. This profile is Development-only and loopback-only.

For automated acceptance, stop your manual API on port 5194, then run:

```bash
bash pilots/CatalogPilot/pilot.sh verify --framework both
```

The runner creates a unique `pilot_catalog_*` schema and two quorum queues per framework,
starts its own API/workers, injects controlled local faults, and stops its own processes.
Results are in `.artifacts/catalog-pilot/<run>/result.json`. Schemas, queues and evidence
are retained for inspection. Use the result's schema with `dashboard.sh` to inspect it.
The runner requires the local RabbitMQ management endpoint for acknowledgement counters.

For manual business-failure recovery, submit a new event in an unconfigured category,
observe `DeadLetter`, configure that category with
`bash pilots/CatalogPilot/pilot.sh category books`, then run
`bash pilots/CatalogPilot/pilot.sh replay <event-id> <stable-request-id> <expected-work-version>`.
These are trusted local CLI commands, not unauthenticated HTTP mutation endpoints.

To run a CLI mode on .NET 8, set `PILOT_FRAMEWORK=net8.0`. The default is `net10.0`.
Pilot configuration and safety guards are in [PilotSettings](pilots/CatalogPilot/Hosting/PilotSettings.cs).
The package-only application can be copied elsewhere by updating its local feed/cache
paths in [NuGet.Config](pilots/CatalogPilot/NuGet.Config) and the feed root in
[prepare-restore.py](pilots/CatalogPilot/prepare-restore.py).
The pilot keeps third-party versions and hashes locked. For the six packages built from
this checkout, restore preparation verifies local archive identities and records their
hashes in a generated lock under `.artifacts`; NuGet then uses locked mode and a cache
isolated by that lock's hash. This avoids reusing stale packages or requiring archives
built on another machine to have the same hash. Normal consumers of published packages
should retain their ordinary immutable package lock files.

## Use the packages in your application

For the published preview, install the packages directly from nuget.org; cloning or
building this repository is not required. Keep all EventGuard packages on the same exact
preview version. If your application has a `NuGet.Config`, ensure nuget.org is enabled
and preserve any other feeds it needs:

```xml
<configuration>
  <packageSources>
    <clear />
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
  </packageSources>
</configuration>
```

```bash
dotnet add package EventGuard.PostgreSql --version 0.1.0-dev.28
dotnet add package EventGuard.Runtime --version 0.1.0-dev.28
dotnet add package EventGuard.RabbitMq --version 0.1.0-dev.28
```

Nuget.org also supplies native-driver and framework dependencies. For repeatable restore,
enable `RestorePackagesWithLockFile` and commit your application's package lock file.

For source development or an unpublished candidate, [build locally](#1-build-locally),
then add `<add key="eventguard-local" value="/absolute/path/to/EventGuard/.artifacts/packages" />`
to `packageSources`, replacing the path with your checkout. The pilot demonstrates
package source mapping and an isolated cache for this development feed.

| Package | Purpose |
| --- | --- |
| `EventGuard.Core` | Identity, models, lifecycle rules and provider ports; no broker/DB/ORM driver |
| `EventGuard.Runtime` | `WorkPump<TSession>`, `OutboxDispatcher`, typed JSON contract, optional telemetry |
| `EventGuard.Hosting` | Optional `AddEventGuard`, scoped typed handlers and supervised hosted workers |
| `EventGuard.Hosting.Kafka` / `.RabbitMq` | Typed consumer + supervised broker ingress registration |
| `EventGuard.EntityFrameworkCore` | Optional EF Core bridge into the session's existing transaction |
| `EventGuard.PostgreSql` | PostgreSQL work store, transactional producer/outbox and maintenance |
| `EventGuard.SqlServer` | SQL Server equivalents and native transaction profile |
| `EventGuard.Kafka` | Durable ingress and bounded native publisher |
| `EventGuard.RabbitMq` | Durable ingress and mandatory confirmed publication |
| `EventGuard.Operations` | Authorized-host-facing query/command contracts and metadata models |
| `EventGuard.Operations.PostgreSql` / `.SqlServer` | Independent bounded metadata readers |
| `EventGuard.Operations.Replay.PostgreSql` / `.SqlServer` | Optional replay, recovery and bulk command adapters |
| `EventGuard.Operations.Prometheus` | Bounded worker observation/history queries |

Core is included transitively by the adapters. The dashboard is a separate ASP.NET Core
host in [src/EventGuard.Dashboard](src/EventGuard.Dashboard), not an embedded dashboard
NuGet package. All packages target modern .NET 8/10, not .NET Framework 4.x.

## Hosted worker quick start

This is the recommended starting point for a Worker Service or ASP.NET Core application.
The [worker template](templates/EventGuard.Worker) separates contracts, handlers,
configuration and database setup. It consumes packages without project references and
supports PostgreSQL and SQL Server. The default is a PostgreSQL product projection;
replace that business model/handler with your own.

The template is distributed in the source repository. Clone it, then run these commands
with `dotnet` on your PATH; building the libraries is unnecessary when using the published
preview. Create the application **outside the EventGuard checkout** so it does not
inherit this repository's central build settings:

```bash
# In the EventGuard checkout:
dotnet new install ./templates/EventGuard.Worker

# In your own projects directory outside this checkout:
dotnet new eventguard-worker --name MyEventWorker --framework net10.0
cd MyEventWorker
```

Use `--framework net8.0` for .NET 8. Configure `NuGet.Config` as shown in
[package installation](#use-the-packages-in-your-application), using nuget.org for the
published preview or the optional local feed for source development. Then:

```bash
dotnet restore
dotnet build -c Release --no-restore
export EVENTGUARD_CONNECTION='Host=127.0.0.1;Port=55432;Database=eventguard_test;Username=eventguard;Password=eventguard-local-only'
dotnet run -c Release --no-build -- migrate
dotnet run -c Release --no-build -- check
dotnet run -c Release --no-build -- submit
```

`migrate` prints `SchemaReady`. `check` reports schema version **11** for PostgreSQL or
**8** for SQL Server. `submit` prints `Accepted <work-id>`; repeating it prints
`Duplicate <same-work-id>`. The sample deliberately uses a fixed message ID. A real producer
must assign a stable ID to each new logical event and preserve it across retries.

Open a second terminal in the generated application's directory, set the same environment
variables, and start the worker:

```bash
export EVENTGUARD_CONNECTION='Host=127.0.0.1;Port=55432;Database=eventguard_test;Username=eventguard;Password=eventguard-local-only'
dotnet run -c Release --no-build -- run
```

In the first terminal, replace the ID below with the one returned by `submit`:

```bash
dotnet run -c Release --no-build -- status YOUR_WORK_ID
```

Expected result: `Completed attempts=1`. Before the worker starts it is `Pending attempts=0`.
Ctrl+C/SIGTERM requests cancellation and waits within the host's 30-second shutdown budget.
The generated handler uses the supplied native transaction and guards the product revision;
revision 17 cannot overwrite 18. Handler cancellation rolls back its transaction and leaves
work eligible for recovery; it does not force completion. Handlers must honor cancellation.

All commands return nonzero on failure. Missing/invalid configuration reports the relevant
setting; native errors omit SQL, payloads and connection details. `check` validates the
EventGuard protocol schema/permissions; it does not prove your application's SQL grants.
Normal worker startup validates but never migrates. Commit the generated `packages.lock.json`
and use `dotnet restore --locked-mode` for repeatable subsequent builds.

For SQL Server, provision a database with RCSI enabled and delayed durability disabled first;
the library does not change those database-wide settings. From the EventGuard checkout,
prepare the disposable local fixture (the script creates `eventguard_test` only if absent
and refuses incompatible existing options):

```bash
bash eng/docker.sh compose up -d --wait sqlserver
bash eng/sqlserver-local-setup.sh
```

Then, in the generated application directory, use:

```bash
export EventGuard__Provider=SqlServer
export EVENTGUARD_CONNECTION='Server=127.0.0.1,51433;Database=eventguard_test;User ID=sa;Password=EventGuard-Local-Only!2026;Encrypt=False;TrustServerCertificate=True'
# Then run the same migrate / check / submit / run / status commands.
```

The credentials above belong only to disposable local fixtures. In your environment,
use migration credentials for `migrate`, then worker credentials for `check` and `run`.
The template's application table and Operations indexes are separate repeatable setup steps
after the provider migration. Run one migration job at a time; application schema changes
belong in your application's migration system. No automatic downgrade is provided.

To inspect these records, run from the EventGuard checkout in another terminal with the
same connection and `EventGuard__*` overrides:

```bash
bash eng/worker-dashboard.sh
```

Open `http://127.0.0.1:5189`. This local dashboard reads the template's `eventguard_worker`
schema; it neither seeds nor migrates it. If you changed appsettings in the generated app,
pass matching overrides to this script. The dashboard needs the same provider, schema,
tenant, consumer, source and event type. Production deployment uses OIDC as described below.

## Add hosting to an existing application

Install `EventGuard.Hosting` and your store provider. Register the data source/store with
DI so the container owns their lifetime. This fragment uses the same contract, handler and
settings classes as the generated template; add it before `builder.Build()`:

```csharp
builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(connection));
builder.Services.AddSingleton(sp =>
    new PostgreSqlWorkStore(sp.GetRequiredService<NpgsqlDataSource>(), settings.Schema));
builder.Services.AddEventGuard()
    .AddJsonConsumer<ProductChanged, PostgreSqlSession, PostgreSqlProductHandler>(
        "products", settings.ConsumerOptions(),
        sp => sp.GetRequiredService<PostgreSqlWorkStore>());
```

Implement `IEventGuardHandler<TMessage, TSession>.HandleAsync` with your business SQL using
`session.CreateCommand(...)`. Each attempt receives an async DI scope; register application
dependencies as scoped. The host also resolves/disposes a handler scope during startup to
catch missing dependencies before processing. Constructors must not perform business effects.
Do not override the handler registration with a singleton if it has scoped/stateful dependencies.

Resolve the named client to submit or find work (ASP.NET endpoints can use
`[FromKeyedServices("products")] IEventGuardConsumer<ProductChanged>`):

```csharp
var consumer = services.GetRequiredKeyedService<IEventGuardConsumer<ProductChanged>>("products");
var result = await consumer.SubmitAsync(message, cancellationToken);
var work = await consumer.FindAsync(result.Work.Id, cancellationToken);
```

`SubmitJsonAsync` accepts original UTF-8 bytes. `Map` returns a `WorkSubmission` for a
broker mapper, for example RabbitMQ's `delivery => consumer.Map(delivery.Body)`. Keep the
broker's configured scope/policy equal to the consumer options. The mapper validates before
durable admission; configure ingress quarantine if rejected input must be retained. Direct
submission throws on invalid input and does not create a quarantine record automatically.
Do not acknowledge broker messages by calling the handler yourself.

`JsonConsumerOptions<T>` requires an explicit `WorkScope`, `IdentityPolicy`,
`JsonMessageContract<T>` and identity selector. The JSON helper accepts one explicit schema
version and an application validation callback. `VersionedMessageContract<T>` remains the
low-level option for upcasting multiple supported versions. There is no implicit latest-version
fallback. The default JSON settings use web naming; optional settings are copied/frozen.
Fingerprinting uses original bytes: reordered properties or whitespace can produce a conflict
for an existing key. Producer retries must preserve bytes; policy migration remains explicit.
Identity selectors/validators must be deterministic, thread-safe and free of external side effects.

For existing ingress/outbox instances registered with DI, supervise their loops in the same host:

```csharp
builder.Services.AddEventGuard()
    .AddWorker("rabbit-input", (sp, ct) => sp.GetRequiredService<RabbitMqIngress>().RunAsync(ct))
    .AddWorker("outbox", (sp, ct) => sp.GetRequiredService<OutboxDispatcher>().RunAsync(ct));
```

`AddWorker` also accepts an optional async validation callback. Names must be unique across
registrations; one durable consumer scope can be registered only once in a process.
Multiple names can use the same message type with different scopes/stores. Hosted consumers
require `IStoreStartupCheck`; unchecked custom stores can still use `WorkPump` directly.
All registered validations finish before any EventGuard loop starts. Typed consumer dependencies,
incompatible capabilities or unmigrated schemas fail startup. Custom `AddWorker` dependency
checks belong in its validation callback. A loop fault/unexpected return
cancels sibling loops and requests host shutdown, allowing the process supervisor to restart
it. Inspect `EventGuardHostStatus.Failure` to choose an exit code or health response; the template
returns exit code 1 on a supervised loop failure. There is no hidden infinite infrastructure retry. Business retries stay in the durable store.
A host stop signals all loops and awaits cooperative cleanup within `HostOptions.ShutdownTimeout`;
configure the container termination grace period above it. External effects are still outside
native database atomicity. The package does not install broker topology, authentication or DDL.

### Typed Kafka and RabbitMQ registration

Install `EventGuard.Hosting.Kafka` or `EventGuard.Hosting.RabbitMq` plus your database
adapter. Each method registers the JSON consumer, its handler scope, ingress and shutdown
management. Use **one** of these alternatives for a given consumer scope:

```csharp
builder.Services.AddEventGuard()
    .AddKafkaJsonConsumer<ProductChanged, PostgreSqlSession, PostgreSqlProductHandler>(
        "products", settings.ConsumerOptions(),
        new ConsumerConfig {
            BootstrapServers = configuration["Kafka:BootstrapServers"],
            GroupId = "product-projection", AutoOffsetReset = AutoOffsetReset.Earliest
        }, ["product.changed"],
        quarantine: new IngressQuarantineOptions("catalog-kafka"),
        storeFactory: sp => sp.GetRequiredService<PostgreSqlWorkStore>());
```

```csharp
builder.Services.AddEventGuard()
    .AddRabbitMqJsonConsumer<ProductChanged, PostgreSqlSession, PostgreSqlProductHandler>(
        "products", settings.ConsumerOptions(), "product-projection", factory => {
            factory.HostName = configuration["RabbitMq:Host"]!;
            factory.UserName = configuration["RabbitMq:User"]!;
            factory.Password = configuration["RabbitMq:Password"]!;
            factory.VirtualHost = configuration["RabbitMq:VirtualHost"]!;
        }, prefetch: 32,
        quarantine: new IngressQuarantineOptions("catalog-rabbitmq"),
        storeFactory: sp => sp.GetRequiredService<PostgreSqlWorkStore>());
```

Add `using EventGuard.Hosting;` and, for Kafka, `using Confluent.Kafka;`. The helpers use
identity from the JSON contract selector; they do not substitute broker headers for that
identity. Kafka tombstones are rejected. Kafka configuration/topics are copied at registration.
Do not retain or mutate RabbitMQ's configured factory. Automatic recovery is disabled;
explicitly enabling it is rejected. TLS/SASL and credentials still belong to your broker
configuration, and topics/queues must already exist. The helper reserves the companion
worker name `<name>.kafka` or `<name>.rabbitmq`; keep the base name at most 119 characters.
Broker connections open when the supervised loop starts. A connection/subscription failure
stops the host; its supervisor chooses restart/backoff. Quarantine is opt-in and requires a
store that implements `IIngressQuarantineStore`. Without it, rejected input stops ingress
without acknowledging/skipping the message. Outbox dispatch remains an explicit `AddWorker`.

### Use an existing EF Core model

Install `EventGuard.EntityFrameworkCore` and your EF database provider. Construct a **fresh**
application `DbContext` for each handler attempt using `session.Connection`. Pass the existing
native transaction to `EventGuardEfCore.ExecuteAsync`; it binds EF, invokes your callback and
saves tracked changes. The store commits the business changes, outbox and Completed state.
An exception/cancellation rolls them back together.

```csharp
using EventGuard.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

// Inside your handler; CatalogDbContext and ApplyProductChange are application-owned.
await using var db = new CatalogDbContext(
    new DbContextOptionsBuilder<CatalogDbContext>()
        .UseNpgsql(session.Connection) // existing connection; context does not own it
        .Options);
await EventGuardEfCore.ExecuteAsync(db, session.Transaction, async (ef, ct) => {
    await ApplyProductChange(ef, message, ct);
    // Optional: await session.Outbox.EnqueueAsync(outgoingMessage, ct);
}, cancellationToken);
```

For SQL Server use `UseSqlServer(session.Connection)` with `SqlServerSession`. Do not use
`EnableRetryOnFailure`, `TransactionScope`, an already bound transaction, or a context with
previously tracked entities. Those configurations are rejected before the callback. Do not
commit/roll back/replace/dispose the native transaction or connection in application code.
Dispose the context after the attempt, including on failure; it must not be reused after
`SaveChanges(false)`. A DI context that creates its own connection cannot share this guarantee.
Use the existing model configuration but construct it on the supplied connection. Run EF
application migrations separately under your migration identity. Domain revision/concurrency
rules still belong to the handler/model; deduplication does not prevent an older update.

| Tested target | EF Core | EF PostgreSQL provider | Npgsql driver | EF SQL Server provider |
| --- | --- | --- | --- | --- |
| .NET 8 | 8.0.31 | 8.0.11 | 8.0.9 | 8.0.31 |
| .NET 10 | 10.0.12 | 10.0.3 | 10.0.3 | 10.0.12 |

Keep EF/provider/driver major versions compatible. In particular, EF PostgreSQL 8.0.11
cannot run on Npgsql 10 even though its NuGet dependency permits that restore. The native
transaction tests cover both rows of this matrix and verify automatic save, explicit save
followed by rollback, outbox atomicity, cancellation and unsafe configuration rejection.
The bridge uses relational EF APIs; other providers remain unqualified.

## Your first consumer

This walkthrough creates a small console application in **your own directory outside
the EventGuard checkout**. It uses PostgreSQL and two packages. It accepts one product
change, persists it, and processes it with the same scope and policy. Kafka/RabbitMQ,
producer outbox and the dashboard are introduced separately below.

After [building the packages](#quick-start) and starting the local PostgreSQL fixture:

```bash
dotnet new console --name MyEventConsumer --framework net10.0
cd MyEventConsumer
```

The pinned SDK creates a .NET 10 project. To target .NET 8 instead, change
`<TargetFramework>net10.0</TargetFramework>` to `net8.0` in `MyEventConsumer.csproj`
after creation; the bootstrap also installs the required .NET 8 runtime.

Create `NuGet.Config` in that directory using the [package-source configuration above](#use-the-packages-in-your-application).
Then install only the packages needed for this consumer:

```bash
dotnet add package EventGuard.PostgreSql --version 0.1.0-dev.28
dotnet add package EventGuard.Runtime --version 0.1.0-dev.28
```

Replace the generated `Program.cs` with this **complete file**:

```csharp
using System.Text.Json;
using EventGuard;
using EventGuard.PostgreSql;
using EventGuard.Runtime;
using Npgsql;

var mode = args.FirstOrDefault();
if (mode is not ("init" or "submit" or "worker" or "status"))
{
    Console.WriteLine("Use: init | submit | worker | status <work-id>");
    return;
}
await using var data = NpgsqlDataSource.Create(
    Environment.GetEnvironmentVariable("EVENTGUARD_CONNECTION")
        ?? throw new InvalidOperationException("Set EVENTGUARD_CONNECTION."));
var store = new PostgreSqlWorkStore(data, "eventguard_onboarding");
var scope = new WorkScope("tenant-a", "catalog-indexer", "catalog-api", "product.changed");
var policy = new IdentityPolicy("event-id/v1", "sha256-bytes/v1");

if (mode == "init")
{
    // Explicit migration command; the worker never runs this block.
    await store.InitializeAsync();
    await using var ddl = data.CreateCommand("""
        CREATE TABLE IF NOT EXISTS eventguard_onboarding.products (
            product_id text PRIMARY KEY, revision bigint NOT NULL, name text NOT NULL
        )
        """);
    await ddl.ExecuteNonQueryAsync();
    Console.WriteLine("Schema ready.");
    return;
}
await store.ValidateStartupAsync(new(scope.Tenant, StoreRole.Consumer));

if (mode == "submit")
{
    // Fixed identity for this demonstration: submitting again repeats the same event.
    var message = new ProductChanged(
        Guid.Parse("6831d70b-c8f8-44b7-b3ce-0d2a940b052d"), "book-001", 18, "Example book");
    byte[] body = JsonSerializer.SerializeToUtf8Bytes(message);
    var submission = new WorkSubmission(
        new(scope, policy, message.EventId.ToString("D")),
        message.EventId, body, ContentFingerprint.FromBytes(body));
    var accepted = await store.AcceptAsync(submission);
    Console.WriteLine($"{accepted.Status}; work ID: {accepted.Work.Id}");
    return;
}
if (mode == "status")
{
    if (args.Length != 2 || !Guid.TryParse(args[1], out var id))
        throw new ArgumentException("Use: status <work-id returned by submit>");
    var work = await store.FindAsync(scope, id);
    Console.WriteLine(work is null ? "Not found in this scope."
        : $"State: {work.State}; attempts: {work.AttemptCount}");
    return;
}

var pump = new WorkPump<PostgreSqlSession>(store, scope,
    async (_, payload, session, ct) =>
    {
        var message = JsonSerializer.Deserialize<ProductChanged>(payload.Span)
            ?? throw new PermanentWorkException("invalid-product");
        if (string.IsNullOrWhiteSpace(message.ProductId)
            || string.IsNullOrWhiteSpace(message.Name) || message.Revision < 0)
            throw new PermanentWorkException("invalid-product");
        // Business SQL runs in EventGuard's transaction, together with Completed.
        await using var command = session.CreateCommand("""
            INSERT INTO eventguard_onboarding.products(product_id, revision, name)
            VALUES (@id, @revision, @name)
            ON CONFLICT (product_id) DO UPDATE
            SET revision = EXCLUDED.revision, name = EXCLUDED.name
            WHERE eventguard_onboarding.products.revision < EXCLUDED.revision
            """);
        command.Parameters.AddWithValue("id", message.ProductId);
        command.Parameters.AddWithValue("revision", message.Revision);
        command.Parameters.AddWithValue("name", message.Name);
        await command.ExecuteNonQueryAsync(ct);
    }, retry: new RetryPolicy(maximumAttempts: 5),
    options: new WorkPumpOptions(maximumConcurrency: 4));

using var stop = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; stop.Cancel(); };
Console.WriteLine("Worker running. Press Ctrl+C to stop.");
await pump.RunAsync(stop.Token);

public sealed record ProductChanged(Guid EventId, string ProductId, long Revision, string Name);
```

The following connection is for the **local development fixture** from the quick start.
For a deployed application, configure secrets and use separate migration/worker identities.

```bash
export EVENTGUARD_CONNECTION='Host=127.0.0.1;Port=55432;Database=eventguard_test;Username=eventguard;Password=eventguard-local-only'
dotnet run -- init
dotnet run -- submit
dotnet run -- submit
```

The first submission prints `Accepted; work ID: ...`; the second prints `Duplicate`
with **the same work ID**. Copy that ID. In the same terminal, start processing:

```bash
dotnet run -- worker
```

In another terminal, open `MyEventConsumer`, set the same `EVENTGUARD_CONNECTION`, and
replace `<work-id>` with the ID printed by `submit`:

```bash
dotnet run -- status <work-id>
```

It should reach `State: Completed; attempts: 1`. The worker stays running for future
input; stop it with Ctrl+C. Submitting again after completion still returns Duplicate.
A new logical change needs a new stable event ID; redelivery of that change keeps its ID.
The stored product can be checked through your SQL client:

```sql
SELECT product_id, revision, name FROM eventguard_onboarding.products;
```

Expected row: `book-001`, `18`, `Example book`. Revision comparison belongs to this
last-state projection; other domains can use different transactional business rules.
The sample `status` mode uses the worker identity for local diagnostics; deployed
operator-facing access should use the authorized Operations APIs.

## Adapt it to your project

| Part you change | Where it belongs | What must stay consistent |
| --- | --- | --- |
| `ProductChanged` and JSON decoding | Your Contracts/application layer | Explicit supported versions and validation; keep the original durable input |
| Scope and identity policy | Your host configuration/composition root | Same tenant/consumer/source/event type and policy for acceptance and processing, including after restart |
| Event ID/business key | Your producer or ingress mapper | A retry/redelivery keeps the same ID; a distinct logical change gets a different ID |
| `session.CreateCommand(...)` business writes | Your handler/infrastructure layer | Use the supplied session for writes that must commit with Completed |
| Database connection and schema | Secrets and migrations | Install schema once explicitly; do not migrate in each worker startup |
| `pump.RunAsync` | Your Worker Service or ASP.NET `BackgroundService` | Await it with the host shutdown token and observe failures |
| Broker queue/topic and mapper | Your ingress composition | Mapper produces the same WorkScope/policy; acknowledge only after durable acceptance |
| Dashboard labels and fields | Separate optional dashboard configuration | Registered scope matches the worker; access is granted separately |

If your application already consumes messages, map each delivery to a `WorkSubmission`,
await `AcceptAsync`, and follow the broker's documented durable-settlement behavior.
Then run the business effect through the pump's handler. Do not also run that same effect
inside the old broker callback, which would bypass EventGuard's transaction boundary.
The supplied adapters already implement their broker-specific settlement protocol.

To add RabbitMQ to the walkthrough, install `EventGuard.RabbitMq`, provision a durable
queue, and use `RabbitMqIngress` instead of the sample's `submit` command. Both ingress
and pump run as supervised tasks in the application host. The pilot's
[CatalogWorker](pilots/CatalogPilot/Hosting/CatalogWorker.cs) shows the complete composition;
[ProductContract](pilots/CatalogPilot/Contracts/ProductContract.cs) shows the mapper, and
[PilotStore](pilots/CatalogPilot/Infrastructure/PilotStore.cs) shows migration and business SQL.
Kafka composition is in the [Kafka sample](samples/EventGuard.Kafka.Sample/Program.cs).

For SQL Server, choose `EventGuard.SqlServer`, `SqlServerDataSource`,
`SqlServerWorkStore` and `WorkPump<SqlServerSession>`, then implement business SQL for
that engine. PostgreSQL's `ON CONFLICT` syntax is not portable. The
[SQL Server sample](samples/EventGuard.SqlServer.Sample/Program.cs) provides the native
transaction setup. Changing a production broker/store also requires a deliberate
identity/data migration; switching package names does not migrate existing work.

The sections below are API explanations and **fragments**, not one combined `Program.cs`.
The complete runnable entry points are this walkthrough and the catalog pilot.

## Message models and identity

Your application defines the message model and serializer. JSON, Avro, Protobuf or
another format can map into `WorkSubmission`. EventGuard does not generate a business
model from a Kafka topic or require a balance/account domain.

```csharp
using EventGuard;

var scope = new WorkScope("tenant-a", "catalog-indexer", "catalog-api", "product.changed");
var policy = new IdentityPolicy("event-id/v1", "sha256-bytes/v1");
var eventId = Guid.Parse("3020c292-00ec-4821-bb73-54a767c4d7ef");
byte[] body = "{\"productId\":\"book-001\",\"revision\":18}"u8.ToArray();

var submission = new WorkSubmission(
    new(scope, policy, eventId.ToString("D")),
    eventId, body, ContentFingerprint.FromBytes(body));
```

Keep scope, business key, policy and event identity stable across restarts. A new pod
name or random consumer name must not produce a new logical scope. Use separate
stores/schemas and deliberate scope configuration for environment isolation.

- Same scoped key and fingerprint: duplicate, with the original input retained.
- Same scoped key and a different fingerprint: conflict; the first payload is preserved.
- Policy binding is durable. Draining a queue does not make changing the key/fingerprint
  rules safe. In-place policy migration is not supplied.
- Byte fingerprints distinguish different serialized representations. If semantic
  canonicalization is needed, supply an explicit stable fingerprint policy.

`VersionedMessageContract<T>` accepts an explicit allowlist of decoders, validates the
application model and maps identity from it. Decoding older input into the current model
preserves the original durable bytes. Unknown versions do not silently become the latest
version. See the [Kafka contract](samples/EventGuard.Kafka.Sample/Contracts/FileUpdated.cs)
and [pilot contract](pilots/CatalogPilot/Contracts/ProductContract.cs).

Deduplication does not order distinct events. For last-state projections, use an atomic
revision comparison in business SQL. Delta operations and ordered workflows need their
own sequencing/reconciliation rules; there is no universal revision or FIFO guarantee.

## Process business data

The [complete consumer walkthrough](#your-first-consumer) demonstrates migration,
acceptance, bounded processing and status lookup. `maximumConcurrency` is per process;
backlog is not loaded into an unbounded task list. With ASP.NET/Worker Service, pass your
host's shutdown token and supervise ingress, pumps and dispatchers together. The pilot's
[worker composition](pilots/CatalogPilot/Hosting/CatalogWorker.cs) is a complete broker example.

The store owns commit. Do not commit/dispose its transaction, swallow transactional
failures, or open an independent connection for writes that must share the guarantee.
`session.Outbox.EnqueueAsync` can add an outgoing intent to the same completion transaction.
Lease expiry or an uncertain commit response alone does not authorize executing business
code again; native row locks, ownership tokens and durable receipts determine recovery.

`RunAsync` validates recognized native stores before starting loops. Direct SDK calls,
manual `DispatchNextAsync`, and standalone ingress hosts should explicitly call the
optional `IStoreStartupCheck.ValidateStartupAsync` during startup. It checks the native
profile/schema and role permissions, not arbitrary business SQL or future config changes.
You can require `StoreCapabilities` or a finite tenant budget through `StoreStartupOptions`.

## Publish with the transactional outbox

Use stable producer request/message/intent IDs. An intent identifies one publication
destination; it is not a broker-generated delivery tag. The following fragment assumes
your `data` source, business table, request ID and event body already exist:

```csharp
var outbox = new PostgreSqlOutboxStore(data, "eventguard");
var producer = new ProducerScope("tenant-a", "catalog-api");
var outgoing = new OutgoingMessage(producer, intentId, eventId,
    new("rabbitmq", "local", "v1", RabbitMqPublisher.DefaultExchange, "catalog.changed"),
    body);
var request = new ProducerRequest(producer, requestId, ContentFingerprint.FromBytes(body));

var result = await outbox.ProduceAsync(request, async (session, ct) =>
{
    await using var command = session.CreateCommand(
        "INSERT INTO eventguard.product_requests(event_id) VALUES(@id)");
    command.Parameters.AddWithValue("id", eventId);
    await command.ExecuteNonQueryAsync(ct);
    await session.Outbox.EnqueueAsync(outgoing, ct);
});
```

Use `EventGuard.RabbitMq` for `RabbitMqPublisher`. `ProduceAsync` saves the business write,
request receipt and intent atomically. Repeating the same committed request returns
`AlreadyCommitted`; it does not invoke the callback again. The fingerprint must cover
all semantically relevant request input, not only an incidental field.

Run an `OutboxDispatcher` with publishers matching the immutable transport/connection/
configuration-version bindings. Database transactions are not held open during broker
publication. Distinguish confirmed acceptance, known non-delivery and unknown delivery.
An expired publication lease may require a repeat send; downstream deduplication remains
necessary. Binding changes need a deployment/migration decision, not an envelope rewrite.

Caller-owned ReadCommitted transactions can use `Enlist`; the caller then owns commit
and business-request idempotency. There is no supplied ORM integration. Complete examples:
[RabbitMQ](samples/EventGuard.RabbitMq.Sample/Program.cs),
[Kafka](samples/EventGuard.Kafka.Sample/Program.cs),
[transactional pilot store](pilots/CatalogPilot/Infrastructure/PilotStore.cs).

## Connect Kafka or RabbitMQ

| Adapter | Required behavior |
| --- | --- |
| Kafka | Manual offset commit/store; a frozen partition next-offset is committed only after durable acceptance or permitted quarantine. Current group profile is classic/eager, `read_committed`, with explicit offset reset. Publication uses native delivery callbacks, idempotence, `acks=all` and bounded pending slots. |
| RabbitMQ | Manual individual acknowledgements after durable acceptance; persistent, mandatory publication with tracked confirms and one in-flight publication per channel. Automatic client recovery is disabled; the host supervises restart/backoff. |

Provision durable topology, routing, credentials and TLS in your application/deployment.
A partial RabbitMQ route failure can still mean unknown delivery. Kafka pending capacity
is not freed by caller cancellation until the native delivery ends. Do not replace these
profiles with automatic settlement merely to increase throughput.

Runnable examples, after starting their corresponding local services and building:

```bash
bash eng/dotnet.sh run --project samples/EventGuard.RabbitMq.Sample -c Release --no-build
bash eng/docker.sh compose up -d --wait kafka
bash eng/dotnet.sh run --project samples/EventGuard.Kafka.Sample -c Release --no-build
```

The small generic `EventGuard.Sample` uses a simulated transport; the Kafka/RabbitMQ
samples and catalog pilot use real brokers. Sample scopes are randomized for isolation;
application scopes must remain stable.

## Retry and recovery

Infrastructure failures and business failures have different meanings. Retry policy
records eligible transient failures; permanent failures or exhausted attempts become
terminal. The default pump stops on an unhandled infrastructure fault and exposes it to
the host. An optional fault callback can report and delay before continuing. Cancellation
does not prove rollback, and arbitrary user handlers must cooperate with shutdown.

| Operation | Rule |
| --- | --- |
| Consumer replay | Stable request ID, authorized actor, reason and expected version; Completed never reopens. Retrying the same request returns its receipt. |
| Outbox replay | Original immutable envelope; Published/Quarantined do not reopen through replay. Uncertain delivery requires explicit duplicate-risk acknowledgement. |
| Incoming quarantine | Only explicit `IngressMessageRejectedException` mapping rejection can be held before settlement. Database/network failures are not permission to acknowledge bad input. Original bounded bytes/native metadata are preserved. |
| Incoming release | Reviewed replacement submission goes through normal identity, policy and acceptance rules. Original raw evidence and a durable release receipt remain. |
| Outbox repair | Restore exactly the original fingerprinted envelope, not a new payload/destination. Preserve raw evidence and receipt; delivery uncertainty remains relevant. |
| Bulk replay | Freeze 1–100 IDs/versions through `IBulkReplayStore`; each member has its own transaction/receipt. Resume the same plan after interruption. Dashboard input is limited to 25 IDs. |

After a timeout, inspect the durable receipt or retry the **same** request. A missing
receipt is not proof that no transaction is running. Changed content under an existing
request ID conflicts. Accepted replay queues work for application workers; the panel
does not run the handler or prove its completion. Authorization belongs to the host;
a scope value or SDK decision object is not permission to issue commands.

## Configure the dashboard

### Local preview and application configuration

For seeded UI examples, run `bash eng/dashboard-demo.sh` and open
http://127.0.0.1:5187. Those records simulate outcomes; the catalog pilot on 5188 shows
actual end-to-end work. The dashboard is optional and runs independently of workers.

Developers configure labels, scopes, operation descriptions, selected data fields and
identity provider through normal ASP.NET configuration. Example `Dashboard` configuration:

```json
{
  "Dashboard": {
    "Provider": "PostgreSql",
    "Schema": "eventguard",
    "StoreId": "catalog-prod",
    "ApplicationLabel": "Product catalog",
    "EnvironmentLabel": "Production",
    "DevelopmentAccess": false,
    "AllowedHosts": ["events.example.com"],
    "Authority": "https://identity.example.com",
    "ClientId": "eventguard-operations",
    "Scopes": [{
      "Key": "catalog",
      "Label": "Catalog indexing",
      "Tenant": "tenant-a",
      "Consumer": "catalog-indexer",
      "Source": "catalog-api",
      "EventType": "product.changed",
      "Operation": "Update the product search projection",
      "Description": "Apply a newer revision and publish an indexing outcome.",
      "DataFields": [
        { "Label": "Product ID", "Pointer": "/productId" },
        { "Label": "Revision", "Pointer": "/revision" }
      ]
    }]
  }
}
```

Supply `Dashboard__ConnectionString` and `Dashboard__ClientSecret` through secrets.
Use `Provider=SqlServer` for SQL Server. Register outgoing producers in `OutboxScopes`
with `Key`, `Label`, `Tenant`, `Producer` and optional operation text/permissions.
See [DashboardOptions](src/EventGuard.Dashboard/Configuration/DashboardOptions.cs)
for supported settings and [pilot dashboard setup](pilots/CatalogPilot/dashboard.sh)
for a complete local configuration. The application's registered scope must match its worker.

### Authentication and permissions

Production uses your OIDC provider over HTTPS, authorization code + PKCE. Register
`https://events.example.com/signin-oidc` as callback. Accounts, passwords, MFA and password
recovery belong to that provider; EventGuard has no separate user/password database.

Issue exact claims in validated tokens. Below, `catalog-prod/catalog` means
`<StoreId>/<registered scope key>`; outbox claims use the producer scope key.

| Claim | Permission |
| --- | --- |
| `eventguard_scope=catalog-prod/catalog` | Consumer metadata |
| `eventguard_data=catalog-prod/catalog` | Selected event fields, plus consumer read permission |
| `eventguard_replay=catalog-prod/catalog` | Consumer replay, plus consumer read permission |
| `eventguard_recovery=catalog-prod/catalog` | Incoming release, plus consumer read permission |
| `eventguard_audit=catalog-prod/catalog` | Consumer audit, plus consumer read permission |
| `eventguard_outbox=catalog-prod/changes` | Outgoing metadata |
| `eventguard_outbox_replay=catalog-prod/changes` | Outgoing replay, plus outgoing read permission |
| `eventguard_outbox_recovery=catalog-prod/changes` | Envelope repair, plus outgoing read permission |
| `eventguard_outbox_audit=catalog-prod/changes` | Producer audit, plus outgoing read permission |
| `eventguard_monitor=catalog-prod` | Store monitoring; does not grant work/data/recovery access |

Replay/recovery/audit also require the corresponding global feature switch and scope's
`AllowReplay`, `AllowRecovery` or `AllowAudit`. Recovery needs a separately configured
`ReplayConnectionString`; consumer recovery also needs `IngressMonitoringEnabled`.
`BulkReplayEnabled` depends on ordinary consumer or outbox replay being enabled.

Use separate migration, worker, metadata-reader and command database identities. The
read and command connections must target the same store, with separate bounded pools.
Apply `OperationsIndexes`, `OutboxReadIndexes` and, when enabled, `AuditIndexes` scripts
from the selected Operations provider during migration. Dashboard startup validates
schemas/indexes/access and never runs DDL. The pilot's initialization shows this setup.

Production requires HTTPS and exact allowed hosts. Configure shared, persistent,
protected Data Protection keys for multiple pods/restarts using `KeyRingPath`,
`KeyProtectionCertificatePath` and its secret password. Do not blindly trust forwarded
headers. Cookies expire after 30 minutes; immediate permission revocation is not supplied.
Local anonymous access requires an explicit flag, Development environment and exclusive
loopback listener; it is not a production authentication mode.

### Rotate key protection certificates

TLS serving certificates and Data Protection certificates have separate lifecycles. For
Data Protection, `KeyProtectionCertificatePath` selects the certificate used to encrypt
new key files. `KeyDecryptionCertificates` accepts up to eight extra `{ Path, Password }`
entries with absolute PFX paths/private keys, supplied through secrets. For example:

```text
Dashboard__KeyDecryptionCertificates__0__Path=/run/key-protection/previous.pfx
Dashboard__KeyDecryptionCertificates__0__Password=<secret>
```

1. Back up the shared key directory and both certificates/private keys securely. Keep the
   application/store discriminator unchanged.
2. Roll out a bridge configuration to **all** replicas: old certificate remains primary,
   new certificate is an additional decryption certificate. Verify every replica is updated.
3. Roll out new-primary + old-additional configuration. Restart each pod to load mounted
   PFX changes. A rollback must use the bridge configuration so it can read newly written keys.
4. Retain old decryption certificates while any retained key file or recoverable backup
   requires them. Rotating the primary does not re-encrypt old files. Do not delete old
   keys/certificates merely because the login cookie lifetime elapsed.

The local rotation test writes a new encrypted key, restarts replicas, reads tokens from
before/after rotation and checks the bridge rollback. OpenShift service-CA renewal and
router TLS rotation require separate live-cluster checks; this test does not establish them.

### Screens, data and commands

Work filters include scope, state, exact work ID, UTC creation interval, minimum attempts
and replay history. Outbox adds intent ID and delivery uncertainty. Previous/next navigation
uses protected cursors tied to the store, scope and filters; list views expire after 15
minutes. Return from details preserves the list position. Concurrent state changes mean
pages are separate reads, not a database snapshot.

`DataFields` is empty by default. Opt-in JSON preview shows at most 16 selected scalar
fields, never whole objects/arrays; input is capped at 64 KiB and depth 32. Selected fields
still need data permission. The allowlist is not a secret/PII detector. Invalid or oversized
input produces an explanation, not a raw payload fallback. Non-JSON processing remains
supported even when this built-in preview cannot decode it. Operation descriptions are
current configuration, not a historical business before/after audit.

Replay/recovery uses reason entry, review and CSRF-protected submission. Reviewed commands
bind actor, identity, version and duplicate-risk acknowledgement. Audit reads are separately
authorized, contain other operators' recorded reasons, and support at most 31 days / 100
requests per page plus JSONL export. Export is a bounded read, not a compliance snapshot.

Useful read routes: `/api/scopes`, `/api/works`, `/api/works/{scope}/{id}`,
`/api/works/{scope}/{id}/data`, `/api/outbox-scopes`, `/api/outbox`, `/api/quarantine`,
`/api/outbox/{scope}/{id}`, `/api/workers` and `/api/backlog` when configured.
Unauthenticated API access returns 401; unavailable/unauthorized scope data is hidden
with 404. Invalid cursors/filters return 400; exhausted read capacity or unavailable
storage returns 503. Responses are not cached.

## Monitoring and deployment

Runtime telemetry is optional and contains no payload, work ID or tenant labels by
default. Export through an application-owned collector. Use a single telemetry object
per process and promptly buffer/export observations; arbitrary synchronous listeners can
still add latency. Telemetry does not authorize retry, ownership or state changes.

The local demonstration `bash eng/worker-monitor-demo.sh` runs two sample hosts and
Prometheus. It uses separate sample work, not the dashboard's seeded queue. To connect
your deployment, enable `MonitoringEnabled`, configure `Workers:Endpoint` and an allowlist
of `Workers:Groups` with `Key`, `Label`, `Job`, `Component` (`consumer` or `outbox`) and
optional `ExpectedInstances` (0–200). Set `Workers:BearerToken` through secrets. Label
scrape targets with `eventguard_store=<StoreId>` and retain unique job/instance labels.
Preserve cumulative counter semantics and the sample's metric-name normalization.

Worker history offers fixed 1h/6h/24h windows with bounded queries and point counts.
Missing data remains a gap. Expected instances are persisted configuration, not an
autoscaler or per-pod ownership registry. Completion/publication observations do not prove
unique business throughput, an empty queue or a failed worker. `/metrics` uses a separate
secret `MetricsToken`; the dashboard login cookie does not authorize scraping.

Deployment templates are in [deploy/openshift](deploy/openshift). Build the optional host
image after the Release build with `bash eng/dashboard-image.sh`. Its Docker build context
is only the published host output. The image supports non-root/arbitrary UID operation;
the local CRC dashboard/OIDC rehearsal below passed. Supply TLS, OIDC, protected shared keys,
read/command secrets, resources, NetworkPolicy and migration rollout for your environment.
A readiness failure does not automatically stop application background workers.

Current schema versions: **PostgreSQL 11 / SQL Server 8**. Use explicit `InitializeAsync`
under a migration identity to upgrade known older schemas, then coordinate all worker and
command binaries. No downgrade or mixed-version protocol is promised. Dashboard readers
support specific historical layouts; command hosts require the current schema.

PostgreSQL requires a writable primary with fsync, synchronous local WAL commit and
full-page writes enabled. SQL Server requires major version 16+, RCSI enabled, delayed
durability disabled and MARS disabled. Initialization does not change database-wide
settings. SQL Server claim RepeatableRead/READPAST, execution XLOCK and pooled connection
ReadCommitted reset are part of the provider contract. Do not relax durability for speed.

### OpenShift Local

For a local single-node OpenShift cluster, install the official
[OpenShift Local / CRC](https://crc.dev/docs/installing/) package for your CPU architecture.
Complete the installer and obtain your pull secret from Red Hat in your own browser. Then run:

```bash
crc setup
crc start
crc console --url
crc console --credentials
```

The last command displays local console credentials; do not paste its output into chat,
issues or source control. Open the URL reported by CRC and log in as `developer` for
application work. The usual local console address is
`https://console-openshift-console.apps-crc.testing`. This is OpenShift's cluster console;
the optional EventGuard dashboard is a separate application deployment.

Use `eval "$(crc oc-env)"` to select CRC's bundled CLI in each new terminal; this also fixes
`zsh: command not found: oc`. Browser login does not require an `oc login` first.
Select your local context and project
explicitly before applying manifests. Run `crc stop` when finished to release the VM's CPU
and memory. On a 16 GiB Mac, stop other container VMs before starting CRC. Keep free host
disk space for the downloaded bundle, expanded VM and application images; the VM also has
its own disk limit. A pod waiting on `node.kubernetes.io/disk-pressure` needs disk diagnosis,
not a permissive SCC or a toleration that bypasses the warning. A temporary pressure state
can remain during kubelet's recovery interval after image garbage collection frees space.

The worker and dashboard manifests target restricted pods with arbitrary non-root UIDs.
Local smoke checks have covered duplicate submission, worker replacement and PostgreSQL
pod replacement with its existing PVC. Workers can exit during a database interruption;
Kubernetes must restart them. These single-node checks do not qualify database/storage HA
or physical-host failure. See [the dashboard rehearsal](#local-openshift-dashboard-rehearsal)
for the separate OIDC and backend certificate checks.

### Developer Sandbox qualification

[Red Hat Developer Sandbox](https://developers.redhat.com/developer-sandbox) requires your
Red Hat account and account verification. Complete that step yourself; do not paste a login
password, verification code or CLI token into an issue/chat. Log the official `oc` CLI into
the assigned cluster in your own terminal. Use the assigned namespace; do not assume permission
to create other namespaces or perform administrator node-failure operations.

Before deploying, select the context explicitly and collect the read-only prerequisite report:

```bash
python3 eng/openshift-preflight.py --context YOUR_CONTEXT --namespace YOUR_NAMESPACE \
  --output .artifacts/openshift-preflight.json
```

The script inspects OpenShift route discovery, required create permissions, quotas/limits and
node-observation permission. It never deploys, reads Secret contents or changes your selected
context. Review the output, adapt namespace/host names and size the manifests to actual quota.
Passing preflight is not a successful deployment. Sandbox does not replace an authorized
production-like cluster for node loss, storage failover, OIDC and certificate/backup drills.
A complete Sandbox or production deployment qualification has not been recorded.
Run the [load experiments](#load-experiments) in your own target environment.

### Upgrade and recovery sequence

| Role / operation in dev.28 | PostgreSQL schema | SQL Server schema |
| --- | --- | --- |
| Worker/producer startup and command hosts | 11 | 8 |
| Explicit provider migration input | New store or 1–11 | New store or 1–8 |
| Migration output | 11 | 8 |

Dev.28 keeps the base schema versions introduced in dev.24 (PostgreSQL 11 / SQL Server 8).
Optional modules require their own explicit migrations; see the module sections below.

The dev.23 → dev.24 PostgreSQL migration creates `ix_claim_due` and removes
`ix_claim_active` in one transaction. It preserves records, leases, attempts and request
receipts. Index creation needs temporary disk space and a maintenance window; size the
migration connection's command timeout for the existing data volume. It is not an online
or mixed-writer upgrade. Pin all EventGuard packages in a process to the same release; rebuild templates,
workers, dashboard and migration images from those packages. Older/newer schema numbers
are rejected by command startup. Reader compatibility with historical layouts is not
permission to run old command binaries against a newer protocol.

1. Record image digests/package locks and current schema version. Rehearse the upgrade
   against a restored copy. Save a consistent backup of **business data and all EventGuard
   tables together**, plus protected dashboard keys/certificates and broker configuration.
2. Pause ingress and producers, then gracefully stop worker/dispatcher/command writers.
   Keep source messages available for redelivery. Do not delete identity tombstones or
   receipts to make an upgrade pass.
3. Run one explicit migration job under the migration identity and wait for success.
   Provider DDL is transactional and serialized; application EF migrations are a separate
   step with their own rollback rules. On failure inspect the actual persisted version
   and startup check before retrying; do not manually edit `schema_version`.
4. Run read-only `check`, start a canary worker/dashboard on the new binaries, verify
   duplicate acceptance, one business effect, retry and outbox behavior; then resume traffic.
5. If rollout fails with unchanged schema, return to the recorded images/configuration
   (including the certificate bridge when rotated). If schema compatibility is uncertain,
   keep writers stopped and restore into a **separate** database first. Verify checks,
   deduplication receipts and business state before switching connections. No down migration
   or automatic in-place database rollback is provided.

A database restore does not undo already delivered broker messages or external side effects.
Reconcile their IDs/receipts and recovery policy before replaying traffic; a database snapshot
alone cannot provide a cross-system point-in-time rollback. Measure recovery time/data-loss
objectives in your deployment rather than deriving them from local timing.

The automated installation test can exercise native backup/restore into fresh local databases:

```bash
python3 eng/verify-onboarding.py --packages .artifacts/packages \
  --output .artifacts/onboarding-restore --backup-restore --optional-adapters
```

It checks native backup restore, schema startup, preserved Completed/attempt count,
deduplication and application projection for .NET 8/10 × PostgreSQL/SQL Server. It saves
backup hashes and elapsed times. `--optional-adapters` also compiles the EF Core and broker
hosting APIs from the packaged dependencies in both clean projects. These disposable-fixture checks do not validate your
production backup credentials, storage system, external broker recovery or retention policy.

### Worker and dashboard deployment walkthrough

The checked-in [worker configuration](deploy/openshift/worker-config.yaml) and
[dashboard configuration](deploy/openshift/dashboard.yaml) use matching production scopes:
PostgreSql / `eventguard_worker` / `production` / `product-projection` / `catalog` /
`product.changed`. Change both when adopting a different scope or SQL Server. The image
examples below target .NET 10; a .NET 8 application needs a matching runtime image.

1. Publish the generated application from its directory, then build its image with the
   repository's [worker Dockerfile](deploy/Dockerfile.worker). Replace the repository path,
   image registry and host placeholders. Push images using your normal release pipeline.

   ```bash
   dotnet publish -c Release -p:AssemblyName=EventGuardWorker -o ./publish
   docker build -f /absolute/path/to/EventGuard/deploy/Dockerfile.worker \
     -t YOUR_REGISTRY/eventguard-worker:0.1.0-dev.28 ./publish
   # From the EventGuard checkout, build the dashboard image:
   bash eng/dashboard-image.sh
   ```

2. Provision the database, namespace, registry access, OIDC application and shared key storage.
   Edit all `YOUR_REGISTRY` and `eventguard.example.com` values before applying manifests.
   Provision these Kubernetes secrets through your secret manager; values are not in Git:

   | Secret | Required keys |
   | --- | --- |
   | `eventguard-worker-migration` | `EVENTGUARD_CONNECTION` with migration credentials |
   | `eventguard-worker-runtime` | `EVENTGUARD_CONNECTION` with worker credentials |
   | `eventguard-dashboard-settings` | `Dashboard__ConnectionString` with metadata-reader credentials, `Dashboard__Authority`, `Dashboard__ClientId`, `Dashboard__ClientSecret`, `Dashboard__KeyProtectionCertificatePassword` |
   | `eventguard-key-protection` | `certificate.pfx`, containing the Data Protection encryption certificate and private key |
   | `eventguard-metrics` | `token`, 32–128 ASCII letters/digits/underscore/hyphen |

   The dashboard example is read-only. Grant `eventguard_scope=products-production/products`
   for metadata and additionally `eventguard_data=products-production/products` for selected
   fields. Register `https://eventguard.example.com/signin-oidc` with your identity provider.
   Shared key storage must permit the cluster-assigned UID/group to write and support RWX
   for two replicas; choose a suitable storage class in
   [dashboard-storage.yaml](deploy/openshift/dashboard-storage.yaml). Plan key/certificate
   backup and rotation with your platform team. The Service requests its TLS certificate
   from OpenShift's service CA; the key-protection PFX is a different certificate.

3. Apply the ConfigMap and run the explicit migration job **before** starting/updating workers.
   Use a fresh Job name for each subsequent release; inspect its exit status and logs.
   Provider schema migrations are serialized, but the template's application DDL should
   have a single migration owner. `backoffLimit: 0` makes migration failure visible.

   ```bash
   oc apply -f deploy/openshift/worker-config.yaml
   oc apply -f deploy/openshift/worker-migration.yaml
   oc wait -n eventguard --for=condition=complete job/eventguard-worker-migrate-dev22 --timeout=300s
   oc apply -f deploy/openshift/worker.yaml
   oc rollout status -n eventguard deployment/eventguard-worker --timeout=180s
   oc apply -f deploy/openshift/dashboard-storage.yaml
   oc apply -f deploy/openshift/dashboard.yaml
   oc rollout status -n eventguard deployment/eventguard-dashboard --timeout=180s
   ```

4. Configure the re-encrypt Route with the actual service CA, using the
   [Route example](deploy/openshift/route.yaml). The
   [service CA ConfigMap](deploy/openshift/service-ca.yaml) requests the cluster's CA bundle;
   wait for `service-ca.crt` to appear, then place its PEM contents in the Route's
   `destinationCACertificate`. Apply the completed Route and verify HTTPS login,
   scope/data permissions, readiness, a completed sample event and restart behavior.

The worker has no HTTP listener; a successful startup check is not continuous readiness.
Monitor its logs, process restarts and optional Runtime telemetry. It cancels work on shutdown
with a 30-second host budget; the Deployment gives it 45 seconds before forced termination.
A forced kill still requires durable lease recovery. Dashboard readiness checks storage; the
UI never executes the application's business handler. Restrict pod egress to the actual
DB, OIDC and broker services using your cluster policies. These are reviewable deployment
examples. The worker passed the limited OpenShift Local smoke run above, and the dashboard
passed the local OIDC/service-certificate rehearsal below. Your production identity provider,
storage, network policy and full deployment still need a live acceptance test.

## Retention and tenant budgets

`IRetentionStore` removes eligible payload data in explicit batches of 1–1,000 records
with cutoff, stable request ID, actor and reason. Default minimum age is seven days;
configuration supports one hour to ten years. Completed/eligible expired consumer data,
Published outbox data and eligible released snapshots may be minimized. Pending work,
unexpired failures, uncertain unpublished output and held quarantine remain protected.
Identity/fingerprint, policy, attempt and request tombstones remain: retention is not
permission to forget deduplication identity. Removed data cannot be replayed. Backups
and external archives need their own lifecycle. Maintenance is outside the worker hot path.

`ITenantBudgetStore` shares fixed-window claim, retry and recovery allowances across
hosts/scopes with the same tenant/database/schema. Limits are nullable (unlimited), zero
(paused category), or finite. Creation expects version -1; updates use the current version.
`TenantBudgetRequest` includes a stable ID, policy, authenticated actor and reason.
Configure policies before starting workers when startup requires a finite budget.

A retry spends a claim and retry allowance. Replay/release/repair share recovery allowance.
Counters and native state transitions commit together. Repeated requests do not spend
again. `RateLimited` is a saved refusal; retrying that ID in a later window does not turn
it into acceptance. After resolving its receipt, a fresh request/version is needed.

These limits cap admissions, not active handlers, storage or external throughput. Fixed
windows can allow boundary bursts; separate shards have separate budgets. The shared
tenant counter has coordination cost and must be measured with the actual workload.

## Architecture and extension points

EventGuard uses **ports and adapters** with a dependency-free domain core. Applications
are the composition roots. This is not a mandatory layered architecture for your project.

```text
Application host ─┬─ Hosting (optional) ── Runtime ── Core
                 ├─ Runtime ────────────────────── Core
                 ├─ PostgreSQL / SQL Server ────── Core + native DB driver
                 └─ Kafka / RabbitMQ ───────────── Core + native broker driver

Dashboard ───────── Operations contracts ────────── Core
                  ├─ metadata adapters ──────────── Operations + native driver
                  └─ command adapters ───────────── Operations + native store protocol
```

Core owns `WorkAcceptance`, `WorkLifecycle`, `OutboxLifecycle` and `OutboxRecovery`
decisions. Providers read under native locks and apply expected state/version/token
with attempts and receipts in the same transaction. Decision objects are not locks or
authorization proofs. Runtime references Core only; providers never reference Runtime/UI.
Hosting references Runtime and Microsoft hosting abstractions; it has no native database or broker dependencies.
Operations metadata readers do not reference worker providers. Dashboard contains no
provider SQL or direct state updates and is not on the worker's synchronous path.

| Directory | Responsibility |
| --- | --- |
| `src/` | Core, Runtime, native providers, Operations and optional dashboard |
| `templates/EventGuard.Worker/` | Installable package-only worker project with explicit migration commands |
| `samples/` | Runnable SDK, broker, dashboard and monitoring integration examples |
| `pilots/CatalogPilot/` | Separate package-only application with Domain/Contracts/Infrastructure/Hosting boundaries |
| `tests/` | Architecture, shared conformance, native provider/broker, auth/UI and tool checks |
| `benchmarks/` | Bounded development load tools; not production packages |
| `eng/` | Pinned bootstrap, build, native verification, packaging and repository checks |
| `deploy/` | Container and OpenShift deployment templates |

A new store must pass [shared conformance](tests/EventGuard.Store.Conformance) and its
own real transaction/fault tests. Preserve native locks, durable request identity,
commit-uncertainty handling and immutable envelopes. New broker adapters must settle
input only after durable acceptance/permitted quarantine. Add explicit dependency rules
in [architecture tests](tests/EventGuard.Architecture.Tests) for any production project.

## Typed jobs, batches and external services

These optional APIs are available in the existing Core, Runtime, PostgreSql and SqlServer
packages; there is no service-specific business model. Your backend owns authentication,
tenant authorization, the filter screen and the typed parameters. A `JobScope` partitions
identities; it does not authorize a caller. Do not accept arbitrary scopes or SQL from a UI.

Run optional migrations with a deployment/migration identity before starting workers:

```csharp
// The same methods exist on PostgreSqlWorkStore and SqlServerWorkStore.
await store.InitializeJobsAsync();             // Jobs schema 2, including item receipts
await store.InitializeExternalActionsAsync(); // External actions schema 4; optional
```

These migrations first check/initialize the base store. They are additive and independently
versioned; they do not change the base schema versions. They require DDL permissions and
must not run on every application request. Back up the processing store before upgrading.
Keep the old runner available while its registered definition version has pending work.
The regular migration command/template currently covers the base schema; invoke the optional
methods explicitly in your own migration program when selecting these modules.

### Submit a typed command

```csharp
using EventGuard;
using EventGuard.Runtime;

var scope = new JobScope("tenant-from-backend", "my-application");
var parameters = new JsonMessageContract<RebuildSelection>("rebuild-selection", 1,
    p => p.ContractVersion,
    p => {
        if (string.IsNullOrWhiteSpace(p.Category) || p.Limit is < 1 or > 1000)
            throw new IngressMessageRejectedException("invalid-selection");
    });
var definition = new JobDefinition(scope, "rebuild", 1, parameters.Name, parameters.Version,
    "handler-v1", "registered-selection-rules-v1"u8);
await store.RegisterAsync(definition);
var client = new JsonJobClient<RebuildSelection>(store, definition, parameters);

// Save and reuse requestId after an ambiguous commit. businessKey identifies the business intent.
var receipt = await client.SubmitAsync(requestId, businessKey, authenticatedActor,
    new RebuildSelection(1, "documents", 100));
var progress = await client.FindAsync(receipt.OperationId);

public sealed record RebuildSelection(int ContractVersion, string Category, int Limit);
```

Register a matching `JsonJobWorker<RebuildSelection, PostgreSqlSession>` (or `SqlServerSession`)
with an application handler. The handler receives the typed parameters, `WorkItem`, native
session and cancellation token. Its business SQL, outbox/external intents and completed work
share one local database transaction. Execute SQL through `session.CreateCommand(...)` or the
same session transaction. Do not call external services or perform a long source scan inside it.
Use existing hosting supervision with
`services.AddEventGuard().AddWorker("rebuild", (_, ct) => worker.RunAsync(ct),
(_, ct) => store.ValidateAsync(definition, ct))` and DI-owned stores/connections.

Identical repeated submissions return the saved operation. Reusing a request ID with changed
input returns `RequestConflict`; a new request ID for the same business identity and changed
semantics returns `BusinessConflict`. Actor, parameters, schedule and correlation are bound to
the request. Business identity includes tenant, application, definition **name** and business
key, so upgrading a definition does not silently authorize a duplicate business action.
`CommitOutcomeUnknownException` requires looking up the same request with `FindRequestAsync`
and retrying the identical submission. Never generate a new request ID merely because a
response was lost. Store admission and the durable receipt commit together.

Definitions freeze contract version, handler version, plan bytes and required capabilities.
The plan is an immutable application manifest, **not an executable workflow DSL**. Registering
different semantics under the same definition version fails; an unknown capability fails
before admission. The application must version changes to validation, selection and mapping.
`JobOperation.State` reports the transactional handler's progress; it does not mean that a
separate remote action has finished.

### Apply bounded batches

`JsonJobBatches.Contract<T>(name, version, itemContract, maximumItems: 100)` accepts a typed
`JobBatch<T>` containing `JobBatchItem<T>(key, value)` entries. The limit is configurable from
1 to 1000; the encoded command and aggregate encoded items are also bounded to 1 MiB. The
last batch may contain fewer items. Duplicate keys or invalid values fail before admission.

Declare `JobCapabilities.TransactionalHandler` and `JobCapabilities.TransactionalItems` in
the definition, then call `JsonJobBatches.ExecuteAsync` inside the job handler. Supply an
application item handler using the same native session. An item's stable key is independent
of batch membership: overlapping batches skip already applied items, while changed content
under the same key fails. `JobBatchResult` reports applied and already-applied counts.
Item receipts, business writes and outgoing intents commit together. Failed item SQL is
rolled back even if application code catches its exception; normally let the exception
propagate so the entire transactional batch rolls back. Keys are acquired in a stable order.

This is atomic **database** batching. For external work, create one `ExternalAction` intent per
item inside the batch and execute those intents separately. A successful item must not be
resent just because another item's service call failed. This version does not provide a
bulk remote request/partial-response adapter. Automatic source preparation is available through the separate snapshot module.

### Send through any service adapter

Inside a transactional producer callback or a job/work handler:

```csharp
await session.ExternalActions.EnqueueAsync(new ExternalAction(
    new ProducerScope(authorizedTenant, "partner-provisioning"),
    stableActionId, "partner-api", "v1", serializedRequest,
    observationTimeout: TimeSpan.FromHours(24)), cancellationToken);
```

The action ID must be stable for the logical action. Reusing it with a changed binding,
payload or transaction origin is a conflict. Producer callbacks must use their producer
scope; consumer/job actions must use the owning work's tenant. Intent and business SQL
commit atomically. Payloads are bounded to 1 MiB; stored result data to 64 KiB. If completion
at acceptance is sufficient, omit `observationTimeout` and do not start an observer.

Implement `IExternalServiceAdapter` using your own client/SDK, authentication and typed
contract. Bind a semantic `Binding` / `BindingVersion` and declare `CanReconcile` and
`CanObserve`. The adapter supplies three independent operations:

| Operation | Required interpretation |
| --- | --- |
| `SubmitAsync` | `Accepted`, `Rejected`, `NotAcceptedRetryable` only when non-acceptance is proven, or `Unknown` |
| `ReconcileAsync` | Read-only lookup using the stable action ID/reference; resolve uncertainty without a new send |
| `ObserveAsync` | `Pending`, `Succeeded`, `Failed` or `Unknown`; never reinterpret observation as a new submission |

Run separate `ExternalActionWorker` instances for `Submit`, `Reconcile` and `Observe` as
needed. Each accepts an `ExternalActionPolicy` and `WorkPumpOptions` with bounded concurrency,
lease duration and idle delay. Use `AddWorker` hosting supervision with
`store.ValidateExternalActionsAsync` as startup validation. Adapters must honor cancellation
and bound their client timeouts. Calls happen outside database transactions.

A timeout, exception or expired sender lease becomes `Unknown`. It is **never automatically
resent**. Only explicit proof of non-acceptance permits another submission, up to the policy's
attempt limit. If the service has no reliable lookup, uncertainty remains visible for
application/operator resolution; the package cannot invent evidence or guarantee exactly-once
remote effects. An idempotency key's retention and semantics belong to the service contract.

Acceptance and observation are separate durable states. Accepted actions can remain pending
until a report arrives. An observation deadline becomes `TimedOut`, not a claim that delivery
failed. Terminal observations cannot regress. Version/token fencing rejects stale owners;
late responses remain in `ReadActionEvidenceAsync` even when they cannot change current state.
`FindActionAsync` and the bounded evidence query are scoped for use behind your authorized
backend. References/result bytes may contain application data: restrict their display and
never place secrets in reason codes. The existing dashboard does not yet expose these
new action/observation queries. New tables currently retain evidence and payloads; define a
qualified retention/archival policy before sustained production use.

### Update your own tables from service results

Create an `ExternalResultTarget` for your application's versioned mapping, and bind it when
creating the intent. A target is opt-in: existing intents without a target behave as before.
In the same transactional handler that creates the application request row:

```csharp
var producerScope = new ProducerScope(authorizedTenant, "partner-provisioning");
var resultTarget = new ExternalResultTarget(producerScope, "request-status", "v1");
await session.ExternalActions.EnqueueAsync(new ExternalAction(
    producerScope, stableRequestId, "partner-api", "v1", serializedRequest,
    observationTimeout: TimeSpan.FromHours(24)), resultTarget, cancellationToken);
```

Run an independent result worker. This PostgreSQL example assumes an application-owned
`app.requests` table with a unique `(tenant_id, request_id)` row, created with the same
`stableRequestId`. Table names, columns, states and payload contracts all belong to your app.
For SQL Server, use `SqlServerSession` and the matching SqlClient parameter types.

```csharp
var results = new ExternalResultWorker<PostgreSqlSession>(store, resultTarget,
    async (update, session, ct) =>
    {
        // Your mapping: service acceptance is distinct from eventual completion.
        var status = update.Observation switch
        {
            ExternalObservationState.Succeeded => "completed",
            ExternalObservationState.Failed => "failed",
            ExternalObservationState.TimedOut => "report-overdue",
            _ => update.State switch
            {
                ExternalActionState.Accepted => "accepted",
                ExternalActionState.Rejected => "rejected",
                ExternalActionState.Unknown => "needs-reconciliation",
                _ => "awaiting-retry"
            }
        };
        await using var sql = session.CreateCommand("""
            UPDATE app.requests SET status=@status, provider_reference=@reference
            WHERE tenant_id=@tenant AND request_id=@id
            """);
        sql.Parameters.AddWithValue("status", status);
        sql.Parameters.AddWithValue("reference", NpgsqlTypes.NpgsqlDbType.Text,
            (object?)update.ProviderReference ?? DBNull.Value);
        sql.Parameters.AddWithValue("tenant", update.Action.Scope.Tenant);
        sql.Parameters.AddWithValue("id", update.Action.Id);
        if (await sql.ExecuteNonQueryAsync(ct) != 1)
            throw new InvalidOperationException("Application request row is missing.");
    }, retryPolicy: new RetryPolicy(),
    options: new WorkPumpOptions(maximumConcurrency: 8));

services.AddEventGuard().AddWorker("request-status", (_, ct) => results.RunAsync(ct),
    (_, ct) => store.ValidateExternalActionsAsync(ct));
```

`update.Action.CopyPayload()` contains your original request; `update.Response` is the adapter
result for this transition and may be null for lease recovery or observation deadline expiry.
`CopyStateData()`, `ProviderReference`, `Reason`, `Phase`, `State` and `Observation` expose the
saved state. Deserialize your own contract and interpret provider-specific result bytes as
needed. Use a stable request/attempt ID to select the row, not a phone number or another
non-unique business field.

The action settlement and its result work are committed in one transaction. The result worker
then commits your SQL, the applied-version checkpoint and work completion in another local
transaction. If your SQL throws, let the exception propagate: only the result work retries;
EventGuard does not resend an accepted service request. Retry limits and dead-letter/replay
use the existing work machinery. Register `resultTarget.WorkScope` in the authorized dashboard
scope configuration to monitor these works; no dedicated action-result dashboard is supplied.

Versions serialize **per action and target**. If a newer result is applied first, a delayed older
result is completed without running your handler. This is a current-state projection, not a
callback guaranteed to run for every intermediate transition. Keep attempt evidence for audit.
Different actions may run concurrently; if they update the same aggregate row, the application
must also enforce its own business revision rule. EventGuard cannot infer cross-action ordering.

SQL must use the supplied session transaction in the processing database. To update another
database, enqueue an outbox message in this handler and apply it with a deduplicating consumer
there. No transaction spanning both databases or the external service is promised. Do not put
another network send inside the result handler: a database rollback cannot undo that effect.

The target (scope/name/version) is immutable for an action. Keep the original handler deployed
until its pending results drain; a different version uses another work scope. Existing intents
cannot acquire a result target retroactively. For an external-schema-1 upgrade: stop writers
and workers, back up the database, call `InitializeExternalActionsAsync()` once with migration
privileges to reach schema 4, then start this package version and validate the schema. The
migration preserves old intents and their fingerprints; do not mix old and new module writers.
Rollback requires restoring the backup, not just downgrading the package. Result snapshots
and positions currently retain data alongside external action evidence; ordinary work payload
retention does not erase them.

### Verify a package-only composition

The [executable fixture](eng/fixtures/composable-execution/Program.cs.txt) uses application-owned
document, inventory and partner-account models with the same primitives. It submits overlapping
batches, checks unique business writes, simulates a service that accepts each request but loses
the response, reconciles acceptance, and observes completion. It maps outcomes to an
application-owned table, injects a failed status transaction and verifies three final statuses
with exactly three remote submissions. No actual external service is called. The [runner](eng/verify-composable.py) builds outside this checkout with a fresh NuGet
cache and runs PostgreSQL/SQL Server on both target frameworks:

```bash
# With the documented local PostgreSQL and SQL Server correctness fixtures running:
bash eng/build.sh
bash eng/pack.sh .artifacts/packages
python3 eng/verify-composable.py --packages .artifacts/packages --output .artifacts/composable-check-1
```

This automated integration proves package APIs and the tested synthetic protocol behavior;
it is not an independent human usability trial or a real-provider production qualification.

## Composable execution modules

Native PostgreSQL and SQL Server
support consistent source capture, sealed batches, frozen local/outbox/CDC routing, shared
service quotas/circuit breaking, durable callback reports and static workflows. These are
optional ports in Core, loops in Runtime and transactions in the native providers; an
ordinary consumer does not need these modules, a dashboard or an external service.

Business models, filter screens, queries, identity/ordering rules and provider contracts
belong to the application. The APIs are independent of any messaging provider or business domain.
Supported combinations and guarantees are documented below.

### Consistent source capture and batches

The source and the processing store are separate connections. Give the source SELECT-only
credentials. PostgreSQL uses a read-only repeatable-read transaction; SQL Server uses
snapshot isolation, which the source administrator must enable with
`ALTER DATABASE ... SET ALLOW_SNAPSHOT_ISOLATION ON` before use. The library does not change
source database options or write progress into the source. A long source transaction may
retain database versions; measure its effect on the source and replication.

```csharp
await store.InitializeSnapshotsAsync(); // explicit optional migration; initializes Jobs too
await store.RegisterAsync(batchDefinition);
var selection = new SnapshotDefinition(batchDefinition, "customer-selection", "replica", "v1",
    consistency: SnapshotConsistency.SessionBound, batchSize: 100,
    delivery: SnapshotDelivery.LocalJob);
var request = new SnapshotRequest(selection, stableRequestId, campaignKey,
    authenticatedActor, JsonSerializer.SerializeToUtf8Bytes(validatedFilters));
var receipt = await store.CreateSnapshotAsync(request);

// PostgreSQL example. The application owns the parameterized query and row mapping.
var source = new PostgreSqlSnapshotSource(readOnlySource, "replica", "v1",
    (connection, transaction, bytes) =>
    {
        var filter = JsonSerializer.Deserialize<MyFilter>(bytes.Span)!;
        var query = new NpgsqlCommand(
            "SELECT entity_id, revision FROM source_projection WHERE category=@category ORDER BY entity_id",
            connection, transaction);
        query.Parameters.AddWithValue("category", filter.Category);
        return query;
    }, row => new SnapshotItem(row.GetString(0), itemContract.Encode(
        new MyItem(row.GetString(0), row.GetInt64(1)))));
var preparation = new SnapshotWorker<MyItem>(store, selection, source, itemContract);
services.AddEventGuard().AddWorker("selection", (_, ct) => preparation.RunAsync(ct));
```

`SqlServerSnapshotSource` has the equivalent SqlClient query/map delegates. Do not accept
SQL text or connection strings from the caller's filter UI. Validate filters in the backend;
authenticate and authorize creation, progress, export and control calls there.

`SessionBound` sources recapture into a new generation after a lost transaction. Nothing
from an incomplete generation is dispatched. A custom `ResumableView` adapter can continue
from a saved cursor only if it preserves the exact immutable view ID; pagination across a
changing table is not a consistent resumable snapshot. Native sources deliberately do not
claim this capability. The application/source adapter must verify replication freshness.

Pages are bounded to 1,000 items / 4 MiB. Native bulk inserts deduplicate stable item keys;
same key with different content is rejected. Select a deterministic record/version when
transactions contain several values for one customer. Batch size defaults to 100, with a
1 MiB encoded limit and a final partial batch. A batch receipt and its local job or outbox
intent commit atomically. `SnapshotState.Batched` means all batches were persisted, not
that the service accepted or delivered them.

Snapshot requests keep immutable semantic fingerprints and stable business identity.
Changing a route, source/rule version or filter under the same business key fails. Keep the
old definition/runner available while it drains. Item receipts use the target job's identity
scope: include a campaign/business-action identifier in item keys when an intentional repeat
must be a new action. A new snapshot or transport attempt alone does not authorize a resend.

### Direct publication and CDC / Qlik

Choose **one frozen mode per snapshot**:

| Mode | Durable output | Consumer |
| --- | --- | --- |
| `LocalJob` | Job accepted in the same processing-store transaction | `JsonJobWorker<JobBatch<T>, TSession>` |
| `Outbox` | Immutable envelope plus outbox intent | Existing Kafka/RabbitMQ publisher, then `SnapshotBatchReceiver` |
| `Cdc` | Immutable `snapshot_batches` ready row | Application CDC/Qlik mapping, then the same receiver |

Outbox/CDC definitions require a `PublishRoute`. `definition.OutputScope` identifies the
outbox publisher. `ReadSnapshotBatchesAsync(definition, snapshotId, afterSequence, limit)`
exports bounded immutable envelopes for backfill or an application archive. For Qlik, select
committed ready rows with `delivery = 2`, preserve the payload bytes and stable batch identity,
and normalize Qlik's change envelope to `SnapshotBatchCodec`'s envelope. Bootstrap/reload
may repeat rows. It must not synthesize new IDs or republish unsealed staging rows.

```csharp
var receiver = new SnapshotBatchReceiver(jobStore, batchDefinition);
await receiver.AcceptAsync(normalizedEnvelope, cancellationToken);
// Acknowledge the Kafka/RabbitMQ/CDC delivery only after durable acceptance returns.
```

The receiver validates binding and batch identity and deduplicates repeated acceptance.
No Qlik task, vendor-specific connector or real Qlik delivery guarantee is bundled. The
package-only composition fixture tests this hand-off with repeated normalized CDC envelopes;
qualify the actual task mapping, retention, restart and authentication in your environment.
Switching new operations to another mode does not modify in-flight operations.

### Shared service limits and durable reports

```csharp
await store.InitializeExternalActionsAsync(); // schema 4: results + limits + notices
var budget = new ExternalCallLimit("tenant/provider-account", "send-api",
    maximumCalls: 500, window: TimeSpan.FromSeconds(1), maximumConcurrency: 8,
    failureThreshold: 5, breakDuration: TimeSpan.FromSeconds(30));
var sender = new ExternalActionWorker(store, producerScope, adapter,
    ExternalPhase.Submit, budget, store);
```

All workers using that account/endpoint and processing store share a database-clock fixed
window, expiring concurrency permits and a circuit with one recovery probe. Obtain capacity
before claiming an action, so denied capacity does not burn submission attempts. Idle
permits are refunded idempotently. Fixed windows can allow bursts across a boundary. Separate
stores do not share a quota. Configuration fingerprints must agree; do not replace a quota
identity just to bypass a live budget. Cooperative adapter cancellation and a bounded HTTP
client timeout are required: an expired local permit cannot stop a remote request already sent.

An adapter that receives callbacks can implement `ICallbackExternalServiceAdapter` without
polling support. The authenticated application endpoint normalizes the actual provider report:

```csharp
// Inside the host's authenticated and tenant-authorized provider endpoint:
var notice = new ExternalObservationNotice(configuredProducerScope, stableProviderReportId,
    knownActionId, configuredBinding, configuredBindingVersion,
    new ExternalResult(ExternalResultKind.Succeeded, "provider-final", providerReference),
    providerRevision);
var receipt = await store.RecordNoticeAsync(notice, cancellationToken);
// Return a successful HTTP response only after this durable receipt returns.

var reports = new ExternalNoticeWorker(store, configuredProducerScope,
    configuredBinding, configuredBindingVersion);
services.AddEventGuard().AddWorker("reports", (_, ct) => reports.RunAsync(ct));
```

The host must verify the provider's signature/token, tenant ownership, request size and
actual acceptance/report contract **before** recording a notice. Scope/binding come from
trusted host configuration; a request body cannot choose a tenant. This API is a durable
inbox, not an unauthenticated public webhook endpoint. A retry after an uncertain commit
must use the same report ID and content.

Early reports wait for acceptance. Duplicate IDs/content return the receipt; changed content
conflicts. When supplied, a monotonic provider revision rejects older reports and conflicting
same-revision values. Once a revision is established, a revisionless report is held as a conflict. Without revisions, terminal state is still protected, but the service
cannot provide ordering that it never reports. Reference mismatches and contradictory terminal
results are held as conflicts. Late final reports can resolve an observation timeout; timeout
never implies delivery failure or a resend. Notices and polling both use the existing durable
application result projections. No automatic correction or remote compensation is inferred.

### Durable workflows and application-owned status

```csharp
await store.InitializeWorkflowsAsync(); // optional schema 1, Jobs schema 2
var definition = new WorkflowDefinition(jobScope, "provision", 1,
[
    new("prepare", WorkflowStepKind.Job, job: preparationJob),
    new("approval", WorkflowStepKind.Signal, [new("prepare")]),
    new("apply", WorkflowStepKind.Job, [new("approval", "approved")], applyJob),
    new("wait", WorkflowStepKind.Delay, [new("apply")], delay: TimeSpan.FromMinutes(5))
]);
var workflow = new WorkflowRequest(definition, stableRequestId, businessKey, actor,
    new Dictionary<string, byte[]> { ["prepare"] = prepareBytes, ["apply"] = applyBytes });
await store.CreateWorkflowAsync(workflow);
var coordinator = new WorkflowWorker(store, definition);
services.AddEventGuard().AddWorker("workflow", (_, ct) => coordinator.RunAsync(ct));

// In a job/result handler, use its native session transaction:
await UpdateApplicationStatusAsync(session, serviceResult, ct);
await session.WorkflowSignals.SignalAsync(new(jobScope, workflow.Id, "approval",
    stableSignalRequestId, "approved", authenticatedActor), ct);
```

The status update and signal commit or roll back together. The remote call is not repeated
because a later status SQL transaction failed. A signal can arrive before its step becomes
active. Each signal step accepts one immutable value; another intentional decision needs a
new step/operation. Use service acceptance or final fulfillment as your application's chosen
completion criterion, not an implicit package-wide SMS assumption.

Definitions contain 1–64 static acyclic steps, up to 1 MiB per job input / 8 MiB total. Jobs
with satisfied dependencies fork; joins wait for completed or skipped dependencies. Conditional
edges compare explicit signal values. A mismatched branch is skipped; a merge can combine a
completed branch and skipped alternatives. All-skipped prerequisites propagate a skip.
Timers persist their due time. Child submission and step progress share a transaction, so
lost commit replies do not create another child. Dead-letter/expired child jobs keep the
parent waiting with a reason until the application repairs/replays or cancels it.

`FindWorkflowAsync` returns step names, job IDs, states, timer due dates, values and reasons.
`ControlWorkflowAsync` and `ControlSnapshotAsync` use expected versions and retain actor/reason
audit records. Pause prevents new dispatch; cancel/deadline does not undo existing jobs or
external effects. There are no cycles, dynamic graph rewrites, arbitrary code evaluation or
automatic saga compensation. Workflow inputs remain frozen and old runners must remain available.

### Module monitoring, export and retention

The dashboard's `/modules` page is optional and read-only. Configure explicit registrations:

```json
"Modules": [
  { "Key": "selections", "Label": "Selections", "Module": "Snapshots", "ScopeId": "JOB_SCOPE_ID" },
  { "Key": "service", "Label": "Service requests", "Module": "ExternalActions", "ScopeId": "PRODUCER_SCOPE_ID" },
  { "Key": "reports", "Label": "Service reports", "Module": "ExternalNotices", "ScopeId": "PRODUCER_SCOPE_ID" },
  { "Key": "flows", "Label": "Workflows", "Module": "Workflows", "ScopeId": "JOB_SCOPE_ID" },
  { "Key": "capacity", "Label": "Service capacity", "Module": "ServiceLimit", "ScopeId": "CALL_LIMIT_ID" }
]
```

Replace IDs with the corresponding `.Id` values. Grant the OIDC claim
`eventguard_module = STORE_ID/REGISTRATION_KEY` for each allowed registration; work, outbox,
monitor and module grants are independent. The dashboard read role needs SELECT only on the
configured module metadata columns. Do not initialize optional schemas from the web process.
Missing schema/grants return unavailable, not an empty successful page. Filters include state
and exact ID, with protected forward/backward cursors and bounded reads. Select a workflow record ID to inspect step states, child job IDs, timer due dates and waiting reasons. Payloads, phone
numbers, provider references, business keys and free-form provider result data are not shown.
State-specific guidance distinguishes waiting for capture, batches, acceptance, reports and
reconciliation. Application-specific controls and data remain in the application's authorized UI.

`PruneSnapshotStagingAsync(definition, id, olderThan, limit)` removes only obsolete capture
generations or staging for fully batched/cancelled snapshots, under the operation lock.
It preserves immutable batch envelopes, request/page/control receipts and business deduplication.
The cutoff is capped at database time. Export batches before an application archive policy
requires it. This deliberately does not delete CDC ready rows, unresolved external actions,
callback conflicts, result work or workflow receipts. Retention of these records requires a
qualified downstream acknowledgement/replay policy; there is no automatic destructive cleanup.
Existing work/outbox retention policies still apply within their documented boundaries.

### Qualification boundaries

The package-only composition fixture covers a native filtered source with duplicates, 205
selected items, 100/100/5 batches, repeated CDC acceptance, shared service capacity, early and
duplicate callbacks, application status SQL and a completion signal/workflow. It also exercises
three unrelated models and a lost service reply with lookup, observation and a failed status
transaction. Services and normalized CDC transport are synthetic; native stores are real local
PostgreSQL/SQL Server fixtures. API compatibility, clean onboarding, fault tests and package
content checks are separate release gates.

Real Qlik/provider contracts, multi-node production recovery, deployment identity/certificate
rotation and an independent team's integration remain external qualification requirements.
Existing OpenShift/local evidence is not a certification for every topology. Million-record
backlog and million-per-second targets are distinct and require measured hardware/workload
profiles; neither follows from these correctness tests. Snapshot preparation, DB-only execution,
remote acceptance and final report latency must be measured separately.

## Testing

Start all native fixtures and run the full verification:

```bash
bash eng/docker.sh compose up -d --wait postgres rabbitmq sqlserver kafka
bash eng/verify.sh
```

This performs locked restore, Release build, both-framework tests and local packaging.
Native tests need Docker/services; tests are not silently replaced with mocks. Logs/TRX
are written under `.artifacts/test-results`. Set `EVENTGUARD_PACKAGE_OUTPUT` to preserve
an existing local package feed while packaging elsewhere. `eng/build.sh` also accepts
an optional output-directory argument.

The dev.28 [hosted Ubuntu CI run](https://github.com/yarasaa/EventGuard/actions/runs/40410916848)
passed **2,084 test cases across .NET 8/10**, all five samples, the four onboarding combinations
and 26 package-only pilot scenarios.
The package-only onboarding and composable-execution checks each passed all four combinations
of framework and store. The latter verifies 205 selected records, 100/100/5 batches, repeated
CDC notifications, service outcomes and application-owned status updates. Its service and
CDC transport are synthetic; PostgreSQL and SQL Server are real local fixtures.

Coverage includes native transactions, duplicate/conflict handling, revision guards, retry,
lease recovery, process failure, shared EF transactions, broker ingestion, dashboard access
control and optional-module state transitions. Use the separate recovery drills below for
backup, node and OIDC scenarios. SQL Server was tested both under macOS ARM emulation
and natively in hosted Ubuntu x64 CI. Independent developer onboarding and acceptance
of a specific production deployment remain separate requirements.

Raw logs, test output and machine-specific evidence stay under the ignored `.artifacts/`
directory. Re-run the commands to produce results for your own environment.

### Reproduce the onboarding check

After building packages and starting PostgreSQL/SQL Server fixtures:

```bash
python3 eng/verify-onboarding.py --packages .artifacts/packages \
  --output .artifacts/onboarding-first-run
```

Choose a new output path for each run. The script installs the template into an isolated
CLI hive, generates .NET 8/10 applications outside the checkout, restores into a new NuGet
cache and uses only packages. It creates unique disposable databases in the repository's
local Docker fixtures and removes only those databases. It verifies missing-schema failure,
repeatable migration, startup check, Accepted → Duplicate → Completed, the actual projection,
SIGTERM shutdown, configuration errors, dashboard detail/selected fields and nonzero worker
exit after a controlled store failure for both providers. The failure check temporarily renames
a table only in the newly created disposable test database. Logs and a JSON result remain
in the output directory. This requires network access for public package dependencies.

This is an automated author-run onboarding check, **not independent developer feedback**.
For a separate developer trial, give someone only this README, the source checkout and
local prerequisites. Ask them to generate a worker, replace the DTO/handler with one of their
own, process/repeat an event, inspect it in the dashboard and stop/restart the application.
Record time spent, questions asked, copy/paste corrections and unresolved failures. The trial
passes when these steps work without undocumented help; no independent participant has
completed this acceptance step yet.

### Custom model and node recovery checks

The [custom model check](eng/verify-custom-model.py) builds an application outside this
checkout with a new dependency cache and only NuGet references. Its own `DocumentIndexed`
DTO and handler exercise typed hosting, repeated delivery, and revision 18 followed by 17.
The optional native backup drill restores to another disposable database, redelivers both
messages and checks that the projection and two committed effects remain unchanged.
Use a dedicated PostgreSQL container named `eventguard-qualification-postgres` on loopback
port 55432, database `eventguard_test`, user `eventguard`, password `eventguard-local-only`.
This name and the generated database prefixes deliberately restrict the script to fixtures.

```bash
python3 eng/verify-custom-model.py --packages .artifacts/packages \
  --output .artifacts/custom-model-first-run --backup-restore
```

The [node recovery drill](eng/verify-node-recovery.py) creates three temporary kind nodes,
opens a business transaction on one worker, lets its lease expire, then kills that node.
The surviving worker must commit exactly one effect and outgoing intent after recovery.
Effects have no uniqueness constraint that could hide a duplicate execution. Restoring the
killed node must not repeat the effect. The generated cluster and schema are removed.

Publish [the fixture worker](tests/EventGuard.ClusterWorker) with .NET 10, build it using
[the load runtime Dockerfile](benchmarks/EventGuard.LoadTests/Dockerfile), then run:

```bash
python3 eng/verify-node-recovery.py --kind /path/to/kind --kubectl /path/to/kubectl \
  --database-container eventguard-qualification-postgres \
  --image eventguard-qualification-worker:dev28 --output .artifacts/node-recovery-first-run
```

The drill checks fencing while the original transaction holds its lock and recovery after
that node disappears. All kind nodes share one physical host; this does not test physical-host
HA, database failover or a network partition.

### Local OpenShift dashboard rehearsal

[The CRC dashboard rehearsal](eng/verify-openshift-dashboard.py) targets only
`https://api.crc.testing:6443`. It expects the existing `eventguard-local-test` PostgreSQL
worker fixture, schema `eventguard_crc_verified`, and a completed synthetic product event.
It creates its own namespace, SELECT-only database identity, pinned Keycloak test realm,
two restricted dashboard replicas and protected persistent cookie keys. Python `requests`,
OpenSSL and `oc` are required. Stop other VM environments before starting CRC on a small host.

```bash
bash eng/dotnet.sh publish src/EventGuard.Dashboard -c Release -f net10.0 \
  --no-restore -o .artifacts/dashboard-qualification-context
cp deploy/Dockerfile.dashboard .artifacts/dashboard-qualification-context/Dockerfile
python3 eng/verify-openshift-dashboard.py --oc /path/to/oc \
  --kubeconfig /path/to/crc-kubeconfig --context admin \
  --published-dashboard .artifacts/dashboard-qualification-context \
  --work-id YOUR_COMPLETED_SYNTHETIC_WORK_ID --output .artifacts/dashboard-qualification
```

This runs the actual HTTPS authorization-code/PKCE flow, checks allowed and denied scopes,
restarts both dashboard replicas, renews the generated backend service certificate, and
checks both existing and fresh login sessions. TLS validation remains enabled. The script
removes its namespace and database role; no passwords, tokens or cookies are included in
its JSON report. Detailed local failure logs remain private in its output directory.
The IdP uses development storage, and the key ring uses RWO storage on CRC's single node;
these choices are test fixtures, not a production identity-provider or storage design.
Router certificate renewal, production identity-provider policy and physical-host/storage
failover require their own deployment-specific acceptance.

The fixture uses `maxSurge: 0, maxUnavailable: 1` to fit two dashboard replica slots on CRC.
The deployment example's default rolling update needs capacity for an extra replica.
This rehearsal is separate from concurrent worker/dashboard capacity measurements.

## Load experiments

The [load tool](benchmarks/EventGuard.LoadTests) measures real consumer acceptance,
claim/execution and transactional effects. It has separate preload/backlog and planned
arrival profiles, bounded buffers/histograms, correctness oracles and failure exits.
It does not include a broker or external service and cannot establish end-to-end capacity.

```bash
bash eng/load-test.sh postgresql .artifacts/load-pg-trial \
  --count 2000 --payload-bytes 2048 --concurrency 1,4,8 --repetitions 3

EVENTGUARD_LOAD_FRAMEWORK=net8.0 bash eng/load-test.sh postgresql \
  .artifacts/load-pg-net8-trial --count 2000
```

Use `sqlserver` for the SQL Server fixture after its test database/profile is initialized.
Each invocation requires a new output directory and defaults to guarded localhost test DBs.
For an explicitly prepared Kubernetes test fixture, the PostgreSQL load tool also accepts
`EVENTGUARD_LOAD_KUBERNETES_TEST_NAMESPACE=eventguard-load-NAME`. In that mode the connection
must target exactly `eventguard-load-postgres.eventguard-load-NAME.svc:5432`, with database
`eventguard_test`. Use a dedicated test role/database, never an application database.
The localhost restrictions remain the default; arbitrary remote database hosts are rejected.

To measure inside OpenShift, publish the load tool for .NET 10 and build
[its Dockerfile](benchmarks/EventGuard.LoadTests/Dockerfile) with only that publish output as
the build context. Run it in a restricted pod with explicit CPU/memory limits, a writable
`/results` volume and the connection supplied by a Secret. The image stays idle until the
measurement command starts. The wrapper below requires an existing load pod and PostgreSQL
fixture deployment, verifies the selected API server, samples kubelet resources every five
seconds, records cgroup CPU throttling and keeps all results, including failed offers:

```bash
python3 eng/openshift-load-case.py \
  --context YOUR_CONTEXT --expected-server https://YOUR_CLUSTER_API:6443 \
  --namespace eventguard-load-NAME --database-namespace YOUR_TEST_DATABASE_NAMESPACE \
  --name backlog-grid --output .artifacts/openshift-load/backlog-grid \
  --timeout-seconds 1800 -- \
  --provider postgresql --count 5000 --payload-bytes 2048 \
  --concurrency 1,2,4,8,16 --pool-size 24 --repetitions 2
```

The wrapper expects the pod `eventguard-load`, the database deployment `eventguard-postgres`
and local `psql -U eventguard -d eventguard` access inside that fixture for read-only database
counters. It needs permission to exec into those test pods and read node statistics; it is
an engineering fixture, not a dashboard feature. It stops on node memory/disk pressure or
low host/VM free space. The load process still has its own per-case deadline. Preserve the
result, environment and resource JSON together. A backlog drain rate excludes preload time;
an arrival rate must also be assessed against queue growth, rejection and latency.

For planned arrivals, use a separate output directory:

```bash
bash eng/load-test.sh postgresql .artifacts/arrivals-pg-trial \
  --profile arrivals --initial-backlog 200 --count 1000 \
  --arrival-rate 50 --arrival-buffer 256 --concurrency 4 --repetitions 1
```

See [LoadOptions](benchmarks/EventGuard.LoadTests/LoadOptions.cs) for exact option bounds.
A failure to offer the requested arrivals is separate from incorrect processing of admitted
work. Report both. Do not convert timeouts, incomplete work, repeated effects or failed
admission into a successful throughput figure. Preserve environment, repetitions and
failed results; do not compare native PostgreSQL to emulated SQL Server as a product ranking.

For a longer planned-arrival run, use a dedicated test environment and record queue growth,
latency, rejected offers and completion tail as well as throughput:

```bash
bash eng/load-test.sh postgresql .artifacts/soak-pg-2k \
  --profile arrivals --count 120000 --arrival-rate 200 --initial-backlog 10000 \
  --payload-bytes 2048 --concurrency 8 --admission-concurrency 8 \
  --repetitions 1 --timeout-seconds 900
```

### Measurement scope

A dev.28 local smoke run used .NET 10, 5,000 preloaded records, 2 KiB payloads, four workers
and one repetition per provider. Every record completed with one matching effect:

| Provider | Observed drain rate |
| --- | ---: |
| PostgreSQL | 494.6 records/s |
| SQL Server (x64 emulation on ARM) | 265.4 records/s |

These are shared-desktop, database-only observations. They exclude preload, brokers, remote
services and report latency; they are neither production sizing figures nor a provider ranking.
Earlier PostgreSQL dev.25 tests reached a 300,000-record backlog with a median of 982 records/s
across two runs, under a different container profile (2 CPU / 768 MiB per load and database
container, eight workers). Variation was material. That is historical evidence, not a dev.28
large-backlog qualification. Earlier OpenShift Local sustained-arrival tests also exceeded
latency targets and rejected offers at higher offered rates despite preserving admitted work.
Short drain tests do not establish sustained capacity.

PostgreSQL schema 11 uses an eligibility-time index to avoid the observed large-queue claim
sorts. Exact-ID queries avoid the partial-index scans observed after queue churn. For query-plan
diagnostics against disposable local fixtures, build the load tool, then run:

```bash
bash eng/dotnet.sh benchmarks/EventGuard.LoadTests/bin/Release/net10.0/EventGuard.LoadTests.dll \
  --claim-due-plans .artifacts/claim-due-first-run
bash eng/dotnet.sh benchmarks/EventGuard.LoadTests/bin/Release/net10.0/EventGuard.LoadTests.dll \
  --point-lookup-plans .artifacts/point-lookup-first-run
```

The diagnostics create and remove their own schemas and use rollback-only plans. Their query
timings are separate from application throughput. WAL, durable records, index maintenance and
retained receipts still use disk; removing claim-sort temporary I/O does not remove those costs.

Millions of queued records and millions of completions per second are unqualified targets.
Measure handler cost, payload size, tenant distribution, database contention, network and durable
commit latency. Sharding must preserve business/reliability data atomicity and stable deduplication
routing; adding workers alone does not remove a store bottleneck.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| EventGuard package cannot be restored | Enable nuget.org, use the exact preview version and check package source mapping. For a source-only candidate, run `eng/build.sh` and add the local feed. Newly uploaded versions must finish NuGet validation/indexing before restore. |
| Native tests cannot connect | Start the corresponding Compose services, check their health and loopback ports. The fixture tools deliberately reject production addresses. |
| Startup rejects schema or permissions | Run explicit migration with the appropriate identity; install enabled Operations read indexes and grant role-specific access. Do not make the worker perform DDL. |
| SQL Server startup fails | Verify major 16+, RCSI on, delayed durability off, MARS off and a writable DB. Local ARM execution uses x64 emulation. |
| Dashboard returns 404 for a record | Check the registered scope and exact read/data/command claim; unknown and unauthorized records intentionally share 404. |
| Cursor is invalid after restart | Local keys are ephemeral; start a fresh list. Production needs shared protected keys. Views also expire after 15 minutes. |
| Replay says Accepted but work is Pending | Run the application's worker; dashboard command acceptance does not run business code. |
| API request timed out | Preserve the request ID and inspect/retry its receipt; do not assume rollback or create a new logical request automatically. |
| Payload preview is missing | Check explicit data fields, data permission, JSON/size limits and whether retention removed the data. |
| Pilot says port 5194 is in use | Stop your manual pilot API before automated verification. The runner will not kill unrelated listeners. |
| Recovered attempt reason says `unrecognized` | Use matching current dashboard and metadata adapters; the standard `lease-recovered` reason is recognized and explained. Unknown custom reasons remain masked. |

## Contributing and release preparation

`eng/pack.sh` runs the [NuGet content allowlist](eng/verify-package-contents.py) for all 16
libraries. Only the package metadata, framework assemblies, logo, README and license may ship;
deployment manifests, samples, tests and source files stay in the repository. A negative
control proves that an accidental deployment file is rejected. This check is separate from
API compatibility and vulnerability auditing.

Use parameterized SQL, bounded concurrency/buffers, deterministic identities and explicit
capability checks. Avoid payload/secret logging and high-cardinality metric labels.
Scope IDs are not authorization. UI commands must use authorized Operations ports.
Telemetry must never decide transaction ownership or recovery. Preserve the distinctions
between local transactional guarantees and external broker/business effects.

Run `python3 eng/check-repository.py` for documentation/package consistency. Run the
relevant architecture/conformance/native tests for behavioral changes, followed by the
full verification for release candidates. A passing mock is not evidence of native
transaction, network-loss or process-kill behavior. Report failed/unavailable checks
and API/schema changes explicitly. Keep dependency/container versions pinned and tested.

The repository contains one maintained Markdown document: this README. Packages embed
this same file and declare MIT licensing. CI checks repository consistency, builds/tests
the pinned matrix and builds the package-only pilot. Only a fully successful run saves the
16 verified packages and their SHA-256 manifest for 14 days. The separate, manually started
NuGet preview workflow publishes those exact files after validating the source commit,
version, CI result and package contents. No workflow deploys applications.
Local caches, binaries, credentials/certificates, raw evidence
and historical research are excluded from the source candidate.

The public tree contains the libraries and dashboard in `src/`, integration examples in
`samples/`, `templates/` and `pilots/`, verification tools in `tests/`, `benchmarks/` and `eng/`,
and optional deployment examples in `deploy/`. These support development and integration;
only the sixteen library packages are installed by NuGet.

Keep real connection strings, cluster credentials, certificates and customer data outside
the source tree. Local overrides such as `appsettings.Local.json`, `.env` and `kubeconfig`
are ignored. The repository check also rejects accidentally tracked generated files and
common credential signatures, reporting only the filename, line and rule. This supplements
manual review; it does not detect every possible secret. The documented Compose/test passwords
belong only to disposable fixtures. Review the staged diff before committing and never
force-add ignored local output.

Before changing an already consumed package, retain its original `.nupkg` files and
run [the API gate](eng/verify-package-api.py) against that explicit baseline:

```bash
python3 eng/verify-package-api.py --baseline /path/to/previous-packages \
  --baseline-version 0.1.0-dev.27 --output .artifacts/api-comparison-first-run
```

Build the candidate first. The gate packs all 16 libraries, compares public APIs (including
parameter names), records package hashes, and requires the SDK validation task to run.
It first checks that removing a public method actually fails. It generates no compatibility
suppressions. API compatibility is separate from behavioral, database and deployment
compatibility: retain the native tests and backup/restore rehearsal. Prereleases require
exact pins and explicit upgrade review. A future stable 1.x release must preserve its
public API within that major version; a breaking API needs a new major version. This
project has not made a stable-release support commitment; preview publication does not
change that status.

Public source releases and NuGet publication are separate steps. Release maintainers must
verify package ownership and qualify the release candidate before publishing. Local build
scripts do not commit, push, publish packages or deploy applications.

### Publish a NuGet preview

The [publication workflow](.github/workflows/publish-nuget.yml) uses
[NuGet Trusted Publishing](https://learn.microsoft.com/en-us/nuget/nuget-org/trusted-publishing).
The configured NuGet account is `yarasa`; the GitHub repository owner is `yarasaa`.
These are different account names. Configure a NuGet trusted publishing policy with:

| Setting | Value |
| --- | --- |
| NuGet policy owner | `yarasa` |
| GitHub repository owner | `yarasaa` |
| Repository | `EventGuard` |
| Workflow file | `publish-nuget.yml` (filename only) |
| Environment | `nuget` |
| Package pattern | `EventGuard.*` |
| Permissions | Push new packages and new versions |

Create the matching `nuget` environment in GitHub repository settings and restrict its
deployment branches to `main`. Add a required reviewer when a separate release approval
is desired. Never paste an API key into the repository or workflow; the login action
exchanges GitHub's identity for a short-lived publishing credential.

1. Push the reviewed candidate to `main` and wait for **build-and-reliability** to pass.
   Its artifact must contain all 16 packages and `release.json`; older runs without this
   artifact do not qualify. Record the numeric run ID from its Actions URL.
2. Open **Actions → publish-nuget-preview → Run workflow**, choose `main`, enter that CI
   run ID and the exact preview version from `Directory.Build.props` (currently `0.1.0-dev.28`).
   The workflow refuses another commit, another branch, a failed/incomplete CI run,
   pull-request builds, stable versions, missing/extra packages or changed package bytes.
   It downloads the tested packages without rebuilding or repeating the native test suite.
3. Wait for all packages to finish NuGet validation/indexing. Verify a fresh application
   using only nuget.org with the exact version, then update this README's release status
   and public installation examples. A successful upload alone is not an installation check.

NuGet publishes packages individually. If an upload fails partway through, inspect the
error and rerun the same workflow for the same commit and CI run; already uploaded versions
are skipped and never overwritten. A changed candidate needs a new version and a new CI run.
Expired artifacts require a new successful CI run. Name availability checks do not reserve
package IDs or prove permission to publish them.

## Known limitations

- Experimental API/schema; no production support or availability promise.
- Production OpenShift/OIDC policy, router certificate renewal,
  storage/database HA and multi-node broker failover need deployment-specific acceptance.
  Local real OIDC, backend certificate renewal, native PostgreSQL backup/restore and a
  three-node worker crash drill passed with the limits described above. The OpenShift Local
  load measurements cover a single-node PostgreSQL fixture. PostgreSQL claim-sort and
  exact-ID lookup improvements have local regression coverage. Deployment-scale
  sustained capacity, other queue distributions and database configurations still need measurement.
- The pilot was built by the package author using synthetic data and an existing dependency
  cache. The generated-worker check uses a new cache, but independent team onboarding, time
  savings and a genuinely fresh machine remain separate validation.
- Direct public message-ID status lookup remains open. The new typed client looks up a work ID
  returned by submission; the older pilot uses a scoped read-only diagnostics bridge.
- Additional adapters, provider-specific HTTP/webhook authentication, saga compensation, external blob storage,
  multi-store federation and global cross-shard budgets are not implemented.
- No measured million-record backlog, million/s guarantee or general exactly-once external
  delivery. Always validate your own business invariants and deployment profile.

MIT licensed; see [LICENSE](LICENSE). Dependency licenses remain their respective owners'.
