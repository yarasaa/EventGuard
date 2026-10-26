#!/usr/bin/env python3
"""Generate package-only apps outside the checkout and exercise both native stores.

Only the repository's disposable Docker fixtures are used. This is an automated
README/template check, not evidence of an independent developer's usability study.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import tempfile
import time
import uuid
import xml.etree.ElementTree as ET
from urllib.request import urlopen
from urllib.error import URLError
from xml.sax.saxutils import escape

ROOT = Path(__file__).resolve().parents[1]
DOTNET = ROOT / ".tools/dotnet/dotnet"
DOCKER = ["bash", str(ROOT / "eng/docker.sh"), "compose", "exec", "-T"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packages", type=Path, default=ROOT / ".artifacts/packages")
    parser.add_argument("--output", type=Path, required=True, help="New evidence directory; never overwritten.")
    parser.add_argument("--backup-restore", action="store_true", help="Restore a native backup into a second disposable database and verify deduplication and projection.")
    parser.add_argument("--optional-adapters", action="store_true", help="Also compile EF Core and Kafka/RabbitMQ hosting APIs from NuGet packages outside the checkout.")
    options = parser.parse_args()
    output = options.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    project_root = Path(tempfile.mkdtemp(prefix="eventguard-onboarding-"))
    env = os.environ.copy()
    for key in list(env):
        if key.startswith(("EventGuard__", "EVENTGUARD_", "Dashboard__", "DOTNET_", "ASPNETCORE_")):
            env.pop(key)
    env.update(DOTNET_ROOT=str(DOTNET.parent), DOTNET_CLI_HOME=str(output / "cli-home"),
               NUGET_PACKAGES=str(output / "nuget-cache"), DOTNET_CLI_TELEMETRY_OPTOUT="1",
               DOTNET_SKIP_FIRST_TIME_EXPERIENCE="1", DOTNET_ENVIRONMENT="Production")
    hive = ["--debug:custom-hive", str(output / "template-hive")]
    evidence = {"packageOnly": True, "newPackageCache": True, "independentDeveloper": False,
                "projectsOutsideCheckout": str(project_root), "cases": []}

    def run(args, *, cwd=ROOT, overrides=None, expected=0, timeout=180):
        completed = subprocess.run([str(a) for a in args], cwd=cwd, env=env | (overrides or {}),
                                   text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
        with (output / "commands.log").open("a") as log:
            log.write("$ " + " ".join(str(a) for a in args) + "\n" + completed.stdout + "\n")
        if completed.returncode != expected:
            raise RuntimeError(f"Command returned {completed.returncode}, expected {expected}: {args[0:3]}\n{completed.stdout}")
        return completed.stdout

    def pg(database, sql):
        return run(DOCKER + ["postgres", "psql", "-U", "eventguard", "-d", database, "-v", "ON_ERROR_STOP=1", "-At", "-c", sql])

    def sql(database, query):
        return run(DOCKER + ["sqlserver", "bash", "-c",
                   'exec /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -b "$@"',
                   "--", "-d", database, "-h", "-1", "-W", "-Q", query])

    def check_restore(provider, database, framework, config, project, dll, work_id):
        restored = "eventguard_test_restore_" + uuid.uuid4().hex
        suffix = ".dump" if provider == "PostgreSql" else ".bak"
        archive = output / f"backup-{framework}-{provider}{suffix}"
        native_backup = "/var/opt/mssql/data/" + restored + ".bak"
        started = time.monotonic()
        try:
            if provider == "PostgreSql":
                with archive.open("xb") as target:
                    subprocess.run(DOCKER + ["postgres", "pg_dump", "-U", "eventguard", "-d", database, "-Fc"],
                                   cwd=ROOT, env=env, stdout=target, check=True, timeout=120)
                pg("postgres", f"CREATE DATABASE {restored}")
                with archive.open("rb") as source:
                    subprocess.run(DOCKER + ["postgres", "pg_restore", "-U", "eventguard", "-d", restored, "--exit-on-error"],
                                   cwd=ROOT, env=env, stdin=source, check=True, timeout=120)
            else:
                sql("master", f"BACKUP DATABASE [{database}] TO DISK=N'{native_backup}' WITH COPY_ONLY, CHECKSUM, COMPRESSION")
                sql("master", f"RESTORE VERIFYONLY FROM DISK=N'{native_backup}' WITH CHECKSUM")
                sql("master", f"RESTORE DATABASE [{restored}] FROM DISK=N'{native_backup}' WITH CHECKSUM, "
                    f"MOVE N'{database}' TO N'/var/opt/mssql/data/{restored}.mdf', "
                    f"MOVE N'{database}_log' TO N'/var/opt/mssql/data/{restored}_log.ldf'")
                run(["bash", ROOT / "eng/docker.sh", "compose", "cp", "sqlserver:" + native_backup, archive])
            restored_config = config | {"EVENTGUARD_CONNECTION": config["EVENTGUARD_CONNECTION"].replace("Database=" + database, "Database=" + restored)}
            def restored_app(*args):
                return run([DOTNET, dll, *args], cwd=project, overrides=restored_config)
            report = json.loads(restored_app("check"))
            assert report["SchemaVersion"] == (11 if provider == "PostgreSql" else 8)
            assert restored_app("status", work_id).strip() == "Completed attempts=1"
            assert restored_app("submit").strip() == "Duplicate " + work_id
            projection = (pg(restored, "SELECT id || '|' || revision || '|' || name FROM eventguard_worker.sample_products")
                          if provider == "PostgreSql" else sql(restored,
                          "SET NOCOUNT ON; SELECT CONCAT(id,'|',revision,'|',name) FROM eventguard_worker.sample_products"))
            assert projection.strip() == "book-001|18|Example book", projection
            return {"verified": True, "seconds": round(time.monotonic() - started, 3),
                    "backupBytes": archive.stat().st_size, "backupSha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
                    "deduplicationPreserved": True, "projectionPreserved": True, "completedAttempts": 1}
        finally:
            # Names are generated here; no production connection or caller-selected database is accepted.
            if provider == "PostgreSql": pg("postgres", f"DROP DATABASE IF EXISTS {restored} WITH (FORCE)")
            else:
                sql("master", f"IF DB_ID('{restored}') IS NOT NULL BEGIN ALTER DATABASE [{restored}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{restored}]; END")
                run(DOCKER + ["sqlserver", "rm", "-f", native_backup])

    def check_dashboard(config, framework, provider, work_id):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        with (output / f"dashboard-{framework}-{provider}.log").open("w") as log:
            process = subprocess.Popen(["bash", "eng/worker-dashboard.sh"], cwd=ROOT,
                                       env=env | config | {"EVENTGUARD_DASHBOARD_PORT": str(port)},
                                       stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                def get(route):
                    with urlopen(f"http://127.0.0.1:{port}{route}", timeout=5) as response:
                        return json.load(response)
                deadline = time.monotonic() + 45
                while True:
                    if process.poll() is not None:
                        raise RuntimeError("Template dashboard exited before readiness. Inspect its log.")
                    try:
                        scopes = get("/api/scopes")
                        break
                    except (URLError, TimeoutError):
                        if time.monotonic() > deadline: raise
                        time.sleep(0.2)
                assert scopes == [{"key": "products", "label": "Product projection"}]
                detail = get(f"/api/works/products/{work_id}")
                fields = get(f"/api/works/products/{work_id}/data")["value"]["fields"]
                assert [(f["label"], f["value"]) for f in fields] == [("Product ID", "book-001"), ("Revision", "18")]
                (output / f"dashboard-{framework}-{provider}.json").write_text(json.dumps(detail, indent=2) + "\n")
            finally:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                    try: process.wait(timeout=35)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL); process.wait(timeout=10)

    try:
        run([DOTNET, "new", "install", ROOT / "templates/EventGuard.Worker", *hive])
        for framework in ("net8.0", "net10.0"):
            name = "Consumer" + framework.replace("net", "").replace(".", "")
            project = project_root / name
            run([DOTNET, "new", "eventguard-worker", "--name", name, "--framework", framework,
                 "--output", project, *hive], cwd=project_root)
            (project / "NuGet.Config").write_text(f'''<configuration><packageSources><clear />
<add key="local" value="{escape(str(options.packages.resolve()))}" />
<add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
</packageSources></configuration>''')
            assert "ProjectReference" not in (project / f"{name}.csproj").read_text()
            if options.optional_adapters:
                version = ET.parse(ROOT / "Directory.Build.props").findtext(".//Version")
                definition = project / f"{name}.csproj"
                tree = ET.parse(definition)
                group = ET.SubElement(tree.getroot(), "ItemGroup")
                packages = {p: version for p in ("EventGuard.EntityFrameworkCore", "EventGuard.Hosting.Kafka", "EventGuard.Hosting.RabbitMq")}
                packages["Npgsql.EntityFrameworkCore.PostgreSQL"] = "8.0.11" if framework == "net8.0" else "10.0.3"
                for package, package_version in packages.items():
                    ET.SubElement(group, "PackageReference", Include=package, Version=package_version)
                tree.write(definition, encoding="unicode")
                (project / "OptionalAdapterCompilation.cs").write_text('''using EventGuard.Hosting;
using EventGuard.EntityFrameworkCore;
using EventGuard.PostgreSql;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Confluent.Kafka;
using RabbitMQ.Client;
using APP.Configuration;
using APP.Contracts;
using APP.Handlers;

public static class OptionalAdapterCompilation
{
    public static void Kafka(IServiceCollection services, WorkerSettings settings, ConsumerConfig config) =>
        services.AddEventGuard().AddKafkaJsonConsumer<ProductChanged, PostgreSqlSession, PostgreSqlProductHandler>(
            "products", settings.ConsumerOptions(), config, ["product.changed"],
            storeFactory: sp => sp.GetRequiredService<PostgreSqlWorkStore>());

    public static void Rabbit(IServiceCollection services, WorkerSettings settings, Action<ConnectionFactory> config) =>
        services.AddEventGuard().AddRabbitMqJsonConsumer<ProductChanged, PostgreSqlSession, PostgreSqlProductHandler>(
            "products", settings.ConsumerOptions(), "product-projection", config,
            storeFactory: sp => sp.GetRequiredService<PostgreSqlWorkStore>());

    public static async ValueTask Ef(PostgreSqlSession session, Func<DbContext, CancellationToken, ValueTask> apply, CancellationToken ct)
    {
        await using var context = new DbContext(new DbContextOptionsBuilder().UseNpgsql(session.Connection).Options);
        await EventGuardEfCore.ExecuteAsync(context, session.Transaction, apply, ct);
    }
}
'''.replace("APP.", name + "."))
            run([DOTNET, "restore"], cwd=project)
            run([DOTNET, "restore", "--locked-mode"], cwd=project)
            run([DOTNET, "build", "-c", "Release", "--no-restore"], cwd=project)
            dll = project / "bin/Release" / framework / (name + ".dll")
            for provider in ("PostgreSql", "SqlServer"):
                database = "eventguard_test_onboard_" + uuid.uuid4().hex
                case = {"framework": framework, "provider": provider, "database": database, "passed": False}
                evidence["cases"].append(case)
                worker = None
                worker_log = None
                created = False
                try:
                    if provider == "PostgreSql":
                        pg("postgres", f"CREATE DATABASE {database}")
                        created = True
                        connection = f"Host=127.0.0.1;Port=55432;Database={database};Username=eventguard;Password=eventguard-local-only;Timeout=5"
                    else:
                        sql("master", f"CREATE DATABASE [{database}]")
                        created = True
                        sql("master", f"ALTER DATABASE [{database}] SET READ_COMMITTED_SNAPSHOT ON")
                        connection = f"Server=127.0.0.1,51433;Database={database};User ID=sa;Password=EventGuard-Local-Only!2026;Encrypt=False;TrustServerCertificate=True;Connect Timeout=5"
                    config = {"EVENTGUARD_CONNECTION": connection, "EventGuard__Provider": provider}
                    def app(*args, expected=0):
                        return run([DOTNET, dll, *args], cwd=project, overrides=config, expected=expected)
                    app("check", expected=1)
                    app("migrate")
                    app("migrate")  # repeat must keep schema/data intact
                    report = json.loads(app("check"))
                    assert report["SchemaVersion"] == (11 if provider == "PostgreSql" else 8)
                    first = app("submit").strip().split()
                    assert first[0] == "Accepted"
                    work_id = first[1]
                    assert app("submit").strip() == "Duplicate " + work_id
                    assert app("status", work_id).strip() == "Pending attempts=0"
                    worker_log = (output / f"worker-{framework}-{provider}.log").open("w")
                    worker = subprocess.Popen([str(DOTNET), str(dll), "run"], cwd=project, env=env | config,
                                              stdout=worker_log, stderr=subprocess.STDOUT)
                    deadline = time.monotonic() + 45
                    while True:
                        if worker.poll() is not None:
                            raise RuntimeError("Hosted worker exited unexpectedly.")
                        state = app("status", work_id).strip()
                        if state == "Completed attempts=1":
                            break
                        if time.monotonic() > deadline:
                            raise TimeoutError(f"Worker did not complete: {state}")
                        time.sleep(0.2)
                    worker.send_signal(signal.SIGTERM)
                    assert worker.wait(timeout=35) == 0
                    worker = None
                    assert app("submit").strip() == "Duplicate " + work_id
                    assert app("status", work_id).strip() == "Completed attempts=1"
                    app("migrate")
                    projection = (pg(database, "SELECT id || '|' || revision || '|' || name FROM eventguard_worker.sample_products")
                                  if provider == "PostgreSql" else sql(database,
                                  "SET NOCOUNT ON; SELECT CONCAT(id,'|',revision,'|',name) FROM eventguard_worker.sample_products"))
                    assert projection.strip() == "book-001|18|Example book", projection
                    # Configuration errors remain readable; native exception details remain hidden.
                    invalid = run([DOTNET, dll, "check"], cwd=project,
                        overrides=config | {"EventGuard__MaximumConcurrency": "0"}, expected=1)
                    assert "EventGuard:MaximumConcurrency" in invalid
                    check_dashboard(config, framework, provider, work_id)
                    # After a healthy start, make only this disposable test store unavailable.
                    # The hosted supervisor must stop the process with a nonzero exit code.
                    failure_log = output / f"worker-failure-{framework}-{provider}.log"
                    with failure_log.open("w") as log:
                        worker = subprocess.Popen([str(DOTNET), str(dll), "run"], cwd=project, env=env | config,
                                                  stdout=log, stderr=subprocess.STDOUT)
                        started_by = time.monotonic() + 30
                        while "Application started." not in failure_log.read_text():
                            if worker.poll() is not None or time.monotonic() > started_by:
                                raise RuntimeError("Worker did not restart before failure injection.")
                            time.sleep(0.1)
                        if provider == "PostgreSql":
                            pg(database, "ALTER TABLE eventguard_worker.works RENAME TO works_unavailable")
                        else:
                            sql(database, "EXEC sp_rename 'eventguard_worker.works', 'works_unavailable'")
                        assert worker.wait(timeout=35) == 1
                        worker = None
                        if provider == "PostgreSql":
                            pg(database, "ALTER TABLE eventguard_worker.works_unavailable RENAME TO works")
                        else:
                            sql(database, "EXEC sp_rename 'eventguard_worker.works_unavailable', 'works'")
                    assert app("status", work_id).strip() == "Completed attempts=1"
                    if options.backup_restore:
                        case["backupRestore"] = check_restore(provider, database, framework, config, project, dll, work_id)
                    case.update(passed=True, schemaVersion=report["SchemaVersion"],
                                workId=work_id, duplicatePreserved=True, completedAttempts=1, gracefulSigterm=True,
                                migrationRepeatPreservedData=True, dashboardDetailAndSelectedFields=True,
                                databaseFailureExitsNonzero=True, optionalAdaptersCompiled=options.optional_adapters)
                    print(f"PASS {framework} / {provider}: migrate, check, Accepted, Duplicate, Completed, SIGTERM", flush=True)
                finally:
                    if worker is not None and worker.poll() is None:
                        worker.terminate()
                        try: worker.wait(timeout=35)
                        except subprocess.TimeoutExpired:
                            worker.kill(); worker.wait(timeout=10)
                    if worker_log: worker_log.close()
                    if created:
                        if provider == "PostgreSql": pg("postgres", f"DROP DATABASE {database} WITH (FORCE)")
                        else: sql("master", f"ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{database}]")
                        case["temporaryDatabaseRemoved"] = True
    finally:
        (output / "verification.json").write_text(json.dumps(evidence, indent=2) + "\n")
    print(f"Evidence: {output / 'verification.json'}")


if __name__ == "__main__":
    main()
