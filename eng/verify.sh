#!/usr/bin/env bash
set -euo pipefail
project_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$project_root"
python3 eng/check-repository.py
bash eng/dotnet.sh restore EventGuard.slnx --locked-mode
bash eng/dotnet.sh build EventGuard.slnx -c Release --no-restore -m:1 -p:UseSharedCompilation=false
bash eng/dotnet.sh test tests/EventGuard.Architecture.Tests -c Release --no-build -m:1 --logger 'trx;LogFilePrefix=verify-architecture' --results-directory .artifacts/test-results
bash eng/dotnet.sh test tests/EventGuard.EntityFrameworkCore.Tests -c Release --no-build -m:1 --logger 'trx;LogFilePrefix=verify-efcore' --results-directory .artifacts/test-results
bash eng/dotnet.sh test tests/EventGuard.Hosting.Tests -c Release --no-build -m:1 --logger 'trx;LogFilePrefix=verify-hosting' --results-directory .artifacts/test-results
bash eng/dotnet.sh test tests/EventGuard.Core.Tests -c Release --no-build -m:1 --logger 'trx;LogFilePrefix=verify-core' --results-directory .artifacts/test-results
bash eng/dotnet.sh test tests/EventGuard.PostgreSql.Tests -c Release --no-build -m:1 --logger 'trx;LogFilePrefix=verify-postgres' --results-directory .artifacts/test-results
bash eng/dotnet.sh test tests/EventGuard.RabbitMq.Tests -c Release --no-build -m:1 --logger 'trx;LogFilePrefix=verify-rabbitmq' --results-directory .artifacts/test-results
# Both frameworks share one development SQL Server; keep their fixture DDL and fault injection sequential.
# Concurrency races inside the tests remain enabled.
bash eng/dotnet.sh test tests/EventGuard.SqlServer.Tests -c Release --no-build -m:1 --logger 'trx;LogFilePrefix=verify-sqlserver' --results-directory .artifacts/test-results
# The Kafka fault proxy owns a fixed loopback listener; do not overlap framework runs.
bash eng/dotnet.sh test tests/EventGuard.Kafka.Tests -c Release --no-build -m:1 --logger 'trx;LogFilePrefix=verify-kafka' --results-directory .artifacts/test-results
bash eng/dotnet.sh test tests/EventGuard.Operations.Tests -c Release --no-build -m:1 --logger 'trx;LogFilePrefix=verify-operations' --results-directory .artifacts/test-results
bash eng/dotnet.sh test tests/EventGuard.Dashboard.Tests -c Release --no-build -m:1 --logger 'trx;LogFilePrefix=verify-dashboard' --results-directory .artifacts/test-results
bash eng/dotnet.sh test tests/EventGuard.Operations.Prometheus.Tests -c Release --no-build -m:1 --logger 'trx;LogFilePrefix=verify-prometheus' --results-directory .artifacts/test-results
# Small correctness smoke only; performance results are never asserted in shared CI.
bash eng/dotnet.sh test tests/EventGuard.LoadTests.Tests -c Release --no-build -m:1 --logger 'trx;LogFilePrefix=verify-load-harness' --results-directory .artifacts/test-results
bash eng/pack.sh "${EVENTGUARD_PACKAGE_OUTPUT:-.artifacts/packages}"
# Exercise composable public APIs from a fresh package-only application after packing.
python3 eng/verify-composable.py --packages "${EVENTGUARD_PACKAGE_OUTPUT:-.artifacts/packages}" \
  --output ".artifacts/composable-verify-$(date +%Y%m%d%H%M%S)-$$"
