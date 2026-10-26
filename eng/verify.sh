#!/usr/bin/env bash
set -euo pipefail
project_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$project_root"
python3 eng/check-repository.py
bash eng/dotnet.sh restore EventGuard.slnx --locked-mode
bash eng/dotnet.sh build EventGuard.slnx -c Release --no-restore -m:1 -p:UseSharedCompilation=false
# Report every failing test project in one run, then fail before packaging. Database
# fixtures and the Kafka proxy stay sequential; races inside each suite remain enabled.
failed_projects=()
for spec in Architecture:architecture EntityFrameworkCore:efcore Hosting:hosting Core:core \
    PostgreSql:postgres RabbitMq:rabbitmq SqlServer:sqlserver Kafka:kafka Operations:operations \
    Dashboard:dashboard Operations.Prometheus:prometheus LoadTests:load-harness; do
  project="tests/EventGuard.${spec%:*}.Tests"
  if ! bash eng/dotnet.sh test "$project" -c Release --no-build -m:1 \
      --logger "trx;LogFilePrefix=verify-${spec#*:}" --results-directory .artifacts/test-results; then
    failed_projects+=("$project")
  fi
done
if [[ ${#failed_projects[@]} -gt 0 ]]; then
  printf 'Failed test project: %s\n' "${failed_projects[@]}" >&2
  exit 1
fi
# LoadTests above is a correctness smoke; shared CI never asserts performance figures.
bash eng/pack.sh "${EVENTGUARD_PACKAGE_OUTPUT:-.artifacts/packages}"
# Exercise composable public APIs from a fresh package-only application after packing.
python3 eng/verify-composable.py --packages "${EVENTGUARD_PACKAGE_OUTPUT:-.artifacts/packages}" \
  --output ".artifacts/composable-verify-$(date +%Y%m%d%H%M%S)-$$"
