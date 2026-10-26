#!/usr/bin/env bash
set -euo pipefail
project_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$project_root"
package_output="${1:-.artifacts/packages}"
projects=(
  EventGuard.Hosting.Kafka EventGuard.Hosting.RabbitMq EventGuard.EntityFrameworkCore EventGuard.Core EventGuard.Runtime EventGuard.Hosting EventGuard.PostgreSql EventGuard.SqlServer
  EventGuard.RabbitMq EventGuard.Kafka EventGuard.Operations
  EventGuard.Operations.PostgreSql EventGuard.Operations.SqlServer
  EventGuard.Operations.Replay.PostgreSql EventGuard.Operations.Replay.SqlServer
  EventGuard.Operations.Prometheus
)
for project in "${projects[@]}"; do
  bash eng/dotnet.sh pack "src/$project/$project.csproj" -c Release --no-build --output "$package_output"
done

python3 eng/verify-package-contents.py --packages "$package_output"
