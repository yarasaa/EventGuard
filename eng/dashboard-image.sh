#!/usr/bin/env bash
set -euo pipefail
project_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$project_root"
bash eng/dotnet.sh publish src/EventGuard.Dashboard -f net10.0 -c Release --no-build -o .artifacts/dashboard-container
bash eng/docker.sh build -f deploy/Dockerfile.dashboard -t eventguard-dashboard:0.1.0-dev.31 .artifacts/dashboard-container
