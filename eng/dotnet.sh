#!/usr/bin/env bash
set -euo pipefail
project_root="$(cd "$(dirname "$0")/.." && pwd)"
export DOTNET_CLI_TELEMETRY_OPTOUT=1
export DOTNET_SKIP_FIRST_TIME_EXPERIENCE=1
export DOTNET_CLI_HOME="$project_root/.tools/cli-home"
export NUGET_PACKAGES="$project_root/.tools/nuget-packages"
if [[ -x "$project_root/.tools/dotnet/dotnet" ]]; then
  export DOTNET_ROOT="$project_root/.tools/dotnet"
  exec "$DOTNET_ROOT/dotnet" "$@"
fi
exec dotnet "$@"
