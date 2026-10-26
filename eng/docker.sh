#!/usr/bin/env bash
set -euo pipefail
project_root="$(cd "$(dirname "$0")/.." && pwd)"
public_config="$project_root/.tools/docker-public"
mkdir -p "$public_config"
if [[ -d /Applications/Docker.app/Contents/Resources/cli-plugins ]]; then
  printf '{"cliPluginsExtraDirs":["/Applications/Docker.app/Contents/Resources/cli-plugins"]}\n' > "$public_config/config.json"
else
  printf '{}\n' > "$public_config/config.json"
fi
# Only public development images; leave the user's credential helper and contexts untouched.
exec docker --config "$public_config" "$@"
