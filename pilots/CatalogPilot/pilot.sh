#!/usr/bin/env bash
set -euo pipefail
pilot_root="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$pilot_root/../.." && pwd)"
cd "$repo_root"
mode="${1:-help}"
if [[ "$#" -gt 0 ]]; then shift; fi
framework="${PILOT_FRAMEWORK:-net10.0}"
case "$framework" in net8.0|net10.0) ;; *) echo 'Use PILOT_FRAMEWORK=net8.0 or net10.0' >&2; exit 1;; esac
case "$mode" in
  build)
    pilot_restore="$(python3 pilots/CatalogPilot/prepare-restore.py)"
    bash eng/dotnet.sh restore pilots/CatalogPilot/CatalogPilot.csproj --locked-mode \
      --configfile pilots/CatalogPilot/NuGet.Config --packages "$pilot_restore/packages" \
      -p:NuGetLockFilePath="$pilot_restore/packages.lock.json"
    exec bash eng/dotnet.sh build pilots/CatalogPilot/CatalogPilot.csproj -c Release --no-restore -m:1 \
      -p:UseSharedCompilation=false -p:NuGetLockFilePath="$pilot_restore/packages.lock.json"
    ;;
  verify) exec python3 pilots/CatalogPilot/verify.py "$@" ;;
  *) exec bash eng/dotnet.sh "pilots/CatalogPilot/bin/Release/$framework/CatalogPilot.dll" "$mode" "$@" ;;
esac
