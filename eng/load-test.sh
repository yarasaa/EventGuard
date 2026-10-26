#!/usr/bin/env bash
set -euo pipefail
project_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$project_root"
if [[ "$#" -lt 2 ]]; then
  printf '%s\n' 'Usage: bash eng/load-test.sh postgresql|sqlserver NEW_OUTPUT_DIRECTORY [--count 2000 --concurrency 1,4,8 --repetitions 3 ...]'
  exit 2
fi
load_provider="$1"
load_output="$2"
shift 2
load_framework="${EVENTGUARD_LOAD_FRAMEWORK:-net10.0}"
if [[ "$load_provider" != postgresql && "$load_provider" != sqlserver ]] || [[ "$load_framework" != net8.0 && "$load_framework" != net10.0 ]]; then exit 2; fi
if [[ -e "$load_output" ]]; then printf '%s\n' 'Choose a new output directory; previous evidence is preserved.'; exit 2; fi
mkdir -p "$load_output"
python3 eng/capture-load-environment.py > "$load_output/environment.json"
bash eng/dotnet.sh build benchmarks/EventGuard.LoadTests -f "$load_framework" -c Release -m:1 -p:UseSharedCompilation=false -p:RestoreLockedMode=true > "$load_output/build.log" 2>&1
bash eng/dotnet.sh "benchmarks/EventGuard.LoadTests/bin/Release/$load_framework/EventGuard.LoadTests.dll" \
  --provider "$load_provider" --output "$load_output/results.json" "$@" | tee "$load_output/run.log"
