#!/usr/bin/env bash
set -euo pipefail
project_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$project_root"
python3 eng/check-repository.py
bash eng/dotnet.sh restore EventGuard.slnx --locked-mode
bash eng/dotnet.sh build EventGuard.slnx -c Release --no-restore -m:1 -p:UseSharedCompilation=false
bash eng/pack.sh "${1:-.artifacts/packages}"
