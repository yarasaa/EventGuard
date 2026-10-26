#!/usr/bin/env bash
set -euo pipefail
project_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$project_root"
# Local sample only. Schema/data initialization is not part of the production monitoring adapter.
bash eng/dotnet.sh build samples/EventGuard.MonitoredWorker.Sample -c Release -m:1 -p:UseSharedCompilation=false
python3 - <<'CHECKPORTS'
import socket
for port in (5190,5191,59090):
    with socket.socket() as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind(('127.0.0.1',port))
CHECKPORTS
sample_config="$(mktemp -d)"
sample_container="eventguard-worker-monitor-$$"
sample_a=""
sample_b=""
cleanup() {
  trap - EXIT INT TERM
  if [[ -n "$sample_a" ]]; then kill "$sample_a" 2>/dev/null || true; wait "$sample_a" 2>/dev/null || true; fi
  if [[ -n "$sample_b" ]]; then kill "$sample_b" 2>/dev/null || true; wait "$sample_b" 2>/dev/null || true; fi
  bash eng/docker.sh rm -f "$sample_container" >/dev/null 2>&1 || true
  rm -rf "$sample_config"
}
trap cleanup EXIT
trap 'exit 0' INT TERM
cat > "$sample_config/prometheus.yml" <<'YAML'
global:
  scrape_interval: 2s
  scrape_timeout: 1s
scrape_configs:
  - job_name: sample-workers
    authorization:
      credentials: local-worker-metrics-token-not-for-production
    metric_relabel_configs:
      - source_labels: [__name__]
        regex: '.*_(eventguard_worker_.+)'
        target_label: __name__
        replacement: '$1'
    static_configs:
      - targets: ['host.docker.internal:5190', 'host.docker.internal:5191']
        labels:
          eventguard_store: local-demo
YAML
bash eng/dotnet.sh samples/EventGuard.MonitoredWorker.Sample/bin/Release/net10.0/EventGuard.MonitoredWorker.Sample.dll --port 5190 --seed true &
sample_a=$!
# Sequential initialization avoids racing the sample-only CREATE TABLE path.
for attempt in {1..60}; do
  if curl --silent --fail --header 'Authorization: Bearer local-worker-metrics-token-not-for-production' http://127.0.0.1:5190/metrics >/dev/null; then break; fi
  if ! kill -0 "$sample_a" 2>/dev/null; then exit 1; fi
  sleep 1
done
curl --silent --fail --header 'Authorization: Bearer local-worker-metrics-token-not-for-production' http://127.0.0.1:5190/metrics >/dev/null
bash eng/dotnet.sh samples/EventGuard.MonitoredWorker.Sample/bin/Release/net10.0/EventGuard.MonitoredWorker.Sample.dll --port 5191 --stop-loops-after 30 &
sample_b=$!
bash eng/docker.sh run --rm -d --name "$sample_container" --publish 127.0.0.1:59090:9090 \
  --volume "$sample_config/prometheus.yml:/etc/prometheus/prometheus.yml:ro" \
  prom/prometheus:v3.14.0@sha256:5ce7540c3c00ef4ab0c9d2c995c6a5b9c421f44b4a115d97a2c7af3b1c21cbb0 \
  --config.file=/etc/prometheus/prometheus.yml --storage.tsdb.retention.time=1h --query.max-concurrency=2 --query.timeout=2s --query.max-samples=100000
printf '%s\n' 'Local worker monitoring demo is running. Set EVENTGUARD_WORKER_MONITOR_DEMO=true when starting dashboard-demo.sh. Ctrl-C stops this demo.'
wait "$sample_a" "$sample_b"
