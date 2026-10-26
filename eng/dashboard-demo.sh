#!/usr/bin/env bash
set -euo pipefail
project_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$project_root"
for demo_project in samples/EventGuard.Dashboard.Sample src/EventGuard.Dashboard; do
  bash eng/dotnet.sh build "$demo_project" -f net10.0 -c Release -m:1 -p:UseSharedCompilation=false
done
bash eng/dotnet.sh run --project samples/EventGuard.Dashboard.Sample -c Release --no-build -- --module-config .artifacts/dashboard-module-ids.json
export ASPNETCORE_ENVIRONMENT=Development
export Dashboard__DevelopmentAccess=true
export Dashboard__StoreId=local-demo
export Dashboard__EnvironmentLabel='Local development'
export Dashboard__ApplicationLabel='Document service'
export Dashboard__BacklogEnabled=true
export Dashboard__MonitoringEnabled=true
export Dashboard__MetricsToken=local-demo-metrics-token-not-for-production
if [[ "${EVENTGUARD_WORKER_MONITOR_DEMO:-false}" == "true" ]]; then
  export Dashboard__Workers__Endpoint="${EVENTGUARD_WORKER_MONITOR_ENDPOINT:-http://127.0.0.1:59090}"
  export Dashboard__Workers__Groups__0__Key=sample-consumers
  export Dashboard__Workers__Groups__0__Label='Sample consumers'
  export Dashboard__Workers__Groups__0__Job=sample-workers
  export Dashboard__Workers__Groups__0__Component=consumer
  export Dashboard__Workers__Groups__0__ExpectedInstances=2
fi
export Dashboard__IngressMonitoringEnabled=true
export Dashboard__RecoveryEnabled=true
export Dashboard__BulkReplayEnabled=true
export Dashboard__AuditEnabled=true
export Dashboard__Scopes__0__AllowRecovery=true
export Dashboard__Scopes__0__AllowAudit=true
export Dashboard__Scopes__1__AllowAudit=true
export Dashboard__OutboxScopes__0__AllowRecovery=true
export Dashboard__OutboxScopes__0__AllowAudit=true
export Dashboard__OutboxScopes__1__AllowAudit=true
export Dashboard__ReplayEnabled=true
export Dashboard__OutboxReplayEnabled=true
export Dashboard__OutboxScopes__0__AllowReplay=true
export Dashboard__ReplayConnectionString='Host=127.0.0.1;Port=55432;Database=eventguard_test;Username=eventguard;Password=eventguard-local-only'
export Dashboard__Scopes__0__AllowReplay=true
python3 - <<'PYMODULE' > .artifacts/dashboard-module-settings.env
import json
from pathlib import Path
ids=json.loads(Path('.artifacts/dashboard-module-ids.json').read_text())
for i,(key,label,kind,scope) in enumerate([('selections','Source selections','Snapshots',ids['Jobs']),('services','External service requests','ExternalActions',ids['External']),('reports','Service reports','ExternalNotices',ids['External']),('flows','Workflows','Workflows',ids['Jobs']),('capacity','Service capacity','ServiceLimit',ids['Limit'])]):
    for field,value in {'Key':key,'Label':label,'Module':kind,'ScopeId':scope,'Description':'Simulated module examples; no external service is called.'}.items():
        print(f'Dashboard__Modules__{i}__{field}={value}')
PYMODULE
while IFS= read -r module_setting; do export "$module_setting"; done < .artifacts/dashboard-module-settings.env
export Dashboard__Provider=PostgreSql
export Dashboard__Schema=eventguard_dashboard_sample_v2
export Dashboard__ConnectionString='Host=127.0.0.1;Port=55432;Database=eventguard_test;Username=eventguard;Password=eventguard-local-only'
export Dashboard__Scopes__0__Key=documents
export Dashboard__Scopes__0__Label='Document indexing'
export Dashboard__Scopes__0__Tenant=demo
export Dashboard__Scopes__0__Consumer=document-indexer
export Dashboard__Scopes__0__Source=document-service
export Dashboard__Scopes__0__EventType=document.updated
export Dashboard__Scopes__1__Key=notifications
export Dashboard__Scopes__1__Label='Notification delivery'
export Dashboard__Scopes__1__Tenant=demo
export Dashboard__Scopes__1__Consumer=notification-sender
export Dashboard__Scopes__1__Source=notification-service
export Dashboard__Scopes__1__EventType=notification.requested
export Dashboard__Scopes__0__Operation='Update the document search index'
export Dashboard__Scopes__0__Description='Refresh the searchable document using the incoming revision. Demo attempts simulate outcomes; no search engine is connected.'
export Dashboard__Scopes__0__DataFields__0__Label='Document reference'
export Dashboard__Scopes__0__DataFields__0__Pointer='/reference'
export Dashboard__Scopes__0__DataFields__1__Label='Requested action'
export Dashboard__Scopes__0__DataFields__1__Pointer='/requestedAction'
export Dashboard__Scopes__0__DataFields__2__Label='Document title'
export Dashboard__Scopes__0__DataFields__2__Pointer='/document/title'
export Dashboard__Scopes__0__DataFields__3__Label='Incoming revision'
export Dashboard__Scopes__0__DataFields__3__Pointer='/document/revision'
export Dashboard__Scopes__1__Operation='Deliver a document update notification'
export Dashboard__Scopes__1__Description='Send a notification through the requested channel and template. Demo attempts simulate outcomes; no message is sent.'
export Dashboard__Scopes__1__DataFields__0__Label='Notification reference'
export Dashboard__Scopes__1__DataFields__0__Pointer='/reference'
export Dashboard__Scopes__1__DataFields__1__Label='Requested action'
export Dashboard__Scopes__1__DataFields__1__Pointer='/requestedAction'
export Dashboard__Scopes__1__DataFields__2__Label='Channel'
export Dashboard__Scopes__1__DataFields__2__Pointer='/notification/channel'
export Dashboard__Scopes__1__DataFields__3__Label='Template'
export Dashboard__Scopes__1__DataFields__3__Pointer='/notification/template'
export Dashboard__OutboxScopes__0__Key=documents
export Dashboard__OutboxScopes__0__Label='Document events'
export Dashboard__OutboxScopes__0__Tenant=demo
export Dashboard__OutboxScopes__0__Producer=document-publisher
export Dashboard__OutboxScopes__0__Operation='Publish document update events'
export Dashboard__OutboxScopes__0__Description='Outgoing events for document subscribers. Demo publication outcomes are simulated; no broker is connected.'
export Dashboard__OutboxScopes__1__Key=notifications
export Dashboard__OutboxScopes__1__Label='Notification events'
export Dashboard__OutboxScopes__1__Tenant=demo
export Dashboard__OutboxScopes__1__Producer=notification-publisher
export Dashboard__OutboxScopes__1__Operation='Publish notification requests'
export Dashboard__OutboxScopes__1__Description='Outgoing requests for notification workers. Demo outcomes are simulated; no messages are sent.'
exec bash eng/dotnet.sh run --project src/EventGuard.Dashboard -f net10.0 -c Release --no-build
