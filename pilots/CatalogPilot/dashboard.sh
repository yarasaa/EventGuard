#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$repo_root"
: "${PILOT_SCHEMA:?Set PILOT_SCHEMA to the schema in a completed pilot result.json, or initialize a manual pilot first.}"
export ASPNETCORE_ENVIRONMENT=Development
export Dashboard__DevelopmentAccess=true
export Dashboard__DevelopmentPort=5188
export Dashboard__Provider=PostgreSql
export Dashboard__Schema="$PILOT_SCHEMA"
export Dashboard__StoreId=catalog-pilot
export Dashboard__EnvironmentLabel='Local integration pilot'
export Dashboard__ApplicationLabel='Product catalog'
export Dashboard__ConnectionString='Host=127.0.0.1;Port=55432;Database=eventguard_test;Username=eventguard;Password=eventguard-local-only'
export Dashboard__ReplayConnectionString="$Dashboard__ConnectionString"
export Dashboard__ReplayEnabled=true
export Dashboard__OutboxReplayEnabled=true
export Dashboard__IngressMonitoringEnabled=true
export Dashboard__RecoveryEnabled=true
export Dashboard__AuditEnabled=true
export Dashboard__BulkReplayEnabled=true
export Dashboard__BacklogEnabled=true
export Dashboard__Scopes__0__Key=catalog
export Dashboard__Scopes__0__Label='Catalog indexing'
export Dashboard__Scopes__0__Tenant=pilot
export Dashboard__Scopes__0__Consumer=catalog-indexer
export Dashboard__Scopes__0__Source=catalog-api
export Dashboard__Scopes__0__EventType=product.changed
export Dashboard__Scopes__0__Operation='Update the product search projection'
export Dashboard__Scopes__0__Description='Apply a newer catalog revision in PostgreSQL and queue a catalog-indexed event in the same transaction. Older revisions preserve the current projection.'
export Dashboard__Scopes__0__AllowReplay=true
export Dashboard__Scopes__0__AllowRecovery=true
export Dashboard__Scopes__0__AllowAudit=true
export Dashboard__Scopes__0__DataFields__0__Label='Product ID'
export Dashboard__Scopes__0__DataFields__0__Pointer='/productId'
export Dashboard__Scopes__0__DataFields__1__Label='Incoming revision'
export Dashboard__Scopes__0__DataFields__1__Pointer='/revision'
export Dashboard__Scopes__0__DataFields__2__Label='Product name · v1'
export Dashboard__Scopes__0__DataFields__2__Pointer='/name'
export Dashboard__Scopes__0__DataFields__3__Label='Product name · v2'
export Dashboard__Scopes__0__DataFields__3__Pointer='/displayName'
export Dashboard__OutboxScopes__0__Key=changes
export Dashboard__OutboxScopes__0__Label='Product changes'
export Dashboard__OutboxScopes__0__Tenant=pilot
export Dashboard__OutboxScopes__0__Producer=catalog-api
export Dashboard__OutboxScopes__0__Operation='Publish catalog changes to RabbitMQ'
export Dashboard__OutboxScopes__0__AllowReplay=true
export Dashboard__OutboxScopes__0__AllowRecovery=true
export Dashboard__OutboxScopes__0__AllowAudit=true
export Dashboard__OutboxScopes__1__Key=indexed
export Dashboard__OutboxScopes__1__Label='Indexing outcomes'
export Dashboard__OutboxScopes__1__Tenant=pilot
export Dashboard__OutboxScopes__1__Producer=catalog-indexer
export Dashboard__OutboxScopes__1__Operation='Publish catalog indexing outcomes to RabbitMQ'
export Dashboard__OutboxScopes__1__AllowReplay=true
export Dashboard__OutboxScopes__1__AllowAudit=true
# The Dashboard is a separate optional host. Its startup never creates pilot data.
exec bash eng/dotnet.sh run --project src/EventGuard.Dashboard -f net10.0 -c Release --no-build
