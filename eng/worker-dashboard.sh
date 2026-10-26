#!/usr/bin/env bash
set -euo pipefail
project_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$project_root"
: "${EVENTGUARD_CONNECTION:?Set EVENTGUARD_CONNECTION to the worker database before starting the local dashboard.}"
# A separate loopback-only, read-only dashboard. No seed/reset/migration is performed.
export ASPNETCORE_ENVIRONMENT=Development
export DOTNET_ENVIRONMENT=Development
export Dashboard__DevelopmentAccess=true
export Dashboard__DevelopmentPort="${EVENTGUARD_DASHBOARD_PORT:-5189}"
export Dashboard__Provider="${EventGuard__Provider:-PostgreSql}"
export Dashboard__Schema="${EventGuard__Schema:-eventguard_worker}"
export Dashboard__ConnectionString="$EVENTGUARD_CONNECTION"
export Dashboard__StoreId=typed-worker-local
export Dashboard__ApplicationLabel='Typed worker'
export Dashboard__EnvironmentLabel=Local
export Dashboard__Scopes__0__Key=products
export Dashboard__Scopes__0__Label='Product projection'
export Dashboard__Scopes__0__Tenant="${EventGuard__Tenant:-development}"
export Dashboard__Scopes__0__Consumer="${EventGuard__Consumer:-product-projection}"
export Dashboard__Scopes__0__Source="${EventGuard__Source:-catalog}"
export Dashboard__Scopes__0__EventType=product.changed
export Dashboard__Scopes__0__Operation='Update product projection'
export Dashboard__Scopes__0__Description='Apply a newer revision to the application product projection.'
export Dashboard__Scopes__0__DataFields__0__Label='Product ID'
export Dashboard__Scopes__0__DataFields__0__Pointer=/productId
export Dashboard__Scopes__0__DataFields__1__Label=Revision
export Dashboard__Scopes__0__DataFields__1__Pointer=/revision
exec bash eng/dotnet.sh run --project src/EventGuard.Dashboard -f net10.0 -c Release --no-build
