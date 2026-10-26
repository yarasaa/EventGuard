#!/usr/bin/env bash
set -euo pipefail
project_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$project_root"
# Fixed local Compose fixture only. Does not alter options on an existing application database.
bash eng/docker.sh compose exec -T sqlserver bash -c \
  'exec /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -b -d master' <<'SQL'
IF DB_ID('eventguard_test') IS NULL
BEGIN
    CREATE DATABASE eventguard_test;
    ALTER DATABASE eventguard_test SET READ_COMMITTED_SNAPSHOT ON;
END;
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name='eventguard_test'
    AND is_read_committed_snapshot_on=1 AND delayed_durability_desc='DISABLED' AND is_read_only=0)
    THROW 51002, 'Existing local database has incompatible options; configure it explicitly.', 1;
PRINT 'LocalDatabaseReady';
SQL
