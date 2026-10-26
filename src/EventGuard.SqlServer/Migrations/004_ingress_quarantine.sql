CREATE TABLE __SCHEMA__.ingress_quarantine (
    id uniqueidentifier PRIMARY KEY, scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
    identity_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL UNIQUE,
    fingerprint nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
    key_version nvarchar(1024) COLLATE Latin1_General_100_BIN2 NOT NULL,
    fingerprint_version nvarchar(1024) COLLATE Latin1_General_100_BIN2 NOT NULL,
    reason_code nvarchar(64) NOT NULL, version bigint NOT NULL DEFAULT 0 CHECK(version>=0), released_work_id uniqueidentifier,
    source_binding nvarchar(1024) NOT NULL, source_position nvarchar(1024) NOT NULL, codec nvarchar(1024) NOT NULL,
    body varbinary(max) NOT NULL CHECK(DATALENGTH(body)<=1048576),
    metadata varbinary(max) NOT NULL CHECK(DATALENGTH(metadata)<=65536), is_null_body bit NOT NULL,
    created_at datetimeoffset(7) NOT NULL DEFAULT TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00'),
    FOREIGN KEY(scope_id) REFERENCES __SCHEMA__.policy_bindings(scope_id),
    FOREIGN KEY(released_work_id) REFERENCES __SCHEMA__.works(id), CHECK(is_null_body=0 OR DATALENGTH(body)=0)
);
CREATE INDEX ingress_quarantine_scope ON __SCHEMA__.ingress_quarantine(scope_id,created_at,id);
CREATE TABLE __SCHEMA__.ingress_release_requests (
    scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL, id uniqueidentifier NOT NULL,
    fingerprint nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
    quarantine_id uniqueidentifier NOT NULL, actor nvarchar(1024) NOT NULL, reason nvarchar(1024) NOT NULL,
    result_status int NOT NULL, result_work_id uniqueidentifier, result_version bigint NOT NULL,
    created_at datetimeoffset(7) NOT NULL DEFAULT TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00'), PRIMARY KEY(scope_id,id)
);
UPDATE __SCHEMA__.schema_version SET version=4 WHERE singleton=1 AND version=3;
