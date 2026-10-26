CREATE TABLE __SCHEMA__.bulk_replay_requests (
    scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL, id uniqueidentifier NOT NULL,
    kind int NOT NULL CHECK(kind IN (0,1)), fingerprint nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
    actor nvarchar(1024) NOT NULL, reason nvarchar(1024) NOT NULL, acknowledge_possible_duplicate bit NOT NULL,
    items nvarchar(max) NOT NULL CHECK(ISJSON(items)=1 AND DATALENGTH(items)<=32768),
    created_at datetimeoffset(7) NOT NULL DEFAULT TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00'), PRIMARY KEY(scope_id,id)
);
UPDATE __SCHEMA__.schema_version SET version=8 WHERE singleton=1 AND version=7;
