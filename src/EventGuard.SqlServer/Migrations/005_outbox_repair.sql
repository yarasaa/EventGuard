CREATE TABLE __SCHEMA__.outbox_repair_requests (
    scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL, id uniqueidentifier NOT NULL,
    fingerprint nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
    intent_id uniqueidentifier NOT NULL, quarantine_id uniqueidentifier NOT NULL,
    actor nvarchar(1024) NOT NULL, reason nvarchar(1024) NOT NULL, acknowledge_possible_duplicate bit NOT NULL,
    result_status int NOT NULL, result_generation int NOT NULL, result_version bigint NOT NULL,
    created_at datetimeoffset(7) NOT NULL DEFAULT TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00'), PRIMARY KEY(scope_id,id)
);
UPDATE __SCHEMA__.schema_version SET version=5 WHERE singleton=1 AND version=4;
