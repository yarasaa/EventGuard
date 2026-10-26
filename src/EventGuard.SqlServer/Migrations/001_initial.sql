IF SCHEMA_ID(REPLACE(REPLACE('__SCHEMA__','[',''),']','')) IS NULL EXEC('CREATE SCHEMA __SCHEMA__');
CREATE TABLE __SCHEMA__.schema_version(singleton bit PRIMARY KEY CHECK(singleton=1), version int NOT NULL);
INSERT INTO __SCHEMA__.schema_version VALUES(1,1);
CREATE TABLE __SCHEMA__.policy_bindings (
    scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 PRIMARY KEY,
    tenant nvarchar(1024) NOT NULL, consumer nvarchar(1024) NOT NULL,
    source nvarchar(1024) NOT NULL, event_type nvarchar(1024) NOT NULL,
    key_version nvarchar(1024) NOT NULL, fingerprint_version nvarchar(1024) NOT NULL
);
CREATE TABLE __SCHEMA__.works (
    id uniqueidentifier PRIMARY KEY, identity_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL UNIQUE,
    scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL REFERENCES __SCHEMA__.policy_bindings(scope_id),
    business_key nvarchar(1024) NOT NULL, message_id uniqueidentifier NOT NULL,
    fingerprint nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL CHECK(DATALENGTH(fingerprint)=128), payload varbinary(max) NOT NULL,
    state int NOT NULL CHECK(state BETWEEN 0 AND 5), version bigint NOT NULL DEFAULT 0 CHECK(version>=0),
    attempt_count int NOT NULL DEFAULT 0 CHECK(attempt_count>=0),
    generation_attempt int NOT NULL DEFAULT 0 CHECK(generation_attempt>=0),
    replay_generation int NOT NULL DEFAULT 0 CHECK(replay_generation>=0),
    next_attempt_at datetimeoffset(7) NOT NULL, expires_at datetimeoffset(7),
    lease_token uniqueidentifier, lease_until datetimeoffset(7), last_reason nvarchar(128), correlation_id nvarchar(256),
    created_at datetimeoffset(7) NOT NULL DEFAULT TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00'), completed_at datetimeoffset(7),
    CHECK((state=1 AND lease_token IS NOT NULL AND lease_until IS NOT NULL) OR (state<>1 AND lease_token IS NULL AND lease_until IS NULL))
);
CREATE INDEX ix_ready ON __SCHEMA__.works(scope_id,next_attempt_at,id) WHERE state IN(0,2);
CREATE INDEX ix_recovery ON __SCHEMA__.works(scope_id,lease_until,id) WHERE state=1;
CREATE TABLE __SCHEMA__.attempts (
    id uniqueidentifier PRIMARY KEY, work_id uniqueidentifier NOT NULL REFERENCES __SCHEMA__.works(id),
    number int NOT NULL, generation int NOT NULL, status nvarchar(64) NOT NULL,
    started_at datetimeoffset(7) NOT NULL, finished_at datetimeoffset(7), reason nvarchar(128), UNIQUE(work_id,number)
);
CREATE TABLE __SCHEMA__.conflicts (
    id uniqueidentifier PRIMARY KEY, work_id uniqueidentifier NOT NULL REFERENCES __SCHEMA__.works(id),
    message_id uniqueidentifier NOT NULL, fingerprint nvarchar(64) NOT NULL, payload varbinary(max) NOT NULL,
    received_at datetimeoffset(7) NOT NULL DEFAULT TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00')
);
CREATE TABLE __SCHEMA__.replay_requests (
    scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL, id uniqueidentifier NOT NULL, fingerprint nvarchar(64) NOT NULL,
    work_id uniqueidentifier NOT NULL, actor nvarchar(1024) NOT NULL, reason nvarchar(1024) NOT NULL,
    result_status int NOT NULL, result_generation int NOT NULL, result_version bigint NOT NULL,
    created_at datetimeoffset(7) NOT NULL DEFAULT TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00'), PRIMARY KEY(scope_id,id)
);
CREATE TABLE __SCHEMA__.producer_requests (
    scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL, id uniqueidentifier NOT NULL, fingerprint nvarchar(64) NOT NULL,
    created_at datetimeoffset(7) NOT NULL DEFAULT TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00'), committed_at datetimeoffset(7), PRIMARY KEY(scope_id,id)
);
CREATE TABLE __SCHEMA__.outbox_messages (
    scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL, id uniqueidentifier NOT NULL,
    tenant nvarchar(1024) NOT NULL, producer nvarchar(1024) NOT NULL,
    message_id uniqueidentifier NOT NULL, transport nvarchar(1024) NOT NULL, connection_name nvarchar(1024) NOT NULL,
    configuration_version nvarchar(1024) NOT NULL, address nvarchar(1024) NOT NULL, routing_key nvarchar(1024),
    payload varbinary(max) NOT NULL, content_type nvarchar(1024) NOT NULL, headers nvarchar(max) NOT NULL,
    not_before datetimeoffset(7), fingerprint nvarchar(64) NOT NULL CHECK(DATALENGTH(fingerprint)=128),
    origin_work_id uniqueidentifier REFERENCES __SCHEMA__.works(id), origin_request_id uniqueidentifier,
    state int NOT NULL DEFAULT 0 CHECK(state BETWEEN 0 AND 5), version bigint NOT NULL DEFAULT 0 CHECK(version>=0),
    attempt_count int NOT NULL DEFAULT 0 CHECK(attempt_count>=0),
    generation_attempt int NOT NULL DEFAULT 0,
    replay_generation int NOT NULL DEFAULT 0 CHECK(replay_generation>=0), quarantine_id uniqueidentifier,
    next_attempt_at datetimeoffset(7) NOT NULL, lease_token uniqueidentifier, lease_until datetimeoffset(7),
    delivery_uncertain bit NOT NULL DEFAULT 0, last_reason nvarchar(128),
    created_at datetimeoffset(7) NOT NULL DEFAULT TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00'), published_at datetimeoffset(7),
    PRIMARY KEY(scope_id,id), FOREIGN KEY(scope_id,origin_request_id) REFERENCES __SCHEMA__.producer_requests(scope_id,id),
    CHECK(origin_work_id IS NULL OR origin_request_id IS NULL),
    CHECK(generation_attempt>=0 AND generation_attempt<=attempt_count),
    CHECK((state=1 AND lease_token IS NOT NULL AND lease_until IS NOT NULL) OR (state<>1 AND lease_token IS NULL AND lease_until IS NULL)),
    CHECK((state=5 AND quarantine_id IS NOT NULL) OR (state<>5 AND quarantine_id IS NULL))
);
CREATE INDEX ix_outbox_ready ON __SCHEMA__.outbox_messages(scope_id,next_attempt_at,id) WHERE state IN(0,2);
CREATE INDEX ix_outbox_recovery ON __SCHEMA__.outbox_messages(scope_id,lease_until,id) WHERE state=1;
CREATE TABLE __SCHEMA__.outbox_attempts (
    scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL, id uniqueidentifier NOT NULL,
    intent_id uniqueidentifier NOT NULL, number int NOT NULL, generation int NOT NULL DEFAULT 0 CHECK(generation>=0),
    status nvarchar(64) NOT NULL, reason nvarchar(128),
    started_at datetimeoffset(7) NOT NULL DEFAULT TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00'), finished_at datetimeoffset(7),
    PRIMARY KEY(scope_id,id), UNIQUE(scope_id,intent_id,number), FOREIGN KEY(scope_id,intent_id) REFERENCES __SCHEMA__.outbox_messages(scope_id,id)
);
CREATE TABLE __SCHEMA__.outbox_quarantine (
    scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL, id uniqueidentifier NOT NULL,
    intent_id uniqueidentifier NOT NULL, observed_version bigint NOT NULL, reason nvarchar(128) NOT NULL,
    original_record nvarchar(max) NOT NULL CHECK(ISJSON(original_record)=1),
    detected_at datetimeoffset(7) NOT NULL DEFAULT TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00'),
    PRIMARY KEY(scope_id,id), UNIQUE(scope_id,intent_id,observed_version), FOREIGN KEY(scope_id,intent_id) REFERENCES __SCHEMA__.outbox_messages(scope_id,id)
);
ALTER TABLE __SCHEMA__.outbox_messages ADD FOREIGN KEY(scope_id,quarantine_id) REFERENCES __SCHEMA__.outbox_quarantine(scope_id,id);
CREATE TABLE __SCHEMA__.outbox_replay_requests (
    scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL, id uniqueidentifier NOT NULL, fingerprint nvarchar(64) NOT NULL,
    intent_id uniqueidentifier NOT NULL, actor nvarchar(1024) NOT NULL, reason nvarchar(1024) NOT NULL,
    acknowledge_possible_duplicate bit NOT NULL, result_status int NOT NULL, result_generation int NOT NULL, result_version bigint NOT NULL,
    created_at datetimeoffset(7) NOT NULL DEFAULT TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00'), PRIMARY KEY(scope_id,id)
);
