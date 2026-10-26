ALTER TABLE __SCHEMA__.outbox_messages DROP CONSTRAINT outbox_messages_state_check;
ALTER TABLE __SCHEMA__.outbox_messages ADD CONSTRAINT outbox_messages_state_check CHECK(state BETWEEN 0 AND 5);
ALTER TABLE __SCHEMA__.outbox_messages
    ADD COLUMN generation_attempt integer NOT NULL DEFAULT 0,
    ADD COLUMN replay_generation integer NOT NULL DEFAULT 0 CHECK(replay_generation>=0),
    ADD COLUMN quarantine_id uuid;
UPDATE __SCHEMA__.outbox_messages SET generation_attempt=attempt_count;
ALTER TABLE __SCHEMA__.outbox_messages
    ADD CONSTRAINT outbox_generation_attempt_check CHECK(generation_attempt>=0 AND generation_attempt<=attempt_count),
    ADD CONSTRAINT outbox_quarantine_state_check CHECK((state=5)=(quarantine_id IS NOT NULL));
ALTER TABLE __SCHEMA__.outbox_attempts ADD COLUMN generation integer NOT NULL DEFAULT 0 CHECK(generation>=0);
CREATE TABLE __SCHEMA__.outbox_quarantine (
    scope_id text NOT NULL, id uuid NOT NULL, intent_id uuid NOT NULL,
    observed_version bigint NOT NULL, reason text NOT NULL,
    original_record jsonb NOT NULL, detected_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(scope_id,id), UNIQUE(scope_id,intent_id,observed_version),
    FOREIGN KEY(scope_id,intent_id) REFERENCES __SCHEMA__.outbox_messages(scope_id,id)
);
ALTER TABLE __SCHEMA__.outbox_messages ADD CONSTRAINT outbox_quarantine_reference
    FOREIGN KEY(scope_id,quarantine_id) REFERENCES __SCHEMA__.outbox_quarantine(scope_id,id);
CREATE TABLE __SCHEMA__.outbox_replay_requests (
    scope_id text NOT NULL, id uuid NOT NULL, fingerprint text NOT NULL,
    intent_id uuid NOT NULL, actor text NOT NULL, reason text NOT NULL,
    acknowledge_possible_duplicate boolean NOT NULL,
    result_status integer NOT NULL, result_generation integer NOT NULL, result_version bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(scope_id,id)
);
UPDATE __SCHEMA__.schema_version SET version=3 WHERE singleton AND version=2;
