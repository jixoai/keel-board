-- The board log, one row per event. Phase 1 runs the memory store; this is
-- the schema the PostgreSQL EventStore will use, kept here so its shape is
-- reviewable now. It is not applied by anything yet.
--
-- TODO(phase 2):
--   * implement store.Postgres.Load and Append against this table;
--   * Append opens a SERIALIZABLE transaction, checks board_heads.head against
--     the caller's expected head, inserts the batch, and advances the head;
--   * the unique index on (tenant, request_key) makes a racing duplicate
--     append fail, not duplicate. Only a keyed batch's first record carries
--     the key, so a batch is one row in that index;
--   * a serialization failure is surfaced so the log's caller can look again.

CREATE TABLE IF NOT EXISTS board_events (
    tenant      text        NOT NULL,
    seq         bigint      NOT NULL CHECK (seq > 0),
    type        text        NOT NULL,
    payload     jsonb       NOT NULL,
    created_at  timestamptz NOT NULL,
    request_key  text,
    digest       text,
    request_last bigint,
    PRIMARY KEY (tenant, seq)
);

CREATE UNIQUE INDEX IF NOT EXISTS board_events_request_key
    ON board_events (tenant, request_key)
    WHERE request_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS board_heads (
    tenant text   PRIMARY KEY,
    head   bigint NOT NULL
);
