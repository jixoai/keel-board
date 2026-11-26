-- The board log, one row per event. A tenant's log is its board_events rows
-- ordered by seq; board_heads is the log's head and fencing version;
-- board_requests is the answer a keyed append's retry gets.
--
-- Appends run in one transaction:
--   * board_requests is checked first, so a retry with the same key and digest
--     answers the first result without writing (another digest is a conflict);
--   * board_heads is compare-and-swapped with seq = expected head, which both
--     locks the tenant's log and refuses a stale append;
--   * the batch's events and its request row are inserted together, so a
--     failure rolls all of it back.
--
-- Only a keyed batch's first event carries its request key, so a batch is one
-- row in board_events' request index and one row in board_requests.

CREATE TABLE IF NOT EXISTS board_heads (
    tenant_id   text   PRIMARY KEY,
    seq         bigint NOT NULL DEFAULT 0 CHECK (seq >= 0),
    version     bigint NOT NULL DEFAULT 0 CHECK (version >= 0)
);

CREATE TABLE IF NOT EXISTS board_events (
    tenant_id   text        NOT NULL,
    seq         bigint      NOT NULL CHECK (seq > 0),
    channel     text,
    kind        text        NOT NULL,
    payload     jsonb       NOT NULL,
    request_key text,
    digest      text,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, seq)
);

CREATE UNIQUE INDEX IF NOT EXISTS board_events_request_key
    ON board_events (tenant_id, request_key)
    WHERE request_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS board_events_channel
    ON board_events (tenant_id, channel, seq);

CREATE INDEX IF NOT EXISTS board_events_kind
    ON board_events (tenant_id, kind, seq);

CREATE INDEX IF NOT EXISTS board_events_created_at
    ON board_events (tenant_id, created_at);

CREATE TABLE IF NOT EXISTS board_requests (
    tenant_id   text  NOT NULL,
    request_key text  NOT NULL,
    digest      text  NOT NULL,
    result      jsonb NOT NULL,
    PRIMARY KEY (tenant_id, request_key)
);
