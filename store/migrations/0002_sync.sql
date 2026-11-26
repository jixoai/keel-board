-- Sync's durable state. It sits beside the board log, never in it: raw
-- platform events and outbound deliveries are not board facts, so they never
-- appear in a tail.
--
-- Every table carries tenant_id, so a worker or a webhook handler can only see
-- the rows of the tenant it resolved. The application supplies the tenant
-- predicate on every statement; this schema does not enable RLS because a
-- deployment that did would need a session variable set on every pooled
-- connection (0001 makes the same choice).
--
-- board_sync_entries is the connection record: a key belongs to a tenant and
-- names a platform. It stores no secret; credentials come from a resolver.
--
-- board_sync_inbox is the durable inbound queue. (tenant_id, sync_key, dedupe)
-- is unique, so a redelivered webhook is stored once even after it was
-- imported. A row is pending, leased by one worker until lease_until, done, or
-- dead-lettered. An event behind a pending-but-not-due or leased row on its
-- lane waits, which keeps an external thread's imports in order.
--
-- board_sync_threads maps a board thread to external targets. board_sync_copies
-- maps one board message to its platform copy per target, which is what makes
-- an edit/delete/reaction address the right platform message.
--
-- board_sync_outbox is the per-board-thread delivery lane. delivery_id is
-- deterministic from the board event and target, so enqueueing the same fact
-- twice is one row. Dead rows stay for an operator; cleared_at is set when a
-- later delivery to the same target succeeds, which is how a failure stays
-- visible until it is cleared.
--
-- board_sync_source_cursors keeps opaque platform cursors per source;
-- board_sync_bridge_nonces refuses a bridge replay inside its window.

CREATE TABLE IF NOT EXISTS board_sync_entries (
    tenant_id  text        NOT NULL,
    sync_key   text        NOT NULL,
    platform   text        NOT NULL,
    enabled    boolean     NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, sync_key)
);

CREATE TABLE IF NOT EXISTS board_sync_inbox (
    seq          bigserial   PRIMARY KEY,
    tenant_id    text        NOT NULL,
    sync_key     text        NOT NULL,
    id           text        NOT NULL,
    dedupe       text        NOT NULL,
    lane         text        NOT NULL,
    platform     text        NOT NULL,
    kind         text        NOT NULL,
    event        jsonb       NOT NULL,
    raw          bytea,
    attempts     integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    state        text        NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'leased', 'done', 'dead')),
    available_at timestamptz NOT NULL DEFAULT now(),
    lease_owner  text        NOT NULL DEFAULT '',
    lease_token  text        NOT NULL DEFAULT '',
    lease_until  timestamptz,
    last_error   text        NOT NULL DEFAULT '',
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    CONSTRAINT board_sync_inbox_dedupe UNIQUE (tenant_id, sync_key, dedupe),
    CONSTRAINT board_sync_inbox_event UNIQUE (tenant_id, sync_key, id)
);

CREATE INDEX IF NOT EXISTS board_sync_inbox_lane
    ON board_sync_inbox (tenant_id, sync_key, lane, seq)
    WHERE state IN ('pending', 'leased');

CREATE INDEX IF NOT EXISTS board_sync_inbox_available
    ON board_sync_inbox (tenant_id, sync_key, available_at)
    WHERE state = 'pending';

CREATE INDEX IF NOT EXISTS board_sync_inbox_dead
    ON board_sync_inbox (tenant_id, sync_key, seq)
    WHERE state = 'dead';

CREATE TABLE IF NOT EXISTS board_sync_threads (
    tenant_id    text        NOT NULL,
    id           text        NOT NULL,
    board_thread bigint      NOT NULL CHECK (board_thread > 0),
    sync_key     text        NOT NULL,
    platform     text        NOT NULL,
    channel      text        NOT NULL,
    thread       text        NOT NULL DEFAULT '',
    subthread    text        NOT NULL DEFAULT '',
    kind         text        NOT NULL CHECK (kind IN ('channel', 'thread')),
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, id),
    CONSTRAINT board_sync_threads_target UNIQUE
        (tenant_id, board_thread, sync_key, channel, thread, subthread)
);

CREATE INDEX IF NOT EXISTS board_sync_threads_board
    ON board_sync_threads (tenant_id, board_thread);

CREATE TABLE IF NOT EXISTS board_sync_copies (
    tenant_id        text        NOT NULL,
    id               text        NOT NULL,
    board_message    bigint      NOT NULL CHECK (board_message > 0),
    board_thread     bigint      NOT NULL CHECK (board_thread > 0),
    sync_key         text        NOT NULL,
    platform         text        NOT NULL,
    channel          text        NOT NULL,
    thread           text        NOT NULL DEFAULT '',
    subthread        text        NOT NULL DEFAULT '',
    kind             text        NOT NULL CHECK (kind IN ('channel', 'thread')),
    external_thread  text        NOT NULL,
    external_message text        NOT NULL,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, id),
    CONSTRAINT board_sync_copies_target UNIQUE
        (tenant_id, sync_key, board_message, channel, thread, subthread)
);

CREATE INDEX IF NOT EXISTS board_sync_copies_message
    ON board_sync_copies (tenant_id, sync_key, board_message);

CREATE TABLE IF NOT EXISTS board_sync_outbox (
    id            bigserial   PRIMARY KEY,
    tenant_id     text        NOT NULL,
    delivery_id   text        NOT NULL,
    lane          text        NOT NULL,
    sync_key      text        NOT NULL,
    event_id      bigint      NOT NULL CHECK (event_id > 0),
    event_type    text        NOT NULL,
    board_thread  bigint      NOT NULL CHECK (board_thread > 0),
    board_message bigint      NOT NULL CHECK (board_message > 0),
    target_key    text        NOT NULL,
    target        jsonb       NOT NULL,
    event         jsonb       NOT NULL,
    message       jsonb       NOT NULL,
    attempts      integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    state         text        NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'leased', 'done', 'dead')),
    available_at  timestamptz NOT NULL DEFAULT now(),
    lease_owner   text        NOT NULL DEFAULT '',
    lease_token   text        NOT NULL DEFAULT '',
    lease_until   timestamptz,
    last_error    text        NOT NULL DEFAULT '',
    cleared_at    timestamptz,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    completed_at  timestamptz,
    CONSTRAINT board_sync_outbox_delivery UNIQUE (tenant_id, delivery_id)
);

CREATE INDEX IF NOT EXISTS board_sync_outbox_lane
    ON board_sync_outbox (tenant_id, lane, id)
    WHERE state IN ('pending', 'leased');

CREATE INDEX IF NOT EXISTS board_sync_outbox_available
    ON board_sync_outbox (tenant_id, available_at)
    WHERE state = 'pending';

CREATE INDEX IF NOT EXISTS board_sync_outbox_dead
    ON board_sync_outbox (tenant_id, sync_key, id)
    WHERE state = 'dead';

CREATE INDEX IF NOT EXISTS board_sync_outbox_failures
    ON board_sync_outbox (tenant_id, sync_key, target_key)
    WHERE state = 'dead' AND cleared_at IS NULL;

CREATE TABLE IF NOT EXISTS board_sync_source_cursors (
    tenant_id  text        NOT NULL,
    sync_key   text        NOT NULL,
    source     text        NOT NULL,
    cursor     text        NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, sync_key, source)
);

CREATE TABLE IF NOT EXISTS board_sync_bridge_nonces (
    tenant_id  text        NOT NULL,
    sync_key   text        NOT NULL,
    nonce      text        NOT NULL,
    seen_at    timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, sync_key, nonce)
);

CREATE INDEX IF NOT EXISTS board_sync_bridge_nonces_expiry
    ON board_sync_bridge_nonces (expires_at);
