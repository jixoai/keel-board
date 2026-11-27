-- Board automation: watchers and schedules. Both are durable consumers of the
-- board log, not clients and not platforms: a watcher runs a code or plugin
-- action that was registered at deploy time, and a schedule posts an ordinary
-- board message on its creator's behalf. Neither table stores a URL, a
-- credential, or a copy of a message body.
--
-- board_watchers is the deployment-time declaration. channel is a channel or a
-- channel subtree (a trailing "/" means the subtree); filter narrows the
-- events the watcher sees; action names a registered action and its bounded
-- parameters. cursor_seq is the last board sequence the planner evaluated for
-- this watcher, so a restart resumes where it stopped instead of replaying the
-- log. cursor_set separates "evaluated through 0" (an empty board) from "never
-- evaluated"; a new watcher starts at the current head, even when it is 0.
--
-- board_watch_deliveries is the durable delivery queue. (tenant_id,
-- watcher_id, event_seq) is the delivery identity: planning the same event
-- twice inserts one row. A row is pending, leased by one worker until
-- lease_until, done, or dead-lettered. only the oldest pending row of a watcher
-- is offered at a time, so one watcher's events stay ordered while watchers
-- run in parallel. A dead row no longer blocks the next event.
--
-- board_schedules is a persisted send plan. next_run_at is the next due time;
-- every_seconds makes it recurring, times and until_at bound it. status is
-- pending, disabled, done, canceled, or dead. The message a run posts carries
-- the deterministic idempotency key schedule:<id>:<run>, so an ack lost after
-- the append replays the same message instead of posting twice. A canceled row
-- records why (revoked when the creator lost channel access).

CREATE TABLE IF NOT EXISTS board_watchers (
    tenant_id  text        NOT NULL,
    watcher_id text        NOT NULL,
    channel    text        NOT NULL,
    filter     jsonb       NOT NULL DEFAULT '{}'::jsonb,
    action     jsonb       NOT NULL,
    bot_id     text        NOT NULL DEFAULT '',
    enabled    boolean     NOT NULL DEFAULT true,
    cursor_seq bigint      NOT NULL DEFAULT 0 CHECK (cursor_seq >= 0),
    cursor_set boolean     NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, watcher_id)
);

CREATE TABLE IF NOT EXISTS board_watch_deliveries (
    tenant_id   text        NOT NULL,
    watcher_id  text        NOT NULL,
    event_seq   bigint      NOT NULL CHECK (event_seq > 0),
    state       text        NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'leased', 'done', 'dead')),
    attempts    integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_at     timestamptz NOT NULL DEFAULT now(),
    lease_owner text        NOT NULL DEFAULT '',
    lease_token text        NOT NULL DEFAULT '',
    lease_until timestamptz,
    last_error  text        NOT NULL DEFAULT '',
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    PRIMARY KEY (tenant_id, watcher_id, event_seq)
);

CREATE INDEX IF NOT EXISTS board_watch_deliveries_due
    ON board_watch_deliveries (tenant_id, next_at)
    WHERE state = 'pending';

CREATE INDEX IF NOT EXISTS board_watch_deliveries_lane
    ON board_watch_deliveries (tenant_id, watcher_id, event_seq)
    WHERE state IN ('pending', 'leased');

CREATE INDEX IF NOT EXISTS board_watch_deliveries_dead
    ON board_watch_deliveries (tenant_id, watcher_id, event_seq)
    WHERE state = 'dead';

CREATE TABLE IF NOT EXISTS board_schedules (
    tenant_id      text        NOT NULL,
    schedule_id    text        NOT NULL,
    channel        text        NOT NULL,
    re             bigint,
    text           text        NOT NULL,
    labels         jsonb       NOT NULL DEFAULT '{}'::jsonb,
    creator_actor  text        NOT NULL,
    next_run_at    timestamptz NOT NULL,
    every_seconds  integer     CHECK (every_seconds IS NULL OR every_seconds > 0),
    until_at       timestamptz,
    times          integer     CHECK (times IS NULL OR times > 0),
    sent           integer     NOT NULL DEFAULT 0 CHECK (sent >= 0),
    status         text        NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'disabled', 'done', 'canceled', 'dead')),
    attempts       integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error     text        NOT NULL DEFAULT '',
    cancel_reason  text        NOT NULL DEFAULT '',
    lease_owner    text        NOT NULL DEFAULT '',
    lease_token    text        NOT NULL DEFAULT '',
    lease_until    timestamptz,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    completed_at   timestamptz,
    PRIMARY KEY (tenant_id, schedule_id)
);

CREATE INDEX IF NOT EXISTS board_schedules_due
    ON board_schedules (tenant_id, next_run_at)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS board_schedules_tenant
    ON board_schedules (tenant_id, status, next_run_at);

-- Tenant isolation is enforced twice: every statement names tenant_id, and
-- these policies make a statement that loses the predicate fail closed. The
-- application sets keel.tenant_id transaction-locally before touching an
-- automation table (the automation store's withTenant); an unset setting
-- yields NULL, which matches no row.
ALTER TABLE board_watchers ENABLE ROW LEVEL SECURITY;
ALTER TABLE board_watchers FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS board_watchers_tenant_isolation ON board_watchers;
CREATE POLICY board_watchers_tenant_isolation ON board_watchers
    USING (tenant_id = current_setting('keel.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('keel.tenant_id', true));

ALTER TABLE board_watch_deliveries ENABLE ROW LEVEL SECURITY;
ALTER TABLE board_watch_deliveries FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS board_watch_deliveries_tenant_isolation ON board_watch_deliveries;
CREATE POLICY board_watch_deliveries_tenant_isolation ON board_watch_deliveries
    USING (tenant_id = current_setting('keel.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('keel.tenant_id', true));

ALTER TABLE board_schedules ENABLE ROW LEVEL SECURITY;
ALTER TABLE board_schedules FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS board_schedules_tenant_isolation ON board_schedules;
CREATE POLICY board_schedules_tenant_isolation ON board_schedules
    USING (tenant_id = current_setting('keel.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('keel.tenant_id', true));
