-- Operational audit rows: bounded facts about sync webhooks, dead letters,
-- retries, and bridge failures (§18.1). A row never holds a raw body, a
-- message body, a token, or a secret: the columns are a kind, a bounded
-- key/platform/target/delivery ID/status, an attempt count, and a small detail
-- object of status and error codes.
--
-- Tenant isolation matches 0003 and 0008: keel.tenant_id is set
-- transaction-locally by the audit package, and keel.bypass_rls='on' is the
-- operator escape hatch. FORCE makes even the table owner subject, so a query
-- that loses its tenant setting sees nothing instead of everything.

CREATE TABLE IF NOT EXISTS board_audit (
    id          bigserial   PRIMARY KEY,
    tenant_id   text        NOT NULL,
    "at"        timestamptz NOT NULL DEFAULT now(),
    kind        text        NOT NULL,
    "key"       text        NOT NULL DEFAULT '',
    platform    text        NOT NULL DEFAULT '',
    target      text        NOT NULL DEFAULT '',
    delivery_id text        NOT NULL DEFAULT '',
    status      text        NOT NULL DEFAULT '',
    attempts    int         NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    detail      jsonb       NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS board_audit_tenant_at
    ON board_audit (tenant_id, "at" DESC, id DESC);

ALTER TABLE board_audit ENABLE ROW LEVEL SECURITY;
ALTER TABLE board_audit FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS board_audit_tenant_isolation ON board_audit;
CREATE POLICY board_audit_tenant_isolation ON board_audit
    USING (
        current_setting('keel.bypass_rls', true) = 'on'
        OR tenant_id = current_setting('keel.tenant_id', true)
    )
    WITH CHECK (
        current_setting('keel.bypass_rls', true) = 'on'
        OR tenant_id = current_setting('keel.tenant_id', true)
    );
