-- Tenant isolation is enforced twice: every statement names tenant_id, and
-- these policies make a statement that loses the predicate fail closed. The
-- application sets keel.tenant_id transaction-locally before touching a sync
-- table (syncstore's withTenant); an unset setting yields NULL, which matches
-- no row. FORCE makes the table owner subject too; only a superuser or a role
-- with BYPASSRLS bypasses it.
--
-- The migration is idempotent: enabling RLS again is a no-op, and each policy
-- is dropped and recreated.

ALTER TABLE board_sync_entries ENABLE ROW LEVEL SECURITY;
ALTER TABLE board_sync_entries FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS board_sync_tenant_isolation ON board_sync_entries;
CREATE POLICY board_sync_tenant_isolation ON board_sync_entries
    USING (tenant_id = current_setting('keel.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('keel.tenant_id', true));

ALTER TABLE board_sync_inbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE board_sync_inbox FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS board_sync_tenant_isolation ON board_sync_inbox;
CREATE POLICY board_sync_tenant_isolation ON board_sync_inbox
    USING (tenant_id = current_setting('keel.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('keel.tenant_id', true));

ALTER TABLE board_sync_threads ENABLE ROW LEVEL SECURITY;
ALTER TABLE board_sync_threads FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS board_sync_tenant_isolation ON board_sync_threads;
CREATE POLICY board_sync_tenant_isolation ON board_sync_threads
    USING (tenant_id = current_setting('keel.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('keel.tenant_id', true));

ALTER TABLE board_sync_copies ENABLE ROW LEVEL SECURITY;
ALTER TABLE board_sync_copies FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS board_sync_tenant_isolation ON board_sync_copies;
CREATE POLICY board_sync_tenant_isolation ON board_sync_copies
    USING (tenant_id = current_setting('keel.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('keel.tenant_id', true));

ALTER TABLE board_sync_outbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE board_sync_outbox FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS board_sync_tenant_isolation ON board_sync_outbox;
CREATE POLICY board_sync_tenant_isolation ON board_sync_outbox
    USING (tenant_id = current_setting('keel.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('keel.tenant_id', true));

ALTER TABLE board_sync_source_cursors ENABLE ROW LEVEL SECURITY;
ALTER TABLE board_sync_source_cursors FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS board_sync_tenant_isolation ON board_sync_source_cursors;
CREATE POLICY board_sync_tenant_isolation ON board_sync_source_cursors
    USING (tenant_id = current_setting('keel.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('keel.tenant_id', true));

ALTER TABLE board_sync_bridge_nonces ENABLE ROW LEVEL SECURITY;
ALTER TABLE board_sync_bridge_nonces FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS board_sync_tenant_isolation ON board_sync_bridge_nonces;
CREATE POLICY board_sync_tenant_isolation ON board_sync_bridge_nonces
    USING (tenant_id = current_setting('keel.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('keel.tenant_id', true));
