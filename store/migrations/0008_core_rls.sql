-- Tenant isolation for the core board log. 0003 protects the sync tables and
-- 0006 the automation tables; the core log was left on application predicates
-- alone, so a query that lost a tenant predicate could read another tenant's
-- board.
--
-- The application sets keel.tenant_id transaction-locally (store.setTenant)
-- before touching a core table. An unset setting yields NULL, which matches no
-- row: a bug fails closed instead of leaking. FORCE makes the table owner
-- subject too.
--
-- keel.bypass_rls='on' is the operator escape hatch for the one cross-tenant
-- read the package performs (Tenants, used by `board rebuild`). It is set only
-- by that read, never by a request path.

ALTER TABLE board_heads ENABLE ROW LEVEL SECURITY;
ALTER TABLE board_heads FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS board_core_tenant_isolation ON board_heads;
CREATE POLICY board_core_tenant_isolation ON board_heads
    USING (
        current_setting('keel.bypass_rls', true) = 'on'
        OR tenant_id = current_setting('keel.tenant_id', true)
    )
    WITH CHECK (
        current_setting('keel.bypass_rls', true) = 'on'
        OR tenant_id = current_setting('keel.tenant_id', true)
    );

ALTER TABLE board_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE board_events FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS board_core_tenant_isolation ON board_events;
CREATE POLICY board_core_tenant_isolation ON board_events
    USING (
        current_setting('keel.bypass_rls', true) = 'on'
        OR tenant_id = current_setting('keel.tenant_id', true)
    )
    WITH CHECK (
        current_setting('keel.bypass_rls', true) = 'on'
        OR tenant_id = current_setting('keel.tenant_id', true)
    );

ALTER TABLE board_requests ENABLE ROW LEVEL SECURITY;
ALTER TABLE board_requests FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS board_core_tenant_isolation ON board_requests;
CREATE POLICY board_core_tenant_isolation ON board_requests
    USING (
        current_setting('keel.bypass_rls', true) = 'on'
        OR tenant_id = current_setting('keel.tenant_id', true)
    )
    WITH CHECK (
        current_setting('keel.bypass_rls', true) = 'on'
        OR tenant_id = current_setting('keel.tenant_id', true)
    );
