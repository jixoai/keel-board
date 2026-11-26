package syncstore_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	boardsync "github.com/jixoai/keel-board/sync"
)

func TestPostgresRLSTenantIsolation(t *testing.T) {
	dsn := testDSN(t)
	schema := newTestSchema(t, dsn)
	store := openPostgres(t, dsn, schema, testConfig())
	ctx := t.Context()
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		if _, err := store.PushInbox(ctx, tenant, "slack", []boardsync.Record{testRecord("m-"+tenant, "C:T1")}); err != nil {
			t.Fatalf("push %s: %v", tenant, err)
		}
	}
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = admin.Close(cleanup)
	})
	if _, err := admin.Exec(ctx, `SET search_path TO `+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatalf("set search_path: %v", err)
	}

	var currentUser string
	if err := admin.QueryRow(ctx, `SELECT current_user`).Scan(&currentUser); err != nil {
		t.Fatalf("current_user: %v", err)
	}
	role := fmt.Sprintf("keel_rls_%d", time.Now().UnixNano())
	roleIdent := pgx.Identifier{role}.Sanitize()
	userIdent := pgx.Identifier{currentUser}.Sanitize()
	if _, err := admin.Exec(ctx, `CREATE ROLE `+roleIdent+` NOLOGIN`); err != nil {
		t.Fatalf("create restricted role: %v", err)
	}
	if _, err := admin.Exec(ctx, `GRANT `+roleIdent+` TO `+userIdent); err != nil {
		_, _ = admin.Exec(ctx, `DROP ROLE `+roleIdent)
		t.Fatalf("grant restricted role: %v", err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = admin.Exec(cleanup, `DROP OWNED BY `+roleIdent)
		_, _ = admin.Exec(cleanup, `REVOKE `+roleIdent+` FROM `+userIdent)
		if _, err := admin.Exec(cleanup, `DROP ROLE `+roleIdent); err != nil {
			t.Logf("dropping role %s: %v", role, err)
		}
	})
	schemaIdent := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, `GRANT USAGE ON SCHEMA `+schemaIdent+` TO `+roleIdent); err != nil {
		t.Fatalf("grant schema usage: %v", err)
	}
	if _, err := admin.Exec(ctx, `GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA `+schemaIdent+` TO `+roleIdent); err != nil {
		t.Fatalf("grant table access: %v", err)
	}
	if _, err := admin.Exec(ctx, `GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA `+schemaIdent+` TO `+roleIdent); err != nil {
		t.Fatalf("grant sequence access: %v", err)
	}

	// Every sync table is both RLS-enabled and forced, so the owner doesn't
	// bypass its own policy.
	rows, err := admin.Query(ctx, `
		SELECT relname, relrowsecurity, relforcerowsecurity
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname LIKE 'board_sync_%' AND c.relkind = 'r'
		ORDER BY relname`, schema)
	if err != nil {
		t.Fatalf("reading RLS flags: %v", err)
	}
	count := 0
	for rows.Next() {
		var name string
		var enabled, forced bool
		if err := rows.Scan(&name, &enabled, &forced); err != nil {
			rows.Close()
			t.Fatalf("scan RLS flags: %v", err)
		}
		if !enabled || !forced {
			rows.Close()
			t.Fatalf("%s: rls=%v force=%v", name, enabled, forced)
		}
		count++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("rls rows: %v", err)
	}
	if count != 7 {
		t.Fatalf("RLS tables = %d, want 7", count)
	}

	// A restricted role sees only the tenant it set.
	tx, err := admin.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE `+roleIdent); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("set role: %v", err)
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('keel.tenant_id', 'tenant-a', true)`); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("set tenant: %v", err)
	}
	var visible int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM board_sync_inbox`).Scan(&visible); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("count as tenant-a: %v", err)
	}
	if visible != 1 {
		tx.Rollback(ctx)
		t.Fatalf("tenant-a visible rows = %d, want 1", visible)
	}
	_, insertErr := tx.Exec(ctx, `
		INSERT INTO board_sync_inbox
			(tenant_id, sync_key, id, dedupe, lane, platform, kind, event)
		VALUES ('tenant-b', 'slack', 'forged', 'forged', 'C:T1', 'slack', 'message', '{}'::jsonb)`)
	if insertErr == nil {
		tx.Rollback(ctx)
		t.Fatal("a cross-tenant insert succeeded under tenant-a's setting")
	}
	tx.Rollback(ctx)

	// With no tenant setting the policy matches nothing.
	tx, err = admin.Begin(ctx)
	if err != nil {
		t.Fatalf("begin unset: %v", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE `+roleIdent); err != nil {
		t.Fatalf("set role unset: %v", err)
	}
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM board_sync_inbox`).Scan(&visible); err != nil {
		t.Fatalf("count unset: %v", err)
	}
	if visible != 0 {
		t.Fatalf("unset tenant saw %d rows", visible)
	}
}
