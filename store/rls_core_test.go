package store_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jixoai/keel-board/store"
)

// TestPostgresCoreRLSTenantIsolation proves the core log is protected by FORCE
// RLS, not only by predicates in the queries. A restricted role that forgets
// keel.tenant_id sees nothing; a role scoped to tenant-a sees tenant-a and
// cannot insert a tenant-b row.
func TestPostgresCoreRLSTenantIsolation(t *testing.T) {
	dsn := testDSN(t)
	schema := newTestSchema(t, dsn)
	postgres := openTestPostgres(t, dsn, schema)
	ctx := t.Context()
	if _, err := postgres.Append(ctx, "tenant-a", []store.Record{{Seq: 1, Type: "test", Payload: []byte(`{"n":1}`)}}, 0); err != nil {
		t.Fatalf("append tenant-a: %v", err)
	}
	if _, err := postgres.Append(ctx, "tenant-b", []store.Record{{Seq: 1, Type: "test", Payload: []byte(`{"n":2}`)}}, 0); err != nil {
		t.Fatalf("append tenant-b: %v", err)
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
	if _, err := admin.Exec(ctx, "SET search_path TO "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatalf("set search_path: %v", err)
	}
	var currentUser string
	if err := admin.QueryRow(ctx, `SELECT current_user`).Scan(&currentUser); err != nil {
		t.Fatalf("current_user: %v", err)
	}
	role := fmt.Sprintf("keel_core_rls_%d", time.Now().UnixNano())
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

	// Every core table is enabled and forced, so an owner-role query is also
	// filtered and a missing tenant setting fails closed.
	rows, err := admin.Query(ctx, `
		SELECT relname, relrowsecurity, relforcerowsecurity
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname IN ('board_heads','board_events','board_requests')
		  AND c.relkind = 'r'
		ORDER BY relname`, schema)
	if err != nil {
		t.Fatalf("reading RLS flags: %v", err)
	}
	seen := 0
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
		seen++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("rls rows: %v", err)
	}
	if seen != 3 {
		t.Fatalf("RLS tables = %d, want 3", seen)
	}

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
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM board_events`).Scan(&visible); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("count as tenant-a: %v", err)
	}
	if visible != 1 {
		tx.Rollback(ctx)
		t.Fatalf("tenant-a visible rows = %d, want 1", visible)
	}
	_, insertErr := tx.Exec(ctx, `
		INSERT INTO board_events (tenant_id, seq, kind, payload)
		VALUES ('tenant-b', 99, 'forged', '{}'::jsonb)`)
	if insertErr == nil {
		tx.Rollback(ctx)
		t.Fatal("a cross-tenant insert succeeded under tenant-a's setting")
	}
	tx.Rollback(ctx)

	tx, err = admin.Begin(ctx)
	if err != nil {
		t.Fatalf("begin unset: %v", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE `+roleIdent); err != nil {
		t.Fatalf("set role unset: %v", err)
	}
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM board_events`).Scan(&visible); err != nil {
		t.Fatalf("count unset: %v", err)
	}
	if visible != 0 {
		t.Fatalf("unset tenant saw %d rows", visible)
	}
}

// TestMigration0005DeduplicatesAsRestrictedOwner proves the upgrade path, not
// just a fresh schema: a non-superuser role that owns the tables applies
// 0001-0004, seeds duplicate thread mappings, and then applies 0005. The old
// unscoped DELETE matched zero rows under FORCE RLS and the unique index
// failed; the migration now lifts FORCE only for its own transaction.
func TestMigration0005DeduplicatesAsRestrictedOwner(t *testing.T) {
	dsn := testDSN(t)
	ctx := t.Context()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = admin.Close(cleanup)
	})
	schema := fmt.Sprintf("keel_mig_%d", time.Now().UnixNano())
	role := fmt.Sprintf("keel_mig_role_%d", time.Now().UnixNano())
	roleIdent := pgx.Identifier{role}.Sanitize()
	userIdent := pgx.Identifier{currentUser(t, ctx, admin)}.Sanitize()
	schemaIdent := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, `CREATE ROLE `+roleIdent+` NOLOGIN`); err != nil {
		t.Fatalf("create role: %v", err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = admin.Exec(cleanup, `DROP SCHEMA IF EXISTS `+schemaIdent+` CASCADE`)
		_, _ = admin.Exec(cleanup, `REVOKE `+roleIdent+` FROM `+userIdent)
		_, _ = admin.Exec(cleanup, `DROP ROLE IF EXISTS `+roleIdent)
	})
	if _, err := admin.Exec(ctx, `GRANT `+roleIdent+` TO `+userIdent); err != nil {
		t.Fatalf("grant role: %v", err)
	}
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schemaIdent+` AUTHORIZATION `+roleIdent); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	config.RuntimeParams["search_path"] = schemaIdent
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatalf("connect schema: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	if _, err := conn.Exec(ctx, `SET ROLE `+roleIdent); err != nil {
		t.Fatalf("set role: %v", err)
	}
	for _, name := range []string{"0001_board_log.sql", "0002_sync.sql", "0003_sync_rls.sql", "0004_sync_copy_files.sql"} {
		statement, err := os.ReadFile("../pgadapter/schema/migrations/" + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if _, err := conn.PgConn().Exec(ctx, string(statement)).ReadAll(); err != nil {
			t.Fatalf("apply %s: %v", name, err)
		}
	}
	if _, err := conn.Exec(ctx, `SELECT set_config('keel.tenant_id', 'tenant-a', false)`); err != nil {
		t.Fatalf("set tenant: %v", err)
	}
	for id, boardThread := range map[string]int{"dup-a": 10, "dup-b": 11} {
		if _, err := conn.Exec(ctx, `
			INSERT INTO board_sync_threads
				(tenant_id, id, board_thread, sync_key, platform, channel, thread, subthread, kind)
			VALUES ('tenant-a', $1, $2, 'slack', 'slack', 'C1', 'T1', '', 'thread')`, id, boardThread); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}
	statement, err := os.ReadFile("../pgadapter/schema/migrations/0005_sync_thread_identity.sql")
	if err != nil {
		t.Fatalf("read 0005: %v", err)
	}
	if _, err := conn.PgConn().Exec(ctx, string(statement)).ReadAll(); err != nil {
		t.Fatalf("apply 0005 as restricted owner: %v", err)
	}
	var remaining int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM board_sync_threads`).Scan(&remaining); err != nil {
		t.Fatalf("count: %v", err)
	}
	if remaining != 1 {
		t.Fatalf("duplicates remaining = %d, want 1", remaining)
	}
	var forced bool
	if err := admin.QueryRow(ctx, `
		SELECT relforcerowsecurity FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = 'board_sync_threads'`, schema).Scan(&forced); err != nil {
		t.Fatalf("read force flag: %v", err)
	}
	if !forced {
		t.Fatal("0005 left board_sync_threads without FORCE RLS")
	}
}

func currentUser(t *testing.T, ctx context.Context, conn *pgx.Conn) string {
	t.Helper()
	var user string
	if err := conn.QueryRow(ctx, `SELECT current_user`).Scan(&user); err != nil {
		t.Fatalf("current_user: %v", err)
	}
	return user
}
