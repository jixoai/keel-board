package audit_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/jixoai/keel-board/audit"
)

func testDSN(t *testing.T) string {
	t.Helper()
	for _, name := range []string{"KEEL_BOARD_TEST_DSN", "TEST_DSN"} {
		if dsn := strings.TrimSpace(os.Getenv(name)); dsn != "" {
			return dsn
		}
	}
	if strings.TrimSpace(os.Getenv("CI")) != "" {
		t.Fatal("CI requires KEEL_BOARD_TEST_DSN (or TEST_DSN); PostgreSQL tests must not be skipped")
	}
	t.Skip("PostgreSQL tests skipped: set KEEL_BOARD_TEST_DSN (or TEST_DSN), for example postgres://localhost:5432/postgres?sslmode=disable")
	return ""
}

func newTestSchema(t *testing.T, dsn string) string {
	t.Helper()
	name := fmt.Sprintf("keel_audit_%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connecting to PostgreSQL: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{name}.Sanitize()); err != nil {
		admin.Close(ctx)
		t.Fatalf("creating test schema: %v", err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(cleanup, "DROP SCHEMA "+pgx.Identifier{name}.Sanitize()+" CASCADE"); err != nil {
			t.Logf("dropping schema %s: %v", name, err)
		}
		_ = admin.Close(cleanup)
	})
	return name
}

func TestMemoryAppendRecentAndTenantIsolation(t *testing.T) {
	store := audit.NewMemory()
	ctx := t.Context()
	for _, tenant := range []string{"tenant-a", "tenant-a", "tenant-b"} {
		if _, err := store.Append(ctx, audit.Event{
			Tenant: tenant, Kind: audit.KindWebhookAccepted,
			Key: "slack", Platform: "slack", Status: audit.StatusAccepted,
			Detail: map[string]any{"http": 200},
		}); err != nil {
			t.Fatalf("append %s: %v", tenant, err)
		}
	}
	rows, err := store.Recent(ctx, "tenant-a", 10)
	if err != nil {
		t.Fatalf("recent: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("tenant-a rows = %d, want 2", len(rows))
	}
	if rows[0].ID <= rows[1].ID {
		t.Fatalf("rows aren't newest first: %d then %d", rows[0].ID, rows[1].ID)
	}
	other, err := store.Recent(ctx, "tenant-b", 10)
	if err != nil || len(other) != 1 || other[0].Tenant != "tenant-b" {
		t.Fatalf("tenant-b rows = %+v, %v", other, err)
	}
	// A limit clamps to the newest rows.
	limited, err := store.Recent(ctx, "tenant-a", 1)
	if err != nil || len(limited) != 1 || limited[0].ID != rows[0].ID {
		t.Fatalf("limited rows = %+v, %v", limited, err)
	}
}

func TestEventValidationRefusesUnboundedFields(t *testing.T) {
	cases := map[string]audit.Event{
		"no tenant":             {Kind: audit.KindWebhookAccepted},
		"tenant control":        {Tenant: "a\nb", Kind: audit.KindWebhookAccepted},
		"kind is a sentence":    {Tenant: "t", Kind: "the webhook was accepted"},
		"kind too long":         {Tenant: "t", Kind: strings.Repeat("a", 65)},
		"target has newline":    {Tenant: "t", Kind: audit.KindOutboxDead, Target: "slack:C1\nbody"},
		"target is a sentence":  {Tenant: "t", Kind: audit.KindOutboxDead, Target: "the deploy failed"},
		"target has a quote":    {Tenant: "t", Kind: audit.KindOutboxDead, Target: `slack:"C1"`},
		"detail nested":         {Tenant: "t", Kind: audit.KindWebhookAccepted, Detail: map[string]any{"body": map[string]any{"text": "secret"}}},
		"detail is a body":      {Tenant: "t", Kind: audit.KindWebhookAccepted, Detail: map[string]any{"body": "the deploy failed for everyone"}},
		"detail has a secret":   {Tenant: "t", Kind: audit.KindWebhookAccepted, Detail: map[string]any{"body": "token=SECRET"}},
		"detail long string":    {Tenant: "t", Kind: audit.KindWebhookAccepted, Detail: map[string]any{"body": strings.Repeat("x", 129)}},
		"detail bad key":        {Tenant: "t", Kind: audit.KindWebhookAccepted, Detail: map[string]any{"Message Body": "x"}},
		"attempts out of range": {Tenant: "t", Kind: audit.KindOutboxDead, Attempts: -1},
		"delivery is free text": {Tenant: "t", Kind: audit.KindOutboxRetry, DeliveryID: "two words"},
	}
	for name, event := range cases {
		if err := event.Validate(); err == nil {
			t.Fatalf("%s: validation accepted %+v", name, event)
		}
	}
	valid := audit.Event{
		Tenant: "tenant-a", Kind: audit.KindOutboxDead, Key: "slack",
		Platform: "slack", Target: "github:acme/web#12", DeliveryID: "delivery_abcd",
		Status: audit.StatusDead, Attempts: 5,
		Detail: map[string]any{
			"code": "rate_limited", "http": 429,
			"event": "slack:C0123:1727.9", "delivery": "8c1e2f3a-9527-4c31-ae42-7b30d1f09e55",
		},
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid event refused: %v", err)
	}
	if !audit.SafeToken("slack:C0123:1727.9") || audit.SafeToken("the deploy failed") {
		t.Fatal("SafeToken doesn't separate IDs from text")
	}
	if audit.SafeDeliveryID("123") || !audit.SafeDeliveryID("delivery_abcd") {
		t.Fatal("SafeDeliveryID doesn't keep numeric message IDs out")
	}
}

func TestPostgresAppendAndRecent(t *testing.T) {
	dsn := testDSN(t)
	schema := newTestSchema(t, dsn)
	store, err := audit.OpenPostgres(t.Context(), audit.PostgresOptions{DSN: dsn, SearchPath: schema})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(store.Close)
	ctx := t.Context()
	first, err := store.Append(ctx, audit.Event{
		Tenant: "tenant-a", Kind: audit.KindInboxDead, Key: "slack",
		Platform: "slack", Status: audit.StatusDead, Attempts: 3,
		Detail: map[string]any{"code": "bad_request", "event": "Ev123"},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if first.ID == 0 || first.At.IsZero() {
		t.Fatalf("append didn't fill identity: %+v", first)
	}
	if _, err := store.Append(ctx, audit.Event{Tenant: "tenant-b", Kind: audit.KindWebhookRejected, Status: "unknown_key"}); err != nil {
		t.Fatalf("append b: %v", err)
	}
	if _, err := store.Append(ctx, audit.Event{Tenant: "tenant-a", Kind: audit.KindOutboxRetry, Status: audit.StatusRetried}); err != nil {
		t.Fatalf("append second a: %v", err)
	}
	rows, err := store.Recent(ctx, "tenant-a", 10)
	if err != nil {
		t.Fatalf("recent: %v", err)
	}
	if len(rows) != 2 || rows[0].Kind != audit.KindOutboxRetry || rows[1].Kind != audit.KindInboxDead {
		t.Fatalf("tenant-a rows = %+v", rows)
	}
	if rows[1].Detail["code"] != "bad_request" || rows[1].Detail["event"] != "Ev123" {
		t.Fatalf("detail didn't round-trip: %+v", rows[1].Detail)
	}
	if rows[1].Attempts != 3 {
		t.Fatalf("attempts = %d, want 3", rows[1].Attempts)
	}
}

// TestPostgresAuditRLSTenantIsolation proves board_audit is protected by FORCE
// RLS: a restricted role without keel.tenant_id sees nothing, tenant-a's
// setting sees only tenant-a, and a forged cross-tenant insert is refused.
func TestPostgresAuditRLSTenantIsolation(t *testing.T) {
	dsn := testDSN(t)
	schema := newTestSchema(t, dsn)
	ctx := t.Context()
	store, err := audit.OpenPostgres(ctx, audit.PostgresOptions{DSN: dsn, SearchPath: schema})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(store.Close)
	if _, err := store.Append(ctx, audit.Event{Tenant: "tenant-a", Kind: audit.KindWebhookAccepted, Status: audit.StatusAccepted}); err != nil {
		t.Fatalf("append a: %v", err)
	}
	if _, err := store.Append(ctx, audit.Event{Tenant: "tenant-b", Kind: audit.KindWebhookRejected, Status: audit.StatusRejected}); err != nil {
		t.Fatalf("append b: %v", err)
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
	role := fmt.Sprintf("keel_audit_rls_%d", time.Now().UnixNano())
	roleIdent := pgx.Identifier{role}.Sanitize()
	userIdent := pgx.Identifier{currentUser}.Sanitize()
	if _, err := admin.Exec(ctx, `CREATE ROLE `+roleIdent+` NOLOGIN`); err != nil {
		t.Fatalf("create role: %v", err)
	}
	if _, err := admin.Exec(ctx, `GRANT `+roleIdent+` TO `+userIdent); err != nil {
		_, _ = admin.Exec(ctx, `DROP ROLE `+roleIdent)
		t.Fatalf("grant role: %v", err)
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
		t.Fatalf("grant schema: %v", err)
	}
	if _, err := admin.Exec(ctx, `GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA `+schemaIdent+` TO `+roleIdent); err != nil {
		t.Fatalf("grant tables: %v", err)
	}
	if _, err := admin.Exec(ctx, `GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA `+schemaIdent+` TO `+roleIdent); err != nil {
		t.Fatalf("grant sequences: %v", err)
	}
	// The audit table carries ENABLE and FORCE.
	var enabled, forced bool
	if err := admin.QueryRow(ctx, `
		SELECT relrowsecurity, relforcerowsecurity
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = 'board_audit'`, schema).Scan(&enabled, &forced); err != nil {
		t.Fatalf("reading RLS flags: %v", err)
	}
	if !enabled || !forced {
		t.Fatalf("board_audit rls=%v force=%v", enabled, forced)
	}

	// tenant-a's setting sees tenant-a only.
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
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM board_audit`).Scan(&visible); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("count: %v", err)
	}
	if visible != 1 {
		tx.Rollback(ctx)
		t.Fatalf("tenant-a rows = %d, want 1", visible)
	}
	_, insertErr := tx.Exec(ctx, `
		INSERT INTO board_audit (tenant_id, kind) VALUES ('tenant-b', 'forged')`)
	if insertErr == nil {
		tx.Rollback(ctx)
		t.Fatal("a cross-tenant audit insert succeeded under tenant-a")
	}
	if !strings.Contains(insertErr.Error(), "row-level security") {
		tx.Rollback(ctx)
		t.Fatalf("the forged insert failed for another reason: %v", insertErr)
	}
	tx.Rollback(ctx)

	// Without a tenant setting the restricted role sees nothing.
	tx, err = admin.Begin(ctx)
	if err != nil {
		t.Fatalf("begin unset: %v", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE `+roleIdent); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("set role unset: %v", err)
	}
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM board_audit`).Scan(&visible); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("count unset: %v", err)
	}
	if visible != 0 {
		tx.Rollback(ctx)
		t.Fatalf("unset tenant saw %d audit rows", visible)
	}
	tx.Rollback(ctx)

	// board_audit has no bypass escape hatch: setting keel.bypass_rls still
	// sees nothing without a tenant, and with tenant-a it sees tenant-a only.
	tx, err = admin.Begin(ctx)
	if err != nil {
		t.Fatalf("begin bypass: %v", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE `+roleIdent); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("set role bypass: %v", err)
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('keel.bypass_rls', 'on', true)`); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("set bypass: %v", err)
	}
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM board_audit`).Scan(&visible); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("count bypass without tenant: %v", err)
	}
	if visible != 0 {
		tx.Rollback(ctx)
		t.Fatalf("bypass without a tenant saw %d audit rows", visible)
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('keel.tenant_id', 'tenant-a', true)`); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("set tenant bypass: %v", err)
	}
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM board_audit`).Scan(&visible); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("count bypass with tenant: %v", err)
	}
	if visible != 1 {
		tx.Rollback(ctx)
		t.Fatalf("bypass with tenant-a saw %d audit rows, want 1", visible)
	}
	_, bypassInsertErr := tx.Exec(ctx, `
		INSERT INTO board_audit (tenant_id, kind) VALUES ('tenant-b', 'forged')`)
	if bypassInsertErr == nil {
		tx.Rollback(ctx)
		t.Fatal("bypass let a cross-tenant audit insert succeed")
	}
	tx.Rollback(ctx)
}

// TestOpenPostgresCreatesOnlyTheAuditTable proves an audit-only DSN doesn't
// ask the role for ownership of the board's or sync's tables.
func TestOpenPostgresCreatesOnlyTheAuditTable(t *testing.T) {
	dsn := testDSN(t)
	schema := newTestSchema(t, dsn)
	store, err := audit.OpenPostgres(t.Context(), audit.PostgresOptions{DSN: dsn, SearchPath: schema})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(store.Close)
	admin, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = admin.Close(cleanup)
	})
	rows, err := admin.Query(t.Context(), `
		SELECT c.relname
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relkind = 'r'
		ORDER BY c.relname`, schema)
	if err != nil {
		t.Fatalf("listing tables: %v", err)
	}
	defer rows.Close()
	tables := make([]string, 0)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan table: %v", err)
		}
		tables = append(tables, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("tables: %v", err)
	}
	if len(tables) != 1 || tables[0] != "board_audit" {
		t.Fatalf("audit-only schema tables = %v, want [board_audit]", tables)
	}
}
