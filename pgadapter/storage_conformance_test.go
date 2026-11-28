package pgadapter_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/jixoai/keel-board/audit"
	"github.com/jixoai/keel-board/automation"
	"github.com/jixoai/keel-board/conformance"
	"github.com/jixoai/keel-board/pgadapter"
	pglogstore "github.com/jixoai/keel-board/pgadapter/logstore"
	"github.com/jixoai/keel-board/storage"
	"github.com/jixoai/keel-board/store"
	"github.com/jixoai/keel-board/syncstore"
)

var storageSchemaSeq atomic.Uint64

func storageTestDSN(t *testing.T) string {
	t.Helper()
	for _, name := range []string{"KEEL_BOARD_TEST_DSN", "TEST_DSN"} {
		if dsn := strings.TrimSpace(os.Getenv(name)); dsn != "" {
			return dsn
		}
	}
	if strings.TrimSpace(os.Getenv("CI")) != "" {
		t.Fatal("CI requires KEEL_BOARD_TEST_DSN (or TEST_DSN); storage conformance must not skip PostgreSQL")
	}
	t.Skip("storage conformance skipped: set KEEL_BOARD_TEST_DSN (or TEST_DSN)")
	return ""
}

func newStorageSchema(t *testing.T, dsn string) string {
	t.Helper()
	name := fmt.Sprintf("keel_storage_conf_%d_%d", time.Now().UnixNano(), storageSchemaSeq.Add(1))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect for schema: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{name}.Sanitize()); err != nil {
		_ = admin.Close(ctx)
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dropCancel()
		if _, err := admin.Exec(dropCtx, "DROP SCHEMA "+pgx.Identifier{name}.Sanitize()+" CASCADE"); err != nil {
			t.Logf("drop schema %s: %v", name, err)
		}
		_ = admin.Close(dropCtx)
	})
	return name
}

// TestStoragePostgres is the production adapter's opt-in to the dialect-neutral
// storage conformance suite. The suite itself imports no driver; this test
// supplies PostgreSQL handles and the capabilities the adapter declares.
func TestStoragePostgres(t *testing.T) {
	dsn := storageTestDSN(t)
	schema := newStorageSchema(t, dsn)

	openLog := func(t *testing.T) *pglogstore.LogStore {
		t.Helper()
		handle, err := pglogstore.OpenLog(t.Context(), pglogstore.LogOptions{DSN: dsn, SearchPath: schema})
		if err != nil {
			t.Fatalf("open log store: %v", err)
		}
		t.Cleanup(handle.Close)
		return handle
	}
	openSync := func(t *testing.T) *pgadapter.SyncStore {
		t.Helper()
		handle, err := pgadapter.OpenSync(t.Context(), pgadapter.SyncOptions{DSN: dsn, SearchPath: schema})
		if err != nil {
			t.Fatalf("open sync store: %v", err)
		}
		t.Cleanup(handle.Close)
		return handle
	}
	openAudit := func(t *testing.T) *pgadapter.AuditStore {
		t.Helper()
		handle, err := pgadapter.OpenAudit(t.Context(), pgadapter.AuditOptions{DSN: dsn, SearchPath: schema})
		if err != nil {
			t.Fatalf("open audit store: %v", err)
		}
		t.Cleanup(handle.Close)
		return handle
	}
	openAutomation := func(t *testing.T) *pgadapter.Automation {
		t.Helper()
		handle, err := pgadapter.OpenAutomation(t.Context(), pgadapter.AutomationOptions{DSN: dsn, SearchPath: schema})
		if err != nil {
			t.Fatalf("open automation store: %v", err)
		}
		t.Cleanup(handle.Close)
		return handle
	}

	capabilities := storage.Capabilities{
		Durable:            true,
		CrossProcessLeases: true,
		Migrations:         true,
		ListenNotify:       false,
	}
	conformance.RunStorage(t, conformance.StorageOptions{
		Name:         "postgres",
		Store:        func(t *testing.T) store.EventStore { return openLog(t) },
		SyncStore:    func(t *testing.T) syncstore.Store { return openSync(t) },
		Audit:        func(t *testing.T) audit.Store { return openAudit(t) },
		Automation:   func(t *testing.T) automation.Store { return openAutomation(t) },
		Capabilities: &capabilities,
		Reporters: []storage.Reporter{
			(*pglogstore.LogStore)(nil), (*pgadapter.SyncStore)(nil),
			(*pgadapter.AuditStore)(nil), (*pgadapter.Automation)(nil),
		},
	})
}
