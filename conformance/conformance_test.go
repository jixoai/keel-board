package conformance_test

import (
	"context"
	"fmt"
	"github.com/jixoai/keel-board/pgadapter/logstore"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jixoai/keel-board/conformance"
	"github.com/jixoai/keel-board/store"
)

// TestMemory runs the conformance suite over the in-memory log.
func TestMemory(t *testing.T) {
	memory := store.NewMemory()
	conformance.Run(t, conformance.Options{
		Name:  "memory",
		Store: func(*testing.T) store.EventStore { return memory },
	})
}

// TestPostgres runs the same suite over PostgreSQL. Every handle the suite
// asks for is a new pool on one schema, so a two-board test models two
// processes sharing a log.
func TestPostgres(t *testing.T) {
	dsn := testDSN(t)
	schema := newTestSchema(t, dsn)
	conformance.Run(t, conformance.Options{
		Name: "postgres",
		Store: func(t *testing.T) store.EventStore {
			t.Helper()
			postgres, err := pglogstore.OpenLog(t.Context(), pglogstore.LogOptions{DSN: dsn, SearchPath: schema})
			if err != nil {
				t.Fatalf("opening PostgreSQL store: %v", err)
			}
			t.Cleanup(postgres.Close)
			return postgres
		},
	})
}

// testDSN returns the PostgreSQL DSN the suite runs against, or skips it. CI
// must set a DSN; a missing one fails there rather than skipping silently.
func testDSN(t *testing.T) string {
	t.Helper()
	for _, name := range []string{"KEEL_BOARD_TEST_DSN", "TEST_DSN"} {
		if dsn := strings.TrimSpace(os.Getenv(name)); dsn != "" {
			return dsn
		}
	}
	if strings.TrimSpace(os.Getenv("CI")) != "" {
		t.Fatal("CI requires KEEL_BOARD_TEST_DSN (or TEST_DSN); conformance must not skip PostgreSQL")
	}
	t.Skip("PostgreSQL conformance skipped: set KEEL_BOARD_TEST_DSN (or TEST_DSN), for example postgres://localhost:5432/postgres?sslmode=disable")
	return ""
}

// testSchemaSeq keeps generated schema names unique within one process.
var testSchemaSeq atomic.Uint64

// newTestSchema creates a schema for one suite run and drops it after.
func newTestSchema(t *testing.T, dsn string) string {
	t.Helper()
	name := fmt.Sprintf("keel_board_conformance_%d_%d", time.Now().UnixNano(), testSchemaSeq.Add(1))
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
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(cleanupCtx, "DROP SCHEMA "+pgx.Identifier{name}.Sanitize()+" CASCADE"); err != nil {
			t.Logf("dropping test schema %s: %v", name, err)
		}
		_ = admin.Close(cleanupCtx)
	})
	return name
}
