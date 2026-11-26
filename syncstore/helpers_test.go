package syncstore_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/syncstore"
)

// testDSN returns the PostgreSQL DSN, or skips the PostgreSQL subtest. Tests
// never start a container: point them at a local server and each test gets its
// own schema, dropped when it ends.
func testDSN(t *testing.T) string {
	t.Helper()
	for _, name := range []string{"KEEL_BOARD_TEST_DSN", "TEST_DSN"} {
		if dsn := strings.TrimSpace(os.Getenv(name)); dsn != "" {
			return dsn
		}
	}
	t.Skip("PostgreSQL tests skipped: set KEEL_BOARD_TEST_DSN (or TEST_DSN), for example postgres://localhost:5432/postgres?sslmode=disable")
	return ""
}

var testSchemaSeq atomic.Uint64

func newTestSchema(t *testing.T, dsn string) string {
	t.Helper()
	name := fmt.Sprintf("keel_sync_%d_%d", time.Now().UnixNano(), testSchemaSeq.Add(1))
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

func openPostgres(t *testing.T, dsn, schema string, cfg syncstore.Config) *syncstore.Postgres {
	t.Helper()
	postgres, err := syncstore.OpenPostgres(t.Context(), syncstore.PostgresOptions{
		DSN: dsn, SearchPath: schema, Config: cfg,
	})
	if err != nil {
		t.Fatalf("opening PostgreSQL sync store: %v", err)
	}
	t.Cleanup(postgres.Close)
	return postgres
}

// testConfig is the low-latency policy contract tests use: a failed attempt is
// due immediately unless a test asks for a backoff.
func testConfig() syncstore.Config {
	return syncstore.Config{
		InboundRetry:  boardsync.RetryPolicy{MaxAttempts: 3, BaseDelay: 0, MaxDelay: 0, Jitter: 0},
		OutboundRetry: boardsync.RetryPolicy{MaxAttempts: 3, BaseDelay: 0, MaxDelay: 0, Jitter: 0},
		LeaseTTL:      time.Minute,
	}
}

// withStores runs fn against an in-memory store and a PostgreSQL store in its
// own schema. The PostgreSQL subtest skips when no DSN is configured; the
// memory subtest always runs.
func withStores(t *testing.T, cfg syncstore.Config, fn func(t *testing.T, store syncstore.Store)) {
	t.Helper()
	t.Run("memory", func(t *testing.T) {
		memory, err := syncstore.NewMemory(cfg)
		if err != nil {
			t.Fatalf("new memory store: %v", err)
		}
		fn(t, memory)
	})
	t.Run("postgres", func(t *testing.T) {
		dsn := testDSN(t)
		schema := newTestSchema(t, dsn)
		fn(t, openPostgres(t, dsn, schema, cfg))
	})
}

func testEvent(id, thread string) boardsync.Event {
	return boardsync.Event{
		ID: id, Key: "slack", Platform: "slack",
		Kind: boardsync.KindMessage, Type: boardsync.TypeMessage,
		Channel: "C0123", Thread: thread, Message: id,
		Text:   "hello " + id,
		Author: boardsync.Author{ID: "slack:user:U1", Handle: "ada", Name: "Ada"},
	}
}

func testRecord(id, thread string) boardsync.Record {
	return boardsync.Record{
		Dedupe: "dedupe-" + id,
		Event:  testEvent(id, thread),
	}
}

func boardMessage(id, thread uint64, text string) boardsync.Message {
	return boardsync.Message{
		ID: id, Thread: thread, Channel: "support", Text: text,
		Actor: "slack:user:U1", ActorName: "Ada", Verified: true,
	}
}

func threadTarget(channel, thread string) boardsync.ResolvedTarget {
	return boardsync.ResolvedTarget{
		Key: "slack", Platform: "slack", Channel: channel, Thread: thread,
		Kind: boardsync.TargetThread,
	}
}

func channelTarget(channel string) boardsync.ResolvedTarget {
	return boardsync.ResolvedTarget{
		Key: "slack", Platform: "slack", Channel: channel, Kind: boardsync.TargetChannel,
	}
}

func seedEntry(t *testing.T, store syncstore.Store, tenant string, key boardsync.Key, enabled bool) {
	t.Helper()
	entry := syncstore.Entry{Tenant: tenant, Key: key, Platform: "slack", Enabled: enabled}
	if _, err := store.UpsertEntry(t.Context(), entry); err != nil {
		t.Fatalf("seed entry: %v", err)
	}
}
