package syncstore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/syncstore"
)

func TestPostgresMigrationIsIdempotent(t *testing.T) {
	dsn := testDSN(t)
	schema := newTestSchema(t, dsn)
	first, err := syncstore.OpenPostgres(t.Context(), syncstore.PostgresOptions{
		DSN: dsn, SearchPath: schema, Config: testConfig(),
	})
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	first.Close()
	second, err := syncstore.OpenPostgres(t.Context(), syncstore.PostgresOptions{
		DSN: dsn, SearchPath: schema, Config: testConfig(),
	})
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	t.Cleanup(second.Close)
	if _, err := second.PushInbox(t.Context(), "t1", "slack", []boardsync.Record{testRecord("m1", "C0123:T1")}); err != nil {
		t.Fatalf("push after the second migration: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect for schema check: %v", err)
	}
	defer admin.Close(ctx)
	var tables int
	err = admin.QueryRow(ctx, `
		SELECT count(*) FROM information_schema.tables
		WHERE table_schema = $1 AND table_name LIKE 'board_sync_%'`, schema).Scan(&tables)
	if err != nil {
		t.Fatalf("counting sync tables: %v", err)
	}
	if tables != 7 {
		t.Fatalf("sync tables = %d, want 7", tables)
	}
}

func TestPostgresRestartRecovery(t *testing.T) {
	dsn := testDSN(t)
	schema := newTestSchema(t, dsn)
	first, err := syncstore.OpenPostgres(t.Context(), syncstore.PostgresOptions{
		DSN: dsn, SearchPath: schema, Config: testConfig(),
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := first.PushInbox(t.Context(), "t1", "slack", []boardsync.Record{testRecord("m1", "C0123:T1")}); err != nil {
		t.Fatalf("push before restart: %v", err)
	}
	if _, err := first.EnqueueOutbox(t.Context(), "t1", "slack", postedEvent(10, 100, 101),
		boardMessage(101, 100, "root"), []boardsync.ResolvedTarget{threadTarget("C0123", "C0123:T1")}); err != nil {
		t.Fatalf("enqueue before restart: %v", err)
	}
	first.Close()

	second, err := syncstore.OpenPostgres(t.Context(), syncstore.PostgresOptions{
		DSN: dsn, SearchPath: schema, Config: testConfig(),
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(second.Close)
	items, err := second.LeaseInbox(t.Context(), "t1", "slack", 10, "worker")
	if err != nil || len(items) != 1 || items[0].ID != "m1" {
		t.Fatalf("inbox after restart = %+v, %v", items, err)
	}
	if _, err := second.AckInbox(t.Context(), "t1", "slack", "m1", items[0].Lease, nil); err != nil {
		t.Fatalf("ack after restart: %v", err)
	}
	jobs, err := second.LeaseOutbox(t.Context(), "t1", []boardsync.Key{"slack"}, 10, "worker")
	if err != nil || len(jobs) != 1 || jobs[0].Message.ID != 101 {
		t.Fatalf("outbox after restart = %+v, %v", jobs, err)
	}
	if _, err := second.AckOutbox(t.Context(), "t1", jobs[0].ID, jobs[0].Lease, nil, nil); err != nil {
		t.Fatalf("ack outbox after restart: %v", err)
	}
	// The dedupe row survived the restart, so a platform retry is still a
	// duplicate rather than a second import.
	if pushed, err := second.PushInbox(t.Context(), "t1", "slack", []boardsync.Record{testRecord("m1", "C0123:T1")}); err != nil || pushed.Added != 0 {
		t.Fatalf("redelivery after restart = %+v, %v", pushed, err)
	}
}

func TestPostgresInboxLeaseExpiry(t *testing.T) {
	cfg := testConfig()
	cfg.LeaseTTL = 2 * time.Second
	dsn := testDSN(t)
	schema := newTestSchema(t, dsn)
	store := openPostgres(t, dsn, schema, cfg)
	ctx := t.Context()
	if _, err := store.PushInbox(ctx, "t1", "slack", []boardsync.Record{testRecord("m1", "C0123:T1")}); err != nil {
		t.Fatalf("push: %v", err)
	}
	first, err := store.LeaseInbox(ctx, "t1", "slack", 1, "worker-a")
	if err != nil || len(first) != 1 {
		t.Fatalf("first lease = %+v, %v", first, err)
	}
	time.Sleep(2500 * time.Millisecond)
	second, err := store.LeaseInbox(ctx, "t1", "slack", 1, "worker-b")
	if err != nil || len(second) != 1 {
		t.Fatalf("second lease = %+v, %v", second, err)
	}
	if first[0].Lease == second[0].Lease {
		t.Fatal("the crashed worker's lease was not replaced")
	}
	if _, err := store.AckInbox(ctx, "t1", "slack", "m1", first[0].Lease, nil); !errors.Is(err, syncstore.ErrLeaseLost) {
		t.Fatalf("stale ack = %v, want ErrLeaseLost", err)
	}
	if _, err := store.AckInbox(ctx, "t1", "slack", "m1", second[0].Lease, nil); err != nil {
		t.Fatalf("live ack = %v", err)
	}
}

func TestPostgresOutboxLeaseExpiry(t *testing.T) {
	cfg := testConfig()
	cfg.LeaseTTL = 2 * time.Second
	dsn := testDSN(t)
	schema := newTestSchema(t, dsn)
	store := openPostgres(t, dsn, schema, cfg)
	ctx := t.Context()
	if _, err := store.EnqueueOutbox(ctx, "t1", "slack", postedEvent(10, 100, 101),
		boardMessage(101, 100, "root"), []boardsync.ResolvedTarget{threadTarget("C0123", "C0123:T1")}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	first, err := store.LeaseOutbox(ctx, "t1", []boardsync.Key{"slack"}, 1, "worker-a")
	if err != nil || len(first) != 1 {
		t.Fatalf("first lease = %+v, %v", first, err)
	}
	time.Sleep(2500 * time.Millisecond)
	second, err := store.LeaseOutbox(ctx, "t1", []boardsync.Key{"slack"}, 1, "worker-b")
	if err != nil || len(second) != 1 {
		t.Fatalf("second lease = %+v, %v", second, err)
	}
	if first[0].Lease == second[0].Lease {
		t.Fatal("the crashed worker's lease was not replaced")
	}
	if _, err := store.AckOutbox(ctx, "t1", first[0].ID, first[0].Lease, nil, nil); !errors.Is(err, syncstore.ErrLeaseLost) {
		t.Fatalf("stale ack = %v, want ErrLeaseLost", err)
	}
	if _, err := store.AckOutbox(ctx, "t1", second[0].ID, second[0].Lease, nil, nil); err != nil {
		t.Fatalf("live ack = %v", err)
	}
}
