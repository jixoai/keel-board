package server_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/server"
	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/chatsdk"
	"github.com/jixoai/keel-board/syncstore"
)

var bridgeSchemaSeq atomic.Uint64

func bridgePGDSN(t *testing.T) string {
	t.Helper()
	for _, name := range []string{"KEEL_BOARD_TEST_DSN", "TEST_DSN"} {
		if dsn := strings.TrimSpace(os.Getenv(name)); dsn != "" {
			return dsn
		}
	}
	if strings.TrimSpace(os.Getenv("CI")) != "" {
		t.Fatal("CI requires KEEL_BOARD_TEST_DSN (or TEST_DSN)")
	}
	t.Skip("PostgreSQL tests skipped: set KEEL_BOARD_TEST_DSN")
	return ""
}

func newBridgeSchema(t *testing.T, dsn string) string {
	t.Helper()
	name := fmt.Sprintf("keel_bridge_%d_%d", time.Now().UnixNano(), bridgeSchemaSeq.Add(1))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect PostgreSQL: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{name}.Sanitize()); err != nil {
		admin.Close(ctx)
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(cleanupCtx, "DROP SCHEMA "+pgx.Identifier{name}.Sanitize()+" CASCADE"); err != nil {
			t.Logf("drop schema %s: %v", name, err)
		}
		_ = admin.Close(cleanupCtx)
	})
	return name
}

// TestBridgeEventsPostgres proves the bridge route's store-before-ack and
// replay path against the real schema, and that an unstorable normalized ID
// is a 400 before PostgreSQL can turn it into a 500.
func TestBridgeEventsPostgres(t *testing.T) {
	dsn := bridgePGDSN(t)
	schema := newBridgeSchema(t, dsn)
	store, err := syncstore.OpenPostgres(t.Context(), syncstore.PostgresOptions{
		DSN: dsn, SearchPath: schema,
	})
	if err != nil {
		t.Fatalf("open sync store: %v", err)
	}
	t.Cleanup(store.Close)

	board := log.New(log.Options{})
	provider, err := chatsdk.New(chatsdk.Config{
		Key: "telegram", Adapter: &chatsdk.FuncAdapter{PlatformName: "telegram"},
		SelfIDs: []string{"telegram:user:bot"},
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := boardsync.NewRegistry()
	if err := registry.Add(boardsync.Entry{
		Tenant: "t1", Key: "telegram", Platform: "telegram", Provider: provider, Sync: boardsync.Noop,
		Routes: func(context.Context, string) (bool, error) { return true, nil },
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertEntry(t.Context(), syncstore.Entry{
		Tenant: "t1", Key: "telegram", Platform: "telegram", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	srv := server.New(server.Options{
		Board: board,
		Sync: &server.SyncOptions{
			Store: store, Registry: registry,
			Tenant:       func(*http.Request) (string, error) { return "t1", nil },
			BridgeSecret: func(string, boardsync.Key) (boardsync.Secret, bool) { return bridgeTestSecret, true },
		},
	})
	api := httptest.NewServer(srv)
	t.Cleanup(api.Close)

	now := time.Now().Unix()
	url := api.URL + server.APIPrefix + "/sync/bridge/telegram/events"
	status, body := doBridge(t, signedBridgeRequest(t, url, bridgeEvent("m1", "-100", "hello"), bridgeTestSecret, now, "nonce-0123456789"))
	if status != http.StatusOK || body["stored"] != 1 {
		t.Fatalf("first event = %d %+v", status, body)
	}
	depth, err := store.InboxDepth(t.Context(), "t1", "telegram")
	if err != nil || depth.Pending != 1 {
		t.Fatalf("PG inbox = %+v, %v", depth, err)
	}
	// Same nonce is a replay even though the event would dedupe.
	if status, _ := doBridge(t, signedBridgeRequest(t, url, bridgeEvent("m1", "-100", "hello"), bridgeTestSecret, now, "nonce-0123456789")); status != http.StatusConflict {
		t.Fatalf("replay = %d", status)
	}
	// A NUL in an indexed ID is refused before it reaches PostgreSQL.
	bad := bridgeEvent("bad", "-100", "hello")
	bad.Events[0].Event.Channel = "C\x00123"
	if status, _ := doBridge(t, signedBridgeRequest(t, url, bad, bridgeTestSecret, now, "nonce-0123456780")); status != http.StatusBadRequest {
		t.Fatalf("NUL ID = %d, want 400", status)
	}
	tooLong := bridgeEvent("long", "-100", "hello")
	tooLong.Events[0].Event.Channel = strings.Repeat("c", 301)
	if status, _ := doBridge(t, signedBridgeRequest(t, url, tooLong, bridgeTestSecret, now, "nonce-0123456782")); status != http.StatusBadRequest {
		t.Fatalf("overlong ID = %d, want 400", status)
	}
	depth, err = store.InboxDepth(t.Context(), "t1", "telegram")
	if err != nil || depth.Pending != 1 {
		t.Fatalf("refused event reached PG: %+v, %v", depth, err)
	}
}
