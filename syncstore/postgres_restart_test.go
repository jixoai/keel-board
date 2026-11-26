package syncstore_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/server"
	"github.com/jixoai/keel-board/store"
	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/fake"
	"github.com/jixoai/keel-board/syncstore"
)

// TestPostgresHostRestart proves Host and an outbound worker over PostgreSQL:
// a mapping, copy, cursor, and pending delivery survive reopening the pool and
// building fresh process-local objects.
func TestPostgresHostRestart(t *testing.T) {
	dsn := testDSN(t)
	schema := newTestSchema(t, dsn)
	boardSchema := newTestSchema(t, dsn)
	cfg := testConfig()
	store1 := openPostgres(t, dsn, schema, cfg)
	boardStore1, err := store.OpenPostgres(t.Context(), store.PostgresOptions{DSN: dsn, SearchPath: boardSchema})
	if err != nil {
		t.Fatalf("open board store: %v", err)
	}
	board1 := log.New(log.Options{Store: boardStore1})
	host1 := server.NewSyncHost(board1, store1, server.SyncHostOptions{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	provider1 := fake.New("slack", boardsync.Capabilities{StartThreads: true})
	posts1 := 0
	provider1.PostFunc = func(_ context.Context, _ boardsync.Key, target boardsync.ResolvedTarget, _ boardsync.OutboundMessage) (boardsync.Posted, error) {
		posts1++
		thread := target.Thread
		if target.Kind == boardsync.TargetChannel {
			thread = target.Channel + ":1"
		}
		return boardsync.Posted{Thread: thread, Message: fmt.Sprintf("m%d", posts1)}, nil
	}
	registry1 := newRegistry(t, provider1, host1)
	worker1 := newWorker(t, store1, registry1, host1)

	rootID := boardRoot(t, board1, "support", "root")
	target := channelTarget("C1")
	if err := host1.Record(t.Context(), boardsync.BoardEvent{
		Tenant: "t1", Type: boardsync.BoardThreadSynced, Thread: rootID, Target: refOf(target), Cause: "attach",
	}); err != nil {
		t.Fatalf("record thread.synced: %v", err)
	}
	if result, err := worker1.Process(t.Context()); err != nil || result.Acked != 1 {
		t.Fatalf("root delivery = %+v, %v", result, err)
	}
	if posts1 != 1 {
		t.Fatalf("root posts = %d", posts1)
	}
	if _, found, err := store1.ThreadByTarget(t.Context(), "t1", "slack", threadTarget("C1", "C1:1")); err != nil || !found {
		t.Fatalf("concrete thread mapping = %v, %v", found, err)
	}
	if err := store1.SetCursor(t.Context(), "t1", "slack", "board-events", "7"); err != nil {
		t.Fatal(err)
	}
	// A reply is queued but not delivered before the restart.
	replyID := boardReply(t, board1, rootID, "reply")
	if _, err := store1.EnqueueOutbox(t.Context(), "t1", "slack", boardsync.BoardEvent{
		Tenant: "t1", ID: replyID, Type: boardsync.BoardMessagePosted, Thread: rootID,
	}, syncMessage(replyID, rootID, "reply"), []boardsync.ResolvedTarget{threadTarget("C1", "C1:1")}); err != nil {
		t.Fatal(err)
	}
	store1.Close()
	boardStore1.Close()

	// Second process.
	store2 := openPostgres(t, dsn, schema, cfg)
	t.Cleanup(store2.Close)
	if cursor, ok, err := store2.Cursor(t.Context(), "t1", "slack", "board-events"); err != nil || !ok || cursor != "7" {
		t.Fatalf("persisted cursor = %q, %v, %v", cursor, ok, err)
	}
	if _, found, err := store2.ThreadByTarget(t.Context(), "t1", "slack", threadTarget("C1", "C1:1")); err != nil || !found {
		t.Fatalf("persisted mapping = %v, %v", found, err)
	}
	if _, found, err := store2.Copy(t.Context(), "t1", "slack", channelTarget("C1"), rootID); err != nil || !found {
		t.Fatalf("persisted copy = %v, %v", found, err)
	}
	boardStore2, err := store.OpenPostgres(t.Context(), store.PostgresOptions{DSN: dsn, SearchPath: boardSchema})
	if err != nil {
		t.Fatalf("reopen board store: %v", err)
	}
	t.Cleanup(boardStore2.Close)
	board2 := log.New(log.Options{Store: boardStore2})
	host2 := server.NewSyncHost(board2, store2, server.SyncHostOptions{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	provider2 := fake.New("slack", boardsync.Capabilities{StartThreads: true})
	posts2 := 0
	provider2.PostFunc = func(_ context.Context, _ boardsync.Key, target boardsync.ResolvedTarget, _ boardsync.OutboundMessage) (boardsync.Posted, error) {
		posts2++
		return boardsync.Posted{Thread: target.Thread, Message: fmt.Sprintf("n%d", posts2)}, nil
	}
	registry2 := newRegistry(t, provider2, host2)
	worker2 := newWorker(t, store2, registry2, host2)
	if result, err := worker2.Process(t.Context()); err != nil || result.Acked != 1 {
		t.Fatalf("restarted delivery = %+v, %v", result, err)
	}
	if posts2 != 1 || len(provider2.Posts()) != 1 || provider2.Posts()[0].Target.Thread != "C1:1" {
		t.Fatalf("restart posts = %+v", provider2.Posts())
	}
}

func newRegistry(t *testing.T, provider *fake.Provider, host *server.SyncHost) *boardsync.Registry {
	t.Helper()
	registry := boardsync.NewRegistry()
	if err := registry.Add(boardsync.Entry{
		Tenant: "t1", Key: "slack", Platform: "slack", Provider: provider, Sync: boardsync.Mirror("support"),
	}); err != nil {
		t.Fatal(err)
	}
	return registry
}

func newWorker(t *testing.T, store syncstore.Store, registry *boardsync.Registry, host *server.SyncHost) *syncstore.OutboundWorker {
	t.Helper()
	worker, err := syncstore.NewOutboundWorker(syncstore.OutboundWorkerConfig{
		Store: store, Registry: registry, Runner: boardsync.NewRunner(host), Host: host,
		Tenant: "t1", Keys: []boardsync.Key{"slack"}, Owner: "pg-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func boardRoot(t *testing.T, board *log.Board, channel, text string) uint64 {
	t.Helper()
	var id uint64
	if err := board.Update(t.Context(), "t1", log.UpdateOptions{}, func(tx *log.Tx) error {
		id = tx.NextID(0)
		tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
			ID: id, Channel: channel, Text: text, Thread: log.ThreadKey(id),
			Labels: map[string]string{},
			By:     protocol.Author{Actor: "local:user:ada", Principal: "local:user:ada"},
		})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return id
}

func boardReply(t *testing.T, board *log.Board, root uint64, text string) uint64 {
	t.Helper()
	var id uint64
	if err := board.Update(t.Context(), "t1", log.UpdateOptions{}, func(tx *log.Tx) error {
		id = tx.NextID(0)
		tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
			ID: id, Channel: "support", Text: text, Re: &root, Thread: log.ThreadKey(root),
			Labels: map[string]string{},
			By:     protocol.Author{Actor: "local:user:ada", Principal: "local:user:ada"},
		})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return id
}

func syncMessage(id, thread uint64, text string) boardsync.Message {
	return boardsync.Message{
		ID: id, Thread: thread, Channel: "support", Text: text,
		Actor: "local:user:ada", ActorName: "ada",
	}
}

func refOf(target boardsync.ResolvedTarget) *boardsync.ThreadRef {
	ref := target.Ref()
	return &ref
}

func TestPostgresConcurrentRootImport(t *testing.T) {
	dsn := testDSN(t)
	schema := newTestSchema(t, dsn)
	store := openPostgres(t, dsn, schema, testConfig())
	t.Cleanup(store.Close)
	board := log.New(log.Options{})
	host := server.NewSyncHost(board, store, server.SyncHostOptions{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	const events = 4
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make(chan boardsync.ImportResult, events)
	for i := 0; i < events; i++ {
		event := boardsync.Event{
			ID: fmt.Sprintf("m%d", i), Key: "slack", Platform: "slack",
			Kind: boardsync.KindMessage, Type: boardsync.TypeMessage,
			Channel: "C1", Thread: "C1:T1", Message: fmt.Sprintf("m%d", i), IsRoot: true,
			Text:   fmt.Sprintf("message %d", i),
			Author: boardsync.Author{ID: "slack:user:U1", Handle: "ada", Name: "Ada"},
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			result, err := host.Import(t.Context(), boardsync.ImportRequest{
				Tenant: "t1", Key: "slack", EventID: event.ID, Channel: "support", Event: event,
			})
			if err != nil {
				t.Errorf("import: %v", err)
			}
			results <- result
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	roots := 0
	for result := range results {
		if result.CreatedRoot {
			roots++
		}
	}
	if roots != 1 {
		t.Fatalf("created roots = %d, want one", roots)
	}
}
