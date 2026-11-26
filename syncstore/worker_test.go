package syncstore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/fake"
	"github.com/jixoai/keel-board/syncstore"
)

func TestInboundWorkerRetriesThenImports(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		registry := boardsync.NewRegistry()
		host := fake.NewHost()
		mirror := boardsync.Mirror("support")
		attempts := 0
		provider := fake.New("slack", boardsync.Capabilities{})
		if err := registry.Add(boardsync.Entry{Tenant: "t1", Key: "slack", Platform: "slack", Provider: provider, Sync: func(ctx context.Context, call *boardsync.Call) error {
			attempts++
			if attempts == 1 {
				return errors.New("temporary failure")
			}
			return mirror(ctx, call)
		}}); err != nil {
			t.Fatalf("register entry: %v", err)
		}
		if _, err := store.PushInbox(ctx, "t1", "slack", []boardsync.Record{testRecord("m1", "C0123:T1")}); err != nil {
			t.Fatalf("push: %v", err)
		}
		worker, err := syncstore.NewInboundWorker(syncstore.InboundWorkerConfig{
			Store: store, Registry: registry, Runner: boardsync.NewRunner(host),
			Tenant: "t1", Key: "slack", Owner: "worker", Limit: 1,
		})
		if err != nil {
			t.Fatalf("new worker: %v", err)
		}
		result, err := worker.Process(ctx)
		if err != nil || result.Retried != 1 {
			t.Fatalf("first pass = %+v, %v", result, err)
		}
		if imports := host.Imports(); len(imports) != 0 {
			t.Fatalf("a failed import wrote to the board: %+v", imports)
		}
		result, err = worker.Process(ctx)
		if err != nil || result.Acked != 1 {
			t.Fatalf("second pass = %+v, %v", result, err)
		}
		if imports := host.Imports(); len(imports) != 1 || imports[0].EventID != "m1" {
			t.Fatalf("imports = %+v", imports)
		}
	})
}

func TestInboundWorkerDefersUnregisteredKey(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		if _, err := store.PushInbox(ctx, "t1", "slack", []boardsync.Record{testRecord("m1", "C0123:T1")}); err != nil {
			t.Fatalf("push: %v", err)
		}
		registry := boardsync.NewRegistry()
		host := fake.NewHost()
		worker, err := syncstore.NewInboundWorker(syncstore.InboundWorkerConfig{
			Store: store, Registry: registry, Runner: boardsync.NewRunner(host),
			Tenant: "t1", Key: "slack", Owner: "worker", Limit: 1, Defer: time.Millisecond,
		})
		if err != nil {
			t.Fatalf("new worker: %v", err)
		}
		result, err := worker.Process(ctx)
		if err != nil || result.Deferred != 1 || result.Dead != 0 {
			t.Fatalf("unready pass = %+v, %v", result, err)
		}
		depth, err := store.InboxDepth(ctx, "t1", "slack")
		if err != nil || depth.Pending != 1 || depth.Dead != 0 {
			t.Fatalf("depth after defer = %+v, %v", depth, err)
		}
		// Once the entry is registered the same item is imported, not dead.
		provider := fake.New("slack", boardsync.Capabilities{})
		if err := registry.Add(boardsync.Entry{Tenant: "t1", Key: "slack", Platform: "slack", Provider: provider, Sync: boardsync.Mirror("support")}); err != nil {
			t.Fatalf("register entry: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
		result, err = worker.Process(ctx)
		if err != nil || result.Acked != 1 {
			t.Fatalf("ready pass = %+v, %v", result, err)
		}
		if imports := host.Imports(); len(imports) != 1 {
			t.Fatalf("imports = %+v", imports)
		}
	})
}

func TestWorkerRunStopsOnContext(t *testing.T) {
	memory, err := syncstore.NewMemory(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	registry := boardsync.NewRegistry()
	provider := fake.New("slack", boardsync.Capabilities{})
	if err := registry.Add(boardsync.Entry{Tenant: "t1", Key: "slack", Platform: "slack", Provider: provider, Sync: boardsync.Noop}); err != nil {
		t.Fatalf("register: %v", err)
	}
	worker, err := syncstore.NewInboundWorker(syncstore.InboundWorkerConfig{
		Store: memory, Registry: registry, Runner: boardsync.NewRunner(fake.NewHost()),
		Tenant: "t1", Key: "slack", PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := worker.Run(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("run = %v, want the context's end", err)
	}
}

// ackLostOnce reports the first ack as a lost lease, the crash window between
// a Host import and its ack.
type ackLostOnce struct {
	syncstore.Store
	lost bool
}

func (a *ackLostOnce) AckInbox(ctx context.Context, tenant string, key boardsync.Key, id, lease string, jobErr error) (syncstore.AckResult, error) {
	if !a.lost {
		a.lost = true
		return syncstore.AckResult{}, syncstore.ErrLeaseLost
	}
	return a.Store.AckInbox(ctx, tenant, key, id, lease, jobErr)
}

// importHost is idempotent by event ID, like a board host must be.
type importHost struct {
	results map[string]boardsync.ImportResult
	calls   int
	next    uint64
}

func newImportHost() *importHost {
	return &importHost{results: make(map[string]boardsync.ImportResult), next: 800}
}

func (h *importHost) Import(_ context.Context, req boardsync.ImportRequest) (boardsync.ImportResult, error) {
	h.calls++
	if result, ok := h.results[req.EventID]; ok {
		return result, nil
	}
	h.next++
	result := boardsync.ImportResult{Root: h.next, Message: h.next, CreatedRoot: req.Channel != ""}
	h.results[req.EventID] = result
	return result, nil
}

func (h *importHost) Record(context.Context, boardsync.BoardEvent) error { return nil }

func TestInboundCrashAfterImportBeforeAck(t *testing.T) {
	cfg := testConfig()
	cfg.LeaseTTL = time.Second
	memory, err := syncstore.NewMemory(cfg)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	memory.WithClock(func() time.Time { return now })
	ctx := t.Context()
	if _, err := memory.PushInbox(ctx, "t1", "slack", []boardsync.Record{testRecord("m1", "C0123:T1")}); err != nil {
		t.Fatalf("push: %v", err)
	}
	registry := boardsync.NewRegistry()
	provider := fake.New("slack", boardsync.Capabilities{})
	if err := registry.Add(boardsync.Entry{Tenant: "t1", Key: "slack", Platform: "slack", Provider: provider, Sync: boardsync.Mirror("support")}); err != nil {
		t.Fatalf("register: %v", err)
	}
	host := newImportHost()
	lost := &ackLostOnce{Store: memory}
	worker, err := syncstore.NewInboundWorker(syncstore.InboundWorkerConfig{
		Store: lost, Registry: registry, Runner: boardsync.NewRunner(host),
		Tenant: "t1", Key: "slack", Owner: "worker", Limit: 1,
	})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	result, err := worker.Process(ctx)
	if err != nil || result.Lost != 1 {
		t.Fatalf("crash pass = %+v, %v", result, err)
	}
	if host.calls != 1 || len(host.results) != 1 {
		t.Fatalf("first import calls=%d unique=%d", host.calls, len(host.results))
	}
	now = now.Add(2 * time.Second)
	result, err = worker.Process(ctx)
	if err != nil || result.Acked != 1 {
		t.Fatalf("recovery pass = %+v, %v", result, err)
	}
	if host.calls != 2 || len(host.results) != 1 {
		t.Fatalf("recovery re-created a board message: calls=%d unique=%d", host.calls, len(host.results))
	}
	depth, err := memory.InboxDepth(ctx, "t1", "slack")
	if err != nil || depth.Done != 1 {
		t.Fatalf("depth after recovery = %+v, %v", depth, err)
	}
}
