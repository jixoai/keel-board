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
		if err := registry.Add(boardsync.Entry{Key: "slack", Provider: provider, Sync: func(ctx context.Context, call *boardsync.Call) error {
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

func TestInboundWorkerDeadLettersUnregisteredKey(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		if _, err := store.PushInbox(ctx, "t1", "slack", []boardsync.Record{testRecord("m1", "C0123:T1")}); err != nil {
			t.Fatalf("push: %v", err)
		}
		worker, err := syncstore.NewInboundWorker(syncstore.InboundWorkerConfig{
			Store: store, Registry: boardsync.NewRegistry(), Runner: boardsync.NewRunner(fake.NewHost()),
			Tenant: "t1", Key: "slack", Owner: "worker", Limit: 1,
		})
		if err != nil {
			t.Fatalf("new worker: %v", err)
		}
		result, err := worker.Process(ctx)
		if err != nil || result.Dead != 1 {
			t.Fatalf("pass = %+v, %v", result, err)
		}
		dead, err := store.DeadInbox(ctx, "t1", "slack", 10)
		if err != nil || len(dead) != 1 {
			t.Fatalf("dead = %+v, %v", dead, err)
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
	if err := registry.Add(boardsync.Entry{Key: "slack", Provider: provider, Sync: boardsync.Noop}); err != nil {
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
