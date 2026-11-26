package store_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jixoai/keel-board/store"
)

func TestMemoryAppendAndLoad(t *testing.T) {
	memory := store.NewMemory()
	ctx := context.Background()
	record := store.Record{Seq: 1, Type: "event", Payload: []byte(`{"a":1}`), At: time.Now()}
	if err := memory.Append(ctx, "t", []store.Record{record}, 0); err != nil {
		t.Fatal(err)
	}
	records, err := memory.Load(ctx, "t")
	if err != nil || len(records) != 1 || string(records[0].Payload) != `{"a":1}` {
		t.Fatalf("load: %+v, %v", records, err)
	}
	// Load returns a copy: a caller can't rewrite the log.
	records[0].Payload[0] = 'X'
	again, _ := memory.Load(ctx, "t")
	if string(again[0].Payload) != `{"a":1}` {
		t.Fatal("load should copy payloads")
	}
	if other, _ := memory.Load(ctx, "other"); len(other) != 0 {
		t.Fatal("another tenant is empty")
	}
}

func TestMemoryConditionalAppend(t *testing.T) {
	memory := store.NewMemory()
	ctx := context.Background()
	first := store.Record{Seq: 1, Type: "event", At: time.Now()}
	if err := memory.Append(ctx, "t", []store.Record{first}, 0); err != nil {
		t.Fatal(err)
	}
	if err := memory.Append(ctx, "t", []store.Record{{Seq: 2, Type: "event"}}, 0); !errors.Is(err, store.ErrHeadMoved) {
		t.Fatalf("stale head should be refused: %v", err)
	}
	if err := memory.Append(ctx, "t", []store.Record{{Seq: 2, Type: "event"}}, 1); err != nil {
		t.Fatal(err)
	}
	// A batch whose own seqs aren't contiguous is refused.
	if err := memory.Append(ctx, "t", []store.Record{{Seq: 3, Type: "a"}, {Seq: 5, Type: "b"}}, 2); !errors.Is(err, store.ErrHeadMoved) {
		t.Fatalf("non-contiguous batch: %v", err)
	}
	if records, _ := memory.Load(ctx, "t"); len(records) != 2 {
		t.Fatalf("a refused batch wrote anyway: %+v", records)
	}
}

func TestMemoryConcurrentAppendsKeepOneOrder(t *testing.T) {
	memory := store.NewMemory()
	ctx := context.Background()
	const writers = 32
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				head := lenOf(memory, ctx)
				err := memory.Append(ctx, "t", []store.Record{{Seq: uint64(head) + 1, Type: "event"}}, uint64(head))
				if err == nil {
					return
				}
				if !errors.Is(err, store.ErrHeadMoved) {
					t.Errorf("append: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if records, _ := memory.Load(ctx, "t"); len(records) != writers {
		t.Fatalf("expected %d records, got %d", writers, len(records))
	}
}

func lenOf(memory *store.Memory, ctx context.Context) int {
	records, _ := memory.Load(ctx, "t")
	return len(records)
}

func TestPostgresIsASeam(t *testing.T) {
	postgres := store.NewPostgres("")
	if _, err := postgres.Load(context.Background(), "t"); !errors.Is(err, store.ErrNotImplemented) {
		t.Fatalf("Postgres should be an explicit seam, got %v", err)
	}
}
