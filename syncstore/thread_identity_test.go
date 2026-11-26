package syncstore_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/syncstore"
)

func TestThreadTargetUniqueAcrossBoardThreads(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		target := threadTarget("C0123", "C0123:1727.9")
		first, err := store.UpsertThread(ctx, syncstore.Thread{
			Tenant: "t1", BoardThread: 10, Key: "slack", Platform: "slack",
			Channel: target.Channel, Thread: target.Thread, Kind: boardsync.TargetThread, Since: 7,
		})
		if err != nil {
			t.Fatalf("first attach: %v", err)
		}
		if first.Since != 7 {
			t.Fatalf("since = %d", first.Since)
		}
		if _, err := store.UpsertThread(ctx, syncstore.Thread{
			Tenant: "t1", BoardThread: 20, Key: "slack", Platform: "slack",
			Channel: target.Channel, Thread: target.Thread, Kind: boardsync.TargetThread, Since: 9,
		}); !errors.Is(err, syncstore.ErrThreadAttached) {
			t.Fatalf("second board thread = %v", err)
		}
		// Re-attaching the same board thread is idempotent and keeps the
		// original since; a later attach must not make it less permissive.
		again, err := store.UpsertThread(ctx, syncstore.Thread{
			Tenant: "t1", BoardThread: 10, Key: "slack", Platform: "slack",
			Channel: target.Channel, Thread: target.Thread, Kind: boardsync.TargetThread, Since: 12,
		})
		if err != nil {
			t.Fatalf("re-attach: %v", err)
		}
		if again.Since != 7 {
			t.Fatalf("since after re-attach = %d", again.Since)
		}
		// Channel targets are not one conversation: two board roots may start
		// threads in the same channel.
		for _, board := range []uint64{10, 20} {
			if _, err := store.UpsertThread(ctx, syncstore.Thread{
				Tenant: "t1", BoardThread: board, Key: "slack", Platform: "slack",
				Channel: "C0123", Kind: boardsync.TargetChannel,
			}); err != nil {
				t.Fatalf("channel target board %d: %v", board, err)
			}
		}
	})
}

func TestLockTargetSerializesOneTarget(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		target := threadTarget("C0123", "C0123:1727.9")
		release, err := store.LockTarget(ctx, "t1", "slack", target)
		if err != nil {
			t.Fatalf("first lock: %v", err)
		}
		acquired := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			second, err := store.LockTarget(ctx, "t1", "slack", target)
			if err != nil {
				t.Errorf("second lock: %v", err)
				return
			}
			close(acquired)
			second()
		}()
		select {
		case <-acquired:
			t.Fatal("the same target's lock was acquired twice")
		case <-time.After(100 * time.Millisecond):
		}
		// A different target is independent.
		other := threadTarget("C0123", "C0123:other")
		otherRelease, err := store.LockTarget(ctx, "t1", "slack", other)
		if err != nil {
			t.Fatalf("other lock: %v", err)
		}
		otherRelease()
		release()
		wg.Wait()
		select {
		case <-acquired:
		default:
			t.Fatal("the second lock was never acquired after release")
		}
	})
}

func TestRemoveCopy(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		target := threadTarget("C0123", "C0123:T1")
		copied, err := store.SaveCopy(ctx, "t1", syncstore.Copy{
			BoardMessage: 5, BoardThread: 5, Key: "slack", Target: target,
			ExternalThread: "C0123:T1", ExternalMessage: "1",
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.RemoveCopy(ctx, "t1", copied.ID); err != nil {
			t.Fatalf("remove: %v", err)
		}
		if _, found, err := store.Copy(ctx, "t1", "slack", target, 5); err != nil || found {
			t.Fatalf("copy after remove = %v, %v", found, err)
		}
	})
}

func TestUpsertThreadConcurrentConflict(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		start := make(chan struct{})
		var wg sync.WaitGroup
		results := make(chan error, 2)
		for _, board := range []uint64{10, 20} {
			board := board
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, err := store.UpsertThread(ctx, syncstore.Thread{
					Tenant: "t1", BoardThread: board, Key: "slack", Platform: "slack",
					Channel: "C0123", Thread: "C0123:1727.9", Kind: boardsync.TargetThread,
				})
				results <- err
			}()
		}
		close(start)
		wg.Wait()
		close(results)
		okCount, conflictCount := 0, 0
		for err := range results {
			switch {
			case err == nil:
				okCount++
			case errors.Is(err, syncstore.ErrThreadAttached):
				conflictCount++
			default:
				t.Fatalf("unexpected: %v", err)
			}
		}
		if okCount != 1 || conflictCount != 1 {
			t.Fatalf("upserts = %d ok, %d conflict", okCount, conflictCount)
		}
	})
}
