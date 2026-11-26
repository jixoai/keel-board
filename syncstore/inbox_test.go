package syncstore_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/syncstore"
)

func TestInboxDedupesOrdersAndLeasesOnePerLane(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		pushed, err := store.PushInbox(ctx, "t1", "slack", []boardsync.Record{
			testRecord("m1", "C0123:T1"),
			testRecord("m2", "C0123:T1"),
			testRecord("m3", "C0123:T2"),
			testRecord("m1", "C0123:T1"),
		})
		if err != nil || pushed.Added != 3 || pushed.Skipped != 1 {
			t.Fatalf("push = %+v, %v", pushed, err)
		}
		if pushed, err := store.PushInbox(ctx, "t1", "slack", []boardsync.Record{testRecord("m1", "C0123:T1")}); err != nil || pushed.Added != 0 {
			t.Fatalf("redelivery push = %+v, %v", pushed, err)
		}
		leased, err := store.LeaseInbox(ctx, "t1", "slack", 10, "worker-a")
		if err != nil || len(leased) != 2 {
			t.Fatalf("lease = %+v, %v; want one per lane", leased, err)
		}
		if leased[0].ID != "m1" || leased[1].ID != "m3" {
			t.Fatalf("lease order = %q, %q", leased[0].ID, leased[1].ID)
		}
		if again, err := store.LeaseInbox(ctx, "t1", "slack", 10, "worker-b"); err != nil || len(again) != 0 {
			t.Fatalf("second lease = %+v, %v; a lane was leased twice", again, err)
		}
		if _, err := store.AckInbox(ctx, "t1", "slack", "m1", leased[0].Lease, nil); err != nil {
			t.Fatalf("ack m1: %v", err)
		}
		next, err := store.LeaseInbox(ctx, "t1", "slack", 10, "worker-a")
		if err != nil || len(next) != 1 || next[0].ID != "m2" {
			t.Fatalf("lease after ack = %+v, %v; want m2", next, err)
		}
		if _, err := store.AckInbox(ctx, "t1", "slack", "m3", leased[1].Lease, nil); err != nil {
			t.Fatalf("ack m3: %v", err)
		}
		if _, err := store.AckInbox(ctx, "t1", "slack", "m2", next[0].Lease, nil); err != nil {
			t.Fatalf("ack m2: %v", err)
		}
		depth, err := store.InboxDepth(ctx, "t1", "slack")
		if err != nil || depth.Done != 3 || depth.Pending != 0 || depth.Leased != 0 {
			t.Fatalf("depth = %+v, %v", depth, err)
		}
		// A redelivery after the import landed is still one row: the dedupe
		// record is durable, not deleted with the work item.
		if pushed, err := store.PushInbox(ctx, "t1", "slack", []boardsync.Record{testRecord("m1", "C0123:T1")}); err != nil || pushed.Added != 0 {
			t.Fatalf("redelivery after ack = %+v, %v", pushed, err)
		}
		if leaked, err := store.LeaseInbox(ctx, "t1", "slack", 10, "worker-a"); err != nil || len(leaked) != 0 {
			t.Fatalf("redelivery leased again: %+v, %v", leaked, err)
		}
	})
}

func TestInboxRetryAndDeadLetter(t *testing.T) {
	cfg := testConfig()
	cfg.InboundRetry = boardsync.RetryPolicy{MaxAttempts: 2, BaseDelay: 0, MaxDelay: 0, Jitter: 0}
	withStores(t, cfg, func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		if _, err := store.PushInbox(ctx, "t1", "slack", []boardsync.Record{
			testRecord("m1", "C0123:T1"),
			testRecord("m2", "C0123:T1"),
		}); err != nil {
			t.Fatalf("push: %v", err)
		}
		first, err := store.LeaseInbox(ctx, "t1", "slack", 1, "w")
		if err != nil || len(first) != 1 {
			t.Fatalf("first lease = %+v, %v", first, err)
		}
		ack, err := store.AckInbox(ctx, "t1", "slack", "m1", first[0].Lease, errors.New("boom"))
		if err != nil || !ack.Retried || ack.Attempts != 1 {
			t.Fatalf("first failure ack = %+v, %v", ack, err)
		}
		second, err := store.LeaseInbox(ctx, "t1", "slack", 1, "w")
		if err != nil || len(second) != 1 || second[0].Attempts != 1 {
			t.Fatalf("retry lease = %+v, %v", second, err)
		}
		ack, err = store.AckInbox(ctx, "t1", "slack", "m1", second[0].Lease, errors.New("boom again"))
		if err != nil || !ack.Dead || ack.Attempts != 2 {
			t.Fatalf("second failure ack = %+v, %v", ack, err)
		}
		// The dead item doesn't block the next event on its lane.
		next, err := store.LeaseInbox(ctx, "t1", "slack", 1, "w")
		if err != nil || len(next) != 1 || next[0].ID != "m2" {
			t.Fatalf("lease after dead letter = %+v, %v", next, err)
		}
		if _, err := store.AckInbox(ctx, "t1", "slack", "m2", next[0].Lease, nil); err != nil {
			t.Fatalf("ack m2: %v", err)
		}
		dead, err := store.DeadInbox(ctx, "t1", "slack", 10)
		if err != nil || len(dead) != 1 || dead[0].ID != "m1" || dead[0].Attempts != 2 {
			t.Fatalf("dead = %+v, %v", dead, err)
		}
		if err := store.RetryInbox(ctx, "t1", "slack", "m1"); err != nil {
			t.Fatalf("retry dead inbox: %v", err)
		}
		retried, err := store.LeaseInbox(ctx, "t1", "slack", 1, "w")
		if err != nil || len(retried) != 1 || retried[0].ID != "m1" || retried[0].Attempts != 0 {
			t.Fatalf("retried lease = %+v, %v", retried, err)
		}
		if _, err := store.AckInbox(ctx, "t1", "slack", "m1", retried[0].Lease, nil); err != nil {
			t.Fatalf("ack retried: %v", err)
		}
		depth, err := store.InboxDepth(ctx, "t1", "slack")
		if err != nil || depth.Done != 2 || depth.Dead != 0 {
			t.Fatalf("depth after retry = %+v, %v", depth, err)
		}
	})
}

func TestInboxBackoffBlocksItsLane(t *testing.T) {
	cfg := testConfig()
	cfg.InboundRetry = boardsync.RetryPolicy{MaxAttempts: 3, BaseDelay: time.Hour, MaxDelay: time.Hour, Jitter: 0}
	withStores(t, cfg, func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		if _, err := store.PushInbox(ctx, "t1", "slack", []boardsync.Record{
			testRecord("m1", "C0123:T1"),
			testRecord("m2", "C0123:T1"),
			testRecord("m3", "C0123:T2"),
		}); err != nil {
			t.Fatalf("push: %v", err)
		}
		leased, err := store.LeaseInbox(ctx, "t1", "slack", 10, "w")
		if err != nil || len(leased) != 2 {
			t.Fatalf("lease = %+v, %v", leased, err)
		}
		for _, item := range leased {
			if item.ID == "m1" {
				if _, err := store.AckInbox(ctx, "t1", "slack", item.ID, item.Lease, errors.New("retry later")); err != nil {
					t.Fatalf("ack m1: %v", err)
				}
			} else {
				if _, err := store.AckInbox(ctx, "t1", "slack", item.ID, item.Lease, nil); err != nil {
					t.Fatalf("ack other lane: %v", err)
				}
			}
		}
		if next, err := store.LeaseInbox(ctx, "t1", "slack", 10, "w"); err != nil || len(next) != 0 {
			t.Fatalf("a backed-off lane leaked the next event: %+v, %v", next, err)
		}
	})
}

func TestInboxLeaseIsExclusiveAcrossWorkers(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		if _, err := store.PushInbox(ctx, "t1", "slack", []boardsync.Record{
			testRecord("m1", "C0123:T1"),
			testRecord("m2", "C0123:T2"),
		}); err != nil {
			t.Fatalf("push: %v", err)
		}
		var (
			mu     sync.Mutex
			leased = make(map[string]boardsync.InboxItem)
			wg     sync.WaitGroup
			start  = make(chan struct{})
		)
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				items, err := store.LeaseInbox(ctx, "t1", "slack", 10, "worker")
				if err != nil {
					return
				}
				mu.Lock()
				defer mu.Unlock()
				for _, item := range items {
					leased[item.ID] = item
				}
			}()
		}
		close(start)
		wg.Wait()
		if len(leased) != 2 {
			t.Fatalf("workers leased %d distinct items: %+v", len(leased), leased)
		}
		for _, item := range leased {
			if _, err := store.AckInbox(ctx, "t1", "slack", item.ID, item.Lease, nil); err != nil {
				t.Fatalf("ack %s: %v", item.ID, err)
			}
		}
	})
}

func TestInboxLeaseExpiryMemory(t *testing.T) {
	memory, err := syncstore.NewMemory(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	memory.WithClock(func() time.Time { return now })
	ctx := t.Context()
	if _, err := memory.PushInbox(ctx, "t1", "slack", []boardsync.Record{testRecord("m1", "C0123:T1")}); err != nil {
		t.Fatalf("push: %v", err)
	}
	first, err := memory.LeaseInbox(ctx, "t1", "slack", 1, "worker-a")
	if err != nil || len(first) != 1 {
		t.Fatalf("first lease = %+v, %v", first, err)
	}
	now = now.Add(2 * time.Minute)
	second, err := memory.LeaseInbox(ctx, "t1", "slack", 1, "worker-b")
	if err != nil || len(second) != 1 {
		t.Fatalf("second lease = %+v, %v", second, err)
	}
	if first[0].Lease == second[0].Lease {
		t.Fatal("the crashed worker's lease was not replaced")
	}
	if _, err := memory.AckInbox(ctx, "t1", "slack", "m1", first[0].Lease, nil); !errors.Is(err, syncstore.ErrLeaseLost) {
		t.Fatalf("stale ack = %v, want ErrLeaseLost", err)
	}
	if _, err := memory.AckInbox(ctx, "t1", "slack", "m1", second[0].Lease, nil); err != nil {
		t.Fatalf("live ack = %v", err)
	}
}

func TestInboxTenantIsolation(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		for _, tenant := range []string{"tenant-a", "tenant-b"} {
			if _, err := store.PushInbox(ctx, tenant, "slack", []boardsync.Record{testRecord("m1", "C0123:T1")}); err != nil {
				t.Fatalf("push %s: %v", tenant, err)
			}
		}
		a, err := store.LeaseInbox(ctx, "tenant-a", "slack", 10, "w")
		if err != nil || len(a) != 1 {
			t.Fatalf("lease tenant-a = %+v, %v", a, err)
		}
		b, err := store.LeaseInbox(ctx, "tenant-b", "slack", 10, "w")
		if err != nil || len(b) != 1 {
			t.Fatalf("lease tenant-b = %+v, %v", b, err)
		}
		if _, err := store.AckInbox(ctx, "tenant-a", "slack", "m1", b[0].Lease, nil); !errors.Is(err, syncstore.ErrLeaseLost) {
			t.Fatalf("tenant-a acked tenant-b's lease: %v", err)
		}
		if _, err := store.AckInbox(ctx, "tenant-a", "slack", "m1", a[0].Lease, nil); err != nil {
			t.Fatalf("ack tenant-a: %v", err)
		}
		if _, err := store.AckInbox(ctx, "tenant-b", "slack", "m1", b[0].Lease, nil); err != nil {
			t.Fatalf("ack tenant-b: %v", err)
		}
	})
}

func TestInboxRetryNamesOnlyDeadRows(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		if err := store.RetryInbox(ctx, "t1", "slack", "missing"); !errors.Is(err, syncstore.ErrNotDead) {
			t.Fatalf("retry missing = %v", err)
		}
		if _, err := store.PushInbox(ctx, "t1", "slack", []boardsync.Record{testRecord("m1", "C0123:T1")}); err != nil {
			t.Fatalf("push: %v", err)
		}
		if err := store.RetryInbox(ctx, "t1", "slack", "m1"); !errors.Is(err, syncstore.ErrNotDead) {
			t.Fatalf("retry pending = %v", err)
		}
	})
}
