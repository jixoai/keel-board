package automation_test

import (
	"errors"
	"testing"

	"github.com/jixoai/keel-board/automation"
)

func TestWatcherStats(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store automation.Store) {
		ctx := t.Context()
		if _, err := store.CreateWatcher(ctx, testWatcher("alerts")); err != nil {
			t.Fatalf("create: %v", err)
		}
		reader, ok := store.(automation.StatsReader)
		if !ok {
			t.Fatal("store doesn't implement StatsReader")
		}
		if stats, err := reader.WatcherStats(ctx, "t1", "alerts"); err != nil || stats.Dead != 0 || stats.LastError != "" {
			t.Fatalf("empty stats = %+v, %v", stats, err)
		}
		if _, err := store.AdvanceWatcher(ctx, "t1", "alerts", 4, []uint64{2, 4}); err != nil {
			t.Fatalf("advance: %v", err)
		}
		leased, err := store.LeaseDeliveries(ctx, "t1", 10, "worker")
		if err != nil || len(leased) != 1 || leased[0].EventSeq != 2 {
			t.Fatalf("lease = %+v, %v", leased, err)
		}
		for attempt := 1; attempt <= 3; attempt++ {
			ack, err := store.AckDelivery(ctx, "t1", "alerts", leased[0].EventSeq, leased[0].Lease, errors.New("rate_limited"))
			if err != nil {
				t.Fatalf("ack %d: %v", attempt, err)
			}
			if ack.Dead {
				break
			}
			leased, err = store.LeaseDeliveries(ctx, "t1", 10, "worker")
			if err != nil || len(leased) != 1 {
				t.Fatalf("retry lease %d = %+v, %v", attempt, leased, err)
			}
		}
		stats, err := reader.WatcherStats(ctx, "t1", "alerts")
		if err != nil {
			t.Fatalf("stats: %v", err)
		}
		if stats.Dead != 1 {
			t.Fatalf("stats dead = %+v, want 1", stats)
		}
		if stats.LastError == "" {
			t.Fatalf("stats lost the error code: %+v", stats)
		}
		if stats.LastDeliveryAt.IsZero() {
			t.Fatalf("stats last delivery = %+v", stats)
		}
		// Another tenant sees nothing.
		if other, err := reader.WatcherStats(ctx, "t2", "alerts"); err != nil || other.Dead != 0 || other.LastError != "" {
			t.Fatalf("cross-tenant stats = %+v, %v", other, err)
		}
	})
}
