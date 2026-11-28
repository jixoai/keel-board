package syncstore_test

import (
	"fmt"
	"testing"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/syncstore"
)

// TestTargetsLagAndFailures proves the read-only health facts against both
// stores: targets per connection, the oldest outstanding row, the 24h dead
// window, and the clear-on-success rule.
func TestTargetsLagAndFailures(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		seedEntry(t, store, "t1", "slack", true)
		if _, err := store.UpsertThread(ctx, syncstore.Thread{
			Tenant: "t1", BoardThread: 100, Key: "slack", Platform: "slack",
			Channel: "C0123", Thread: "C0123:T1", Kind: boardsync.TargetThread,
		}); err != nil {
			t.Fatalf("thread target: %v", err)
		}
		if _, err := store.UpsertThread(ctx, syncstore.Thread{
			Tenant: "t1", BoardThread: 200, Key: "slack", Platform: "slack",
			Channel: "C0999", Kind: boardsync.TargetChannel,
		}); err != nil {
			t.Fatalf("channel target: %v", err)
		}
		if _, err := store.PushInbox(ctx, "t1", "slack", []boardsync.Record{
			testRecord("m1", "C0123:T1"),
			testRecord("m2", "C0123:T1"),
		}); err != nil {
			t.Fatalf("push inbox: %v", err)
		}
		targetA := threadTarget("C0123", "C0123:T1")
		targetB := channelTarget("C0999")
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack", postedEvent(10, 100, 100),
			boardMessage(100, 100, "root"), []boardsync.ResolvedTarget{targetA}); err != nil {
			t.Fatalf("enqueue A: %v", err)
		}
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack", postedEvent(20, 200, 200),
			boardMessage(200, 200, "root"), []boardsync.ResolvedTarget{targetB}); err != nil {
			t.Fatalf("enqueue B: %v", err)
		}

		lag, err := store.OldestPending(ctx, "t1", "slack")
		if err != nil {
			t.Fatalf("oldest pending: %v", err)
		}
		if lag.Inbox == nil || lag.Inbox.Outstanding != 2 || lag.Inbox.OldestAt.IsZero() {
			t.Fatalf("inbox lag = %+v, want two outstanding rows with a timestamp", lag.Inbox)
		}
		if lag.Outbox == nil || lag.Outbox.Outstanding != 2 || lag.Outbox.OldestAt.IsZero() {
			t.Fatalf("outbox lag = %+v, want two outstanding rows with a timestamp", lag.Outbox)
		}

		targets, err := store.Targets(ctx, "t1", "slack", 10)
		if err != nil {
			t.Fatalf("targets: %v", err)
		}
		if len(targets) != 2 {
			t.Fatalf("targets = %+v, want two", targets)
		}
		byRoot := map[uint64]syncstore.TargetHealth{}
		for _, target := range targets {
			byRoot[target.BoardThread] = target
			if target.Target == "" || target.Status != syncstore.TargetStatusPending {
				t.Fatalf("target = %+v, want a pending external target", target)
			}
			if target.Undelivered != 1 || target.Pending != 1 {
				t.Fatalf("target %d queue = %+v, want one pending", target.BoardThread, target)
			}
		}
		if byRoot[100].Target != "slack:C0123:T1" || byRoot[200].Target != "slack:C0999" {
			t.Fatalf("external targets = %+v", byRoot)
		}

		if failures, err := store.Failures24h(ctx, "t1", "slack", time.Now().Add(-24*time.Hour)); err != nil || failures != 0 {
			t.Fatalf("failures24h before failure = %d, %v", failures, err)
		}

		// Drain both lanes; a target with nothing outstanding is synced and its
		// lag is nil, not zero.
		jobs, err := store.LeaseOutbox(ctx, "t1", []boardsync.Key{"slack"}, 10, "worker")
		if err != nil || len(jobs) != 2 {
			t.Fatalf("lease outbox = %+v, %v", jobs, err)
		}
		for _, job := range jobs {
			if _, err := store.AckOutbox(ctx, "t1", job.ID, job.Lease, nil, nil); err != nil {
				t.Fatalf("ack %s: %v", job.ID, err)
			}
		}
		targets, err = store.Targets(ctx, "t1", "slack", 10)
		if err != nil {
			t.Fatalf("targets after drain: %v", err)
		}
		for _, target := range targets {
			if target.Status != syncstore.TargetStatusSynced || target.Undelivered != 0 {
				t.Fatalf("drained target = %+v, want synced", target)
			}
		}
		lag, err = store.OldestPending(ctx, "t1", "slack")
		if err != nil {
			t.Fatalf("oldest pending after outbox drain: %v", err)
		}
		if lag.Outbox != nil {
			t.Fatalf("outbox lag after drain = %+v, want nil", lag.Outbox)
		}

		// A permanent failure dead-letters one delivery; the 24h count sees it.
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack", postedEvent(30, 100, 101),
			boardMessage(101, 100, "reply"), []boardsync.ResolvedTarget{targetA}); err != nil {
			t.Fatalf("enqueue doomed: %v", err)
		}
		doomed, err := store.LeaseOutbox(ctx, "t1", []boardsync.Key{"slack"}, 10, "worker")
		if err != nil || len(doomed) != 1 {
			t.Fatalf("lease doomed = %+v, %v", doomed, err)
		}
		if ack, err := store.AckOutbox(ctx, "t1", doomed[0].ID, doomed[0].Lease,
			boardsync.Permanent(fmt.Errorf("rate_limited")), nil); err != nil || !ack.Dead {
			t.Fatalf("dead letter = %+v, %v", ack, err)
		}
		if failures, err := store.Failures24h(ctx, "t1", "slack", time.Now().Add(-24*time.Hour)); err != nil || failures != 1 {
			t.Fatalf("failures24h after dead letter = %d, %v; want 1", failures, err)
		}
		if failures, err := store.Failures24h(ctx, "t1", "slack", time.Now().Add(time.Minute)); err != nil || failures != 0 {
			t.Fatalf("failures24h in the future = %d, %v; want 0", failures, err)
		}
		targets, err = store.Targets(ctx, "t1", "slack", 10)
		if err != nil {
			t.Fatalf("targets after dead letter: %v", err)
		}
		failed := syncstore.TargetHealth{}
		for _, target := range targets {
			if target.BoardThread == 100 {
				failed = target
			}
		}
		if failed.Status != syncstore.TargetStatusFailed || failed.Dead != 1 || failed.LastError == "" {
			t.Fatalf("failed target = %+v", failed)
		}

		// A later success to the same target clears the dead row.
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack", postedEvent(40, 100, 102),
			boardMessage(102, 100, "recovered"), []boardsync.ResolvedTarget{targetA}); err != nil {
			t.Fatalf("enqueue recovery: %v", err)
		}
		recovery, err := store.LeaseOutbox(ctx, "t1", []boardsync.Key{"slack"}, 10, "worker")
		if err != nil || len(recovery) != 1 {
			t.Fatalf("lease recovery = %+v, %v", recovery, err)
		}
		if _, err := store.AckOutbox(ctx, "t1", recovery[0].ID, recovery[0].Lease, nil, nil); err != nil {
			t.Fatalf("ack recovery: %v", err)
		}
		if failures, err := store.Failures24h(ctx, "t1", "slack", time.Now().Add(-24*time.Hour)); err != nil || failures != 0 {
			t.Fatalf("failures24h after recovery = %d, %v; want 0", failures, err)
		}

		// Drain the inbox; its lag also becomes nil. Both records share one
		// external lane, so the store hands them out one at a time.
		for drained := 0; drained < 2; drained++ {
			items, err := store.LeaseInbox(ctx, "t1", "slack", 10, "worker")
			if err != nil || len(items) != 1 {
				t.Fatalf("lease inbox = %+v, %v", items, err)
			}
			if _, err := store.AckInbox(ctx, "t1", "slack", items[0].ID, items[0].Lease, nil); err != nil {
				t.Fatalf("ack inbox %s: %v", items[0].ID, err)
			}
		}
		lag, err = store.OldestPending(ctx, "t1", "slack")
		if err != nil {
			t.Fatalf("oldest pending after inbox drain: %v", err)
		}
		if lag.Inbox != nil {
			t.Fatalf("inbox lag after drain = %+v, want nil", lag.Inbox)
		}

		// Another tenant and another key see nothing.
		if other, err := store.Targets(ctx, "t2", "slack", 10); err != nil || len(other) != 0 {
			t.Fatalf("cross-tenant targets = %+v, %v", other, err)
		}
		if other, err := store.Targets(ctx, "t1", "github", 10); err != nil || len(other) != 0 {
			t.Fatalf("cross-key targets = %+v, %v", other, err)
		}
	})
}

func TestOldestPendingEmptyIsNil(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		lag, err := store.OldestPending(t.Context(), "t1", "slack")
		if err != nil {
			t.Fatalf("oldest pending: %v", err)
		}
		if lag.Inbox != nil || lag.Outbox != nil {
			t.Fatalf("empty lag = %+v, want both nil", lag)
		}
	})
}
