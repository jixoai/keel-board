package syncstore_test

import (
	"context"
	"errors"
	"testing"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/syncstore"
)

// TestReceiptRetryKeepsSubthread covers a crash between the platform post and
// the first receipt. The copy's target carries the review sub-thread; the
// retried message.synced must reconstruct it, or a host would later edit or
// delete the review comment on the issue-comment endpoint.
func TestReceiptRetryKeepsSubthread(t *testing.T) {
	cfg := testConfig()
	withStores(t, cfg, func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		_, host, worker := outboundFixture(t, store, cfg)
		target := threadTarget("C0123", "C0123:T1")
		target.Subthread = "C0123:T1:rc:9"
		receiptFailures := 0
		host.RecordFunc = func(_ context.Context, event boardsync.BoardEvent) error {
			if event.Type == boardsync.BoardMessageSynced && receiptFailures == 0 {
				receiptFailures++
				return errors.New("board log unavailable")
			}
			return nil
		}
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack",
			postedEvent(10, 100, 101), boardMessage(101, 100, "root"),
			[]boardsync.ResolvedTarget{target}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		result, err := worker.Process(ctx)
		if err != nil || result.Retried != 1 {
			t.Fatalf("receipt-failure pass = %+v, %v", result, err)
		}
		copy, found, err := store.Copy(ctx, "t1", "slack", target, 101)
		if err != nil || !found {
			t.Fatalf("copy after failure = %+v, %v, %v", copy, found, err)
		}
		if copy.Target.Subthread != target.Subthread {
			t.Fatalf("copy target lost subthread: %+v", copy.Target)
		}
		result, err = worker.Process(ctx)
		if err != nil || result.Skipped != 1 || result.Acked != 1 {
			t.Fatalf("receipt-retry pass = %+v, %v", result, err)
		}
		var synced []boardsync.BoardEvent
		for _, record := range host.Records() {
			if record.Type == boardsync.BoardMessageSynced {
				synced = append(synced, record)
			}
		}
		if len(synced) != 2 {
			t.Fatalf("message.synced records = %d, want the failed one and its retry", len(synced))
		}
		last := synced[len(synced)-1]
		if last.External == nil {
			t.Fatal("retried receipt has no external thread")
		}
		if last.External.Thread != "C0123:T1" || last.External.Subthread != "C0123:T1:rc:9" {
			t.Fatalf("retried receipt lost its subthread: %+v", last.External)
		}
	})
}
