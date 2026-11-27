package syncstore_test

import (
	"context"
	"errors"
	"testing"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/fake"
	"github.com/jixoai/keel-board/syncstore"
)

// TestWorkerHooksReportDeadLettersAndFailures proves the worker seam the
// server's metrics and audit use sees bounded facts: key, platform, attempts,
// and an error code, never a platform body.
func TestWorkerHooksReportDeadLettersAndFailures(t *testing.T) {
	t.Run("inbound", func(t *testing.T) {
		cfg := testConfig()
		cfg.InboundRetry = boardsync.RetryPolicy{MaxAttempts: 1, BaseDelay: 0, MaxDelay: 0, Jitter: 0}
		withStores(t, cfg, func(t *testing.T, store syncstore.Store) {
			ctx := t.Context()
			provider := fake.New("slack", boardsync.Capabilities{})
			registry := boardsync.NewRegistry()
			if err := registry.Add(boardsync.Entry{
				Tenant: "t1", Key: "slack", Platform: "slack", Provider: provider, Sync: boardsync.Mirror("support"),
			}); err != nil {
				t.Fatalf("register: %v", err)
			}
			host := fake.NewHost()
			host.ImportFunc = func(context.Context, boardsync.ImportRequest) (boardsync.ImportResult, error) {
				return boardsync.ImportResult{}, errors.New("token=SECRET body=hello")
			}
			if _, err := store.PushInbox(ctx, "t1", "slack", []boardsync.Record{testRecord("m1", "C0123:T1")}); err != nil {
				t.Fatalf("push: %v", err)
			}
			type dead struct {
				key, platform, id, code string
				attempts                int
			}
			var got []dead
			worker, err := syncstore.NewInboundWorker(syncstore.InboundWorkerConfig{
				Store: store, Registry: registry, Runner: boardsync.NewRunner(host),
				Tenant: "t1", Key: "slack", Owner: "worker", Limit: 1,
				Hooks: syncstore.WorkerHooks{
					InboxDead: func(_ string, key boardsync.Key, platform boardsync.Platform, id string, attempts int, code string) {
						got = append(got, dead{string(key), string(platform), id, code, attempts})
					},
				},
			})
			if err != nil {
				t.Fatalf("worker: %v", err)
			}
			result, err := worker.Process(ctx)
			if err != nil || result.Dead != 1 {
				t.Fatalf("pass = %+v, %v", result, err)
			}
			if len(got) != 1 {
				t.Fatalf("inbox dead hooks = %+v", got)
			}
			event := got[0]
			if event.key != "slack" || event.platform != "slack" || event.id != "m1" ||
				event.attempts != 1 || event.code != "failed" {
				t.Fatalf("inbox dead hook = %+v", event)
			}
		})
	})

	t.Run("outbound", func(t *testing.T) {
		cfg := testConfig()
		cfg.OutboundRetry = boardsync.RetryPolicy{MaxAttempts: 1, BaseDelay: 0, MaxDelay: 0, Jitter: 0}
		withStores(t, cfg, func(t *testing.T, store syncstore.Store) {
			ctx := t.Context()
			provider := fake.New("slack", boardsync.Capabilities{StartThreads: true})
			provider.PostFunc = func(context.Context, boardsync.Key, boardsync.ResolvedTarget, boardsync.OutboundMessage) (boardsync.Posted, error) {
				return boardsync.Posted{}, errors.New("token=SECRET body=hello")
			}
			registry := boardsync.NewRegistry()
			if err := registry.Add(boardsync.Entry{
				Tenant: "t1", Key: "slack", Platform: "slack", Provider: provider, Sync: boardsync.Mirror("support"),
			}); err != nil {
				t.Fatalf("register: %v", err)
			}
			host := fake.NewHost()
			target := threadTarget("C0123", "C0123:T1")
			if _, err := store.EnqueueOutbox(ctx, "t1", "slack",
				postedEvent(10, 100, 101), boardMessage(101, 100, "first"),
				[]boardsync.ResolvedTarget{target}); err != nil {
				t.Fatalf("enqueue: %v", err)
			}
			var deadCalls, failureCalls int
			var deadKey, deadPlatform, deadTarget, deadCode string
			var deadAttempts int
			var deadDelivery string
			var failureCode string
			worker, err := syncstore.NewOutboundWorker(syncstore.OutboundWorkerConfig{
				Store: store, Registry: registry, Runner: boardsync.NewRunner(host), Host: host,
				Tenant: "t1", Keys: []boardsync.Key{"slack"}, Owner: "worker",
				Hooks: syncstore.WorkerHooks{
					OutboxDead: func(_ string, key boardsync.Key, platform boardsync.Platform, delivery, target string, attempts int, code string) {
						deadCalls++
						deadKey, deadPlatform, deadDelivery = string(key), string(platform), delivery
						deadTarget, deadAttempts, deadCode = target, attempts, code
					},
					DeliveryFailed: func(_ string, _ boardsync.Key, _ boardsync.Platform, code string) {
						failureCalls++
						failureCode = code
					},
				},
			})
			if err != nil {
				t.Fatalf("worker: %v", err)
			}
			result, err := worker.Process(ctx)
			if err != nil || result.Dead != 1 {
				t.Fatalf("pass = %+v, %v", result, err)
			}
			if deadCalls != 1 || deadKey != "slack" || deadPlatform != "slack" ||
				deadTarget != target.External() || deadAttempts != 1 || deadCode != "failed" || deadDelivery == "" {
				t.Fatalf("outbox dead hook = %d calls, key=%q platform=%q target=%q attempts=%d code=%q delivery=%q",
					deadCalls, deadKey, deadPlatform, deadTarget, deadAttempts, deadCode, deadDelivery)
			}
			if failureCalls != 1 || failureCode != "failed" {
				t.Fatalf("delivery failure hook = %d calls, code=%q", failureCalls, failureCode)
			}
			if deadDelivery != "" && (len(deadDelivery) < 10 || deadDelivery[:9] != "delivery_") {
				t.Fatalf("dead delivery isn't a delivery ID: %q", deadDelivery)
			}
		})
	})
}
