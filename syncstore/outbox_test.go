package syncstore_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/fake"
	"github.com/jixoai/keel-board/syncstore"
)

func postedEvent(id, thread, message uint64) boardsync.BoardEvent {
	return boardsync.BoardEvent{ID: id, Type: boardsync.BoardMessagePosted, Thread: thread, Message: &boardsync.Message{ID: message}}
}

func outboundFixture(t *testing.T, store syncstore.Store, cfg syncstore.Config) (*fake.Provider, *fake.Host, *syncstore.OutboundWorker) {
	t.Helper()
	return outboundFixtureWithRenderer(t, store, cfg, nil)
}

func outboundFixtureWithRenderer(t *testing.T, store syncstore.Store, cfg syncstore.Config, renderer boardsync.Renderer) (*fake.Provider, *fake.Host, *syncstore.OutboundWorker) {
	t.Helper()
	provider := fake.New("slack", boardsync.Capabilities{
		StartThreads: true, Edits: true, Deletes: true, Reactions: true,
	})
	registry := boardsync.NewRegistry()
	if err := registry.Add(boardsync.Entry{Tenant: "t1", Key: "slack", Platform: "slack", Provider: provider, Sync: boardsync.Mirror("support")}); err != nil {
		t.Fatalf("register entry: %v", err)
	}
	host := fake.NewHost()
	runner := boardsync.NewRunner(host)
	worker, err := syncstore.NewOutboundWorker(syncstore.OutboundWorkerConfig{
		Store: store, Registry: registry, Runner: runner, Host: host,
		Tenant: "t1", Keys: []boardsync.Key{"slack"}, Renderer: renderer,
		Owner: "worker", Limit: 10,
	})
	if err != nil {
		t.Fatalf("new outbound worker: %v", err)
	}
	return provider, host, worker
}

func TestOutboxDeterministicAndLaneOrder(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		targetA := threadTarget("C0123", "C0123:T1")
		targetB := threadTarget("C0999", "C0999:T9")
		event := postedEvent(10, 100, 100)
		message := boardMessage(100, 100, "root")
		enqueued, err := store.EnqueueOutbox(ctx, "t1", "slack", event, message, []boardsync.ResolvedTarget{targetA, targetB})
		if err != nil || enqueued.Added != 2 || enqueued.Skipped != 0 {
			t.Fatalf("enqueue = %+v, %v", enqueued, err)
		}
		again, err := store.EnqueueOutbox(ctx, "t1", "slack", event, message, []boardsync.ResolvedTarget{targetA, targetB})
		if err != nil || again.Added != 0 || again.Skipped != 2 {
			t.Fatalf("duplicate enqueue = %+v, %v", again, err)
		}
		if want := syncstore.DeliveryID("t1", "slack", event, targetA); want == "" {
			t.Fatal("delivery ID is empty")
		}
		// A second board thread is a second lane and may run in parallel.
		event2 := postedEvent(20, 200, 200)
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack", event2,
			boardMessage(200, 200, "other"), []boardsync.ResolvedTarget{threadTarget("C0777", "C0777:T7")}); err != nil {
			t.Fatalf("enqueue second lane: %v", err)
		}
		first, err := store.LeaseOutbox(ctx, "t1", []boardsync.Key{"slack"}, 10, "worker-a")
		if err != nil || len(first) != 2 {
			t.Fatalf("first lease = %+v, %v; want one job per lane", first, err)
		}
		lanes := map[string]bool{}
		for _, job := range first {
			lanes[job.Lane] = true
			if job.Lane == "100" && job.Target.Channel != targetA.Channel {
				t.Fatalf("lane 100's first job = %+v, want target %q", job.Target, targetA.Channel)
			}
		}
		if !lanes["100"] || !lanes["200"] {
			t.Fatalf("leased lanes = %+v", lanes)
		}
		if again, err := store.LeaseOutbox(ctx, "t1", []boardsync.Key{"slack"}, 10, "worker-b"); err != nil || len(again) != 0 {
			t.Fatalf("second lease = %+v, %v; a lane was leased twice", again, err)
		}
		for _, job := range first {
			if _, err := store.AckOutbox(ctx, "t1", job.ID, job.Lease, nil, nil); err != nil {
				t.Fatalf("ack %s: %v", job.ID, err)
			}
		}
		// The second target on lane 100 is next; the other lane has no job.
		next, err := store.LeaseOutbox(ctx, "t1", []boardsync.Key{"slack"}, 10, "worker-a")
		if err != nil || len(next) != 1 || next[0].Lane != "100" || next[0].Target.Channel != targetB.Channel {
			t.Fatalf("next lease = %+v, %v", next, err)
		}
		if _, err := store.AckOutbox(ctx, "t1", next[0].ID, next[0].Lease, nil, nil); err != nil {
			t.Fatalf("ack second target: %v", err)
		}
		if pushed, err := store.EnqueueOutbox(ctx, "t1", "slack", event, message, []boardsync.ResolvedTarget{targetA}); err != nil || pushed.Added != 0 {
			t.Fatalf("a landed delivery was queued again: %+v, %v", pushed, err)
		}
		depth, err := store.OutboxDepth(ctx, "t1")
		if err != nil || depth.Done != 3 || depth.Pending != 0 {
			t.Fatalf("depth = %+v, %v", depth, err)
		}
	})
}

func TestOutboxPostCopyEditDeleteReact(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		provider, _, worker := outboundFixture(t, store, testConfig())
		provider.PostFunc = func(_ context.Context, _ boardsync.Key, target boardsync.ResolvedTarget, msg boardsync.OutboundMessage) (boardsync.Posted, error) {
			thread := target.Thread
			if target.Kind == boardsync.TargetChannel {
				thread = target.Channel + ":root"
			}
			return boardsync.Posted{Thread: thread, Message: fmt.Sprintf("posted-%d", msg.BoardMessage)}, nil
		}
		target := threadTarget("C0123", "C0123:T1")
		message := boardMessage(101, 101, "ship it")
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack",
			postedEvent(10, 101, 101), message, []boardsync.ResolvedTarget{target}); err != nil {
			t.Fatalf("enqueue post: %v", err)
		}
		if result, err := worker.Process(ctx); err != nil || result.Dead != 0 || result.Acked != 1 {
			t.Fatalf("post pass = %+v, %v", result, err)
		}
		copy, found, err := store.Copy(ctx, "t1", "slack", target, 101)
		if err != nil || !found || copy.ExternalMessage != "posted-101" {
			t.Fatalf("copy = %+v, %v, %v", copy, found, err)
		}
		edited := boardMessage(101, 101, "ship it now")
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack",
			boardsync.BoardEvent{ID: 11, Type: boardsync.BoardMessageEdited, Thread: 101}, edited,
			[]boardsync.ResolvedTarget{target}); err != nil {
			t.Fatalf("enqueue edit: %v", err)
		}
		if result, err := worker.Process(ctx); err != nil || result.Dead != 0 {
			t.Fatalf("edit pass = %+v, %v", result, err)
		}
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack",
			boardsync.BoardEvent{ID: 12, Type: boardsync.BoardReactionAdded, Thread: 101, Emoji: "thumbsup"}, edited,
			[]boardsync.ResolvedTarget{target}); err != nil {
			t.Fatalf("enqueue reaction: %v", err)
		}
		if result, err := worker.Process(ctx); err != nil || result.Dead != 0 {
			t.Fatalf("reaction pass = %+v, %v", result, err)
		}
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack",
			boardsync.BoardEvent{ID: 13, Type: boardsync.BoardMessageDeleted, Thread: 101}, edited,
			[]boardsync.ResolvedTarget{target}); err != nil {
			t.Fatalf("enqueue delete: %v", err)
		}
		if result, err := worker.Process(ctx); err != nil || result.Dead != 0 {
			t.Fatalf("delete pass = %+v, %v", result, err)
		}
		if posts := provider.Posts(); len(posts) != 1 {
			t.Fatalf("posts = %d, want one", len(posts))
		}
		if edits := provider.Edits(); len(edits) != 1 || edits[0].Message != "posted-101" {
			t.Fatalf("edits = %+v", edits)
		}
		if deletes := provider.Deletes(); len(deletes) != 1 || deletes[0].Message != "posted-101" {
			t.Fatalf("deletes = %+v", deletes)
		}
		if reactions := provider.Reactions(); len(reactions) != 1 || reactions[0].Message != "posted-101" || !reactions[0].Added {
			t.Fatalf("reactions = %+v", reactions)
		}
		if depth, err := store.OutboxDepth(ctx, "t1"); err != nil || depth.Done != 4 || depth.Dead != 0 {
			t.Fatalf("depth = %+v, %v", depth, err)
		}
	})
}

func TestOutboxCopyFallsBackFromChannelToThread(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		provider, _, worker := outboundFixture(t, store, testConfig())
		provider.PostFunc = func(_ context.Context, _ boardsync.Key, target boardsync.ResolvedTarget, msg boardsync.OutboundMessage) (boardsync.Posted, error) {
			return boardsync.Posted{Thread: "C0123:T9", Message: fmt.Sprintf("posted-%d", msg.BoardMessage)}, nil
		}
		channel := channelTarget("C0123")
		root := boardMessage(101, 101, "root")
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack",
			boardsync.BoardEvent{ID: 10, Type: boardsync.BoardThreadSynced, Thread: 101}, root,
			[]boardsync.ResolvedTarget{channel}); err != nil {
			t.Fatalf("enqueue root: %v", err)
		}
		if result, err := worker.Process(ctx); err != nil || result.Dead != 0 {
			t.Fatalf("root pass = %+v, %v", result, err)
		}
		if copy, found, err := store.Copy(ctx, "t1", "slack", channel, 101); err != nil || !found || copy.ExternalThread != "C0123:T9" {
			t.Fatalf("channel copy = %+v, %v, %v", copy, found, err)
		}
		target := threadTarget("C0123", "C0123:T9")
		if _, found, err := store.Copy(ctx, "t1", "slack", target, 101); err != nil || found {
			t.Fatalf("exact thread copy should be absent: %v, %v", found, err)
		}
		edited := boardMessage(101, 101, "edited root")
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack",
			boardsync.BoardEvent{ID: 11, Type: boardsync.BoardMessageEdited, Thread: 101}, edited,
			[]boardsync.ResolvedTarget{target}); err != nil {
			t.Fatalf("enqueue edit: %v", err)
		}
		if result, err := worker.Process(ctx); err != nil || result.Dead != 0 {
			t.Fatalf("edit pass = %+v, %v", result, err)
		}
		if edits := provider.Edits(); len(edits) != 1 || edits[0].Message != "posted-101" {
			t.Fatalf("edit used the wrong copy: %+v", edits)
		}
	})
}

func TestOutboxFailureDeadThenContinueAndClear(t *testing.T) {
	cfg := testConfig()
	cfg.OutboundRetry = boardsync.RetryPolicy{MaxAttempts: 1, BaseDelay: 0, MaxDelay: 0, Jitter: 0}
	withStores(t, cfg, func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		provider, host, worker := outboundFixture(t, store, cfg)
		calls := 0
		provider.PostFunc = func(_ context.Context, _ boardsync.Key, target boardsync.ResolvedTarget, msg boardsync.OutboundMessage) (boardsync.Posted, error) {
			calls++
			if calls == 1 {
				return boardsync.Posted{}, errors.New("token=SECRET body=ship it")
			}
			return boardsync.Posted{Thread: target.Thread, Message: fmt.Sprintf("posted-%d", msg.BoardMessage)}, nil
		}
		target := threadTarget("C0123", "C0123:T1")
		first := postedEvent(10, 100, 101)
		second := postedEvent(20, 100, 102)
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack", first, boardMessage(101, 100, "first"), []boardsync.ResolvedTarget{target}); err != nil {
			t.Fatalf("enqueue first: %v", err)
		}
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack", second, boardMessage(102, 100, "second"), []boardsync.ResolvedTarget{target}); err != nil {
			t.Fatalf("enqueue second: %v", err)
		}
		result, err := worker.Process(ctx)
		if err != nil || result.Dead != 1 {
			t.Fatalf("first pass = %+v, %v", result, err)
		}
		dead, err := store.DeadOutbox(ctx, "t1", "slack", 10)
		if err != nil || len(dead) != 1 || dead[0].Attempts != 1 || dead[0].LastError != "failed" {
			t.Fatalf("dead = %+v, %v", dead, err)
		}
		if strings.Contains(dead[0].LastError, "SECRET") || strings.Contains(dead[0].LastError, "ship it") {
			t.Fatalf("dead delivery leaked content: %q", dead[0].LastError)
		}
		failures, err := store.TargetFailures(ctx, "t1", "slack", 10)
		if err != nil || len(failures) != 1 {
			t.Fatalf("target failures = %+v, %v", failures, err)
		}
		records := host.Records()
		if len(records) != 1 || records[0].Type != boardsync.BoardMessageSyncFailed || records[0].Error != "failed" {
			t.Fatalf("failure receipt = %+v", records)
		}
		if strings.Contains(records[0].Error, "SECRET") || strings.Contains(records[0].Error, "ship it") {
			t.Fatalf("failure receipt leaked content: %+v", records[0])
		}
		// The dead delivery doesn't block the next message on its lane.
		result, err = worker.Process(ctx)
		if err != nil || result.Acked != 1 || result.Dead != 0 {
			t.Fatalf("second pass = %+v, %v", result, err)
		}
		failures, err = store.TargetFailures(ctx, "t1", "slack", 10)
		if err != nil || len(failures) != 0 {
			t.Fatalf("failure survived a later success: %+v, %v", failures, err)
		}
		if copy, found, err := store.Copy(ctx, "t1", "slack", target, 102); err != nil || !found || copy.ExternalMessage != "posted-102" {
			t.Fatalf("second copy = %+v, %v, %v", copy, found, err)
		}
		depth, err := store.OutboxDepth(ctx, "t1")
		if err != nil || depth.Dead != 1 || depth.Done != 1 {
			t.Fatalf("depth = %+v, %v", depth, err)
		}
		if posts := provider.Posts(); len(posts) != 2 {
			t.Fatalf("posts = %d, want two attempts", len(posts))
		}
		synced := 0
		for _, record := range host.Records() {
			if record.Type == boardsync.BoardMessageSynced {
				synced++
			}
		}
		if synced != 1 {
			t.Fatalf("message.synced receipts = %d, want one", synced)
		}
	})
}

func TestOutboxMissingCopyIsSkippedAndDoesNotBlock(t *testing.T) {
	cfg := testConfig()
	withStores(t, cfg, func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		provider, host, worker := outboundFixture(t, store, cfg)
		provider.PostFunc = func(_ context.Context, _ boardsync.Key, target boardsync.ResolvedTarget, msg boardsync.OutboundMessage) (boardsync.Posted, error) {
			return boardsync.Posted{Thread: target.Thread, Message: fmt.Sprintf("posted-%d", msg.BoardMessage)}, nil
		}
		target := threadTarget("C0123", "C0123:T1")
		edit := boardMessage(101, 100, "no copy")
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack",
			boardsync.BoardEvent{ID: 10, Type: boardsync.BoardMessageEdited, Thread: 100}, edit,
			[]boardsync.ResolvedTarget{target}); err != nil {
			t.Fatalf("enqueue edit: %v", err)
		}
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack",
			postedEvent(20, 100, 102), boardMessage(102, 100, "later"), []boardsync.ResolvedTarget{target}); err != nil {
			t.Fatalf("enqueue post: %v", err)
		}
		result, err := worker.Process(ctx)
		if err != nil || result.Acked != 1 || result.Skipped != 1 || result.Dead != 0 {
			t.Fatalf("edit pass = %+v, %v", result, err)
		}
		if edits := provider.Edits(); len(edits) != 0 {
			t.Fatalf("an edit without a copy reached the provider: %+v", edits)
		}
		if records := host.Records(); len(records) != 0 {
			t.Fatalf("a best-effort edit wrote a failure receipt: %+v", records)
		}
		result, err = worker.Process(ctx)
		if err != nil || result.Acked != 1 {
			t.Fatalf("later pass = %+v, %v", result, err)
		}
		if posts := provider.Posts(); len(posts) != 1 {
			t.Fatalf("posts = %d, want the later message", len(posts))
		}
	})
}

func TestOutboxLeaseExclusiveAcrossWorkers(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		for i, thread := range []uint64{100, 200} {
			event := postedEvent(uint64(10+i), thread, thread)
			if _, err := store.EnqueueOutbox(ctx, "t1", "slack", event,
				boardMessage(thread, thread, "root"), []boardsync.ResolvedTarget{threadTarget("C0123", fmt.Sprintf("C0123:T%d", thread))}); err != nil {
				t.Fatalf("enqueue lane %d: %v", thread, err)
			}
		}
		var (
			mu     sync.Mutex
			leased = make(map[string]syncstore.Delivery)
			wg     sync.WaitGroup
			start  = make(chan struct{})
			errs   = make(chan error, 8)
		)
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				jobs, err := store.LeaseOutbox(ctx, "t1", []boardsync.Key{"slack"}, 10, "worker")
				if err != nil {
					errs <- err
					return
				}
				mu.Lock()
				defer mu.Unlock()
				for _, job := range jobs {
					leased[job.ID] = job
				}
			}()
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("concurrent lease: %v", err)
		}
		if len(leased) != 2 {
			t.Fatalf("workers leased %d distinct deliveries: %+v", len(leased), leased)
		}
		for _, job := range leased {
			if _, err := store.AckOutbox(ctx, "t1", job.ID, job.Lease, nil, nil); err != nil {
				t.Fatalf("ack %s: %v", job.ID, err)
			}
		}
	})
}

func TestOutboxLeaseExpiryMemory(t *testing.T) {
	memory, err := syncstore.NewMemory(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	memory.WithClock(func() time.Time { return now })
	ctx := t.Context()
	event := postedEvent(10, 100, 101)
	if _, err := memory.EnqueueOutbox(ctx, "t1", "slack", event, boardMessage(101, 100, "root"),
		[]boardsync.ResolvedTarget{threadTarget("C0123", "C0123:T1")}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	first, err := memory.LeaseOutbox(ctx, "t1", []boardsync.Key{"slack"}, 1, "worker-a")
	if err != nil || len(first) != 1 {
		t.Fatalf("first lease = %+v, %v", first, err)
	}
	now = now.Add(2 * time.Minute)
	second, err := memory.LeaseOutbox(ctx, "t1", []boardsync.Key{"slack"}, 1, "worker-b")
	if err != nil || len(second) != 1 {
		t.Fatalf("second lease = %+v, %v", second, err)
	}
	if first[0].Lease == second[0].Lease {
		t.Fatal("the crashed worker's lease was not replaced")
	}
	if _, err := memory.AckOutbox(ctx, "t1", first[0].ID, first[0].Lease, nil, nil); !errors.Is(err, syncstore.ErrLeaseLost) {
		t.Fatalf("stale ack = %v, want ErrLeaseLost", err)
	}
	if _, err := memory.AckOutbox(ctx, "t1", second[0].ID, second[0].Lease, nil, nil); err != nil {
		t.Fatalf("live ack = %v", err)
	}
}

func TestOutboxTenantIsolation(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		target := threadTarget("C0123", "C0123:T1")
		for _, tenant := range []string{"tenant-a", "tenant-b"} {
			event := postedEvent(10, 100, 101)
			if _, err := store.EnqueueOutbox(ctx, tenant, "slack", event,
				boardMessage(101, 100, "root"), []boardsync.ResolvedTarget{target}); err != nil {
				t.Fatalf("enqueue %s: %v", tenant, err)
			}
		}
		a, err := store.LeaseOutbox(ctx, "tenant-a", []boardsync.Key{"slack"}, 10, "w")
		if err != nil || len(a) != 1 {
			t.Fatalf("lease tenant-a = %+v, %v", a, err)
		}
		b, err := store.LeaseOutbox(ctx, "tenant-b", []boardsync.Key{"slack"}, 10, "w")
		if err != nil || len(b) != 1 {
			t.Fatalf("lease tenant-b = %+v, %v", b, err)
		}
		if _, err := store.AckOutbox(ctx, "tenant-a", a[0].ID, b[0].Lease, nil, nil); !errors.Is(err, syncstore.ErrLeaseLost) {
			t.Fatalf("cross-tenant ack = %v", err)
		}
		if _, err := store.AckOutbox(ctx, "tenant-a", a[0].ID, a[0].Lease, nil, nil); err != nil {
			t.Fatalf("ack tenant-a: %v", err)
		}
		if _, err := store.AckOutbox(ctx, "tenant-b", b[0].ID, b[0].Lease, nil, nil); err != nil {
			t.Fatalf("ack tenant-b: %v", err)
		}
	})
}

func TestOutboxRejectsUndeliverableFacts(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		target := threadTarget("C0123", "C0123:T1")
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack",
			boardsync.BoardEvent{Type: boardsync.BoardMessageSynced, ID: 1, Thread: 100},
			boardMessage(101, 100, "root"), []boardsync.ResolvedTarget{target}); !errors.Is(err, boardsync.ErrBadRequest) {
			t.Fatalf("receipt enqueue = %v", err)
		}
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack",
			boardsync.BoardEvent{Type: boardsync.BoardMessagePosted, Thread: 100},
			boardMessage(101, 100, "root"), []boardsync.ResolvedTarget{target}); !errors.Is(err, boardsync.ErrBadRequest) {
			t.Fatalf("event without an ID = %v", err)
		}
		noSync := boardMessage(101, 100, "root")
		noSync.NoSync = true
		if pushed, err := store.EnqueueOutbox(ctx, "t1", "slack",
			postedEvent(10, 100, 101), noSync, []boardsync.ResolvedTarget{target}); err != nil || pushed.Added != 0 {
			t.Fatalf("noSync enqueue = %+v, %v", pushed, err)
		}
		origin := boardMessage(102, 100, "came from slack")
		origin.From = "slack"
		if pushed, err := store.EnqueueOutbox(ctx, "t1", "slack",
			postedEvent(20, 100, 102), origin, []boardsync.ResolvedTarget{target}); err != nil || pushed.Added != 0 {
			t.Fatalf("origin enqueue = %+v, %v", pushed, err)
		}
		noSyncEvent := postedEvent(30, 100, 103)
		noSyncEvent.NoSync = true
		if pushed, err := store.EnqueueOutbox(ctx, "t1", "slack",
			noSyncEvent, boardMessage(103, 100, "event noSync"), []boardsync.ResolvedTarget{target}); err != nil || pushed.Added != 0 {
			t.Fatalf("event noSync enqueue = %+v, %v", pushed, err)
		}
	})
}

func TestOutboxCopyIsDurableBeforeReceipt(t *testing.T) {
	cfg := testConfig()
	cfg.OutboundRetry = boardsync.RetryPolicy{MaxAttempts: 3, BaseDelay: 0, MaxDelay: 0, Jitter: 0}
	withStores(t, cfg, func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		provider, host, worker := outboundFixture(t, store, cfg)
		posts := 0
		provider.PostFunc = func(_ context.Context, _ boardsync.Key, target boardsync.ResolvedTarget, msg boardsync.OutboundMessage) (boardsync.Posted, error) {
			posts++
			return boardsync.Posted{Thread: target.Thread, Message: "posted-once"}, nil
		}
		receiptFailures := 0
		host.RecordFunc = func(_ context.Context, event boardsync.BoardEvent) error {
			if event.Type == boardsync.BoardMessageSynced && receiptFailures == 0 {
				receiptFailures++
				return errors.New("board log unavailable")
			}
			return nil
		}
		target := threadTarget("C0123", "C0123:T1")
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack",
			postedEvent(10, 100, 101), boardMessage(101, 100, "root"), []boardsync.ResolvedTarget{target}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		result, err := worker.Process(ctx)
		if err != nil || result.Retried != 1 {
			t.Fatalf("receipt-failure pass = %+v, %v", result, err)
		}
		if posts != 1 {
			t.Fatalf("posts after receipt failure = %d, want one", posts)
		}
		if copy, found, err := store.Copy(ctx, "t1", "slack", target, 101); err != nil || !found || copy.ExternalMessage != "posted-once" {
			t.Fatalf("copy before receipt retry = %+v, %v, %v", copy, found, err)
		}
		result, err = worker.Process(ctx)
		if err != nil || result.Skipped != 1 || result.Acked != 1 {
			t.Fatalf("receipt-retry pass = %+v, %v", result, err)
		}
		if posts != 1 {
			t.Fatalf("receipt retry posted again: %d", posts)
		}
	})
}

func TestOutboxLabelsChangedLabelerWinsOverRenderer(t *testing.T) {
	renderer := func(in boardsync.RenderInput) (string, error) {
		return "rendered:" + in.Message.Text, nil
	}
	cfg := testConfig()
	withStores(t, cfg, func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		provider, _, worker := outboundFixtureWithRenderer(t, store, cfg, renderer)
		provider.PostFunc = func(_ context.Context, _ boardsync.Key, target boardsync.ResolvedTarget, msg boardsync.OutboundMessage) (boardsync.Posted, error) {
			return boardsync.Posted{Thread: target.Thread, Message: "posted-101"}, nil
		}
		target := threadTarget("C0123", "C0123:T1")
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack",
			postedEvent(10, 101, 101), boardMessage(101, 101, "root"), []boardsync.ResolvedTarget{target}); err != nil {
			t.Fatalf("enqueue post: %v", err)
		}
		if result, err := worker.Process(ctx); err != nil || result.Acked != 1 {
			t.Fatalf("post pass = %+v, %v", result, err)
		}
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack",
			boardsync.BoardEvent{ID: 11, Type: boardsync.BoardLabelsChanged, Thread: 101},
			boardMessage(101, 101, "root"), []boardsync.ResolvedTarget{target}); err != nil {
			t.Fatalf("enqueue labels: %v", err)
		}
		result, err := worker.Process(ctx)
		if err != nil || result.Dead != 0 || result.Acked != 1 {
			t.Fatalf("labels pass = %+v, %v", result, err)
		}
		if edits := provider.Edits(); len(edits) != 0 {
			t.Fatalf("renderer overwrote the issue: %+v", edits)
		}
		if calls := provider.LabelsCalls(); len(calls) != 1 || calls[0].Thread.External() != "slack:C0123:T1" {
			t.Fatalf("label calls = %+v", calls)
		}
	})
}

func TestOutboxLabelsChangedChannelRootReachesLabeler(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		provider, _, worker := outboundFixture(t, store, testConfig())
		provider.PostFunc = func(_ context.Context, _ boardsync.Key, target boardsync.ResolvedTarget, msg boardsync.OutboundMessage) (boardsync.Posted, error) {
			thread := target.Thread
			if target.Kind == boardsync.TargetChannel {
				thread = target.Channel + ":root"
			}
			return boardsync.Posted{Thread: thread, Message: "posted-101"}, nil
		}
		target := channelTarget("C0123")
		root := boardMessage(101, 101, "root")
		root.Labels = map[string]string{"state": "started"}
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack",
			postedEvent(10, 101, 101), root, []boardsync.ResolvedTarget{target}); err != nil {
			t.Fatalf("enqueue post: %v", err)
		}
		if result, err := worker.Process(ctx); err != nil || result.Acked != 1 {
			t.Fatalf("post pass = %+v, %v", result, err)
		}
		changed := root
		changed.Labels = map[string]string{"state": "completed"}
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack",
			boardsync.BoardEvent{ID: 11, Type: boardsync.BoardLabelsChanged, Thread: 101,
				Before: &boardsync.Message{ID: 101, Thread: 101, Labels: root.Labels}},
			changed, []boardsync.ResolvedTarget{target}); err != nil {
			t.Fatalf("enqueue labels: %v", err)
		}
		result, err := worker.Process(ctx)
		if err != nil || result.Dead != 0 || result.Acked != 1 {
			t.Fatalf("labels pass = %+v, %v", result, err)
		}
		calls := provider.LabelsCalls()
		if len(calls) != 1 {
			t.Fatalf("label calls = %+v", calls)
		}
		if calls[0].Thread.External() != "slack:C0123:root" {
			t.Fatalf("channel root thread = %+v", calls[0].Thread)
		}
		if calls[0].Labels["state"] != "completed" || calls[0].Before["state"] != "started" {
			t.Fatalf("labels/before = %+v / %+v", calls[0].Labels, calls[0].Before)
		}
	})
}

func TestOutboxLabelsChangedWithoutRendererIsNoop(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		provider, _, worker := outboundFixture(t, store, testConfig())
		target := threadTarget("C0123", "C0123:T1")
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack",
			boardsync.BoardEvent{ID: 11, Type: boardsync.BoardLabelsChanged, Thread: 101},
			boardMessage(101, 101, "root"), []boardsync.ResolvedTarget{target}); err != nil {
			t.Fatalf("enqueue labels: %v", err)
		}
		result, err := worker.Process(ctx)
		if err != nil || result.Dead != 0 || result.Acked != 1 {
			t.Fatalf("labels pass = %+v, %v", result, err)
		}
		if edits := provider.Edits(); len(edits) != 0 {
			t.Fatalf("a no-op label change edited: %+v", edits)
		}
	})
}

func TestOutboxAckRejectsCopyForAnotherTarget(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		target := threadTarget("C0123", "C0123:T1")
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack",
			postedEvent(10, 100, 101), boardMessage(101, 100, "root"), []boardsync.ResolvedTarget{target}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		jobs, err := store.LeaseOutbox(ctx, "t1", []boardsync.Key{"slack"}, 1, "worker")
		if err != nil || len(jobs) != 1 {
			t.Fatalf("lease = %+v, %v", jobs, err)
		}
		bad := syncstore.Copy{
			BoardMessage: 101, BoardThread: 100, Key: "slack",
			Target:         threadTarget("C9999", "C9999:T9"),
			ExternalThread: "C9999:T9", ExternalMessage: "wrong",
		}
		if _, err := store.AckOutbox(ctx, "t1", jobs[0].ID, jobs[0].Lease, nil, &bad); !errors.Is(err, boardsync.ErrBadRequest) {
			t.Fatalf("foreign copy ack = %v", err)
		}
		good := syncstore.Copy{
			BoardMessage: 101, BoardThread: 100, Key: "slack", Target: target,
			ExternalThread: "C0123:T1", ExternalMessage: "posted",
		}
		if _, err := store.AckOutbox(ctx, "t1", jobs[0].ID, jobs[0].Lease, nil, &good); err != nil {
			t.Fatalf("good ack after rejected copy: %v", err)
		}
	})
}

func TestOutboxReactionWaitsForCopy(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		provider, host, worker := outboundFixture(t, store, testConfig())
		target := threadTarget("C0123", "C0123:T1")
		message := boardMessage(201, 200, "react later")
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack",
			boardsync.BoardEvent{ID: 20, Type: boardsync.BoardReactionAdded, Thread: 200, Emoji: "eyes"},
			message, []boardsync.ResolvedTarget{target}); err != nil {
			t.Fatal(err)
		}
		result, err := worker.Process(ctx)
		if err != nil || result.Retried != 1 || result.Dead != 0 {
			t.Fatalf("reaction before copy = %+v, %v", result, err)
		}
		if len(provider.Reactions()) != 0 || len(host.Records()) != 0 {
			t.Fatalf("a pending reaction reached the provider or wrote a receipt")
		}
		if _, err := store.SaveCopy(ctx, "t1", syncstore.Copy{
			BoardMessage: 201, BoardThread: 200, Key: "slack", Target: target,
			ExternalThread: "C0123:T1", ExternalMessage: "copy-201",
		}); err != nil {
			t.Fatal(err)
		}
		result, err = worker.Process(ctx)
		if err != nil || result.Acked != 1 {
			t.Fatalf("reaction after copy = %+v, %v", result, err)
		}
		reactions := provider.Reactions()
		if len(reactions) != 1 || reactions[0].Message != "copy-201" {
			t.Fatalf("reactions = %+v", reactions)
		}
	})
}

func TestOutboxUnsupportedChangeIsSkippedWithoutReceipt(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		provider, host, worker := outboundFixture(t, store, testConfig())
		provider.EditFunc = func(context.Context, boardsync.Key, boardsync.ThreadRef, string, boardsync.OutboundMessage) error {
			return boardsync.ErrUnsupported
		}
		target := threadTarget("C0123", "C0123:T1")
		if _, err := store.SaveCopy(ctx, "t1", syncstore.Copy{
			BoardMessage: 301, BoardThread: 300, Key: "slack", Target: target,
			ExternalThread: "C0123:T1", ExternalMessage: "copy-301",
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack",
			boardsync.BoardEvent{ID: 30, Type: boardsync.BoardMessageEdited, Thread: 300},
			boardMessage(301, 300, "edited"), []boardsync.ResolvedTarget{target}); err != nil {
			t.Fatal(err)
		}
		result, err := worker.Process(ctx)
		if err != nil || result.Acked != 1 || result.Dead != 0 {
			t.Fatalf("unsupported edit = %+v, %v", result, err)
		}
		if records := host.Records(); len(records) != 0 {
			t.Fatalf("an unsupported change wrote a receipt: %+v", records)
		}
	})
}
