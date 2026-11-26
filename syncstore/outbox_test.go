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
	provider := fake.New("slack", boardsync.Capabilities{
		StartThreads: true, Edits: true, Deletes: true, Reactions: true,
	})
	registry := boardsync.NewRegistry()
	if err := registry.Add(boardsync.Entry{Key: "slack", Provider: provider, Sync: boardsync.Mirror("support")}); err != nil {
		t.Fatalf("register entry: %v", err)
	}
	host := fake.NewHost()
	runner := boardsync.NewRunner(host)
	worker, err := syncstore.NewOutboundWorker(syncstore.OutboundWorkerConfig{
		Store: store, Registry: registry, Runner: runner, Host: host,
		Tenant: "t1", Owner: "worker", Limit: 10,
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
		first, err := store.LeaseOutbox(ctx, "t1", 10, "worker-a")
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
		if again, err := store.LeaseOutbox(ctx, "t1", 10, "worker-b"); err != nil || len(again) != 0 {
			t.Fatalf("second lease = %+v, %v; a lane was leased twice", again, err)
		}
		for _, job := range first {
			if _, err := store.AckOutbox(ctx, "t1", job.ID, job.Lease, nil, nil); err != nil {
				t.Fatalf("ack %s: %v", job.ID, err)
			}
		}
		// The second target on lane 100 is next; the other lane has no job.
		next, err := store.LeaseOutbox(ctx, "t1", 10, "worker-a")
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
			boardsync.BoardEvent{ID: 12, Type: boardsync.BoardMessageDeleted, Thread: 101}, edited,
			[]boardsync.ResolvedTarget{target}); err != nil {
			t.Fatalf("enqueue delete: %v", err)
		}
		if result, err := worker.Process(ctx); err != nil || result.Dead != 0 {
			t.Fatalf("delete pass = %+v, %v", result, err)
		}
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack",
			boardsync.BoardEvent{ID: 13, Type: boardsync.BoardReactionAdded, Thread: 101, Emoji: "thumbsup"}, edited,
			[]boardsync.ResolvedTarget{target}); err != nil {
			t.Fatalf("enqueue reaction: %v", err)
		}
		if result, err := worker.Process(ctx); err != nil || result.Dead != 0 {
			t.Fatalf("reaction pass = %+v, %v", result, err)
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

func TestOutboxMissingCopyIsDeadAndDoesNotBlock(t *testing.T) {
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
		if err != nil || result.Dead != 1 {
			t.Fatalf("edit pass = %+v, %v", result, err)
		}
		if edits := provider.Edits(); len(edits) != 0 {
			t.Fatalf("an edit without a copy reached the provider: %+v", edits)
		}
		records := host.Records()
		if len(records) != 1 || records[0].Type != boardsync.BoardMessageSyncFailed || records[0].Error != "permanent" {
			t.Fatalf("failure receipt = %+v", records)
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
		)
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				jobs, err := store.LeaseOutbox(ctx, "t1", 10, "worker")
				if err != nil {
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
	first, err := memory.LeaseOutbox(ctx, "t1", 1, "worker-a")
	if err != nil || len(first) != 1 {
		t.Fatalf("first lease = %+v, %v", first, err)
	}
	now = now.Add(2 * time.Minute)
	second, err := memory.LeaseOutbox(ctx, "t1", 1, "worker-b")
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
		a, err := store.LeaseOutbox(ctx, "tenant-a", 10, "w")
		if err != nil || len(a) != 1 {
			t.Fatalf("lease tenant-a = %+v, %v", a, err)
		}
		b, err := store.LeaseOutbox(ctx, "tenant-b", 10, "w")
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
	})
}
