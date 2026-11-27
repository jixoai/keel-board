package automation_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jixoai/keel-board/automation"
	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/store"
)

type allowAll struct{}

func (allowAll) Authorize(context.Context, string, protocol.Actor, string, protocol.Permission) error {
	return nil
}

type denyBotPost struct{}

func (denyBotPost) Authorize(_ context.Context, _ string, actor protocol.Actor, _ string, permission protocol.Permission) error {
	if strings.HasPrefix(actor.ID, "local:bot:") && permission == protocol.PermissionPost {
		return protocol.Forbidden("the bot may not post here")
	}
	return nil
}

type denyCreatorPost struct{ creator string }

func (d denyCreatorPost) Authorize(_ context.Context, _ string, actor protocol.Actor, _ string, permission protocol.Permission) error {
	if actor.ID == d.creator && permission == protocol.PermissionPost {
		return protocol.Forbidden("revoked")
	}
	return nil
}

func newTestBoard(t *testing.T) *log.Board {
	t.Helper()
	return log.New(log.Options{Store: store.NewMemory()})
}

func postMessage(t *testing.T, board *log.Board, tenant, channel, text string, labels map[string]string) uint64 {
	t.Helper()
	var id uint64
	err := board.Update(context.Background(), tenant, log.UpdateOptions{}, func(tx *log.Tx) error {
		id = tx.NextID(0)
		if labels == nil {
			labels = map[string]string{}
		}
		tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
			ID: id, Channel: channel, Text: text, Thread: log.ThreadKey(id),
			Labels: labels, By: protocol.Author{Actor: "local:user:alice", Principal: "local:user:alice"},
		})
		tx.RecordResult(id)
		return nil
	})
	if err != nil {
		t.Fatalf("post message: %v", err)
	}
	return id
}

func messagesIn(t *testing.T, board *log.Board, tenant, channel string) []*log.Message {
	t.Helper()
	var messages []*log.Message
	if err := board.Read(tenant, func(snapshot *log.Snapshot) error {
		messages = append(messages, snapshot.MessagesInChannel(channel)...)
		return nil
	}); err != nil {
		t.Fatalf("read channel: %v", err)
	}
	return messages
}

func TestWatcherPlansFutureEventsAndRunsAsBot(t *testing.T) {
	board := newTestBoard(t)
	ctx := t.Context()
	memory, err := automation.NewMemory(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	registry, err := automation.NewRegistryWith(automation.PostAction{})
	if err != nil {
		t.Fatal(err)
	}
	postMessage(t, board, "t1", "work", "before the watcher", map[string]string{"kind": "alert"})
	if _, err := memory.CreateWatcher(ctx, automation.Watcher{
		Tenant: "t1", ID: "alerts", Channel: "work", Enabled: true,
		Filter: automation.Filter{Types: []string{"message.posted"}, Labels: map[string]string{"kind": "alert"}},
		Action: automation.ActionSpec{Name: "post", Params: []byte(`{"text":"watcher says hi"}`)},
	}); err != nil {
		t.Fatalf("create watcher: %v", err)
	}
	worker, err := automation.NewWatcherWorker(automation.WatcherWorkerConfig{
		Store: memory, Board: board, Actions: registry, Authorize: allowAll{},
		Tenant: "t1", PollInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	// The first pass fixes a new watcher's cursor at the current head; it
	// must not replay the event from before its declaration.
	if _, err := worker.Process(ctx); err != nil {
		t.Fatalf("initial pass: %v", err)
	}
	if got := len(messagesIn(t, board, "t1", "work")); got != 1 {
		t.Fatalf("initial pass posted %d messages, want 1", got)
	}
	eventID := postMessage(t, board, "t1", "work", "new alert", map[string]string{"kind": "alert"})
	postMessage(t, board, "t1", "other", "elsewhere", map[string]string{"kind": "alert"})
	postMessage(t, board, "t1", "work", "no label", nil)
	if _, err := worker.Process(ctx); err != nil {
		t.Fatalf("worker pass: %v", err)
	}
	var botPosts []*log.Message
	for _, message := range messagesIn(t, board, "t1", "work") {
		if message.By.Actor == "local:bot:alerts" {
			botPosts = append(botPosts, message)
		}
	}
	if len(botPosts) != 1 {
		t.Fatalf("bot posts = %d, want 1", len(botPosts))
	}
	posted := botPosts[0]
	if posted.Text != "watcher says hi" {
		t.Fatalf("bot text = %q", posted.Text)
	}
	if posted.Re != nil || posted.ID <= eventID {
		t.Fatalf("bot post = %+v", posted)
	}
	depth, err := memory.DeliveryDepth(ctx, "t1", "alerts")
	if err != nil || depth.Done != 1 || depth.Dead != 0 {
		t.Fatalf("depth = %+v, %v", depth, err)
	}
	// The bot's own post is a board event on the watched channel; planning it
	// must not wake the same watcher again.
	if _, err := worker.Process(ctx); err != nil {
		t.Fatalf("self-wake pass: %v", err)
	}
	depth, err = memory.DeliveryDepth(ctx, "t1", "alerts")
	if err != nil || depth.Done != 1 || depth.Dead != 0 || depth.Pending != 0 {
		t.Fatalf("self-wake depth = %+v, %v", depth, err)
	}
	count := 0
	for _, message := range messagesIn(t, board, "t1", "work") {
		if message.By.Actor == "local:bot:alerts" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("bot posts = %d, want 1", count)
	}
}

func TestWatcherFailureIsolationAndLaneProgress(t *testing.T) {
	board := newTestBoard(t)
	ctx := t.Context()
	cfg := testConfig()
	cfg.Retry.MaxAttempts = 2
	memory, err := automation.NewMemory(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var goodCalls atomic.Int64
	registry, err := automation.NewRegistryWith(
		automation.ActionFunc{ActionName: "fail", Fn: func(context.Context, *automation.ActionCall) error {
			return errors.New("plugin is down")
		}},
		automation.ActionFunc{ActionName: "record", Fn: func(_ context.Context, call *automation.ActionCall) error {
			goodCalls.Add(1)
			return nil
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	postMessage(t, board, "t1", "work", "first", nil)
	for id, action := range map[string]string{"bad": "fail", "good": "record"} {
		if _, err := memory.CreateWatcher(ctx, automation.Watcher{
			Tenant: "t1", ID: id, Channel: "work", Cursor: 1, Enabled: true,
			Action: automation.ActionSpec{Name: action},
		}); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	worker, err := automation.NewWatcherWorker(automation.WatcherWorkerConfig{
		Store: memory, Board: board, Actions: registry, Authorize: allowAll{},
		Tenant: "t1", PollInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	postMessage(t, board, "t1", "work", "second", nil)
	// Two passes exhaust the bad action's attempts; the good watcher's
	// delivery is acked on the first.
	for pass := 0; pass < 2; pass++ {
		if _, err := worker.Process(ctx); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
	}
	if goodCalls.Load() != 1 {
		t.Fatalf("good action calls = %d, want 1", goodCalls.Load())
	}
	depth, err := memory.DeliveryDepth(ctx, "t1", "bad")
	if err != nil || depth.Dead != 1 {
		t.Fatalf("bad depth = %+v, %v; want one dead letter", depth, err)
	}
	// The dead letter no longer blocks the lane: a later event still reaches
	// the bad watcher, while the good one keeps working and the board stays
	// writable.
	postMessage(t, board, "t1", "work", "third", nil)
	for pass := 0; pass < 2; pass++ {
		if _, err := worker.Process(ctx); err != nil {
			t.Fatalf("later pass %d: %v", pass, err)
		}
	}
	if goodCalls.Load() != 2 {
		t.Fatalf("good action calls after the dead letter = %d, want 2", goodCalls.Load())
	}
	postMessage(t, board, "t1", "work", "board still works", nil)
}

func TestWatcherBotPermissionDenialIsPermanent(t *testing.T) {
	board := newTestBoard(t)
	ctx := t.Context()
	memory, err := automation.NewMemory(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	registry, err := automation.NewRegistryWith(automation.ActionFunc{
		ActionName: "postbot",
		Fn: func(ctx context.Context, call *automation.ActionCall) error {
			_, err := call.Post(ctx, call.Channel(), "must not land", nil)
			return err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	postMessage(t, board, "t1", "work", "trigger", nil)
	if _, err := memory.CreateWatcher(ctx, automation.Watcher{
		Tenant: "t1", ID: "alerts", Channel: "work", Cursor: 1, Enabled: true,
		Action: automation.ActionSpec{Name: "postbot"},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	worker, err := automation.NewWatcherWorker(automation.WatcherWorkerConfig{
		Store: memory, Board: board, Actions: registry, Authorize: denyBotPost{},
		Tenant: "t1", PollInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	postMessage(t, board, "t1", "work", "second", nil)
	if _, err := worker.Process(ctx); err != nil {
		t.Fatalf("pass: %v", err)
	}
	if got := len(messagesIn(t, board, "t1", "work")); got != 2 {
		t.Fatalf("work messages = %d, want 2 (no bot post)", got)
	}
	depth, err := memory.DeliveryDepth(ctx, "t1", "alerts")
	if err != nil || depth.Dead != 1 {
		t.Fatalf("depth = %+v, %v; want an immediate dead letter", depth, err)
	}
}

func TestWatcherNeverFetchesURLs(t *testing.T) {
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	board := newTestBoard(t)
	ctx := t.Context()
	memory, err := automation.NewMemory(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	var seenText string
	registry, err := automation.NewRegistryWith(automation.ActionFunc{
		ActionName: "record",
		Fn: func(_ context.Context, call *automation.ActionCall) error {
			if message := call.Message(); message != nil {
				seenText = message.Text
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	postMessage(t, board, "t1", "work", "callback "+server.URL+"/hook", nil)
	if _, err := memory.CreateWatcher(ctx, automation.Watcher{
		Tenant: "t1", ID: "alerts", Channel: "work", Cursor: 1, Enabled: true,
		Action: automation.ActionSpec{Name: "record"},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	worker, err := automation.NewWatcherWorker(automation.WatcherWorkerConfig{
		Store: memory, Board: board, Actions: registry, Tenant: "t1", PollInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	postMessage(t, board, "t1", "work", "run "+server.URL+"/hook", nil)
	if _, err := worker.Process(ctx); err != nil {
		t.Fatalf("pass: %v", err)
	}
	if !strings.Contains(seenText, server.URL) {
		t.Fatalf("action saw %q, want the event's URL as data", seenText)
	}
	if hits.Load() != 0 {
		t.Fatalf("watcher fetched a URL %d times", hits.Load())
	}
	// A parameter that tries to declare an HTTP target is refused outright.
	if err := (automation.ActionSpec{Name: "record", Params: []byte(`{"url":"https://example.com"}`)}).Validate(); err == nil {
		t.Fatal("a URL parameter was accepted")
	}
}

// flakyAckStore loses one schedule ack after the message append, modelling a
// crash between the board write and the ack.
type flakyAckStore struct {
	automation.Store
	mu      sync.Mutex
	failAck bool
}

func (s *flakyAckStore) AckSchedule(ctx context.Context, tenant, id, lease string, result automation.ScheduleResult) (automation.Schedule, error) {
	s.mu.Lock()
	if s.failAck {
		s.failAck = false
		s.mu.Unlock()
		return automation.Schedule{}, errors.New("simulated ack loss")
	}
	s.mu.Unlock()
	return s.Store.AckSchedule(ctx, tenant, id, lease, result)
}

// failUpdateBoard fails appends, modelling a database error during a send.
type failUpdateBoard struct {
	automation.Board
	mu      sync.Mutex
	failing bool
}

func (b *failUpdateBoard) Update(ctx context.Context, tenant string, opts log.UpdateOptions, fn func(*log.Tx) error) error {
	b.mu.Lock()
	failing := b.failing
	b.mu.Unlock()
	if failing {
		return errors.New("database is unavailable")
	}
	return b.Board.Update(ctx, tenant, opts, fn)
}

func TestScheduleIdempotentAfterAckLoss(t *testing.T) {
	board := newTestBoard(t)
	ctx := t.Context()
	cfg := testConfig()
	cfg.LeaseTTL = 100 * time.Millisecond
	memory, err := automation.NewMemory(cfg)
	if err != nil {
		t.Fatal(err)
	}
	flaky := &flakyAckStore{Store: memory, failAck: true}
	id, _ := automation.NewScheduleID()
	if _, err := memory.CreateSchedule(ctx, automation.Schedule{
		Tenant: "t1", ID: id, Channel: "work", Text: "scheduled text",
		Labels: map[string]string{"kind": "reminder"}, Creator: "local:user:alice",
		NextRunAt: time.Now().Add(-time.Second),
	}); err != nil {
		t.Fatalf("create schedule: %v", err)
	}
	worker, err := automation.NewScheduleWorker(automation.ScheduleWorkerConfig{
		Store: flaky, Board: board, Authorize: allowAll{}, Tenant: "t1", PollInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.Process(ctx); err == nil {
		t.Fatal("the simulated ack loss should surface")
	}
	if got := len(messagesIn(t, board, "t1", "work")); got != 1 {
		t.Fatalf("messages after the lost ack = %d, want 1", got)
	}
	time.Sleep(150 * time.Millisecond)
	if _, err := worker.Process(ctx); err != nil {
		t.Fatalf("retry pass: %v", err)
	}
	messages := messagesIn(t, board, "t1", "work")
	if len(messages) != 1 {
		t.Fatalf("messages after the retry = %d, want 1 (idempotent key)", len(messages))
	}
	if messages[0].Text != "scheduled text" || messages[0].By.Actor != "local:user:alice" {
		t.Fatalf("scheduled message = %+v", messages[0])
	}
	if messages[0].Labels["kind"] != "reminder" {
		t.Fatalf("labels = %+v", messages[0].Labels)
	}
	schedule, _, err := memory.Schedule(ctx, "t1", id)
	if err != nil || schedule.Status != automation.ScheduleDone || schedule.Sent != 1 {
		t.Fatalf("schedule after retry = %+v, %v", schedule, err)
	}
}

func TestScheduleRevokedPermission(t *testing.T) {
	board := newTestBoard(t)
	ctx := t.Context()
	memory, err := automation.NewMemory(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	id, _ := automation.NewScheduleID()
	if _, err := memory.CreateSchedule(ctx, automation.Schedule{
		Tenant: "t1", ID: id, Channel: "work", Text: "must not send",
		Creator: "local:user:alice", NextRunAt: time.Now().Add(-time.Second),
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	worker, err := automation.NewScheduleWorker(automation.ScheduleWorkerConfig{
		Store: memory, Board: board, Authorize: denyCreatorPost{creator: "local:user:alice"},
		Tenant: "t1", PollInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.Process(ctx); err != nil {
		t.Fatalf("pass: %v", err)
	}
	if got := len(messagesIn(t, board, "t1", "work")); got != 0 {
		t.Fatalf("revoked schedule posted %d messages", got)
	}
	schedule, _, err := memory.Schedule(ctx, "t1", id)
	if err != nil || schedule.Status != automation.ScheduleCanceled || schedule.CancelReason != "revoked" {
		t.Fatalf("schedule = %+v, %v", schedule, err)
	}
}

func TestScheduleRetryThenDeadThenManualRetry(t *testing.T) {
	board := newTestBoard(t)
	ctx := t.Context()
	cfg := testConfig()
	cfg.Retry.MaxAttempts = 2
	memory, err := automation.NewMemory(cfg)
	if err != nil {
		t.Fatal(err)
	}
	failing := &failUpdateBoard{Board: board, failing: true}
	id, _ := automation.NewScheduleID()
	if _, err := memory.CreateSchedule(ctx, automation.Schedule{
		Tenant: "t1", ID: id, Channel: "work", Text: "eventually",
		Creator: "local:user:alice", NextRunAt: time.Now().Add(-time.Second),
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	worker, err := automation.NewScheduleWorker(automation.ScheduleWorkerConfig{
		Store: memory, Board: failing, Authorize: allowAll{}, Tenant: "t1", PollInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.Process(ctx); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	schedule, _, _ := memory.Schedule(ctx, "t1", id)
	if schedule.Status != automation.SchedulePending || schedule.Attempts != 1 {
		t.Fatalf("after one failure = %+v", schedule)
	}
	if _, err := worker.Process(ctx); err != nil {
		t.Fatalf("second pass: %v", err)
	}
	schedule, _, _ = memory.Schedule(ctx, "t1", id)
	if schedule.Status != automation.ScheduleDead {
		t.Fatalf("after two failures = %+v", schedule)
	}
	if err := memory.RetrySchedule(ctx, "t1", id); err != nil {
		t.Fatalf("manual retry: %v", err)
	}
	failing.mu.Lock()
	failing.failing = false
	failing.mu.Unlock()
	if _, err := worker.Process(ctx); err != nil {
		t.Fatalf("recovery pass: %v", err)
	}
	schedule, _, _ = memory.Schedule(ctx, "t1", id)
	if schedule.Status != automation.ScheduleDone || schedule.Sent != 1 {
		t.Fatalf("after recovery = %+v", schedule)
	}
	if got := len(messagesIn(t, board, "t1", "work")); got != 1 {
		t.Fatalf("messages = %d, want 1", got)
	}
}

func TestWatcherConcurrentWorkersDoNotDuplicateBoardWrites(t *testing.T) {
	board := newTestBoard(t)
	ctx := t.Context()
	memory, err := automation.NewMemory(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	registry, err := automation.NewRegistryWith(automation.PostAction{})
	if err != nil {
		t.Fatal(err)
	}
	postMessage(t, board, "t1", "work", "first", nil)
	if _, err := memory.CreateWatcher(ctx, automation.Watcher{
		Tenant: "t1", ID: "alerts", Channel: "work", Cursor: 1, Enabled: true,
		Action: automation.ActionSpec{Name: "post", Params: []byte(`{"text":"once"}`)},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	postMessage(t, board, "t1", "work", "second", nil)
	workerA, _ := automation.NewWatcherWorker(automation.WatcherWorkerConfig{
		Store: memory, Board: board, Actions: registry, Authorize: allowAll{},
		Tenant: "t1", Owner: "a", PollInterval: time.Hour,
	})
	workerB, _ := automation.NewWatcherWorker(automation.WatcherWorkerConfig{
		Store: memory, Board: board, Actions: registry, Authorize: allowAll{},
		Tenant: "t1", Owner: "b", PollInterval: time.Hour,
	})
	var wg sync.WaitGroup
	for _, worker := range []*automation.WatcherWorker{workerA, workerB} {
		wg.Add(1)
		go func(worker *automation.WatcherWorker) {
			defer wg.Done()
			if _, err := worker.Process(ctx); err != nil {
				t.Errorf("pass: %v", err)
			}
		}(worker)
	}
	wg.Wait()
	var botPosts []*log.Message
	for _, message := range messagesIn(t, board, "t1", "work") {
		if message.By.Actor == "local:bot:alerts" {
			botPosts = append(botPosts, message)
		}
	}
	if len(botPosts) != 1 {
		t.Fatalf("bot posts = %d, want exactly one", len(botPosts))
	}
	if botPosts[0].Text != "once" {
		t.Fatalf("bot text = %q", botPosts[0].Text)
	}
	depth, _ := memory.DeliveryDepth(ctx, "t1", "alerts")
	if depth.Done != 1 {
		t.Fatalf("depth = %+v", depth)
	}
}

func TestWatcherOnEmptyBoardDoesNotSkipFirstEvent(t *testing.T) {
	board := newTestBoard(t)
	ctx := t.Context()
	memory, err := automation.NewMemory(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	registry, err := automation.NewRegistryWith(automation.ActionFunc{
		ActionName: "record",
		Fn: func(context.Context, *automation.ActionCall) error {
			calls.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := memory.CreateWatcher(ctx, automation.Watcher{
		Tenant: "t1", ID: "empty", Channel: "work", Enabled: true,
		Action: automation.ActionSpec{Name: "record"},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	worker, err := automation.NewWatcherWorker(automation.WatcherWorkerConfig{
		Store: memory, Board: board, Actions: registry, Tenant: "t1", PollInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	// An empty board has head 0. The initialization pass must still mark the
	// cursor as evaluated, or event 1 would be mistaken for history.
	if _, err := worker.Process(ctx); err != nil {
		t.Fatalf("initial pass: %v", err)
	}
	watcher, _, _ := memory.Watcher(ctx, "t1", "empty")
	if !watcher.CursorSet {
		t.Fatal("an empty board left the cursor uninitialized")
	}
	postMessage(t, board, "t1", "work", "the first event", nil)
	if _, err := worker.Process(ctx); err != nil {
		t.Fatalf("event pass: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("action calls = %d, want 1", calls.Load())
	}
}

func TestScheduleRunKeyConflictCountsAsSent(t *testing.T) {
	board := newTestBoard(t)
	ctx := t.Context()
	memory, err := automation.NewMemory(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	id, _ := automation.NewScheduleID()
	// A prior attempt already posted this run under its key; its digest
	// differs because the schedule body was edited before the retry.
	err = board.Update(ctx, "t1", log.UpdateOptions{Idempotency: &log.Idempotency{
		Key: "schedule:" + id + ":1", Digest: "an-earlier-digest",
	}}, func(tx *log.Tx) error {
		postID := tx.NextID(0)
		tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
			ID: postID, Channel: "work", Text: "old text", Thread: log.ThreadKey(postID),
			Labels: map[string]string{}, By: protocol.Author{Actor: "local:user:alice", Principal: "local:user:alice"},
		})
		tx.RecordResult(postID)
		return nil
	})
	if err != nil {
		t.Fatalf("seed prior run: %v", err)
	}
	if _, err := memory.CreateSchedule(ctx, automation.Schedule{
		Tenant: "t1", ID: id, Channel: "work", Text: "new text",
		Creator: "local:user:alice", NextRunAt: time.Now().Add(-time.Second),
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	worker, err := automation.NewScheduleWorker(automation.ScheduleWorkerConfig{
		Store: memory, Board: board, Authorize: allowAll{}, Tenant: "t1", PollInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.Process(ctx); err != nil {
		t.Fatalf("pass: %v", err)
	}
	schedule, _, err := memory.Schedule(ctx, "t1", id)
	if err != nil || schedule.Status != automation.ScheduleDone || schedule.Sent != 1 {
		t.Fatalf("schedule = %+v, %v", schedule, err)
	}
	messages := messagesIn(t, board, "t1", "work")
	if len(messages) != 1 || messages[0].Text != "old text" {
		t.Fatalf("messages = %+v", messages)
	}
}

func TestWatcherActionPanicIsIsolated(t *testing.T) {
	board := newTestBoard(t)
	ctx := t.Context()
	memory, err := automation.NewMemory(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	registry, err := automation.NewRegistryWith(automation.ActionFunc{
		ActionName: "panic",
		Fn: func(context.Context, *automation.ActionCall) error {
			panic("plugin bug")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	postMessage(t, board, "t1", "work", "first", nil)
	if _, err := memory.CreateWatcher(ctx, automation.Watcher{
		Tenant: "t1", ID: "panics", Channel: "work", Cursor: 1, Enabled: true,
		Action: automation.ActionSpec{Name: "panic"},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	worker, err := automation.NewWatcherWorker(automation.WatcherWorkerConfig{
		Store: memory, Board: board, Actions: registry, Tenant: "t1", PollInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	postMessage(t, board, "t1", "work", "second", nil)
	if _, err := worker.Process(ctx); err != nil {
		t.Fatalf("a panicking action should not fail the pass: %v", err)
	}
	depth, err := memory.DeliveryDepth(ctx, "t1", "panics")
	if err != nil || depth.Dead != 1 {
		t.Fatalf("depth = %+v, %v; want a dead letter", depth, err)
	}
	// The board and other workers keep working.
	postMessage(t, board, "t1", "work", "third", nil)
}

func TestWatcherActionTimeoutRetriesThenDead(t *testing.T) {
	board := newTestBoard(t)
	ctx := t.Context()
	cfg := testConfig()
	cfg.Retry.MaxAttempts = 2
	memory, err := automation.NewMemory(cfg)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := automation.NewRegistryWith(automation.ActionFunc{
		ActionName: "hang",
		Fn: func(ctx context.Context, _ *automation.ActionCall) error {
			<-ctx.Done()
			return ctx.Err()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	postMessage(t, board, "t1", "work", "first", nil)
	if _, err := memory.CreateWatcher(ctx, automation.Watcher{
		Tenant: "t1", ID: "hang", Channel: "work", Cursor: 1, Enabled: true,
		Action: automation.ActionSpec{Name: "hang"},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	worker, err := automation.NewWatcherWorker(automation.WatcherWorkerConfig{
		Store: memory, Board: board, Actions: registry, Tenant: "t1",
		PollInterval: time.Hour, ActionTimeout: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	postMessage(t, board, "t1", "work", "second", nil)
	if _, err := worker.Process(ctx); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	depth, _ := memory.DeliveryDepth(ctx, "t1", "hang")
	if depth.Pending != 1 {
		t.Fatalf("depth after timeout = %+v", depth)
	}
	if _, err := worker.Process(ctx); err != nil {
		t.Fatalf("second pass: %v", err)
	}
	depth, _ = memory.DeliveryDepth(ctx, "t1", "hang")
	if depth.Dead != 1 {
		t.Fatalf("depth after two timeouts = %+v", depth)
	}
}

type failingAuthorizer struct{ err error }

func (a failingAuthorizer) Authorize(context.Context, string, protocol.Actor, string, protocol.Permission) error {
	return a.err
}

func TestWatcherAuthorizationOutageRetriesThenDead(t *testing.T) {
	board := newTestBoard(t)
	ctx := t.Context()
	cfg := testConfig()
	cfg.Retry.MaxAttempts = 2
	registry, err := automation.NewRegistryWith(automation.ActionFunc{
		ActionName: "postbot",
		Fn: func(ctx context.Context, call *automation.ActionCall) error {
			_, err := call.Post(ctx, call.Channel(), "must not land", nil)
			return err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	postMessage(t, board, "t1", "work", "first", nil)
	postMessage(t, board, "t1", "work", "second", nil)
	for _, test := range []struct {
		name string
		err  error
	}{
		{"store 500", errors.New("grant store unavailable")},
		{"timeout", context.DeadlineExceeded},
		{"canceled", context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			fresh, _ := automation.NewMemory(cfg)
			worker, err := automation.NewWatcherWorker(automation.WatcherWorkerConfig{
				Store: fresh, Board: board, Actions: registry, Authorize: failingAuthorizer{err: test.err},
				Tenant: "t1", PollInterval: time.Hour,
			})
			if err != nil {
				t.Fatal(err)
			}
			// Recreate the watcher in the fresh store and feed the same event.
			if _, err := fresh.CreateWatcher(ctx, automation.Watcher{
				Tenant: "t1", ID: "outage", Channel: "work", Cursor: 1, Enabled: true,
				Action: automation.ActionSpec{Name: "postbot"},
			}); err != nil {
				t.Fatalf("create fresh: %v", err)
			}
			if _, err := worker.Process(ctx); err != nil {
				t.Fatalf("first pass: %v", err)
			}
			depth, _ := fresh.DeliveryDepth(ctx, "t1", "outage")
			if depth.Dead != 0 || depth.Pending != 1 {
				t.Fatalf("after an authorization outage = %+v; want a retry, not a dead letter", depth)
			}
			// The retried delivery is due immediately under the test policy.
			if _, err := worker.Process(ctx); err != nil {
				t.Fatalf("second pass: %v", err)
			}
			depth, _ = fresh.DeliveryDepth(ctx, "t1", "outage")
			if depth.Dead != 1 {
				t.Fatalf("after two attempts = %+v; want a dead letter", depth)
			}
		})
	}
}

func TestScheduleAuthorizationOutageRetriesNotCancels(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{"store 500", errors.New("grant store unavailable")},
		{"timeout", context.DeadlineExceeded},
		{"canceled", context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			board := newTestBoard(t)
			ctx := t.Context()
			cfg := testConfig()
			cfg.Retry.MaxAttempts = 2
			memory, err := automation.NewMemory(cfg)
			if err != nil {
				t.Fatal(err)
			}
			id, _ := automation.NewScheduleID()
			if _, err := memory.CreateSchedule(ctx, automation.Schedule{
				Tenant: "t1", ID: id, Channel: "work", Text: "must not send now",
				Creator: "local:user:alice", NextRunAt: time.Now().Add(-time.Second),
			}); err != nil {
				t.Fatalf("create: %v", err)
			}
			worker, err := automation.NewScheduleWorker(automation.ScheduleWorkerConfig{
				Store: memory, Board: board, Authorize: failingAuthorizer{err: test.err},
				Tenant: "t1", PollInterval: time.Hour,
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := worker.Process(ctx); err != nil {
				t.Fatalf("first pass: %v", err)
			}
			schedule, _, _ := memory.Schedule(ctx, "t1", id)
			if schedule.Status != automation.SchedulePending || schedule.Attempts != 1 || schedule.CancelReason != "" {
				t.Fatalf("after an authorization outage = %+v; want a retry, not a cancellation", schedule)
			}
			if _, err := worker.Process(ctx); err != nil {
				t.Fatalf("second pass: %v", err)
			}
			schedule, _, _ = memory.Schedule(ctx, "t1", id)
			if schedule.Status != automation.ScheduleDead || schedule.CancelReason != "" {
				t.Fatalf("after two attempts = %+v; want a dead letter, not a cancellation", schedule)
			}
			if got := len(messagesIn(t, board, "t1", "work")); got != 0 {
				t.Fatalf("messages = %d, want 0", got)
			}
		})
	}
}

func TestWatcherWorkerRejectsActionTimeoutNotShorterThanLease(t *testing.T) {
	memory, err := automation.NewMemory(automation.Config{LeaseTTL: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := automation.NewRegistryWith(automation.ActionFunc{
		ActionName: "noop", Fn: func(context.Context, *automation.ActionCall) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	board := newTestBoard(t)
	if _, err := automation.NewWatcherWorker(automation.WatcherWorkerConfig{
		Store: memory, Board: board, Actions: registry, Tenant: "t1",
		ActionTimeout: 100 * time.Millisecond,
	}); err == nil {
		t.Fatal("an action timeout equal to the lease was accepted")
	}
	if _, err := automation.NewWatcherWorker(automation.WatcherWorkerConfig{
		Store: memory, Board: board, Actions: registry, Tenant: "t1",
		ActionTimeout: 50 * time.Millisecond,
	}); err != nil {
		t.Fatalf("a shorter action timeout was refused: %v", err)
	}
}

func TestWatcherHeartbeatPreventsConcurrentExecution(t *testing.T) {
	board := newTestBoard(t)
	ctx := t.Context()
	cfg := testConfig()
	cfg.LeaseTTL = 300 * time.Millisecond
	memory, err := automation.NewMemory(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var starts atomic.Int64
	registry, err := automation.NewRegistryWith(automation.ActionFunc{
		ActionName: "slow",
		Fn: func(context.Context, *automation.ActionCall) error {
			starts.Add(1)
			// Deliberately ignore the action context: the heartbeat must keep
			// the lease alive even when a plugin overruns its timeout.
			time.Sleep(450 * time.Millisecond)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	postMessage(t, board, "t1", "work", "first", nil)
	if _, err := memory.CreateWatcher(ctx, automation.Watcher{
		Tenant: "t1", ID: "slow", Channel: "work", Cursor: 1, Enabled: true,
		Action: automation.ActionSpec{Name: "slow"},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	postMessage(t, board, "t1", "work", "second", nil)
	workerA, err := automation.NewWatcherWorker(automation.WatcherWorkerConfig{
		Store: memory, Board: board, Actions: registry, Tenant: "t1", Owner: "a",
		PollInterval: time.Hour, ActionTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	workerB, err := automation.NewWatcherWorker(automation.WatcherWorkerConfig{
		Store: memory, Board: board, Actions: registry, Tenant: "t1", Owner: "b",
		PollInterval: time.Hour, ActionTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan automation.WorkResult, 1)
	go func() {
		result, _ := workerA.Process(ctx)
		done <- result
	}()
	time.Sleep(350 * time.Millisecond)
	if result, err := workerB.Process(ctx); err != nil {
		t.Fatalf("second worker: %v", err)
	} else if result.Leased != 0 {
		t.Fatalf("second worker leased %d deliveries while the first action ran", result.Leased)
	}
	<-done
	if starts.Load() != 1 {
		t.Fatalf("action starts = %d, want 1", starts.Load())
	}
	depth, err := memory.DeliveryDepth(ctx, "t1", "slow")
	if err != nil || depth.Done != 1 {
		t.Fatalf("depth = %+v, %v", depth, err)
	}
}

func TestBoardGeneratedTaskEventsWakeBotWatcher(t *testing.T) {
	board := newTestBoard(t)
	ctx := t.Context()
	memory, err := automation.NewMemory(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	registry, err := automation.NewRegistryWith(automation.ActionFunc{
		ActionName: "record",
		Fn: func(context.Context, *automation.ActionCall) error {
			calls.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var messageID uint64
	err = board.Update(ctx, "t1", log.UpdateOptions{}, func(tx *log.Tx) error {
		messageID = tx.NextID(0)
		tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
			ID: messageID, Channel: "work", Text: "bot task", Thread: log.ThreadKey(messageID),
			Labels: map[string]string{}, By: protocol.Author{Actor: "local:bot:alerts", Principal: "local:bot:alerts"},
		})
		tx.RecordResult(messageID)
		return nil
	})
	if err != nil {
		t.Fatalf("post bot task: %v", err)
	}
	if err := board.Update(ctx, "t1", log.UpdateOptions{}, func(tx *log.Tx) error {
		tx.Emit(log.EventTaskReady, &log.TaskReadyPayload{ID: messageID, Thread: log.ThreadKey(messageID)})
		return nil
	}); err != nil {
		t.Fatalf("emit task.ready: %v", err)
	}
	if err := board.Update(ctx, "t1", log.UpdateOptions{}, func(tx *log.Tx) error {
		tx.Emit(log.EventTaskExpired, &log.TaskExpiredPayload{ID: messageID, Thread: log.ThreadKey(messageID)})
		return nil
	}); err != nil {
		t.Fatalf("emit task.expired: %v", err)
	}
	if _, err := memory.CreateWatcher(ctx, automation.Watcher{
		Tenant: "t1", ID: "tasks", Channel: "work", Cursor: 1, Enabled: true,
		Bot:    "local:bot:alerts",
		Filter: automation.Filter{Types: []string{"task.ready", "task.expired"}},
		Action: automation.ActionSpec{Name: "record"},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	worker, err := automation.NewWatcherWorker(automation.WatcherWorkerConfig{
		Store: memory, Board: board, Actions: registry, Authorize: allowAll{},
		Tenant: "t1", PollInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	for pass := 0; pass < 2; pass++ {
		if _, err := worker.Process(ctx); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("action calls = %d, want 2 (ready and expired must wake the bot's watcher)", calls.Load())
	}
}

func TestActionCallIsReadOnlyAndCannotBypassWrites(t *testing.T) {
	board := newTestBoard(t)
	ctx := t.Context()
	memory, err := automation.NewMemory(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	var exported []string
	var seenMessage string
	var seenLabel string
	var seenPayload string
	var seenWatcherChannel string
	var seenBot string
	registry, err := automation.NewRegistryWith(automation.ActionFunc{
		ActionName: "mutate",
		Fn: func(_ context.Context, call *automation.ActionCall) error {
			// A read-only view must detach every mutable part.
			if message := call.Message(); message != nil {
				message.Text = "tampered"
				message.Labels["tampered"] = "yes"
			}
			if event, ok := call.Event().Payload.(*log.MessagePostedPayload); ok {
				event.Text = "tampered"
				event.Labels["tampered"] = "yes"
			}
			watcher := call.Watcher()
			watcher.Channel = "elsewhere"
			watcher.Action.Params = []byte(`{"text":"tampered"}`)
			bot := call.Bot()
			bot.ID = "local:bot:evil"
			// What a second read sees proves the copy.
			seenMessage = call.Message().Text
			seenLabel = call.Message().Labels["tampered"]
			if event, ok := call.Event().Payload.(*log.MessagePostedPayload); ok {
				seenPayload = event.Text
			}
			seenWatcherChannel = call.Watcher().Channel
			seenBot = call.Bot().ID
			typ := reflect.TypeOf(*call)
			for index := 0; index < typ.NumField(); index++ {
				if typ.Field(index).PkgPath == "" {
					exported = append(exported, typ.Field(index).Name)
				}
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	postMessage(t, board, "t1", "work", "original", map[string]string{"kind": "alert"})
	if _, err := memory.CreateWatcher(ctx, automation.Watcher{
		Tenant: "t1", ID: "readonly", Channel: "work", Cursor: 1, Enabled: true,
		Action: automation.ActionSpec{Name: "mutate", Params: []byte(`{"text":"original"}`)},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	postMessage(t, board, "t1", "work", "trigger", map[string]string{"kind": "alert"})
	worker, err := automation.NewWatcherWorker(automation.WatcherWorkerConfig{
		Store: memory, Board: board, Actions: registry, Authorize: allowAll{},
		Tenant: "t1", PollInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.Process(ctx); err != nil {
		t.Fatalf("pass: %v", err)
	}
	if len(exported) != 0 {
		t.Fatalf("ActionCall exposes fields %v; Board/write seams must stay unexported", exported)
	}
	if seenMessage != "trigger" || seenLabel != "" || seenPayload != "trigger" {
		t.Fatalf("mutating a read-only view changed it: message=%q label=%q payload=%q", seenMessage, seenLabel, seenPayload)
	}
	if seenWatcherChannel != "work" || seenBot != "local:bot:readonly" {
		t.Fatalf("mutating watcher/bot views changed them: channel=%q bot=%q", seenWatcherChannel, seenBot)
	}
	messages := messagesIn(t, board, "t1", "work")
	for _, message := range messages {
		if message.Text == "tampered" || message.Labels["tampered"] == "yes" {
			t.Fatalf("action mutated the board: %+v", message)
		}
	}
	stored, _, err := memory.Watcher(ctx, "t1", "readonly")
	if err != nil || stored.Channel != "work" || string(stored.Action.Params) != `{"text":"original"}` {
		t.Fatalf("action mutated the watcher: %+v, %v", stored, err)
	}
}

func TestWatcherSubtreeMatchesOnlyWithin(t *testing.T) {
	board := newTestBoard(t)
	ctx := t.Context()
	memory, err := automation.NewMemory(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	var seenChannel string
	registry, err := automation.NewRegistryWith(automation.ActionFunc{
		ActionName: "record",
		Fn: func(_ context.Context, call *automation.ActionCall) error {
			if message := call.Message(); message != nil {
				seenChannel = message.Channel
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	postMessage(t, board, "t1", "work", "root", nil)
	if _, err := memory.CreateWatcher(ctx, automation.Watcher{
		Tenant: "t1", ID: "subtree", Channel: "work/", Cursor: 1, Enabled: true,
		Action: automation.ActionSpec{Name: "record"},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	postMessage(t, board, "t1", "work/deep", "inside", nil)
	postMessage(t, board, "t1", "workshop", "outside", nil)
	if _, err := workerProcess(t, memory, board, registry); err != nil {
		t.Fatalf("pass: %v", err)
	}
	if seenChannel != "work/deep" {
		t.Fatalf("subtree watcher saw %q, want work/deep", seenChannel)
	}
	depth, _ := memory.DeliveryDepth(ctx, "t1", "subtree")
	if depth.Done != 1 {
		t.Fatalf("depth = %+v; an unrelated channel must not match", depth)
	}
}

func workerProcess(t *testing.T, store automation.Store, board *log.Board, registry *automation.Registry) (automation.WorkResult, error) {
	t.Helper()
	worker, err := automation.NewWatcherWorker(automation.WatcherWorkerConfig{
		Store: store, Board: board, Actions: registry, Authorize: allowAll{},
		Tenant: "t1", PollInterval: time.Hour,
	})
	if err != nil {
		return automation.WorkResult{}, err
	}
	return worker.Process(t.Context())
}

func TestWatcherBotAllowlistWorker(t *testing.T) {
	board := newTestBoard(t)
	ctx := t.Context()
	memory, err := automation.NewMemory(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	registry, err := automation.NewRegistryWith(automation.ActionFunc{
		ActionName: "record",
		Fn: func(context.Context, *automation.ActionCall) error {
			calls.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	postMessage(t, board, "t1", "work", "first", nil)
	if _, err := memory.CreateWatcher(ctx, automation.Watcher{
		Tenant: "t1", ID: "blocked", Channel: "work", Cursor: 1, Enabled: true,
		Bot: "local:bot:blocked", Action: automation.ActionSpec{Name: "record"},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	postMessage(t, board, "t1", "work", "second", nil)
	worker, err := automation.NewWatcherWorker(automation.WatcherWorkerConfig{
		Store: memory, Board: board, Actions: registry, Authorize: allowAll{},
		Tenant: "t1", PollInterval: time.Hour,
		BotAllowlist: []string{"local:bot:allowed"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.Process(ctx); err != nil {
		t.Fatalf("pass: %v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("blocked bot action ran %d times", calls.Load())
	}
	depth, _ := memory.DeliveryDepth(ctx, "t1", "blocked")
	if depth.Dead != 1 {
		t.Fatalf("depth = %+v; want a dead letter", depth)
	}
}

func TestScheduleDeletedReplyTargetCancels(t *testing.T) {
	board := newTestBoard(t)
	ctx := t.Context()
	memory, err := automation.NewMemory(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	rootID := postMessage(t, board, "t1", "work", "root", nil)
	id, _ := automation.NewScheduleID()
	if _, err := memory.CreateSchedule(ctx, automation.Schedule{
		Tenant: "t1", ID: id, Channel: "work", Re: &rootID, Text: "reply later",
		Creator: "local:user:alice", NextRunAt: time.Now().Add(-time.Second),
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := board.Update(ctx, "t1", log.UpdateOptions{}, func(tx *log.Tx) error {
		tx.Emit(log.EventMessageDeleted, &log.MessageDeletedPayload{
			ID: rootID, Thread: log.ThreadKey(rootID),
			By: protocol.Author{Actor: "local:user:alice", Principal: "local:user:alice"},
		})
		return nil
	}); err != nil {
		t.Fatalf("delete root: %v", err)
	}
	worker, err := automation.NewScheduleWorker(automation.ScheduleWorkerConfig{
		Store: memory, Board: board, Authorize: allowAll{}, Tenant: "t1", PollInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.Process(ctx); err != nil {
		t.Fatalf("pass: %v", err)
	}
	schedule, _, _ := memory.Schedule(ctx, "t1", id)
	if schedule.Status != automation.ScheduleCanceled || schedule.CancelReason != "message_deleted" {
		t.Fatalf("schedule = %+v, want a message_deleted cancellation", schedule)
	}
	if got := len(messagesIn(t, board, "t1", "work")); got != 1 {
		t.Fatalf("messages = %d, want only the tombstone", got)
	}
}

func TestWatcherFilterUsesEventTimeLabels(t *testing.T) {
	board := newTestBoard(t)
	ctx := t.Context()
	memory, err := automation.NewMemory(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	registry, err := automation.NewRegistryWith(automation.ActionFunc{
		ActionName: "record",
		Fn: func(context.Context, *automation.ActionCall) error {
			calls.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	postMessage(t, board, "t1", "work", "before", nil)
	if _, err := memory.CreateWatcher(ctx, automation.Watcher{
		Tenant: "t1", ID: "labels", Channel: "work", Cursor: 1, Enabled: true,
		Filter: automation.Filter{Types: []string{"message.posted"}, Labels: map[string]string{"kind": "alert"}},
		Action: automation.ActionSpec{Name: "record"},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	alertID := postMessage(t, board, "t1", "work", "alert", map[string]string{"kind": "alert"})
	// Move the label before planning: the post's own labels must decide.
	if err := board.Update(ctx, "t1", log.UpdateOptions{}, func(tx *log.Tx) error {
		tx.Emit(log.EventLabelsChanged, &log.LabelsChangedPayload{
			ID: alertID, Thread: log.ThreadKey(alertID),
			Set: map[string]string{"kind": "other"},
			By:  protocol.Author{Actor: "local:user:alice", Principal: "local:user:alice"},
		})
		return nil
	}); err != nil {
		t.Fatalf("labels: %v", err)
	}
	if _, err := workerProcess(t, memory, board, registry); err != nil {
		t.Fatalf("pass: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want the post matched at its event time", calls.Load())
	}
}

func TestWatcherFilterUsesEventTimeLabelChanges(t *testing.T) {
	board := newTestBoard(t)
	ctx := t.Context()
	memory, err := automation.NewMemory(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	registry, err := automation.NewRegistryWith(automation.ActionFunc{
		ActionName: "record",
		Fn: func(context.Context, *automation.ActionCall) error {
			calls.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	messageID := postMessage(t, board, "t1", "work", "root", nil)
	if _, err := memory.CreateWatcher(ctx, automation.Watcher{
		Tenant: "t1", ID: "changes", Channel: "work", Cursor: 1, Enabled: true,
		Filter: automation.Filter{Types: []string{"labels.changed"}, Labels: map[string]string{"kind": "alert"}},
		Action: automation.ActionSpec{Name: "record"},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, value := range []string{"alert", "other"} {
		value := value
		if err := board.Update(ctx, "t1", log.UpdateOptions{}, func(tx *log.Tx) error {
			tx.Emit(log.EventLabelsChanged, &log.LabelsChangedPayload{
				ID: messageID, Thread: log.ThreadKey(messageID),
				Set: map[string]string{"kind": value},
				By:  protocol.Author{Actor: "local:user:alice", Principal: "local:user:alice"},
			})
			return nil
		}); err != nil {
			t.Fatalf("label %s: %v", value, err)
		}
	}
	if _, err := workerProcess(t, memory, board, registry); err != nil {
		t.Fatalf("pass: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want only the alert label change", calls.Load())
	}
}
