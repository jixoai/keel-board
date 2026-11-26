package log_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jixoai/keel-board/board"
	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/store"
)

func postEvent(channel, text string) log.Event {
	return log.Event{Type: log.EventMessagePosted, Payload: &log.MessagePostedPayload{
		Channel: channel,
		Text:    text,
		Thread:  "",
		Labels:  map[string]string{"status": "open"},
		By:      protocol.Author{Actor: "local:user:alice", Principal: "local:user:alice", Profile: nil},
	}}
}

func TestAppendMonotonicAndReplay(t *testing.T) {
	events := store.NewMemory()
	first := log.New(log.Options{Store: events})

	result, err := first.Append(context.Background(), log.AppendRequest{
		Tenant: "t1",
		Events: []log.Event{postEvent("work", "one"), postEvent("work", "two")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.FirstSeq != 1 || result.LastSeq != 2 || len(result.MessageIDs) != 2 || result.Replayed {
		t.Fatalf("unexpected append result: %+v", result)
	}
	if result.MessageIDs[0] != 1 || result.MessageIDs[1] != 2 {
		t.Fatalf("a post's ID is its event's seq: %+v", result.MessageIDs)
	}
	head, err := first.Head("t1")
	if err != nil || head != 2 {
		t.Fatalf("head = %d, %v", head, err)
	}

	// A board rebuilt from the same store folds the same state.
	second := log.New(log.Options{Store: events})
	err = second.Read("t1", func(snapshot *log.Snapshot) error {
		if snapshot.Head() != 2 {
			t.Fatalf("replayed head = %d", snapshot.Head())
		}
		one := snapshot.Message(1)
		two := snapshot.Message(2)
		if one == nil || two == nil || one.Text != "one" || two.Text != "two" {
			t.Fatalf("replayed messages: %+v / %+v", one, two)
		}
		if channel := snapshot.Channel("work"); channel == nil || channel.Messages != 2 || channel.Threads != 2 {
			t.Fatalf("replayed channel: %+v", channel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestIdempotentAppend(t *testing.T) {
	events := store.NewMemory()
	board := log.New(log.Options{Store: events})
	request := log.AppendRequest{
		Tenant:     "t1",
		Events:     []log.Event{postEvent("work", "once")},
		RequestKey: "post:alice:abc",
		Digest:     "channel=work",
	}
	first, err := board.Append(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := board.Append(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !replay.Replayed || replay.MessageIDs[0] != first.MessageIDs[0] || replay.LastSeq != first.LastSeq {
		t.Fatalf("a retry should answer the first write: %+v vs %+v", replay, first)
	}
	if head, _ := board.Head("t1"); head != 1 {
		t.Fatalf("a retry must not append: head %d", head)
	}

	other := request
	other.Digest = "channel=other"
	_, err = board.Append(context.Background(), other)
	var apiErr *protocol.Error
	if !errors.As(err, &apiErr) || apiErr.Code != protocol.CodeDuplicateID {
		t.Fatalf("expected duplicate_id, got %v", err)
	}
	if apiErr.Status != 409 {
		t.Fatalf("duplicate_id should be a 409, got %d", apiErr.Status)
	}

	// A board rebuilt from the log answers the retry too: the key and digest
	// ride the records.
	rebuilt := log.New(log.Options{Store: events})
	replay, err = rebuilt.Append(context.Background(), request)
	if err != nil || !replay.Replayed || replay.MessageIDs[0] != first.MessageIDs[0] {
		t.Fatalf("a rebuilt board should replay the key: %+v, %v", replay, err)
	}
	if head, _ := rebuilt.Head("t1"); head != 1 {
		t.Fatalf("a replay after restart must not append: head %d", head)
	}
	_, err = rebuilt.Append(context.Background(), other)
	if !errors.As(err, &apiErr) || apiErr.Code != protocol.CodeDuplicateID {
		t.Fatalf("a rebuilt board should refuse another digest: %v", err)
	}
}

func TestConditionalHead(t *testing.T) {
	board := log.New(log.Options{})
	zero, one := uint64(0), uint64(1)
	_, err := board.Append(context.Background(), log.AppendRequest{
		Tenant: "t", Events: []log.Event{postEvent("work", "x")}, ExpectHead: &one,
	})
	if !errors.Is(err, log.ErrHeadMoved) {
		t.Fatalf("expected ErrHeadMoved, got %v", err)
	}
	if head, _ := board.Head("t"); head != 0 {
		t.Fatalf("a refused append must write nothing: head %d", head)
	}
	result, err := board.Append(context.Background(), log.AppendRequest{
		Tenant: "t", Events: []log.Event{postEvent("work", "x")}, ExpectHead: &zero,
	})
	if err != nil || result.LastSeq != 1 {
		t.Fatalf("conditional append: %+v, %v", result, err)
	}
}

func TestUpdateRollsBackOnError(t *testing.T) {
	board := log.New(log.Options{})
	want := errors.New("refused")
	err := board.Update(context.Background(), "t", log.UpdateOptions{}, func(tx *log.Tx) error {
		tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{Channel: "work", Text: "never", Thread: "1"})
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("expected the closure's error, got %v", err)
	}
	head, _ := board.Head("t")
	if head != 0 {
		t.Fatalf("an error must write nothing: head %d", head)
	}
}

func TestConcurrentAppends(t *testing.T) {
	events := store.NewMemory()
	board := log.New(log.Options{Store: events})
	const writers = 64
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := board.Append(context.Background(), log.AppendRequest{
				Tenant: "t",
				Events: []log.Event{postEvent("work", "concurrent")},
			})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent append: %v", err)
		}
	}
	head, err := board.Head("t")
	if err != nil || head != writers {
		t.Fatalf("head = %d, want %d (%v)", head, writers, err)
	}
	seen := map[uint64]bool{}
	err = board.Read("t", func(snapshot *log.Snapshot) error {
		for id := uint64(1); id <= writers; id++ {
			message := snapshot.Message(id)
			if message == nil || message.Text != "concurrent" {
				t.Fatalf("message %d missing: %+v", id, message)
			}
			if seen[id] {
				t.Fatalf("seq %d repeated", id)
			}
			seen[id] = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestFoldTaskEventsAndReplay(t *testing.T) {
	events := store.NewMemory()
	first := log.New(log.Options{Store: events})
	until := protocol.FormatTime(time.Now().Add(time.Hour))
	hash := board.HashToken("0123456789012345678901234567890123456789012")
	author := protocol.Author{Actor: "local:user:alice", Principal: "local:user:alice", Profile: nil}
	err := first.Update(context.Background(), "t", log.UpdateOptions{}, func(tx *log.Tx) error {
		id := tx.NextID(0)
		tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
			ID: id, Channel: "work", Text: "task", Thread: log.ThreadKey(id), Labels: map[string]string{}, By: author,
		})
		tx.Emit(log.EventTaskCreated, &log.TaskCreatedPayload{ID: id, Thread: log.ThreadKey(id), By: author})
		tx.Emit(log.EventTaskClaimed, &log.TaskClaimedPayload{
			ID: id, Thread: log.ThreadKey(id), Epoch: 1, Until: until, Token: hash, By: author,
		})
		tx.Emit(log.EventMessageEdited, &log.MessageEditedPayload{ID: id, Thread: log.ThreadKey(id), Text: "task, edited", By: author})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	check := func(snapshot *log.Snapshot) error {
		message := snapshot.Message(1)
		if message == nil || message.Text != "task, edited" {
			t.Fatalf("message: %+v", message)
		}
		if message.Revision != 4 || message.Version != 4 {
			t.Fatalf("version %d revision %d, want 4 and 4", message.Version, message.Revision)
		}
		task := message.Task
		if task == nil || task.State != "claimed" || task.Epoch != 1 || task.Claim == nil {
			t.Fatalf("task: %+v", task)
		}
		if task.Claim.Until.Unix() != mustUnix(t, until) {
			t.Fatalf("claim until %v", task.Claim.Until)
		}
		if message.Labels[protocol.TaskLabel] != "claimed" {
			t.Fatalf("state label: %+v", message.Labels)
		}
		return nil
	}
	if err := first.Read("t", check); err != nil {
		t.Fatal(err)
	}
	// The same state comes back from the persisted log.
	second := log.New(log.Options{Store: events})
	if err := second.Read("t", check); err != nil {
		t.Fatal(err)
	}
}

func mustUnix(t *testing.T, value string) int64 {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed.Unix()
}

func TestWaitAndHistory(t *testing.T) {
	board := log.New(log.Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- board.Wait(ctx, "t", 0) }()
	// Give the waiter a moment to park, then append.
	time.Sleep(10 * time.Millisecond)
	if _, err := board.Append(context.Background(), log.AppendRequest{Tenant: "t", Events: []log.Event{postEvent("work", "wake")}}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("wait: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("wait didn't wake on an append")
	}
	history, err := board.History("t", 1, 1)
	if err != nil || len(history) != 1 || history[0].Seq != 1 {
		t.Fatalf("history: %+v, %v", history, err)
	}
	tenantHead, _ := board.Head("other")
	if tenantHead != 0 {
		t.Fatalf("another tenant has a head: %d", tenantHead)
	}
}

func TestTenantLogsAreSeparate(t *testing.T) {
	board := log.New(log.Options{})
	if _, err := board.Append(context.Background(), log.AppendRequest{Tenant: "a", Events: []log.Event{postEvent("work", "a")}}); err != nil {
		t.Fatal(err)
	}
	if _, err := board.Append(context.Background(), log.AppendRequest{Tenant: "b", Events: []log.Event{postEvent("work", "b")}}); err != nil {
		t.Fatal(err)
	}
	err := board.Read("a", func(snapshot *log.Snapshot) error {
		if snapshot.Head() != 1 {
			t.Fatalf("tenant a head %d", snapshot.Head())
		}
		if message := snapshot.Message(1); message == nil || message.Text != "a" {
			t.Fatalf("tenant a message: %+v", message)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
