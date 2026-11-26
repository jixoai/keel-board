package sync_test

import (
	"context"
	"errors"
	"testing"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
)

func inboxEvent(id, thread string) boardsync.Event {
	return boardsync.Event{
		ID: id, Key: "slack", Platform: "slack",
		Kind: boardsync.KindMessage, Type: boardsync.TypeMessage,
		Channel: "C0123", Thread: thread, Message: id,
		Text: "hello", Author: boardsync.Author{ID: "slack:user:U1", Handle: "ada", Name: "Ada"},
	}
}

func TestInboxDedupesAndOrders(t *testing.T) {
	inbox := boardsync.NewMemoryInbox(boardsync.RetryPolicy{MaxAttempts: 3, BaseDelay: 0}, time.Minute)
	ctx := context.Background()
	first := inboxEvent("m1", "C0123:T1")
	second := inboxEvent("m2", "C0123:T1")
	other := inboxEvent("m3", "C0123:T2")
	records := []boardsync.Record{
		{Dedupe: "d1", Event: first},
		{Dedupe: "d2", Event: second},
		{Dedupe: "d3", Event: other},
		{Dedupe: "d1", Event: first},
	}
	added, err := inbox.Push(ctx, "slack", records)
	if err != nil || added != 3 {
		t.Fatalf("Push = %d, %v; want 3 added", added, err)
	}
	if added, err := inbox.Push(ctx, "slack", []boardsync.Record{{Dedupe: "d1", Event: first}}); err != nil || added != 0 {
		t.Fatalf("redelivery added %d, %v", added, err)
	}

	leased, err := inbox.Lease(ctx, "slack", 10)
	if err != nil || len(leased) != 2 {
		t.Fatalf("Lease = %+v, %v; want one per thread", leased, err)
	}
	if leased[0].ID != "d1" || leased[1].ID != "d3" {
		t.Fatalf("lease order = %v", []string{leased[0].ID, leased[1].ID})
	}
	// The second event of thread T1 waits behind the first.
	if secondLease, _ := inbox.Lease(ctx, "slack", 10); len(secondLease) != 0 {
		t.Fatalf("leased a thread twice: %+v", secondLease)
	}
	if err := inbox.Ack(ctx, "slack", "d1", leased[0].Lease, nil); err != nil {
		t.Fatal(err)
	}
	next, err := inbox.Lease(ctx, "slack", 10)
	if err != nil || len(next) != 1 || next[0].ID != "d2" {
		t.Fatalf("after ack = %+v, %v; want d2", next, err)
	}
	if err := inbox.Ack(ctx, "slack", "d3", leased[1].Lease, nil); err != nil {
		t.Fatal(err)
	}
	if err := inbox.Ack(ctx, "slack", "d2", next[0].Lease, nil); err != nil {
		t.Fatal(err)
	}
	if inbox.Pending("slack") != 0 {
		t.Fatalf("pending = %d", inbox.Pending("slack"))
	}
}

func TestInboxRetryAndDeadLetter(t *testing.T) {
	inbox := boardsync.NewMemoryInbox(boardsync.RetryPolicy{MaxAttempts: 2, BaseDelay: 0}, time.Minute)
	ctx := context.Background()
	if _, err := inbox.Push(ctx, "slack", []boardsync.Record{{Dedupe: "d1", Event: inboxEvent("m1", "C0123:T1")}}); err != nil {
		t.Fatal(err)
	}
	leased, err := inbox.Lease(ctx, "slack", 1)
	if err != nil || len(leased) != 1 {
		t.Fatalf("Lease = %+v, %v", leased, err)
	}
	if err := inbox.Ack(ctx, "slack", "d1", leased[0].Lease, errors.New("boom")); err != nil {
		t.Fatal(err)
	}
	leased, err = inbox.Lease(ctx, "slack", 1)
	if err != nil || len(leased) != 1 || leased[0].Attempts != 1 {
		t.Fatalf("retry lease = %+v, %v", leased, err)
	}
	if err := inbox.Ack(ctx, "slack", "d1", leased[0].Lease, errors.New("boom again")); err != nil {
		t.Fatal(err)
	}
	if inbox.Pending("slack") != 0 {
		t.Fatalf("pending = %d, want dead-lettered", inbox.Pending("slack"))
	}
	dead := inbox.Dead("slack")
	if len(dead) != 1 || dead[0].Attempts != 2 {
		t.Fatalf("dead = %+v", dead)
	}
}

func TestInboxLeaseExpiry(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	inbox := boardsync.NewMemoryInbox(boardsync.RetryPolicy{MaxAttempts: 3, BaseDelay: 0}, time.Second).
		WithClock(func() time.Time { return now })
	ctx := context.Background()
	if _, err := inbox.Push(ctx, "slack", []boardsync.Record{{Dedupe: "d1", Event: inboxEvent("m1", "C0123:T1")}}); err != nil {
		t.Fatal(err)
	}
	first, err := inbox.Lease(ctx, "slack", 1)
	if err != nil || len(first) != 1 {
		t.Fatalf("first lease = %+v, %v", first, err)
	}
	now = now.Add(2 * time.Second)
	second, err := inbox.Lease(ctx, "slack", 1)
	if err != nil || len(second) != 1 {
		t.Fatalf("second lease = %+v, %v", second, err)
	}
	if first[0].Lease == second[0].Lease {
		t.Fatal("lease was not renewed")
	}
	if err := inbox.Ack(ctx, "slack", "d1", first[0].Lease, nil); !errors.Is(err, boardsync.ErrLeaseLost) {
		t.Fatalf("stale ack = %v, want ErrLeaseLost", err)
	}
	if err := inbox.Ack(ctx, "slack", "d1", second[0].Lease, nil); err != nil {
		t.Fatal(err)
	}
}

func TestInboxRejectsPartialPush(t *testing.T) {
	inbox := boardsync.NewMemoryInbox(boardsync.DefaultRetry, time.Minute)
	ctx := context.Background()
	_, err := inbox.Push(ctx, "slack", []boardsync.Record{
		{Dedupe: "d1", Event: inboxEvent("m1", "C0123:T1")},
		{Dedupe: "d2", Event: boardsync.Event{Key: "slack"}},
	})
	if err == nil {
		t.Fatal("a malformed record should refuse the push")
	}
	if inbox.Pending("slack") != 0 {
		t.Fatalf("partial push stored %d items", inbox.Pending("slack"))
	}
}
