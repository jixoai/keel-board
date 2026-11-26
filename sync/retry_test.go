package sync_test

import (
	"context"
	"errors"
	"testing"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
)

func TestRetryDo(t *testing.T) {
	policy := boardsync.RetryPolicy{MaxAttempts: 3, BaseDelay: 0, MaxDelay: 0}
	attempts := 0
	err := boardsync.Do(context.Background(), policy, func(context.Context) error {
		attempts++
		if attempts < 3 {
			return errors.New("not yet")
		}
		return nil
	})
	if err != nil || attempts != 3 {
		t.Fatalf("Do = %v after %d attempts", err, attempts)
	}

	attempts = 0
	err = boardsync.Do(context.Background(), policy, func(context.Context) error {
		attempts++
		return errors.New("always")
	})
	if err == nil || attempts != 3 {
		t.Fatalf("Do = %v after %d attempts, want the last failure after 3", err, attempts)
	}

	attempts = 0
	err = boardsync.Do(context.Background(), policy, func(context.Context) error {
		attempts++
		return boardsync.Permanent(errors.New("no"))
	})
	if err == nil || attempts != 1 || !boardsync.IsPermanent(err) {
		t.Fatalf("permanent Do = %v after %d attempts", err, attempts)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	attempts = 0
	err = boardsync.Do(ctx, policy, func(context.Context) error {
		attempts++
		return errors.New("no")
	})
	if !errors.Is(err, context.Canceled) || attempts != 0 {
		t.Fatalf("canceled Do = %v after %d attempts", err, attempts)
	}
}

func TestRetryPolicyDelay(t *testing.T) {
	policy := boardsync.RetryPolicy{MaxAttempts: 5, BaseDelay: time.Second, MaxDelay: 3 * time.Second}
	if got := policy.Delay(1); got != time.Second {
		t.Fatalf("delay 1 = %s", got)
	}
	if got := policy.Delay(2); got != 2*time.Second {
		t.Fatalf("delay 2 = %s", got)
	}
	if got := policy.Delay(10); got != 3*time.Second {
		t.Fatalf("delay 10 = %s", got)
	}
	bad := boardsync.RetryPolicy{MaxAttempts: 0, BaseDelay: time.Second}
	if _, err := bad.Validate(); err == nil {
		t.Fatal("a policy with no attempts should be refused")
	}
}

func TestOutboxLaneOrderAndDeadLetter(t *testing.T) {
	policy := boardsync.RetryPolicy{MaxAttempts: 2, BaseDelay: 0, MaxDelay: 0}
	outbox := boardsync.NewOutbox(policy, 10)
	var order []string
	for _, name := range []string{"a1", "a2", "a3"} {
		name := name
		if err := outbox.Enqueue("lane-a", func(context.Context) error {
			order = append(order, name)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := outbox.Enqueue("lane-b", func(context.Context) error {
		order = append(order, "b1")
		return boardsync.Permanent(errors.New("bad"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := outbox.Enqueue("lane-b", func(context.Context) error {
		order = append(order, "b2")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	result := outbox.Drain(context.Background())
	if result.Done != 4 || result.Dead != 1 || result.Err != nil {
		t.Fatalf("Drain = %+v", result)
	}
	if got := order; len(got) != 5 || got[0] != "a1" || got[1] != "a2" || got[2] != "a3" || got[3] != "b1" || got[4] != "b2" {
		t.Fatalf("order = %v", got)
	}
	dead := outbox.DeadLetters()
	if len(dead) != 1 || dead[0].Lane != "lane-b" || dead[0].Attempts != 1 {
		t.Fatalf("dead letters = %+v", dead)
	}
	if outbox.Len() != 0 {
		t.Fatalf("outbox left %d jobs", outbox.Len())
	}
}

func TestOutboxRetriesThenSucceeds(t *testing.T) {
	policy := boardsync.RetryPolicy{MaxAttempts: 3, BaseDelay: 0, MaxDelay: 0}
	outbox := boardsync.NewOutbox(policy, 10)
	attempts := 0
	if err := outbox.Enqueue("lane", func(context.Context) error {
		attempts++
		if attempts < 3 {
			return errors.New("again")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	result := outbox.Drain(context.Background())
	if result.Done != 1 || result.Retried != 2 || result.Dead != 0 {
		t.Fatalf("Drain = %+v after %d attempts", result, attempts)
	}
}

func TestOutboxBackpressure(t *testing.T) {
	outbox := boardsync.NewOutbox(boardsync.DefaultRetry, 1)
	noop := func(context.Context) error { return nil }
	if err := outbox.Enqueue("lane", noop); err != nil {
		t.Fatal(err)
	}
	if err := outbox.Enqueue("lane", noop); !errors.Is(err, boardsync.ErrOutboxFull) {
		t.Fatalf("second enqueue = %v, want ErrOutboxFull", err)
	}
}
