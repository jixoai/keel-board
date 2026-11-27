package log

import (
	"sync"
	"testing"
	"time"

	"github.com/jixoai/keel-board/protocol"
)

// captureObserver records the board's bounded observations.
type captureObserver struct {
	mu      sync.Mutex
	kinds   [][]EventType
	claims  []int
	durs    []time.Duration
	rebuilt int
}

func (c *captureObserver) Appended(_ string, kinds []EventType, liveClaims int, took time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.kinds = append(c.kinds, append([]EventType(nil), kinds...))
	c.claims = append(c.claims, liveClaims)
	c.durs = append(c.durs, took)
}

func (c *captureObserver) Rebuilt(took time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rebuilt++
	c.durs = append(c.durs, took)
}

func (c *captureObserver) snapshot() ([][]EventType, []int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]EventType(nil), c.kinds...), append([]int(nil), c.claims...), c.rebuilt
}

func TestObserverSeesCommittedAppendsAndRebuild(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	observer := &captureObserver{}
	board := New(Options{Now: clock, Observer: observer})
	ctx := t.Context()

	if _, err := board.Append(ctx, AppendRequest{Events: []Event{{
		Type:    EventMessagePosted,
		Payload: &MessagePostedPayload{Channel: "general", Text: "hello"},
	}}}); err != nil {
		t.Fatalf("append post: %v", err)
	}
	kinds, claims, _ := observer.snapshot()
	if len(kinds) != 1 || len(kinds[0]) != 1 || kinds[0][0] != EventMessagePosted {
		t.Fatalf("observer kinds = %+v", kinds)
	}
	if len(claims) != 1 || claims[0] != 0 {
		t.Fatalf("observer claims = %+v", claims)
	}

	// A live claim is reported as one.
	until := protocol.FormatTime(now.Add(time.Hour))
	if _, err := board.Append(ctx, AppendRequest{Events: []Event{
		{Type: EventTaskCreated, Payload: &TaskCreatedPayload{ID: 1, Thread: "general"}},
		{Type: EventTaskClaimed, Payload: &TaskClaimedPayload{ID: 1, Thread: "general",
			Epoch: 1, Until: until, Token: "hash", By: Author{Actor: "local:user:dev", Principal: "local:user:dev"}}},
	}}); err != nil {
		t.Fatalf("append task: %v", err)
	}
	_, claims, _ = observer.snapshot()
	if len(claims) != 2 || claims[1] != 1 {
		t.Fatalf("live claims after a claim = %+v", claims)
	}

	// An expired claim is not live.
	clock = func() time.Time { return now.Add(2 * time.Hour) }
	board.SetNow(clock)
	if _, err := board.Append(ctx, AppendRequest{Events: []Event{{
		Type: EventTaskExpired, Payload: &TaskExpiredPayload{ID: 1, Thread: "general", Epoch: 1},
	}}}); err != nil {
		t.Fatalf("append expiry: %v", err)
	}
	kinds, claims, _ = observer.snapshot()
	if len(kinds) != 3 || kinds[2][0] != EventTaskExpired {
		t.Fatalf("expiry kinds = %+v", kinds)
	}
	if len(claims) != 3 || claims[2] != 0 {
		t.Fatalf("live claims after expiry = %+v", claims)
	}

	// A replayed keyed write lands nothing and is not observed twice.
	request := AppendRequest{
		RequestKey: "k1", Digest: "d1",
		Events: []Event{{Type: EventMessagePosted, Payload: &MessagePostedPayload{Channel: "general", Text: "keyed"}}},
	}
	if _, err := board.Append(ctx, request); err != nil {
		t.Fatalf("append keyed: %v", err)
	}
	if _, err := board.Append(ctx, request); err != nil {
		t.Fatalf("replay keyed: %v", err)
	}
	kinds, _, _ = observer.snapshot()
	if len(kinds) != 4 {
		t.Fatalf("a replay was observed as an append: %d observations", len(kinds))
	}

	if _, err := board.Rebuild(ctx, "default"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if _, _, rebuilt := observer.snapshot(); rebuilt != 1 {
		t.Fatalf("observer rebuilds = %d, want 1", rebuilt)
	}
}

func TestObserverIsOptional(t *testing.T) {
	board := New(Options{})
	ctx := t.Context()
	if _, err := board.Append(ctx, AppendRequest{Events: []Event{{
		Type:    EventMessagePosted,
		Payload: &MessagePostedPayload{Channel: "general", Text: "hello"},
	}}}); err != nil {
		t.Fatalf("append without an observer: %v", err)
	}
	observer := &captureObserver{}
	board.SetObserver(observer)
	if _, err := board.Append(ctx, AppendRequest{Events: []Event{{
		Type:    EventMessagePosted,
		Payload: &MessagePostedPayload{Channel: "general", Text: "second"},
	}}}); err != nil {
		t.Fatalf("append with an observer: %v", err)
	}
	if kinds, _, _ := observer.snapshot(); len(kinds) != 1 {
		t.Fatalf("observations after SetObserver = %d, want 1", len(kinds))
	}
}

// panicObserver is a broken instrumentation implementation.
type panicObserver struct{}

func (panicObserver) Appended(string, []EventType, int, time.Duration) { panic("observer bug") }
func (panicObserver) Rebuilt(time.Duration)                            { panic("observer bug") }

// TestPanickingObserverDoesNotFailWork proves instrumentation cannot undo a
// committed append or fail a rebuild.
func TestPanickingObserverDoesNotFailWork(t *testing.T) {
	board := New(Options{Observer: panicObserver{}})
	ctx := t.Context()
	if _, err := board.Append(ctx, AppendRequest{Events: []Event{{
		Type:    EventMessagePosted,
		Payload: &MessagePostedPayload{Channel: "general", Text: "hello"},
	}}}); err != nil {
		t.Fatalf("append with a panicking observer: %v", err)
	}
	if _, err := board.Rebuild(ctx, "default"); err != nil {
		t.Fatalf("rebuild with a panicking observer: %v", err)
	}
}

// BenchmarkLiveClaims records the per-append scan's cost on a board with
// 10,000 tasks.
func BenchmarkLiveClaims(b *testing.B) {
	now := time.Now()
	state := NewState()
	for id := uint64(1); id <= 10000; id++ {
		state.Messages[id] = &Message{ID: id, Task: &Task{
			State: "claimed",
			Claim: &Claim{Actor: "local:user:dev", Until: now.Add(time.Hour)},
		}}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := liveClaims(state, now); got != 10000 {
			b.Fatalf("liveClaims = %d", got)
		}
	}
}
