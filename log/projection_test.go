package log

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/store"
)

// projectionAuthor is the actor the projection tests write as.
var projectionAuthor = protocol.Author{Actor: "local:user:alice", Principal: "local:user:alice"}

// newProjectionBoard opens a board over events with a fixed clock and no
// freshness check; the tests drive catch-up themselves.
func newProjectionBoard(events store.EventStore) *Board {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return New(Options{Store: events, Freshness: -1, Now: func() time.Time { return now }})
}

// seedProjection writes a message, a reply, labels, a task and a claim, so the
// digest covers every folded collection the board has.
func seedProjection(t *testing.T, board *Board, tenant string) (root, reply uint64) {
	t.Helper()
	ctx := context.Background()
	err := board.Update(ctx, tenant, UpdateOptions{}, func(tx *Tx) error {
		tx.Emit(EventActorSeen, &ActorSeenPayload{Actor: projectionAuthor.Actor, Kind: "person", Handle: "alice", Name: "alice", Verified: true})
		root = tx.NextID(1)
		tx.Emit(EventMessagePosted, &MessagePostedPayload{
			ID: root, Channel: "work", Text: "root", Thread: ThreadKey(root),
			Labels: map[string]string{"step": "fix"}, By: projectionAuthor,
		})
		tx.RecordResult(root)
		return nil
	})
	if err != nil {
		t.Fatalf("seed root: %v", err)
	}
	err = board.Update(ctx, tenant, UpdateOptions{}, func(tx *Tx) error {
		reply = tx.NextID(0)
		tx.Emit(EventMessagePosted, &MessagePostedPayload{
			ID: reply, Channel: "work", Text: "reply", Re: &root, Thread: ThreadKey(root),
			By: projectionAuthor,
		})
		tx.RecordResult(reply)
		tx.Emit(EventReactionAdded, &ReactionPayload{ID: root, Emoji: "thumbs_up", By: projectionAuthor})
		return nil
	})
	if err != nil {
		t.Fatalf("seed reply: %v", err)
	}
	err = board.Update(ctx, tenant, UpdateOptions{}, func(tx *Tx) error {
		tx.Emit(EventTaskCreated, &TaskCreatedPayload{ID: root, Thread: ThreadKey(root), By: projectionAuthor})
		tx.Emit(EventTaskClaimed, &TaskClaimedPayload{
			ID: root, Thread: ThreadKey(root), Epoch: 1,
			Until: protocol.FormatTime(time.Now().Add(time.Hour)), Token: "hash", By: projectionAuthor,
		})
		return nil
	})
	if err != nil {
		t.Fatalf("seed task: %v", err)
	}
	return root, reply
}

// corruptProjection rewrites a cached message as a broken fold would leave it.
func corruptProjection(t *testing.T, board *Board, tenant string, id uint64) {
	t.Helper()
	board.mu.Lock()
	defer board.mu.Unlock()
	current := board.tenants[tenant]
	if current == nil || current.state.Messages[id] == nil {
		t.Fatalf("no cached message #%d for %s", id, tenant)
	}
	current.state.Messages[id].Text = "corrupted"
}

// lagProjection replaces a cached projection with a replay through seq,
// modelling a cache that stopped early.
func lagProjection(t *testing.T, board *Board, tenant string, through uint64) {
	t.Helper()
	ctx := context.Background()
	records, err := board.store.Load(ctx, tenant)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	prefix, err := foldRecords(records, through)
	if err != nil {
		t.Fatalf("folding through #%d: %v", through, err)
	}
	board.mu.Lock()
	defer board.mu.Unlock()
	current := board.tenants[tenant]
	current.state = prefix
	current.events = current.events[:0]
	for _, record := range records {
		if record.Seq > through {
			break
		}
		event, err := decodeEvent(record)
		if err != nil {
			t.Fatalf("decoding #%d: %v", record.Seq, err)
		}
		current.events = append(current.events, event)
	}
	current.idem = make(map[string]idemEntry)
	current.checked = time.Time{}
}

// messageText reads one message's text from the live projection.
func messageText(t *testing.T, board *Board, tenant string, id uint64) string {
	t.Helper()
	text := ""
	if err := board.Read(tenant, func(snapshot *Snapshot) error {
		if message := snapshot.Message(id); message != nil {
			text = message.Text
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
	return text
}

// recordCount returns how many events a tenant's store holds.
func recordCount(t *testing.T, events store.EventStore, tenant string) int {
	t.Helper()
	records, err := events.Load(context.Background(), tenant)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return len(records)
}

func TestProjectionDigestMatchesReplay(t *testing.T) {
	ctx := context.Background()
	events := store.NewMemory()
	board := newProjectionBoard(events)
	root, _ := seedProjection(t, board, "default")

	report, err := board.CheckProjection(ctx, "default")
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !report.Consistent {
		t.Fatalf("a live projection should be consistent: %+v", report)
	}
	if report.Digest == "" || report.Digest != report.ReplayDigest || report.Digest != report.StoreDigest {
		t.Fatalf("digests disagree: %+v", report)
	}
	if report.LogHead == 0 || report.ProjectionHead != report.LogHead || report.HeadRow != report.LogHead {
		t.Fatalf("watermarks: %+v", report)
	}

	// A new process folds the same log to the same digest.
	replayed := newProjectionBoard(events)
	other, err := replayed.CheckProjection(ctx, "default")
	if err != nil {
		t.Fatalf("replay check: %v", err)
	}
	if !other.Consistent || other.Digest != report.Digest {
		t.Fatalf("a replayed projection differs: %+v vs %+v", other, report)
	}

	// The digest is content-sensitive: a broken message changes it.
	corruptProjection(t, board, "default", root)
	corrupt, err := board.CheckProjection(ctx, "default")
	if err != nil {
		t.Fatalf("check after corruption: %v", err)
	}
	if corrupt.Consistent || corrupt.Digest == corrupt.ReplayDigest {
		t.Fatalf("corruption wasn't detected: %+v", corrupt)
	}
}

func TestCheckProjectionReportsDriftAndRebuildRepairs(t *testing.T) {
	ctx := context.Background()
	events := store.NewMemory()
	board := newProjectionBoard(events)
	root, reply := seedProjection(t, board, "default")
	before := recordCount(t, events, "default")

	corruptProjection(t, board, "default", root)
	report, err := board.CheckProjection(ctx, "default")
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if report.Consistent {
		t.Fatalf("drift wasn't reported: %+v", report)
	}
	if !hasProblem(report, "digest") {
		t.Fatalf("expected a digest problem: %+v", report.Problems)
	}

	rebuilt, err := board.Rebuild(ctx, "default")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if !rebuilt.Consistent {
		t.Fatalf("a rebuild should be consistent: %+v", rebuilt)
	}
	if rebuilt.Digest != rebuilt.ReplayDigest || rebuilt.Digest != rebuilt.StoreDigest {
		t.Fatalf("rebuilt digests disagree: %+v", rebuilt)
	}
	if rebuilt.ProjectionHead != rebuilt.LogHead {
		t.Fatalf("rebuilt watermark: %+v", rebuilt)
	}
	if text := messageText(t, board, "default", root); text != "root" {
		t.Fatalf("the rebuilt message text is %q", text)
	}
	if text := messageText(t, board, "default", reply); text != "reply" {
		t.Fatalf("the rebuilt reply text is %q", text)
	}
	if after := recordCount(t, events, "default"); after != before {
		t.Fatalf("a rebuild changed the log: %d -> %d events", before, after)
	}

	// A repeated rebuild is idempotent and makes no events.
	again, err := board.Rebuild(ctx, "default")
	if err != nil {
		t.Fatalf("second rebuild: %v", err)
	}
	if !again.Consistent || again.Digest != rebuilt.Digest {
		t.Fatalf("a repeated rebuild changed the projection: %+v vs %+v", again, rebuilt)
	}
	if after := recordCount(t, events, "default"); after != before {
		t.Fatalf("a repeated rebuild changed the log: %d -> %d events", before, after)
	}
}

func TestCheckProjectionReportsLagAndRebuildCatchesUp(t *testing.T) {
	ctx := context.Background()
	events := store.NewMemory()
	board := newProjectionBoard(events)
	root, reply := seedProjection(t, board, "default")
	records, err := events.Load(ctx, "default")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	through := records[0].Seq
	lagProjection(t, board, "default", through)

	report, err := board.CheckProjection(ctx, "default")
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if report.Consistent {
		t.Fatalf("lag wasn't reported: %+v", report)
	}
	if report.ProjectionHead != through || report.LogHead != uint64(len(records)) {
		t.Fatalf("watermarks: %+v", report)
	}
	if !hasProblem(report, "lags") {
		t.Fatalf("expected a lag problem: %+v", report.Problems)
	}
	if report.Digest != report.ReplayDigest {
		t.Fatalf("a lagging prefix should still match its own replay: %+v", report)
	}
	if messageText(t, board, "default", reply) != "" {
		t.Fatal("a lagging projection should not have the later message")
	}

	if _, err := board.Rebuild(ctx, "default"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if text := messageText(t, board, "default", reply); text != "reply" {
		t.Fatalf("the caught-up reply is %q", text)
	}
	if text := messageText(t, board, "default", root); text != "root" {
		t.Fatalf("the caught-up root is %q", text)
	}
}

func TestCheckProjectionReportsAheadAndRefreshRefuses(t *testing.T) {
	ctx := context.Background()
	events := store.NewMemory()
	board := newProjectionBoard(events)
	seedProjection(t, board, "default")

	board.mu.Lock()
	board.tenants["default"].state.Head += 5
	board.mu.Unlock()

	report, err := board.CheckProjection(ctx, "default")
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if report.Consistent || !hasProblem(report, "ahead") {
		t.Fatalf("ahead wasn't reported: %+v", report)
	}
	if err := board.Refresh(ctx, "default"); !errors.Is(err, ErrProjectionAhead) {
		t.Fatalf("refresh should refuse an ahead projection: %v", err)
	}
	if _, err := board.Rebuild(ctx, "default"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	report, err = board.CheckProjection(ctx, "default")
	if err != nil || !report.Consistent {
		t.Fatalf("rebuilt check: %+v, %v", report, err)
	}
}

// flakyStore fails one Load with the flag set, as an interrupted rebuild's
// read would.
type flakyStore struct {
	*store.Memory
	fail atomic.Bool
}

func (f *flakyStore) Load(ctx context.Context, tenant string) ([]store.Record, error) {
	if f.fail.CompareAndSwap(true, false) {
		return nil, errors.New("injected load failure")
	}
	return f.Memory.Load(ctx, tenant)
}

func TestRebuildKeepsTheProjectionWhenLoadFails(t *testing.T) {
	ctx := context.Background()
	memory := store.NewMemory()
	events := &flakyStore{Memory: memory}
	board := newProjectionBoard(events)
	root, _ := seedProjection(t, board, "default")
	corruptProjection(t, board, "default", root)

	events.fail.Store(true)
	if _, err := board.Rebuild(ctx, "default"); err == nil {
		t.Fatal("an interrupted rebuild should fail")
	}
	// The old projection still serves; no half-rebuilt state replaced it.
	if text := messageText(t, board, "default", root); text != "corrupted" {
		t.Fatalf("the interrupted rebuild replaced the cache: %q", text)
	}
	report, err := board.CheckProjection(ctx, "default")
	if err != nil || report.Consistent {
		t.Fatalf("the check should still see the old projection: %+v, %v", report, err)
	}

	rebuilt, err := board.Rebuild(ctx, "default")
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if !rebuilt.Consistent || messageText(t, board, "default", root) != "root" {
		t.Fatalf("the retry didn't rebuild: %+v", rebuilt)
	}
}

func TestRebuildWakesWaiters(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	events := store.NewMemory()
	board := newProjectionBoard(events)
	seedProjection(t, board, "default")
	head, err := board.Head("default")
	if err != nil {
		t.Fatalf("head: %v", err)
	}

	waited := make(chan error, 1)
	go func() { waited <- board.Wait(ctx, "default", head) }()
	time.Sleep(20 * time.Millisecond)
	if _, err := board.Rebuild(context.Background(), "default"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	select {
	case err := <-waited:
		t.Fatalf("a rebuild at the same head shouldn't release the waiter: %v", err)
	case <-time.After(30 * time.Millisecond):
	}

	// A write after the rebuild still reaches the waiter that holds the
	// pre-rebuild tenant pointer.
	if err := board.Update(context.Background(), "default", UpdateOptions{}, func(tx *Tx) error {
		id := tx.NextID(0)
		tx.Emit(EventMessagePosted, &MessagePostedPayload{
			ID: id, Channel: "work", Text: "after", Thread: ThreadKey(id),
			By: projectionAuthor, Labels: map[string]string{},
		})
		return nil
	}); err != nil {
		t.Fatalf("append after rebuild: %v", err)
	}
	select {
	case err := <-waited:
		if err != nil {
			t.Fatalf("wait: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("the waiter didn't see the write after a rebuild")
	}
}

func TestRebuildWithConcurrentWrites(t *testing.T) {
	ctx := context.Background()
	events := store.NewMemory()
	board := newProjectionBoard(events)
	const writers = 25
	errs := make(chan error, 1)
	var wait sync.WaitGroup
	wait.Add(1)
	go func() {
		defer wait.Done()
		for i := 0; i < writers; i++ {
			_, err := board.Append(ctx, AppendRequest{Tenant: "default", Events: []Event{{
				Type: EventMessagePosted, Payload: &MessagePostedPayload{
					Channel: "work", Text: fmt.Sprintf("write %d", i), Thread: "",
					By: projectionAuthor, Labels: map[string]string{},
				},
			}}})
			if err != nil {
				select {
				case errs <- err:
				default:
				}
				return
			}
		}
	}()
	for i := 0; i < 10; i++ {
		if _, err := board.Rebuild(ctx, "default"); err != nil {
			t.Fatalf("rebuild: %v", err)
		}
	}
	wait.Wait()
	select {
	case err := <-errs:
		t.Fatalf("concurrent write: %v", err)
	default:
	}
	report, err := board.CheckProjection(ctx, "default")
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !report.Consistent {
		t.Fatalf("a rebuild lost a concurrent write: %+v", report)
	}
	if got := recordCount(t, events, "default"); got != writers {
		t.Fatalf("expected %d events after concurrent writes, got %d", writers, got)
	}
}

func TestRebuildIsTenantScoped(t *testing.T) {
	ctx := context.Background()
	events := store.NewMemory()
	board := newProjectionBoard(events)
	rootA, _ := seedProjection(t, board, "a")
	seedProjection(t, board, "b")

	beforeB, err := board.CheckProjection(ctx, "b")
	if err != nil || !beforeB.Consistent {
		t.Fatalf("check b: %+v, %v", beforeB, err)
	}
	corruptProjection(t, board, "a", rootA)
	reportA, err := board.CheckProjection(ctx, "a")
	if err != nil || reportA.Consistent {
		t.Fatalf("check a: %+v, %v", reportA, err)
	}
	reportB, err := board.CheckProjection(ctx, "b")
	if err != nil || !reportB.Consistent || reportB.Digest != beforeB.Digest {
		t.Fatalf("tenant b changed by tenant a's check: %+v, %v", reportB, err)
	}

	if _, err := board.Rebuild(ctx, "a"); err != nil {
		t.Fatalf("rebuild a: %v", err)
	}
	afterB, err := board.CheckProjection(ctx, "b")
	if err != nil || !afterB.Consistent || afterB.Digest != beforeB.Digest {
		t.Fatalf("tenant b changed by tenant a's rebuild: %+v, %v", afterB, err)
	}
}

func TestProjectionDigestIgnoresMapOrder(t *testing.T) {
	events := store.NewMemory()
	board := newProjectionBoard(events)
	seedProjection(t, board, "default")
	records, err := events.Load(context.Background(), "default")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	first, err := foldRecords(records, ^uint64(0))
	if err != nil {
		t.Fatalf("fold: %v", err)
	}
	second, err := foldRecords(records, ^uint64(0))
	if err != nil {
		t.Fatalf("fold: %v", err)
	}
	// Rebuild the map in another order; the canonical document must not
	// change.
	ids := make([]uint64, 0, len(second.Messages))
	for id := range second.Messages {
		ids = append(ids, id)
	}
	second.Messages = make(map[uint64]*Message, len(ids))
	for i := len(ids) - 1; i >= 0; i-- {
		second.Messages[ids[i]] = first.Messages[ids[i]]
	}
	second.Replies = make(map[uint64][]uint64, len(first.Replies))
	for root, replies := range first.Replies {
		second.Replies[root] = append([]uint64(nil), replies...)
	}
	second.Channels = make(map[string]*Channel, len(first.Channels))
	for path, channel := range first.Channels {
		second.Channels[path] = channel
	}
	second.Actors = make(map[string]*ActorRecord, len(first.Actors))
	for id, actor := range first.Actors {
		second.Actors[id] = actor
	}
	firstDigest, err := ProjectionDigest(first)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	secondDigest, err := ProjectionDigest(second)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	if firstDigest != secondDigest {
		t.Fatalf("map order changed the digest: %s vs %s", firstDigest, secondDigest)
	}
}

// hasProblem reports whether any problem contains want.
func hasProblem(report ProjectionReport, want string) bool {
	for _, problem := range report.Problems {
		if strings.Contains(problem, want) {
			return true
		}
	}
	return false
}
