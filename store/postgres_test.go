package store_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/server"
	"github.com/jixoai/keel-board/store"
)

// testDSN returns the PostgreSQL DSN the tests run against, or skips them. CI
// must set a DSN; a missing one fails there rather than skipping silently. The
// tests never start a container: point them at a local server, and each test
// gets its own schema, dropped when it ends.
func testDSN(t *testing.T) string {
	t.Helper()
	for _, name := range []string{"KEEL_BOARD_TEST_DSN", "TEST_DSN"} {
		if dsn := strings.TrimSpace(os.Getenv(name)); dsn != "" {
			return dsn
		}
	}
	if strings.TrimSpace(os.Getenv("CI")) != "" {
		t.Fatal("CI requires KEEL_BOARD_TEST_DSN (or TEST_DSN); PostgreSQL tests must not be skipped")
	}
	t.Skip("PostgreSQL tests skipped: set KEEL_BOARD_TEST_DSN (or TEST_DSN), for example postgres://localhost:5432/postgres?sslmode=disable")
	return ""
}

var testSchemaSeq atomic.Uint64

// newTestSchema creates a schema, so tests can run against one database
// without touching each other, and drops it when the test ends.
func newTestSchema(t *testing.T, dsn string) string {
	t.Helper()
	name := fmt.Sprintf("keel_board_%d_%d", time.Now().UnixNano(), testSchemaSeq.Add(1))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connecting to PostgreSQL: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{name}.Sanitize()); err != nil {
		admin.Close(ctx)
		t.Fatalf("creating test schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(cleanupCtx, "DROP SCHEMA "+pgx.Identifier{name}.Sanitize()+" CASCADE"); err != nil {
			t.Logf("dropping test schema %s: %v", name, err)
		}
		_ = admin.Close(cleanupCtx)
	})
	return name
}

// openTestPostgres opens a store in the schema and closes it after the test.
func openTestPostgres(t *testing.T, dsn, schema string) *store.Postgres {
	t.Helper()
	postgres, err := store.OpenPostgres(t.Context(), store.PostgresOptions{DSN: dsn, SearchPath: schema})
	if err != nil {
		t.Fatalf("opening PostgreSQL store: %v", err)
	}
	t.Cleanup(postgres.Close)
	return postgres
}

// connectSchema opens a bare connection that sees the test schema, for
// assertions the store's own interface doesn't expose (head version, indexes).
func connectSchema(t *testing.T, dsn, schema string) *pgx.Conn {
	t.Helper()
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parsing the test DSN: %v", err)
	}
	config.RuntimeParams["search_path"] = pgx.Identifier{schema}.Sanitize()
	conn, err := pgx.ConnectConfig(t.Context(), config)
	if err != nil {
		t.Fatalf("connecting to the test schema: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// TestPostgresContract runs the store contract against PostgreSQL.
func TestPostgresContract(t *testing.T) {
	dsn := testDSN(t)
	storeContract(t, func(t *testing.T) store.EventStore {
		return openTestPostgres(t, dsn, newTestSchema(t, dsn))
	})
}

// TestPostgresReplayAfterRestart writes through one board, drops its store,
// and proves a new board replays the log and answers the keyed retry.
func TestPostgresReplayAfterRestart(t *testing.T) {
	dsn := testDSN(t)
	schema := newTestSchema(t, dsn)
	ctx := t.Context()

	first := openTestPostgres(t, dsn, schema)
	before := log.New(log.Options{Store: first})
	request := log.AppendRequest{
		Tenant:     "t",
		RequestKey: "post:alice:deploy-a1b2c3",
		Digest:     "channel=work",
		Events:     []log.Event{postedEvent("work", "one")},
	}
	written, err := before.Append(ctx, request)
	if err != nil {
		t.Fatalf("first append: %v", err)
	}
	if written.Replayed || written.FirstSeq != 1 || written.LastSeq != 1 || !reflect.DeepEqual(written.MessageIDs, []uint64{1}) {
		t.Fatalf("first append: %+v", written)
	}
	first.Close()

	// A new process, a new pool: the log comes back from PostgreSQL.
	second := openTestPostgres(t, dsn, schema)
	after := log.New(log.Options{Store: second})
	if err := after.Read("t", func(snapshot *log.Snapshot) error {
		if snapshot.Head() != 1 {
			t.Fatalf("replayed head = %d", snapshot.Head())
		}
		message := snapshot.Message(1)
		if message == nil || message.Text != "one" || message.Channel != "work" {
			t.Fatalf("replayed message: %+v", message)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// The same key and digest answers the first write, without appending.
	replay, err := after.Append(ctx, request)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !replay.Replayed || replay.FirstSeq != 1 || replay.LastSeq != 1 || !reflect.DeepEqual(replay.MessageIDs, []uint64{1}) {
		t.Fatalf("replay: %+v", replay)
	}
	// The same key with another digest is 409 duplicate_id.
	other := request
	other.Digest = "channel=other"
	_, err = after.Append(ctx, other)
	var apiErr *protocol.Error
	if !errors.As(err, &apiErr) || apiErr.Code != protocol.CodeDuplicateID || apiErr.Status != 409 {
		t.Fatalf("different digest: %v", err)
	}
	// The log goes on at the next seq.
	next, err := after.Append(ctx, log.AppendRequest{Tenant: "t", Events: []log.Event{postedEvent("work", "two")}})
	if err != nil {
		t.Fatalf("next append: %v", err)
	}
	if next.FirstSeq != 2 || next.LastSeq != 2 {
		t.Fatalf("next append: %+v", next)
	}
}

// TestPostgresHeadVersion checks the head row's seq and fencing version move
// once per landed append, never for a replay.
func TestPostgresHeadVersion(t *testing.T) {
	dsn := testDSN(t)
	schema := newTestSchema(t, dsn)
	ctx := t.Context()
	events := openTestPostgres(t, dsn, schema)
	conn := connectSchema(t, dsn, schema)

	first := seqRecord(1, "event", `{"n":1}`)
	first.RequestKey = "key"
	first.Digest = "digest"
	first.RequestResult = resultJSON(t, 1, 1, nil)
	mustAppend(t, events, "t", 0, first)
	mustAppend(t, events, "t", 1, seqRecord(2, "event", `{"n":2}`))

	head := func() (seq, version uint64) {
		t.Helper()
		if err := conn.QueryRow(ctx, `SELECT seq, version FROM board_heads WHERE tenant_id = $1`, "t").Scan(&seq, &version); err != nil {
			t.Fatalf("reading head: %v", err)
		}
		return seq, version
	}
	if seq, version := head(); seq != 2 || version != 2 {
		t.Fatalf("head after two appends: seq=%d version=%d", seq, version)
	}
	// A replay of the first key changes neither.
	retry := seqRecord(3, "event", `{"n":3}`)
	retry.RequestKey = "key"
	retry.Digest = "digest"
	retry.RequestResult = resultJSON(t, 3, 3, nil)
	replay, err := events.Append(ctx, "t", []store.Record{retry}, 2)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !replay.Replayed {
		t.Fatalf("expected a replay: %+v", replay)
	}
	if seq, version := head(); seq != 2 || version != 2 {
		t.Fatalf("a replay moved the head: seq=%d version=%d", seq, version)
	}
	mustAppend(t, events, "t", 2, seqRecord(3, "event", `{"n":3}`))
	if seq, version := head(); seq != 3 || version != 3 {
		t.Fatalf("head after three appends: seq=%d version=%d", seq, version)
	}
}

// TestPostgresAppendRollsBack writes a batch whose second event PostgreSQL
// refuses (a NUL in its channel text): the head, the request row, and the first
// event must all be gone.
func TestPostgresAppendRollsBack(t *testing.T) {
	dsn := testDSN(t)
	schema := newTestSchema(t, dsn)
	ctx := t.Context()
	events := openTestPostgres(t, dsn, schema)
	conn := connectSchema(t, dsn, schema)

	first := seqRecord(1, "event", `{"n":1}`)
	first.RequestKey = "key"
	first.Digest = "digest"
	first.RequestResult = resultJSON(t, 1, 2, nil)
	broken := store.Record{
		Seq: 2, Type: "event", Channel: "bad\x00channel",
		Payload: []byte(`{"n":2}`), At: time.Now().UTC(),
	}
	if _, err := events.Append(ctx, "t", []store.Record{first, broken}, 0); err == nil {
		t.Fatal("a NUL in a text column should fail the batch")
	}
	loaded, err := events.Load(ctx, "t")
	if err != nil || len(loaded) != 0 {
		t.Fatalf("a failed batch wrote events: %+v, %v", loaded, err)
	}
	var heads int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM board_heads WHERE tenant_id = $1`, "t").Scan(&heads); err != nil {
		t.Fatal(err)
	}
	if heads != 0 {
		t.Fatalf("a failed batch wrote a head row (%d)", heads)
	}
	var requests int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM board_requests WHERE tenant_id = $1`, "t").Scan(&requests); err != nil {
		t.Fatal(err)
	}
	if requests != 0 {
		t.Fatalf("a failed batch wrote a request row (%d)", requests)
	}
	// The same key is free after the rollback.
	retry := seqRecord(1, "event", `{"n":1}`)
	retry.RequestKey = "key"
	retry.Digest = "digest"
	retry.RequestResult = resultJSON(t, 1, 1, nil)
	mustAppend(t, events, "t", 0, retry)
}

// TestPostgresConcurrentSameKey races one idempotent request: exactly one
// event lands, and every loser either replays or sees the head move.
func TestPostgresConcurrentSameKey(t *testing.T) {
	dsn := testDSN(t)
	schema := newTestSchema(t, dsn)
	ctx := t.Context()
	events := openTestPostgres(t, dsn, schema)

	const racers = 8
	errs := make(chan error, racers)
	for i := 0; i < racers; i++ {
		go func() {
			record := seqRecord(1, "message.posted", `{"id":1,"text":"once"}`)
			record.RequestKey = "same"
			record.Digest = "digest"
			record.RequestResult = resultJSON(t, 1, 1, []uint64{1})
			_, err := events.Append(ctx, "t", []store.Record{record}, 0)
			errs <- err
		}()
	}
	var moved int
	for i := 0; i < racers; i++ {
		err := <-errs
		switch {
		case err == nil:
		case errors.Is(err, store.ErrHeadMoved):
			moved++
		default:
			t.Fatalf("racing append: %v", err)
		}
	}
	// A loser that checked after the winner committed sees the request row and
	// replays; one that raced the head is told to look again. Either is fine.
	loaded, err := events.Load(ctx, "t")
	if err != nil || len(loaded) != 1 {
		t.Fatalf("racing appends wrote %d records: %v", len(loaded), err)
	}
	// Every move is retried once: the request is there, so each replays.
	for i := 0; i < moved; i++ {
		record := seqRecord(2, "message.posted", `{"id":2,"text":"again"}`)
		record.RequestKey = "same"
		record.Digest = "digest"
		record.RequestResult = resultJSON(t, 2, 2, []uint64{2})
		result, err := events.Append(ctx, "t", []store.Record{record}, 1)
		if err != nil || !result.Replayed {
			t.Fatalf("retry %d: %+v, %v", i, result, err)
		}
	}
}

// TestPostgresConcurrentClaim races claims on a task through a PG-backed
// board: one claim lands, the rest see the task claimed.
func TestPostgresConcurrentClaim(t *testing.T) {
	dsn := testDSN(t)
	schema := newTestSchema(t, dsn)
	ctx := t.Context()
	board := log.New(log.Options{Store: openTestPostgres(t, dsn, schema)})
	author := protocol.Author{Actor: "local:user:alice", Principal: "local:user:alice"}

	if err := board.Update(ctx, "t", log.UpdateOptions{}, func(tx *log.Tx) error {
		id := tx.NextID(0)
		tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
			ID: id, Channel: "work", Text: "fix", Thread: log.ThreadKey(id),
			Labels: map[string]string{}, By: author,
		})
		tx.Emit(log.EventTaskCreated, &log.TaskCreatedPayload{
			ID: id, Thread: log.ThreadKey(id), By: author,
		})
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	until := protocol.FormatTime(time.Now().Add(time.Hour))
	const claimers = 8
	var claimed atomic.Int32
	var failures atomic.Int32
	var wait sync.WaitGroup
	for i := 0; i < claimers; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			err := board.Update(ctx, "t", log.UpdateOptions{}, func(tx *log.Tx) error {
				root := tx.Message(1)
				if root == nil || root.Task == nil {
					return errors.New("task disappeared")
				}
				if root.Task.State != "ready" {
					return protocol.NewError(protocol.CodeTaskClaimed, "already claimed")
				}
				tx.Emit(log.EventTaskClaimed, &log.TaskClaimedPayload{
					ID: 1, Thread: root.Thread, Epoch: root.Task.Epoch + 1,
					Until: until, Token: "hash", By: author,
				})
				return nil
			})
			if err != nil {
				var apiErr *protocol.Error
				if !errors.As(err, &apiErr) || apiErr.Code != protocol.CodeTaskClaimed {
					t.Errorf("claim: %v", err)
				}
				failures.Add(1)
				return
			}
			claimed.Add(1)
		}()
	}
	wait.Wait()
	if claimed.Load() != 1 || failures.Load() != claimers-1 {
		t.Fatalf("claims landed=%d failed=%d", claimed.Load(), failures.Load())
	}
	if err := board.Read("t", func(snapshot *log.Snapshot) error {
		root := snapshot.Message(1)
		if root == nil || root.Task == nil || root.Task.State != "claimed" || root.Task.Epoch != 1 {
			t.Fatalf("claimed task: %+v", root)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestPostgresMigrationIdempotent opens the same schema twice: applying the
// schema is safe on every boot.
func TestPostgresMigrationIdempotent(t *testing.T) {
	dsn := testDSN(t)
	schema := newTestSchema(t, dsn)
	first := openTestPostgres(t, dsn, schema)
	first.Close()
	second := openTestPostgres(t, dsn, schema)
	mustAppend(t, second, "t", 0, seqRecord(1, "event", `{"n":1}`))
}

// TestPostgresBoardWaitSeesAnotherStore primes a board's cache, has another
// store write, and proves Wait catches the reader up: the path an SSE feed
// takes when another process appends.
func TestPostgresBoardWaitSeesAnotherStore(t *testing.T) {
	dsn := testDSN(t)
	schema := newTestSchema(t, dsn)
	// This timeout bounds the whole test, including opening two stores and
	// their idempotent migrations. Under `-count=10` with packages in parallel,
	// 10s was reached before the append on a busy PostgreSQL; the Wait itself
	// only polls, so a looser bound does not weaken what is proved.
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	writer := log.New(log.Options{Store: openTestPostgres(t, dsn, schema)})
	readerStore := openTestPostgres(t, dsn, schema)
	reader := log.New(log.Options{Store: readerStore})

	if err := reader.Read("t", func(snapshot *log.Snapshot) error {
		if snapshot.Head() != 0 {
			t.Fatalf("reader cache starts at head %d", snapshot.Head())
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- reader.Wait(ctx, "t", 0) }()
	time.Sleep(25 * time.Millisecond)
	if _, err := writer.Append(ctx, log.AppendRequest{
		Tenant: "t",
		Events: []log.Event{postedEvent("work", "from the other process")},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("wait: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("the reader never saw the other store's write")
	}
	if err := reader.Read("t", func(snapshot *log.Snapshot) error {
		if snapshot.Head() != 1 {
			t.Fatalf("reader head = %d", snapshot.Head())
		}
		message := snapshot.Message(1)
		if message == nil || message.Text != "from the other process" || message.Channel != "work" {
			t.Fatalf("reader message: %+v", message)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Another tenant stays out of this log.
	if err := reader.Read("other", func(snapshot *log.Snapshot) error {
		if snapshot.Head() != 0 {
			t.Fatalf("another tenant's head = %d", snapshot.Head())
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// A keyed write the reader's cache predates answers the writer's result,
	// not a second message.
	keyed := log.AppendRequest{
		Tenant:     "t",
		RequestKey: "cross-process",
		Digest:     "channel=work",
		Events:     []log.Event{postedEvent("work", "keyed elsewhere")},
	}
	written, err := writer.Append(ctx, keyed)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := reader.Append(ctx, keyed)
	if err != nil {
		t.Fatalf("cross-process replay: %v", err)
	}
	if !retry.Replayed || retry.FirstSeq != written.FirstSeq || retry.LastSeq != written.LastSeq ||
		!reflect.DeepEqual(retry.MessageIDs, written.MessageIDs) {
		t.Fatalf("cross-process replay: %+v vs %+v", retry, written)
	}

	// An unkeyed write on the stale reader hits the writer's newer head; the
	// board catches up and retries the write once, so it lands after it.
	if _, err := writer.Append(ctx, log.AppendRequest{
		Tenant: "t",
		Events: []log.Event{postedEvent("work", "writer again")},
	}); err != nil {
		t.Fatal(err)
	}
	landed, err := reader.Append(ctx, log.AppendRequest{
		Tenant: "t",
		Events: []log.Event{postedEvent("work", "reader at last")},
	})
	if err != nil {
		t.Fatalf("stale write after catch-up: %v", err)
	}
	if landed.FirstSeq != 4 || landed.LastSeq != 4 {
		t.Fatalf("stale write after catch-up: %+v", landed)
	}
}

func postedEvent(channel, text string) log.Event {
	return log.Event{Type: log.EventMessagePosted, Payload: &log.MessagePostedPayload{
		Channel: channel,
		Text:    text,
		Labels:  map[string]string{},
		By:      protocol.Author{Actor: "local:user:alice", Principal: "local:user:alice"},
	}}
}

// adopt appends a keyed task.created for an existing root and no
// message.posted, the shape a task adopted from a thread has.
func adopt(ctx context.Context, board *log.Board, tenant string, opts log.UpdateOptions, rootID uint64) (log.AppendResult, error) {
	author := protocol.Author{Actor: "local:user:alice", Principal: "local:user:alice"}
	var result log.AppendResult
	err := board.Update(ctx, tenant, opts, func(tx *log.Tx) error {
		if replay, ok := tx.Replay(); ok {
			result = replay
			return nil
		}
		root := tx.Message(rootID)
		if root == nil {
			return fmt.Errorf("root #%d isn't there", rootID)
		}
		tx.Emit(log.EventTaskCreated, &log.TaskCreatedPayload{
			ID: rootID, Thread: root.Thread, By: author,
		})
		tx.RecordResult(rootID)
		result = tx.Result()
		return nil
	})
	return result, err
}

// TestPostgresRestartReplaysAdoptedTask writes a keyed adopt batch (no
// message.posted) and proves a rebuilt board answers the retry with the task's
// ID, not duplicate_id.
func TestPostgresRestartReplaysAdoptedTask(t *testing.T) {
	dsn := testDSN(t)
	schema := newTestSchema(t, dsn)
	ctx := t.Context()

	first := openTestPostgres(t, dsn, schema)
	before := log.New(log.Options{Store: first})
	root, err := before.Append(ctx, log.AppendRequest{
		Tenant: "t",
		Events: []log.Event{postedEvent("work", "root")},
	})
	if err != nil {
		t.Fatal(err)
	}
	rootID := root.MessageIDs[0]
	opts := log.UpdateOptions{Idempotency: &log.Idempotency{
		Key: "task:local:user:alice:adopt-1", Digest: "channel=work;re=1",
	}}
	written, err := adopt(ctx, before, "t", opts, rootID)
	if err != nil {
		t.Fatalf("adopt: %v", err)
	}
	if written.Replayed || !reflect.DeepEqual(written.MessageIDs, []uint64{rootID}) {
		t.Fatalf("adopt result: %+v", written)
	}
	first.Close()

	second := openTestPostgres(t, dsn, schema)
	rebuilt := log.New(log.Options{Store: second})
	replay, err := adopt(ctx, rebuilt, "t", opts, rootID)
	if err != nil {
		t.Fatalf("restart replay: %v", err)
	}
	if !replay.Replayed || !reflect.DeepEqual(replay.MessageIDs, []uint64{rootID}) {
		t.Fatalf("restart replay: %+v", replay)
	}
	if err := rebuilt.Read("t", func(snapshot *log.Snapshot) error {
		message := snapshot.Message(rootID)
		if message == nil || message.Task == nil || message.Task.State != "ready" {
			t.Fatalf("rebuilt task: %+v", message)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestPostgresConcurrentAdoptAcrossBoards races one keyed adopt through two
// boards and two pools: every caller gets the task's ID, one batch lands.
func TestPostgresConcurrentAdoptAcrossBoards(t *testing.T) {
	dsn := testDSN(t)
	schema := newTestSchema(t, dsn)
	ctx := t.Context()
	aStore := openTestPostgres(t, dsn, schema)
	bStore := openTestPostgres(t, dsn, schema)
	a := log.New(log.Options{Store: aStore})
	b := log.New(log.Options{Store: bStore})

	root, err := a.Append(ctx, log.AppendRequest{
		Tenant: "t",
		Events: []log.Event{postedEvent("work", "root")},
	})
	if err != nil {
		t.Fatal(err)
	}
	rootID := root.MessageIDs[0]
	if err := b.Read("t", func(*log.Snapshot) error { return nil }); err != nil {
		t.Fatal(err)
	}
	opts := log.UpdateOptions{Idempotency: &log.Idempotency{
		Key: "task:local:user:alice:adopt-race", Digest: "channel=work;re=1",
	}}

	const racers = 8
	type outcome struct {
		result log.AppendResult
		err    error
	}
	results := make(chan outcome, racers)
	for index := 0; index < racers; index++ {
		board := a
		if index%2 == 1 {
			board = b
		}
		go func() {
			result, err := adopt(ctx, board, "t", opts, rootID)
			results <- outcome{result: result, err: err}
		}()
	}
	fresh, replayed := 0, 0
	for index := 0; index < racers; index++ {
		got := <-results
		if got.err != nil {
			t.Fatalf("adopt %d: %v", index, got.err)
		}
		if !reflect.DeepEqual(got.result.MessageIDs, []uint64{rootID}) {
			t.Fatalf("adopt %d answered %+v", index, got.result)
		}
		if got.result.Replayed {
			replayed++
		} else {
			fresh++
		}
	}
	if fresh != 1 || replayed != racers-1 {
		t.Fatalf("adopt race: fresh=%d replayed=%d", fresh, replayed)
	}
	loaded, err := aStore.Load(ctx, "t")
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 2 {
		t.Fatalf("adopt race wrote %d records, want 2", len(loaded))
	}
}

// TestPostgresBoundedFreshness proves a second board catches up within its
// freshness window and that Read, Head, and History all trigger the catch-up.
func TestPostgresBoundedFreshness(t *testing.T) {
	dsn := testDSN(t)
	schema := newTestSchema(t, dsn)
	ctx := t.Context()
	writer := log.New(log.Options{Store: openTestPostgres(t, dsn, schema)})
	reader := log.New(log.Options{
		Store:     openTestPostgres(t, dsn, schema),
		Freshness: 10 * time.Millisecond,
	})
	if err := reader.Read("t", func(snapshot *log.Snapshot) error {
		if snapshot.Head() != 0 {
			t.Fatalf("reader primes at head %d", snapshot.Head())
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := writer.Append(ctx, log.AppendRequest{Tenant: "t", Events: []log.Event{postedEvent("work", "one")}}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if err := reader.Read("t", func(snapshot *log.Snapshot) error {
		if snapshot.Head() != 1 || snapshot.Message(1) == nil || snapshot.Message(1).Text != "one" {
			t.Fatalf("reader after its window: head %d, %+v", snapshot.Head(), snapshot.Message(1))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := writer.Append(ctx, log.AppendRequest{Tenant: "t", Events: []log.Event{postedEvent("work", "two")}}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	head, err := reader.Head("t")
	if err != nil || head != 2 {
		t.Fatalf("reader head: %d, %v", head, err)
	}

	if _, err := writer.Append(ctx, log.AppendRequest{Tenant: "t", Events: []log.Event{postedEvent("work", "three")}}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	events, err := reader.History("t", 1, 3)
	if err != nil || len(events) != 3 || events[2].Seq != 3 {
		t.Fatalf("reader history: %d events, %v", len(events), err)
	}
}

// TestPostgresHeadMovedRetriesBusinessFn has a stale board emit from an old
// read; the store refuses, the board catches up, and the retried fn sees the
// new state and answers the correct conflict instead of appending.
func TestPostgresHeadMovedRetriesBusinessFn(t *testing.T) {
	dsn := testDSN(t)
	schema := newTestSchema(t, dsn)
	ctx := t.Context()
	a := log.New(log.Options{Store: openTestPostgres(t, dsn, schema)})
	// b never refreshes on its own: the store's head-moved refusal is the only
	// catch-up path this test exercises.
	b := log.New(log.Options{Store: openTestPostgres(t, dsn, schema), Freshness: -1})
	author := protocol.Author{Actor: "local:user:alice", Principal: "local:user:alice"}

	posted := postedEvent("work", "root")
	posted.Payload.(*log.MessagePostedPayload).Labels = map[string]string{"status": "old"}
	if _, err := a.Append(ctx, log.AppendRequest{Tenant: "t", Events: []log.Event{posted}}); err != nil {
		t.Fatal(err)
	}
	if err := b.Read("t", func(*log.Snapshot) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := a.Update(ctx, "t", log.UpdateOptions{}, func(tx *log.Tx) error {
		root := tx.Message(1)
		if root == nil {
			return errors.New("root disappeared")
		}
		tx.Emit(log.EventLabelsChanged, &log.LabelsChangedPayload{
			ID: 1, Thread: root.Thread, Set: map[string]string{"status": "new"}, By: author,
		})
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	attempts := 0
	conflict := protocol.NewError(protocol.CodeLabelConflict, "status moved on")
	err := b.Update(ctx, "t", log.UpdateOptions{}, func(tx *log.Tx) error {
		attempts++
		root := tx.Message(1)
		if root == nil {
			return errors.New("root disappeared")
		}
		if root.Labels["status"] != "old" {
			return conflict
		}
		tx.Emit(log.EventLabelsChanged, &log.LabelsChangedPayload{
			ID: 1, Thread: root.Thread, Set: map[string]string{"status": "from-b"}, By: author,
		})
		return nil
	})
	var apiErr *protocol.Error
	if !errors.As(err, &apiErr) || apiErr.Code != protocol.CodeLabelConflict {
		t.Fatalf("stale write: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("business fn ran %d times, want 2", attempts)
	}
	if err := b.Read("t", func(snapshot *log.Snapshot) error {
		root := snapshot.Message(1)
		if root == nil || root.Labels["status"] != "new" {
			t.Fatalf("labels after retry: %+v", root)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestPostgresConcurrentClaimAcrossStores races claims through two boards and
// two pools: one lands, every loser answers task_claimed (not a 503).
func TestPostgresConcurrentClaimAcrossStores(t *testing.T) {
	dsn := testDSN(t)
	schema := newTestSchema(t, dsn)
	ctx := t.Context()
	a := log.New(log.Options{Store: openTestPostgres(t, dsn, schema)})
	b := log.New(log.Options{Store: openTestPostgres(t, dsn, schema)})
	author := protocol.Author{Actor: "local:user:alice", Principal: "local:user:alice"}

	if err := a.Update(ctx, "t", log.UpdateOptions{}, func(tx *log.Tx) error {
		id := tx.NextID(0)
		tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
			ID: id, Channel: "work", Text: "fix", Thread: log.ThreadKey(id),
			Labels: map[string]string{}, By: author,
		})
		tx.Emit(log.EventTaskCreated, &log.TaskCreatedPayload{ID: id, Thread: log.ThreadKey(id), By: author})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.Read("t", func(*log.Snapshot) error { return nil }); err != nil {
		t.Fatal(err)
	}
	until := protocol.FormatTime(time.Now().Add(time.Hour))
	const claimers = 8
	var claimed, failed atomic.Int32
	var wait sync.WaitGroup
	for index := 0; index < claimers; index++ {
		board := a
		if index%2 == 1 {
			board = b
		}
		wait.Add(1)
		go func() {
			defer wait.Done()
			err := board.Update(ctx, "t", log.UpdateOptions{}, func(tx *log.Tx) error {
				root := tx.Message(1)
				if root == nil || root.Task == nil {
					return errors.New("task disappeared")
				}
				if root.Task.State != "ready" {
					return protocol.NewError(protocol.CodeTaskClaimed, "already claimed")
				}
				tx.Emit(log.EventTaskClaimed, &log.TaskClaimedPayload{
					ID: 1, Thread: root.Thread, Epoch: root.Task.Epoch + 1,
					Until: until, Token: "hash", By: author,
				})
				return nil
			})
			if err != nil {
				var apiErr *protocol.Error
				if !errors.As(err, &apiErr) || apiErr.Code != protocol.CodeTaskClaimed {
					t.Errorf("claim: %v", err)
				}
				failed.Add(1)
				return
			}
			claimed.Add(1)
		}()
	}
	wait.Wait()
	if claimed.Load() != 1 || failed.Load() != claimers-1 {
		t.Fatalf("claims landed=%d failed=%d", claimed.Load(), failed.Load())
	}
	if err := b.Read("t", func(snapshot *log.Snapshot) error {
		root := snapshot.Message(1)
		if root == nil || root.Task == nil || root.Task.State != "claimed" || root.Task.Epoch != 1 {
			t.Fatalf("claimed task: %+v", root)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestPostgresConcurrentSameKeyAcrossStores races one keyed post through two
// boards and pools: every caller answers the same message, one lands.
func TestPostgresConcurrentSameKeyAcrossStores(t *testing.T) {
	dsn := testDSN(t)
	schema := newTestSchema(t, dsn)
	ctx := t.Context()
	aStore := openTestPostgres(t, dsn, schema)
	a := log.New(log.Options{Store: aStore})
	b := log.New(log.Options{Store: openTestPostgres(t, dsn, schema)})
	author := protocol.Author{Actor: "local:user:alice", Principal: "local:user:alice"}
	opts := log.UpdateOptions{Idempotency: &log.Idempotency{Key: "post:alice:cross", Digest: "channel=work"}}

	const racers = 8
	type outcome struct {
		result log.AppendResult
		err    error
	}
	results := make(chan outcome, racers)
	for index := 0; index < racers; index++ {
		board := a
		if index%2 == 1 {
			board = b
		}
		go func() {
			var result log.AppendResult
			err := board.Update(ctx, "t", opts, func(tx *log.Tx) error {
				if replay, ok := tx.Replay(); ok {
					result = replay
					return nil
				}
				id := tx.NextID(0)
				tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
					ID: id, Channel: "work", Text: "once", Thread: log.ThreadKey(id),
					Labels: map[string]string{}, By: author,
				})
				tx.RecordResult(id)
				result = tx.Result()
				return nil
			})
			results <- outcome{result: result, err: err}
		}()
	}
	fresh, replayed := 0, 0
	for index := 0; index < racers; index++ {
		got := <-results
		if got.err != nil {
			t.Fatalf("same-key %d: %v", index, got.err)
		}
		if !reflect.DeepEqual(got.result.MessageIDs, []uint64{1}) {
			t.Fatalf("same-key %d answered %+v", index, got.result)
		}
		if got.result.Replayed {
			replayed++
		} else {
			fresh++
		}
	}
	if fresh != 1 || replayed != racers-1 {
		t.Fatalf("same-key race: fresh=%d replayed=%d", fresh, replayed)
	}
	loaded, err := aStore.Load(ctx, "t")
	if err != nil || len(loaded) != 1 {
		t.Fatalf("same-key race wrote %d records: %v", len(loaded), err)
	}
}

// newTestServer serves a board over httptest for the feed tests.
func newTestServer(t *testing.T, board *log.Board) *httptest.Server {
	t.Helper()
	api := server.New(server.Options{
		Board:  board,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	httpServer := httptest.NewServer(api)
	t.Cleanup(httpServer.Close)
	return httpServer
}

// openSSE opens the event feed and consumes its connected comment.
func openSSE(t *testing.T, httpServer *httptest.Server, query string, headers map[string]string) (*http.Response, *bufio.Reader) {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, httpServer.URL+server.APIPrefix+"/events"+query, nil)
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := httpServer.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		t.Fatalf("events: status %d: %s", response.StatusCode, body)
	}
	reader := bufio.NewReader(response.Body)
	line, err := reader.ReadString('\n')
	if err != nil || !strings.HasPrefix(line, ": connected") {
		response.Body.Close()
		t.Fatalf("events: expected a connected comment, got %q (%v)", line, err)
	}
	if blank, err := reader.ReadString('\n'); err != nil || strings.TrimSpace(blank) != "" {
		response.Body.Close()
		t.Fatalf("events: expected a blank line, got %q (%v)", blank, err)
	}
	return response, reader
}

// readSSEEvent reads one event or fails after five seconds.
func readSSEEvent(t *testing.T, reader *bufio.Reader) protocol.EventView {
	t.Helper()
	type result struct {
		view protocol.EventView
		err  error
	}
	done := make(chan result, 1)
	go func() {
		for {
			var id, data string
			for {
				line, err := reader.ReadString('\n')
				if err != nil {
					done <- result{err: err}
					return
				}
				line = strings.TrimRight(line, "\r\n")
				if line == "" {
					break
				}
				switch {
				case strings.HasPrefix(line, "id: "):
					id = strings.TrimPrefix(line, "id: ")
				case strings.HasPrefix(line, "data: "):
					data = strings.TrimPrefix(line, "data: ")
				}
			}
			if data == "" {
				continue
			}
			var view protocol.EventView
			if err := json.Unmarshal([]byte(data), &view); err != nil {
				done <- result{err: err}
				return
			}
			if id != "" && string(view.Cursor) != id {
				done <- result{err: fmt.Errorf("event id %q doesn't match cursor %q", id, view.Cursor)}
				return
			}
			done <- result{view: view}
			return
		}
	}()
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("reading an event: %v", got.err)
		}
		return got.view
	case <-time.After(5 * time.Second):
		t.Fatal("timed out reading an SSE event")
		return protocol.EventView{}
	}
}

// TestPostgresFeedRefreshesBeforeCursorAndLast proves the feed catches up
// before replaying `last` and never clamps a client cursor below the store.
func TestPostgresFeedRefreshesBeforeCursorAndLast(t *testing.T) {
	dsn := testDSN(t)
	schema := newTestSchema(t, dsn)
	ctx := t.Context()
	tenant := "default"
	writer := log.New(log.Options{Store: openTestPostgres(t, dsn, schema)})
	reader := log.New(log.Options{
		Store:     openTestPostgres(t, dsn, schema),
		Freshness: 10 * time.Millisecond,
	})
	httpServer := newTestServer(t, reader)

	// The reader's cache is empty and its window has not been asked yet. A
	// `last` feed must refresh first and answer the newest event.
	if _, err := writer.Append(ctx, log.AppendRequest{Tenant: tenant, Events: []log.Event{postedEvent("work", "one")}}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Append(ctx, log.AppendRequest{Tenant: tenant, Events: []log.Event{postedEvent("work", "two")}}); err != nil {
		t.Fatal(err)
	}
	response, stream := openSSE(t, httpServer, "?last=1", nil)
	event := readSSEEvent(t, stream)
	if event.Message == nil || event.Message.Text != "two" || event.Cursor != "2" {
		t.Fatalf("last=1: %+v", event)
	}
	response.Body.Close()

	// A fresh cache and a client cursor at the store's head: no replay, and the
	// next event is delivered without clamping the cursor down.
	reader2 := log.New(log.Options{Store: openTestPostgres(t, dsn, schema)})
	httpServer2 := newTestServer(t, reader2)
	if _, err := writer.Append(ctx, log.AppendRequest{Tenant: tenant, Events: []log.Event{postedEvent("work", "three")}}); err != nil {
		t.Fatal(err)
	}
	response2, stream2 := openSSE(t, httpServer2, "", map[string]string{"Last-Event-ID": "3"})
	if _, err := writer.Append(ctx, log.AppendRequest{Tenant: tenant, Events: []log.Event{postedEvent("work", "four")}}); err != nil {
		t.Fatal(err)
	}
	next := readSSEEvent(t, stream2)
	if next.Message == nil || next.Message.Text != "four" || next.Cursor != "4" {
		t.Fatalf("cursor after head: %+v", next)
	}
	response2.Body.Close()

	// A cursor ahead of the log is refused, not silently clamped.
	future, err := http.Get(httpServer2.URL + server.APIPrefix + "/events?after=99")
	if err != nil {
		t.Fatal(err)
	}
	defer future.Body.Close()
	if future.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(future.Body)
		t.Fatalf("future cursor: status %d: %s", future.StatusCode, body)
	}
	var body protocol.ErrorBody
	if err := json.NewDecoder(future.Body).Decode(&body); err != nil {
		t.Fatalf("decoding the future-cursor error: %v", err)
	}
	if body.Error.Code != protocol.CodeInvalidCursor {
		t.Fatalf("future cursor code %s", body.Error.Code)
	}
}
