package store_test

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/jixoai/keel-board/store"
)

// testDSN returns the PostgreSQL DSN the tests run against, or skips them.
// The tests never start a container: point them at a local server, and each
// test gets its own schema, dropped when it ends.
func testDSN(t *testing.T) string {
	t.Helper()
	for _, name := range []string{"KEEL_BOARD_TEST_DSN", "TEST_DSN"} {
		if dsn := strings.TrimSpace(os.Getenv(name)); dsn != "" {
			return dsn
		}
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

// TestPostgresAppendRollsBack writes a batch whose second event is invalid:
// the head, the request row, and the first event must all be gone.
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
	broken := store.Record{Seq: 2, Type: "event", Payload: []byte(`not json`), At: time.Now().UTC()}
	if _, err := events.Append(ctx, "t", []store.Record{first, broken}, 0); err == nil {
		t.Fatal("an invalid jsonb payload should fail the batch")
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
	mustAppend(t, events, "t", 0, seqRecord(1, "event", `{"n":1}`))
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
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
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

	// An unkeyed write on the stale reader is refused once, catches the cache
	// up, and the retry lands after the writer's event.
	if _, err := writer.Append(ctx, log.AppendRequest{
		Tenant: "t",
		Events: []log.Event{postedEvent("work", "writer again")},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Append(ctx, log.AppendRequest{
		Tenant: "t",
		Events: []log.Event{postedEvent("work", "reader at last")},
	}); !errors.Is(err, log.ErrHeadMoved) {
		t.Fatalf("stale reader should be refused, got %v", err)
	}
	landed, err := reader.Append(ctx, log.AppendRequest{
		Tenant: "t",
		Events: []log.Event{postedEvent("work", "reader at last")},
	})
	if err != nil {
		t.Fatalf("retry after catch-up: %v", err)
	}
	if landed.FirstSeq != 4 || landed.LastSeq != 4 {
		t.Fatalf("retry after catch-up: %+v", landed)
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
