package log

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jixoai/keel-board/store"
)

// pgTestDSN returns the PostgreSQL DSN these tests run against, or skips
// them. CI must set a DSN; a missing one fails there rather than skipping.
func pgTestDSN(t *testing.T) string {
	t.Helper()
	for _, name := range []string{"KEEL_BOARD_TEST_DSN", "TEST_DSN"} {
		if dsn := strings.TrimSpace(os.Getenv(name)); dsn != "" {
			return dsn
		}
	}
	if strings.TrimSpace(os.Getenv("CI")) != "" {
		t.Fatal("CI requires KEEL_BOARD_TEST_DSN (or TEST_DSN); projection tests must not skip PostgreSQL")
	}
	t.Skip("PostgreSQL projection tests skipped: set KEEL_BOARD_TEST_DSN (or TEST_DSN), for example postgres://localhost:5432/postgres?sslmode=disable")
	return ""
}

// pgSchemaSeq keeps generated schema names unique within one process.
var pgSchemaSeq atomic.Uint64

// newPGTestSchema creates a schema for one test and drops it after.
func newPGTestSchema(t *testing.T, dsn string) string {
	t.Helper()
	name := fmt.Sprintf("keel_board_projection_%d_%d", time.Now().UnixNano(), pgSchemaSeq.Add(1))
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

// openPGTestStore opens a pool on the test schema and closes it after.
func openPGTestStore(t *testing.T, dsn, schema string) *store.Postgres {
	t.Helper()
	postgres, err := store.OpenPostgres(t.Context(), store.PostgresOptions{DSN: dsn, SearchPath: schema})
	if err != nil {
		t.Fatalf("opening PostgreSQL store: %v", err)
	}
	t.Cleanup(postgres.Close)
	return postgres
}

// connectPGTestSchema opens a bare connection on the test schema, for the
// tampering only a test should do.
func connectPGTestSchema(t *testing.T, dsn, schema string) *pgx.Conn {
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

// appendProjectionMessage appends one message through the board.
func appendProjectionMessage(t *testing.T, board *Board, tenant, text string) uint64 {
	t.Helper()
	var id uint64
	err := board.Update(context.Background(), tenant, UpdateOptions{}, func(tx *Tx) error {
		id = tx.NextID(0)
		tx.Emit(EventMessagePosted, &MessagePostedPayload{
			ID: id, Channel: "work", Text: text, Thread: ThreadKey(id),
			By: projectionAuthor, Labels: map[string]string{},
		})
		tx.RecordResult(id)
		return nil
	})
	if err != nil {
		t.Fatalf("append %q: %v", text, err)
	}
	return id
}

func TestPostgresProjectionCheckAndRebuild(t *testing.T) {
	dsn := pgTestDSN(t)
	schema := newPGTestSchema(t, dsn)
	events := openPGTestStore(t, dsn, schema)
	board := newProjectionBoard(events)
	root, _ := seedProjection(t, board, "default")
	before := recordCount(t, events, "default")

	report, err := board.CheckProjection(t.Context(), "default")
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !report.Consistent {
		t.Fatalf("a PostgreSQL projection should be consistent: %+v", report)
	}
	corruptProjection(t, board, "default", root)
	drifted, err := board.CheckProjection(t.Context(), "default")
	if err != nil || drifted.Consistent || drifted.Digest == drifted.ReplayDigest {
		t.Fatalf("drift: %+v, %v", drifted, err)
	}
	rebuilt, err := board.Rebuild(t.Context(), "default")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if !rebuilt.Consistent || rebuilt.Digest != report.Digest {
		t.Fatalf("rebuild: %+v, want the original digest %s", rebuilt, report.Digest)
	}
	if messageText(t, board, "default", root) != "root" {
		t.Fatalf("the rebuilt text isn't the log's")
	}
	if after := recordCount(t, events, "default"); after != before {
		t.Fatalf("a rebuild changed the log: %d -> %d", before, after)
	}
}

func TestPostgresProjectionLagAndRebuild(t *testing.T) {
	dsn := pgTestDSN(t)
	schema := newPGTestSchema(t, dsn)
	events := openPGTestStore(t, dsn, schema)
	board := newProjectionBoard(events)
	root, reply := seedProjection(t, board, "default")
	records, err := events.Load(t.Context(), "default")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	through := records[0].Seq
	lagProjection(t, board, "default", through)

	report, err := board.CheckProjection(t.Context(), "default")
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if report.Consistent || !hasProblem(report, "lags") {
		t.Fatalf("lag: %+v", report)
	}
	if report.ProjectionHead != through || report.LogHead != uint64(len(records)) {
		t.Fatalf("watermarks: %+v", report)
	}
	if report.Digest != report.ReplayDigest {
		t.Fatalf("a lagging prefix should match its own replay: %+v", report)
	}
	if _, err := board.Rebuild(t.Context(), "default"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if text := messageText(t, board, "default", root); text != "root" {
		t.Fatalf("root text after rebuild: %q", text)
	}
	if text := messageText(t, board, "default", reply); text != "reply" {
		t.Fatalf("reply text after rebuild: %q", text)
	}
}

func TestPostgresInconsistentHeadRejectsWrites(t *testing.T) {
	dsn := pgTestDSN(t)
	schema := newPGTestSchema(t, dsn)
	events := openPGTestStore(t, dsn, schema)
	conn := connectPGTestSchema(t, dsn, schema)
	board := newProjectionBoard(events)
	message := appendProjectionMessage(t, board, "default", "one")
	corruptProjection(t, board, "default", message)

	// Move the head row without an event: the store must refuse to extend the
	// log.
	if _, err := conn.Exec(t.Context(), `UPDATE board_heads SET seq = seq + 1 WHERE tenant_id = $1`, "default"); err != nil {
		t.Fatalf("tampering with the head: %v", err)
	}
	err := board.Update(t.Context(), "default", UpdateOptions{}, func(tx *Tx) error {
		id := tx.NextID(0)
		tx.Emit(EventMessagePosted, &MessagePostedPayload{
			ID: id, Channel: "work", Text: "refused", Thread: ThreadKey(id),
			By: projectionAuthor, Labels: map[string]string{},
		})
		return nil
	})
	if !errors.Is(err, store.ErrLogInconsistent) {
		t.Fatalf("a write over a mismatched head: %v", err)
	}
	if got := recordCount(t, events, "default"); got != 1 {
		t.Fatalf("a refused write appended: %d events", got)
	}
	report, err := board.CheckProjection(t.Context(), "default")
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if report.Consistent || !hasProblem(report, "head row") {
		t.Fatalf("the check should name the head mismatch: %+v", report)
	}
	// A replay can't repair a broken head row; the rebuild refuses to swap in
	// a projection that omits the event the head names.
	if _, err := board.Rebuild(t.Context(), "default"); !errors.Is(err, store.ErrLogInconsistent) {
		t.Fatalf("rebuild over a mismatched head: %v", err)
	}
	if text := messageText(t, board, "default", message); text != "corrupted" {
		t.Fatalf("the refused rebuild replaced the cache: %q", text)
	}

	// Restoring the head makes a rebuild work again, and it repairs the
	// deliberately corrupted cache.
	if _, err := conn.Exec(t.Context(), `UPDATE board_heads SET seq = 1 WHERE tenant_id = $1`, "default"); err != nil {
		t.Fatalf("restoring the head: %v", err)
	}
	rebuilt, err := board.Rebuild(t.Context(), "default")
	if err != nil || !rebuilt.Consistent {
		t.Fatalf("rebuild after restore: %+v, %v", rebuilt, err)
	}
	if text := messageText(t, board, "default", message); text != "one" {
		t.Fatalf("the repaired cache is %q", text)
	}
	appendProjectionMessage(t, board, "default", "two")
	if got := recordCount(t, events, "default"); got != 2 {
		t.Fatalf("the log after restore: %d events", got)
	}
}

func TestPostgresMissingTailRejectsWrites(t *testing.T) {
	dsn := pgTestDSN(t)
	schema := newPGTestSchema(t, dsn)
	events := openPGTestStore(t, dsn, schema)
	conn := connectPGTestSchema(t, dsn, schema)
	board := newProjectionBoard(events)
	message := appendProjectionMessage(t, board, "default", "one")
	appendProjectionMessage(t, board, "default", "two")
	corruptProjection(t, board, "default", message)

	// Remove the newest event; the head row now names a seq the log lacks.
	if _, err := conn.Exec(t.Context(), `DELETE FROM board_events WHERE tenant_id = $1 AND seq = 2`, "default"); err != nil {
		t.Fatalf("deleting the tail: %v", err)
	}
	err := board.Update(t.Context(), "default", UpdateOptions{}, func(tx *Tx) error {
		id := tx.NextID(0)
		tx.Emit(EventMessagePosted, &MessagePostedPayload{
			ID: id, Channel: "work", Text: "refused", Thread: ThreadKey(id),
			By: projectionAuthor, Labels: map[string]string{},
		})
		return nil
	})
	if !errors.Is(err, store.ErrLogInconsistent) {
		t.Fatalf("a write past a missing tail: %v", err)
	}
	report, err := board.CheckProjection(t.Context(), "default")
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if report.Consistent || !hasProblem(report, "head row") {
		t.Fatalf("the check should name the missing tail: %+v", report)
	}
	if _, err := board.Rebuild(t.Context(), "default"); !errors.Is(err, store.ErrLogInconsistent) {
		t.Fatalf("rebuild over a missing tail: %v", err)
	}
	if text := messageText(t, board, "default", message); text != "corrupted" {
		t.Fatalf("the refused rebuild replaced the cache: %q", text)
	}
}

func TestPostgresIntegrityDirections(t *testing.T) {
	dsn := pgTestDSN(t)
	schema := newPGTestSchema(t, dsn)
	events := openPGTestStore(t, dsn, schema)
	conn := connectPGTestSchema(t, dsn, schema)
	board := newProjectionBoard(events)

	for _, tc := range []struct {
		tenant string
		tamper string
	}{
		{"behind", `UPDATE board_heads SET seq = 0 WHERE tenant_id = $1`},
		{"missing", `DELETE FROM board_heads WHERE tenant_id = $1`},
	} {
		t.Run(tc.tenant, func(t *testing.T) {
			message := appendProjectionMessage(t, board, tc.tenant, "one")
			if _, err := conn.Exec(t.Context(), tc.tamper, tc.tenant); err != nil {
				t.Fatalf("tampering: %v", err)
			}
			integrity, err := events.Integrity(t.Context(), tc.tenant)
			if err != nil {
				t.Fatalf("integrity: %v", err)
			}
			if integrity.Head != 0 || integrity.MaxSeq != 1 || integrity.Consistent() {
				t.Fatalf("integrity after %s: %+v", tc.tenant, integrity)
			}
			report, err := board.CheckProjection(t.Context(), tc.tenant)
			if err != nil {
				t.Fatalf("check: %v", err)
			}
			if report.Consistent || !hasProblem(report, "head row") {
				t.Fatalf("the check should name the %s head: %+v", tc.tenant, report)
			}
			err = board.Update(t.Context(), tc.tenant, UpdateOptions{}, func(tx *Tx) error {
				id := tx.NextID(0)
				tx.Emit(EventMessagePosted, &MessagePostedPayload{
					ID: id, Channel: "work", Text: "refused", Thread: ThreadKey(id),
					By: projectionAuthor, Labels: map[string]string{},
				})
				return nil
			})
			if !errors.Is(err, store.ErrLogInconsistent) {
				t.Fatalf("a write with a %s head: %v", tc.tenant, err)
			}
			if text := messageText(t, board, tc.tenant, message); text != "one" {
				t.Fatalf("the cache changed after a refused write: %q", text)
			}
		})
	}
}

func TestPostgresRebuildSurvivesACanceledLoad(t *testing.T) {
	dsn := pgTestDSN(t)
	schema := newPGTestSchema(t, dsn)
	events := openPGTestStore(t, dsn, schema)
	board := newProjectionBoard(events)
	root, _ := seedProjection(t, board, "default")
	corruptProjection(t, board, "default", root)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := board.Rebuild(canceled, "default"); err == nil {
		t.Fatal("a canceled load should fail the rebuild")
	}
	if text := messageText(t, board, "default", root); text != "corrupted" {
		t.Fatalf("a canceled rebuild replaced the cache: %q", text)
	}
	rebuilt, err := board.Rebuild(t.Context(), "default")
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if !rebuilt.Consistent || messageText(t, board, "default", root) != "root" {
		t.Fatalf("the retry didn't rebuild: %+v", rebuilt)
	}
}

func TestPostgresRebuildWithConcurrentWrites(t *testing.T) {
	dsn := pgTestDSN(t)
	schema := newPGTestSchema(t, dsn)
	writerStore := openPGTestStore(t, dsn, schema)
	rebuilderStore := openPGTestStore(t, dsn, schema)
	writer := newProjectionBoard(writerStore)
	rebuilder := newProjectionBoard(rebuilderStore)

	const writes = 20
	errs := make(chan error, 1)
	var wait sync.WaitGroup
	wait.Add(1)
	go func() {
		defer wait.Done()
		for i := 0; i < writes; i++ {
			var id uint64
			err := writer.Update(context.Background(), "default", UpdateOptions{}, func(tx *Tx) error {
				id = tx.NextID(0)
				tx.Emit(EventMessagePosted, &MessagePostedPayload{
					ID: id, Channel: "work", Text: fmt.Sprintf("concurrent %d", i), Thread: ThreadKey(id),
					By: projectionAuthor, Labels: map[string]string{},
				})
				tx.RecordResult(id)
				return nil
			})
			if err != nil {
				select {
				case errs <- err:
				default:
				}
				return
			}
		}
	}()
	for i := 0; i < 5; i++ {
		if _, err := rebuilder.Rebuild(context.Background(), "default"); err != nil {
			t.Fatalf("rebuild: %v", err)
		}
	}
	wait.Wait()
	select {
	case err := <-errs:
		t.Fatalf("concurrent write: %v", err)
	default:
	}

	// A final rebuild reads the whole log; no concurrent write is lost.
	report, err := rebuilder.Rebuild(context.Background(), "default")
	if err != nil {
		t.Fatalf("final rebuild: %v", err)
	}
	if !report.Consistent {
		t.Fatalf("final rebuild: %+v", report)
	}
	if got := recordCount(t, writerStore, "default"); got != writes {
		t.Fatalf("expected %d events, got %d", writes, got)
	}
	if head, err := rebuilder.Head("default"); err != nil || head != writes {
		t.Fatalf("rebuilt head = %d, %v, want %d", head, err, writes)
	}
}

func TestPostgresRebuildIsTenantScoped(t *testing.T) {
	dsn := pgTestDSN(t)
	schema := newPGTestSchema(t, dsn)
	events := openPGTestStore(t, dsn, schema)
	board := newProjectionBoard(events)
	rootA, _ := seedProjection(t, board, "a")
	seedProjection(t, board, "b")

	beforeB, err := board.CheckProjection(t.Context(), "b")
	if err != nil || !beforeB.Consistent {
		t.Fatalf("check b: %+v, %v", beforeB, err)
	}
	corruptProjection(t, board, "a", rootA)
	if _, err := board.Rebuild(t.Context(), "a"); err != nil {
		t.Fatalf("rebuild a: %v", err)
	}
	afterB, err := board.CheckProjection(t.Context(), "b")
	if err != nil || !afterB.Consistent || afterB.Digest != beforeB.Digest {
		t.Fatalf("tenant b changed by tenant a's rebuild: %+v, %v", afterB, err)
	}
}
