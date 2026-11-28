package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/jixoai/keel-board/pgadapter/logstore"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/server"
)

// projectionTestDSN returns the PostgreSQL DSN, or skips. CI must set one.
func projectionTestDSN(t *testing.T) string {
	t.Helper()
	for _, name := range []string{"KEEL_BOARD_TEST_DSN", "TEST_DSN"} {
		if dsn := strings.TrimSpace(os.Getenv(name)); dsn != "" {
			return dsn
		}
	}
	if strings.TrimSpace(os.Getenv("CI")) != "" {
		t.Fatal("CI requires KEEL_BOARD_TEST_DSN (or TEST_DSN); PostgreSQL admin tests must not skip")
	}
	t.Skip("PostgreSQL admin tests skipped: set KEEL_BOARD_TEST_DSN (or TEST_DSN)")
	return ""
}

// projectionSchemaSeq keeps generated schema names unique.
var projectionSchemaSeq atomic.Uint64

// newProjectionSchema creates a schema for one test and drops it after.
func newProjectionSchema(t *testing.T, dsn string) string {
	t.Helper()
	name := fmt.Sprintf("keel_board_admin_%d_%d", time.Now().UnixNano(), projectionSchemaSeq.Add(1))
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

// TestPostgresProjectionAdmin proves the admin surface and the write refusal
// against a real PostgreSQL log: a tampered head row answers 503 and no event
// lands; the restore and a rebuild answer a consistent report.
func TestPostgresProjectionAdmin(t *testing.T) {
	dsn := projectionTestDSN(t)
	schema := newProjectionSchema(t, dsn)
	postgres, err := pglogstore.OpenLog(t.Context(), pglogstore.LogOptions{DSN: dsn, SearchPath: schema})
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	defer postgres.Close()
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parsing DSN: %v", err)
	}
	config.RuntimeParams["search_path"] = pgx.Identifier{schema}.Sanitize()
	conn, err := pgx.ConnectConfig(t.Context(), config)
	if err != nil {
		t.Fatalf("connecting to schema: %v", err)
	}
	defer conn.Close(context.Background())

	board := log.New(log.Options{Store: postgres})
	api := server.New(server.Options{
		Board:  board,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	httpServer := httptest.NewServer(api)
	defer httpServer.Close()
	client := httpServer.Client()
	const actor = "local:user:alice"
	call := func(method, path string, body any) (int, []byte) {
		t.Helper()
		var reader io.Reader
		if body != nil {
			encoded, err := json.Marshal(body)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			reader = bytes.NewReader(encoded)
		}
		request, err := http.NewRequestWithContext(t.Context(), method, httpServer.URL+server.APIPrefix+path, reader)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		if body != nil {
			request.Header.Set("content-type", "application/json")
		}
		request.Header.Set("X-Board-Actor", actor)
		response, err := client.Do(request)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		return response.StatusCode, data
	}

	status, data := call(http.MethodPost, "/messages", map[string]any{"channel": "work", "text": "one"})
	if status != http.StatusCreated {
		t.Fatalf("post: %d %s", status, data)
	}
	records, err := postgres.Load(t.Context(), "default")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	eventsBefore := len(records)
	status, data = call(http.MethodPost, "/admin/projection/check", nil)
	if status != http.StatusOK {
		t.Fatalf("check: %d %s", status, data)
	}
	var report log.ProjectionReport
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	if !report.Consistent || report.LogHead != uint64(eventsBefore) {
		t.Fatalf("check: %+v", report)
	}

	// Tamper with the head row: a write must answer 503 unavailable, and the
	// log must not grow.
	head := report.HeadRow
	if _, err := conn.Exec(t.Context(), `UPDATE board_heads SET seq = seq + 1 WHERE tenant_id = $1`, "default"); err != nil {
		t.Fatalf("tampering: %v", err)
	}
	status, data = call(http.MethodPost, "/messages", map[string]any{"channel": "work", "text": "refused"})
	if status != http.StatusServiceUnavailable {
		t.Fatalf("a write over a tampered head: %d %s", status, data)
	}
	if failure := decode[protocol.ErrorBody](t, data); failure.Error.Code != protocol.CodeUnavailable {
		t.Fatalf("expected unavailable, got %s", failure.Error.Code)
	}
	after, err := postgres.Load(t.Context(), "default")
	if err != nil {
		t.Fatalf("load after refused write: %v", err)
	}
	if len(after) != eventsBefore {
		t.Fatalf("a refused write landed: %d -> %d events", eventsBefore, len(after))
	}
	status, data = call(http.MethodPost, "/admin/projection/check", nil)
	if status != http.StatusOK {
		t.Fatalf("check after tamper: %d %s", status, data)
	}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("decode tampered report: %v", err)
	}
	if report.Consistent {
		t.Fatalf("a tampered head looked consistent: %+v", report)
	}
	// A rebuild cannot repair a broken head row: it answers 503, not a report
	// that silently omits the event the head names.
	status, data = call(http.MethodPost, "/admin/projection/rebuild", nil)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("rebuild over a tampered head: %d %s", status, data)
	}
	if failure := decode[protocol.ErrorBody](t, data); failure.Error.Code != protocol.CodeUnavailable {
		t.Fatalf("expected unavailable, got %s", failure.Error.Code)
	}

	// Restoring the head and rebuilding reconciles the cache and the log.
	if _, err := conn.Exec(t.Context(), `UPDATE board_heads SET seq = $2 WHERE tenant_id = $1`, "default", head); err != nil {
		t.Fatalf("restoring: %v", err)
	}
	status, data = call(http.MethodPost, "/admin/projection/rebuild", nil)
	if status != http.StatusOK {
		t.Fatalf("rebuild: %d %s", status, data)
	}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("decode rebuilt report: %v", err)
	}
	if !report.Consistent {
		t.Fatalf("the rebuilt report: %+v", report)
	}
	status, data = call(http.MethodPost, "/messages", map[string]any{"channel": "work", "text": "two"})
	if status != http.StatusCreated {
		t.Fatalf("post after restore: %d %s", status, data)
	}
}
