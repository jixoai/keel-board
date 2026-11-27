package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/server"
	"github.com/jixoai/keel-board/store"
)

// TestProjectionAdminCheckAndRebuild drives the operations routes over an
// in-memory board: a check reports a consistent projection, a rebuild keeps
// it consistent and makes no events.
func TestProjectionAdminCheckAndRebuild(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"
	message := h.postMessage(alice, "work", "admin")
	head, err := h.board.Head("default")
	if err != nil {
		t.Fatalf("head: %v", err)
	}

	status, data := h.request(http.MethodPost, "/admin/projection/check", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	report := decode[log.ProjectionReport](t, data)
	if !report.Consistent || report.Tenant != "default" || report.LogHead != head {
		t.Fatalf("check: %+v", report)
	}
	if report.Digest == "" || report.Digest != report.ReplayDigest {
		t.Fatalf("check digests: %+v", report)
	}

	status, data = h.request(http.MethodPost, "/admin/projection/rebuild", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	rebuilt := decode[log.ProjectionReport](t, data)
	if !rebuilt.Consistent || rebuilt.Digest != report.Digest {
		t.Fatalf("rebuild: %+v, want digest %s", rebuilt, report.Digest)
	}
	// A rebuild is idempotent and doesn't append.
	status, data = h.request(http.MethodPost, "/admin/projection/rebuild", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	again := decode[log.ProjectionReport](t, data)
	if !again.Consistent || again.Digest != rebuilt.Digest {
		t.Fatalf("second rebuild: %+v", again)
	}
	after, err := h.board.Head("default")
	if err != nil || after != head {
		t.Fatalf("rebuild moved the head: %d, %v, want %d", after, err, head)
	}

	// The rebuilt projection still serves the message.
	status, data = h.request(http.MethodGet, "/messages/"+string(message.ID), nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if thread := decode[protocol.ThreadView](t, data); thread.Message.ID != message.ID {
		t.Fatalf("read after rebuild: %+v", thread.Message)
	}
}

// TestProjectionAdminRequiresManage proves the operations routes ask for
// manage at `*`, and an ordinary write is unaffected by that refusal.
func TestProjectionAdminRequiresManage(t *testing.T) {
	h := newHarness(t, server.Options{Authorize: server.AuthorizerFunc(
		func(_ context.Context, _ string, _ protocol.Actor, channel string, permission protocol.Permission) error {
			if channel == "*" && permission == protocol.PermissionManage {
				return protocol.Forbidden("no board management")
			}
			return nil
		},
	)})
	const alice = "local:user:alice"
	status, data := h.request(http.MethodPost, "/admin/projection/check", nil, alice, "", nil)
	h.expect(status, data, http.StatusForbidden)
	status, data = h.request(http.MethodPost, "/admin/projection/rebuild", nil, alice, "", nil)
	h.expect(status, data, http.StatusForbidden)

	// A caller that may write normally still can.
	h.postMessage(alice, "work", "not an admin")
}

// TestProjectionCheckIsTenantScoped proves the report concerns the request's
// board, never another's.
func TestProjectionCheckIsTenantScoped(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"
	status, data := h.request(http.MethodPost, "/messages", map[string]any{"channel": "work", "text": "for a"}, alice, "a", nil)
	h.expect(status, data, http.StatusCreated)

	status, data = h.request(http.MethodPost, "/admin/projection/check", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if report := decode[log.ProjectionReport](t, data); report.Tenant != "default" || report.LogHead != 0 {
		t.Fatalf("default board report: %+v", report)
	}
	status, data = h.request(http.MethodPost, "/admin/projection/check", nil, alice, "b", nil)
	h.expect(status, data, http.StatusOK)
	if report := decode[log.ProjectionReport](t, data); report.Tenant != "b" || report.LogHead != 0 {
		t.Fatalf("tenant b report: %+v", report)
	}
	status, data = h.request(http.MethodPost, "/admin/projection/check", nil, alice, "a", nil)
	h.expect(status, data, http.StatusOK)
	if report := decode[log.ProjectionReport](t, data); report.Tenant != "a" || report.LogHead == 0 {
		t.Fatalf("tenant a report: %+v", report)
	}
}

// failingAppendStore refuses every append as an inconsistent log.
type failingAppendStore struct {
	*store.Memory
	fail atomic.Bool
}

func (f *failingAppendStore) Append(ctx context.Context, tenant string, records []store.Record, expectedHead uint64) (store.AppendResult, error) {
	if f.fail.Load() {
		return store.AppendResult{}, store.ErrLogInconsistent
	}
	return f.Memory.Append(ctx, tenant, records, expectedHead)
}

// TestProjectionWriteRefusedAsUnavailable proves an inconsistent log's write
// is the board's 503 unavailable, not a 500 or a partial write.
func TestProjectionWriteRefusedAsUnavailable(t *testing.T) {
	events := &failingAppendStore{Memory: store.NewMemory()}
	board := log.New(log.Options{Store: events})
	httpServer := httptest.NewServer(server.New(server.Options{Board: board}))
	defer httpServer.Close()

	post := func(text string) (int, protocol.ErrorBody) {
		body, err := json.Marshal(map[string]any{"channel": "work", "text": text})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		response, err := http.Post(httpServer.URL+server.APIPrefix+"/messages", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		defer response.Body.Close()
		var failure protocol.ErrorBody
		if response.StatusCode != http.StatusCreated {
			if err := json.NewDecoder(response.Body).Decode(&failure); err != nil {
				t.Fatalf("decode failure: %v", err)
			}
		}
		return response.StatusCode, failure
	}

	events.fail.Store(true)
	status, failure := post("refused")
	if status != http.StatusServiceUnavailable || failure.Error.Code != protocol.CodeUnavailable {
		t.Fatalf("inconsistent log write: %d %+v", status, failure)
	}
	if head, err := board.Head("default"); err != nil || head != 0 {
		t.Fatalf("a refused write landed: head %d, %v", head, err)
	}
	events.fail.Store(false)
	if status, _ := post("allowed"); status != http.StatusCreated {
		t.Fatalf("a healthy write: %d", status)
	}
}
