package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
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
	// The idempotent path goes through the same refusal.
	keyed, err := http.NewRequest(http.MethodPost, httpServer.URL+server.APIPrefix+"/messages",
		bytes.NewReader([]byte(`{"channel":"work","text":"keyed"}`)))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	keyed.Header.Set("content-type", "application/json")
	keyed.Header.Set("Idempotency-Key", "inconsistent")
	response, err := httpServer.Client().Do(keyed)
	if err != nil {
		t.Fatalf("keyed post: %v", err)
	}
	data, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("keyed write over an inconsistent log: %d %s", response.StatusCode, data)
	}
	if head, err := board.Head("default"); err != nil || head != 0 {
		t.Fatalf("a refused write landed: head %d, %v", head, err)
	}
	events.fail.Store(false)
	if status, _ := post("allowed"); status != http.StatusCreated {
		t.Fatalf("a healthy write: %d", status)
	}
}

// behindHeadStore reports a head older than the events its board has cached,
// as a corrupt projection would.
type behindHeadStore struct {
	*store.Memory
}

func (s *behindHeadStore) Head(context.Context, string) (uint64, error) { return 0, nil }

// TestProjectionAheadIsUnavailableOnSSEAndIdempotentWrites proves both the
// stream's preflight and an idempotent retry answer 503 when the projection is
// ahead of the log, rather than a 500 or a stale success.
func TestProjectionAheadIsUnavailableOnSSEAndIdempotentWrites(t *testing.T) {
	events := &behindHeadStore{Memory: store.NewMemory()}
	board := log.New(log.Options{Store: events})
	if err := board.Update(t.Context(), "default", log.UpdateOptions{}, func(tx *log.Tx) error {
		id := tx.NextID(0)
		tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
			ID: id, Channel: "work", Text: "seeded", Thread: log.ThreadKey(id),
			By:     protocol.Author{Actor: "local:user:alice", Principal: "local:user:alice"},
			Labels: map[string]string{},
		})
		tx.RecordResult(id)
		return nil
	}); err != nil {
		t.Fatalf("seeding: %v", err)
	}
	httpServer := httptest.NewServer(server.New(server.Options{Board: board}))
	defer httpServer.Close()
	client := httpServer.Client()

	post, err := http.NewRequest(http.MethodPost, httpServer.URL+server.APIPrefix+"/messages",
		bytes.NewReader([]byte(`{"channel":"work","text":"retry"}`)))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	post.Header.Set("content-type", "application/json")
	post.Header.Set("Idempotency-Key", "ahead-retry")
	response, err := client.Do(post)
	if err != nil {
		t.Fatalf("idempotent post: %v", err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("idempotent write over an ahead projection: %d %s", response.StatusCode, body)
	}
	if failure := decode[protocol.ErrorBody](t, body); failure.Error.Code != protocol.CodeUnavailable {
		t.Fatalf("expected unavailable, got %s", failure.Error.Code)
	}

	stream, err := client.Get(httpServer.URL + server.APIPrefix + "/events")
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	streamBody, _ := io.ReadAll(stream.Body)
	stream.Body.Close()
	if stream.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("stream over an ahead projection: %d %s", stream.StatusCode, streamBody)
	}
	if failure := decode[protocol.ErrorBody](t, streamBody); failure.Error.Code != protocol.CodeUnavailable {
		t.Fatalf("expected unavailable, got %s", failure.Error.Code)
	}
}

// badLogStore serves a fixed, deliberately broken log and refuses writes.
type badLogStore struct {
	records []store.Record
}

func (s *badLogStore) Load(context.Context, string) ([]store.Record, error) {
	return append([]store.Record(nil), s.records...), nil
}

func (s *badLogStore) Append(context.Context, string, []store.Record, uint64) (store.AppendResult, error) {
	return store.AppendResult{}, store.ErrLogInconsistent
}

// TestProjectionRebuildClassifiesBrokenLogs proves a gap and an undecodable
// known event are 503 unavailable and are logged, not a generic 500.
func TestProjectionRebuildClassifiesBrokenLogs(t *testing.T) {
	valid := func(seq uint64) store.Record {
		return store.Record{
			Seq: seq, Type: string(log.EventMessagePosted), Channel: "work",
			Payload: []byte(`{"id":` + string(rune('0'+seq)) + `,"channel":"work","text":"one","thread":"1","labels":{},"by":{"actor":"local:user:alice","principal":"local:user:alice"}}`),
		}
	}
	cases := []struct {
		name    string
		records []store.Record
	}{
		{"gap", []store.Record{valid(1), valid(3)}},
		{"undecodable", []store.Record{{
			Seq: 1, Type: string(log.EventMessagePosted), Channel: "work",
			Payload: []byte(`{"id":"not-a-number"}`),
		}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var logged bytes.Buffer
			board := log.New(log.Options{Store: &badLogStore{records: tc.records}})
			api := server.New(server.Options{
				Board:  board,
				Logger: slog.New(slog.NewTextHandler(&logged, nil)),
			})
			httpServer := httptest.NewServer(api)
			defer httpServer.Close()

			request, err := http.NewRequest(http.MethodPost, httpServer.URL+server.APIPrefix+"/admin/projection/rebuild", nil)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			response, err := httpServer.Client().Do(request)
			if err != nil {
				t.Fatalf("rebuild: %v", err)
			}
			body, _ := io.ReadAll(response.Body)
			response.Body.Close()
			if response.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("%s rebuild: %d %s", tc.name, response.StatusCode, body)
			}
			if failure := decode[protocol.ErrorBody](t, body); failure.Error.Code != protocol.CodeUnavailable {
				t.Fatalf("expected unavailable, got %s", failure.Error.Code)
			}
			if !strings.Contains(logged.String(), "log and its projection disagree") {
				t.Fatalf("%s rebuild wasn't logged: %q", tc.name, logged.String())
			}
		})
	}
}

// brokenHeadStore reports a log-inconsistent head.
type brokenHeadStore struct {
	*store.Memory
}

func (s *brokenHeadStore) Head(context.Context, string) (uint64, error) {
	return 0, store.ErrLogInconsistent
}

// TestLogInconsistentIsUnavailableOnSSEAndIdempotentWrites covers the other
// sentinel on the stream preflight and the idempotent lookup path.
func TestLogInconsistentIsUnavailableOnSSEAndIdempotentWrites(t *testing.T) {
	events := &brokenHeadStore{Memory: store.NewMemory()}
	board := log.New(log.Options{Store: events, Freshness: -1})
	if err := board.Update(t.Context(), "default", log.UpdateOptions{}, func(tx *log.Tx) error {
		id := tx.NextID(0)
		tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
			ID: id, Channel: "work", Text: "seeded", Thread: log.ThreadKey(id),
			By:     protocol.Author{Actor: "local:user:alice", Principal: "local:user:alice"},
			Labels: map[string]string{},
		})
		tx.RecordResult(id)
		return nil
	}); err != nil {
		t.Fatalf("seeding: %v", err)
	}
	httpServer := httptest.NewServer(server.New(server.Options{
		Board:  board,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}))
	defer httpServer.Close()
	client := httpServer.Client()

	post, err := http.NewRequest(http.MethodPost, httpServer.URL+server.APIPrefix+"/messages",
		bytes.NewReader([]byte(`{"channel":"work","text":"retry"}`)))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	post.Header.Set("content-type", "application/json")
	post.Header.Set("Idempotency-Key", "inconsistent-retry")
	response, err := client.Do(post)
	if err != nil {
		t.Fatalf("idempotent post: %v", err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("idempotent write with a broken head: %d %s", response.StatusCode, body)
	}

	stream, err := client.Get(httpServer.URL + server.APIPrefix + "/events")
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	streamBody, _ := io.ReadAll(stream.Body)
	stream.Body.Close()
	if stream.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("stream with a broken head: %d %s", stream.StatusCode, streamBody)
	}
}
