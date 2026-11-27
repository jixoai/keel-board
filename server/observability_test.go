package server_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jixoai/keel-board/audit"
	"github.com/jixoai/keel-board/metrics"
	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/server"
	boardsync "github.com/jixoai/keel-board/sync"
)

// metricsAt fetches GET /metrics from a harness base URL and returns the body.
func metricsAt(t *testing.T, baseURL string) string {
	t.Helper()
	response, err := http.Get(baseURL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics = %d", response.StatusCode)
	}
	if contentType := response.Header.Get("content-type"); !strings.HasPrefix(contentType, "text/plain") {
		t.Fatalf("metrics content-type = %q", contentType)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("reading metrics: %v", err)
	}
	return string(body)
}

// waitForMetric polls the exposition until check passes or the deadline ends.
func waitForMetricAt(t *testing.T, baseURL, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		last = metricsAt(t, baseURL)
		if strings.Contains(last, want) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("metric %q never appeared:\n%s", want, last)
}

// TestMetricsEndpointExposesBoundedBoardMetrics proves GET /metrics answers
// outside /v1/board, carries the design's names, and leaks no board content.
func TestMetricsEndpointExposesBoundedBoardMetrics(t *testing.T) {
	registry := metrics.NewRegistry()
	h := newHarness(t, server.Options{Metrics: registry})
	status, data := h.request(http.MethodPost, "/channels", map[string]any{"path": "work"}, "", "", nil)
	h.expect(status, data, http.StatusCreated)
	const secret = "top-secret-body-that-must-not-be-a-label"
	status, data = h.request(http.MethodPost, "/messages", map[string]any{"channel": "work", "text": secret}, "", "", nil)
	h.expect(status, data, http.StatusCreated)
	// A rebuild records its duration.
	status, data = h.request(http.MethodPost, "/admin/projection/rebuild", nil, "", "", nil)
	h.expect(status, data, http.StatusOK)

	text := metricsAt(t, h.http.URL)
	for _, want := range []string{
		"# TYPE board_events_total counter",
		`board_events_total{kind="channel.created"}`,
		`board_events_total{kind="message.posted"}`,
		"# TYPE board_append_duration_ms histogram",
		"board_append_duration_ms_count",
		"# TYPE board_tasks_live_claims gauge",
		"# TYPE board_lease_expired_total counter",
		"# TYPE board_sse_connections gauge",
		"# TYPE board_sse_lag_seq gauge",
		"# TYPE board_projection_rebuild_duration_ms histogram",
		"board_projection_rebuild_duration_ms_count 1",
		"# TYPE board_sync_inbox_depth gauge",
		"# TYPE board_sync_outbox_depth gauge",
		"# TYPE board_sync_delivery_failures_total counter",
		"# TYPE board_sync_dead_letters_total counter",
		"# TYPE board_sync_webhook_duration_ms histogram",
		"# TYPE board_sync_dedupe_hits_total counter",
		"# TYPE board_sync_bridge_errors_total counter",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("metrics missing %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, secret) {
		t.Fatalf("the exposition leaked a message body:\n%s", text)
	}
	if strings.Contains(text, "default") {
		t.Fatalf("the exposition leaked a tenant:\n%s", text)
	}
	// Without metrics configured, the route doesn't exist.
	plain := newHarness(t)
	response, err := http.Get(plain.http.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics without metrics: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /metrics without metrics = %d, want 404", response.StatusCode)
	}
}

// TestSSEConnectionGauge tracks one open stream.
func TestSSEConnectionGauge(t *testing.T) {
	registry := metrics.NewRegistry()
	h := newHarness(t, server.Options{Metrics: registry})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, h.http.URL+server.APIPrefix+"/events", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	reader := bufio.NewReader(response.Body)
	line, err := reader.ReadString('\n')
	if err != nil || !strings.Contains(line, "connected") {
		t.Fatalf("stream open line = %q, %v", line, err)
	}
	waitForMetricAt(t, h.http.URL, "board_sse_connections 1")
	waitForMetricAt(t, h.http.URL, "board_sse_lag_seq 0")
	cancel()
	response.Body.Close()
	waitForMetricAt(t, h.http.URL, "board_sse_connections 0")
}

// TestLeaseExpiryMetrics proves a committed task.expired counts and the live
// claim gauge follows the board.
func TestLeaseExpiryMetrics(t *testing.T) {
	registry := metrics.NewRegistry()
	h := newHarness(t, server.Options{Metrics: registry})
	const alice = "local:user:alice"
	status, data := h.request(http.MethodPost, "/tasks", map[string]any{
		"channel": "work", "text": "deploy", "claim": true, "lease": 30,
	}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	created := decode[taskBody](t, data)
	waitForMetricAt(t, h.http.URL, "board_tasks_live_claims 1")
	waitForMetricAt(t, h.http.URL, `board_events_total{kind="task.claimed"} 1`)

	h.clock.Advance(31 * time.Second)
	// Claiming a lapsed task writes task.expired first, then the new claim.
	status, data = h.request(http.MethodPost, "/tasks/"+created.Task.ID+"/claim", map[string]any{"lease": 60},
		"local:user:bob", "", nil)
	h.expect(status, data, http.StatusOK)
	waitForMetricAt(t, h.http.URL, "board_lease_expired_total 1")
	waitForMetricAt(t, h.http.URL, `board_events_total{kind="task.expired"} 1`)
	// The new claim is live again.
	waitForMetricAt(t, h.http.URL, "board_tasks_live_claims 1")
}

// TestSyncMetricsAndAudit proves the webhook path, dedupe, inbox/outbox depth,
// and bridge failures reach the exposition and the durable audit.
func TestSyncMetricsAndAudit(t *testing.T) {
	registry := metrics.NewRegistry()
	auditStore := audit.NewMemory()
	h := newSyncHarness(t, syncHarnessOptions{mirrorChannels: true, metrics: registry, audit: auditStore})
	event := syncEvent("m1", "C0123:1727.9")
	h.provider.ReceiveFunc = func(context.Context, boardsync.Key, *boardsync.WebhookRequest) (boardsync.Intake, error) {
		return boardsync.Intake{
			Records:  []boardsync.Record{{Dedupe: "d1", Event: event}},
			Response: boardsync.OK(),
		}, nil
	}
	for attempt := 0; attempt < 2; attempt++ {
		status, body := h.request(http.MethodPost, "/webhooks/slack", map[string]any{"event": "message"}, nil)
		if status != http.StatusOK {
			t.Fatalf("webhook %d = %d %s", attempt, status, body)
		}
	}
	// One outbox row so the per-key outbox depth has a value to export.
	if _, err := h.store.EnqueueOutbox(t.Context(), "t1", "slack",
		boardsync.BoardEvent{Tenant: "t1", ID: 1, Type: boardsync.BoardMessagePosted, Thread: 1},
		boardsync.Message{ID: 1, Thread: 1, Channel: "support", Text: "hello"},
		[]boardsync.ResolvedTarget{{Key: "slack", Platform: "slack", Channel: "C0123", Kind: boardsync.TargetChannel}},
	); err != nil {
		t.Fatalf("enqueue outbox: %v", err)
	}
	status, body := h.request(http.MethodGet, server.APIPrefix+"/sync/status", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("sync status = %d %s", status, body)
	}
	waitForMetricAt(t, h.http.URL, `board_sync_inbox_depth{key="slack",status="pending"} 1`)
	waitForMetricAt(t, h.http.URL, `board_sync_outbox_depth{key="slack",status="pending"} 1`)
	waitForMetricAt(t, h.http.URL, `board_sync_dedupe_hits_total{key="slack"} 1`)
	waitForMetricAt(t, h.http.URL, `board_sync_webhook_duration_ms_count{key="slack",platform="slack"} 2`)

	// A bridge request with no configured secret fails closed; that is a
	// counted bridge error and an audit row.
	status, _ = h.request(http.MethodPost, server.APIPrefix+"/sync/bridge/slack/events", map[string]any{}, nil)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("bridge without a secret = %d, want 503", status)
	}
	waitForMetricAt(t, h.http.URL, `board_sync_bridge_errors_total{key="slack"} 1`)

	status, body = h.request(http.MethodGet, server.APIPrefix+"/admin/audit?limit=50", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("admin audit = %d %s", status, body)
	}
	var view struct {
		Events []struct {
			Kind   string `json:"kind"`
			Key    string `json:"key"`
			Status string `json:"status"`
			Detail map[string]any
		} `json:"events"`
		Count int `json:"count"`
	}
	if err := json.Unmarshal(body, &view); err != nil {
		t.Fatalf("decode audit: %v (%s)", err, body)
	}
	kinds := map[string]int{}
	for _, event := range view.Events {
		kinds[event.Kind]++
	}
	if kinds[audit.KindWebhookAccepted] != 2 {
		t.Fatalf("webhook audit rows = %v, want 2 accepted", kinds)
	}
	if kinds[audit.KindBridgeRejected] != 1 {
		t.Fatalf("bridge audit rows = %v, want 1", kinds)
	}
	if strings.Contains(string(body), `"tenant"`) || strings.Contains(string(body), "t1") {
		t.Fatalf("audit exposed a tenant: %s", body)
	}
}

// TestAdminAuditPermissionsAndRedaction proves the audit route needs manage
// and answers 501 without a store.
func TestAdminAuditPermissionsAndRedaction(t *testing.T) {
	auditStore := audit.NewMemory()
	ctx := t.Context()
	for _, event := range []audit.Event{
		{Tenant: "default", Kind: audit.KindInboxDead, Key: "slack", Platform: "slack", Status: audit.StatusDead, Attempts: 3,
			Detail: map[string]any{"code": "rate_limited", "event": "Ev1"}},
		{Tenant: "default", Kind: audit.KindOutboxRetry, DeliveryID: "delivery_abcd", Status: audit.StatusRetried},
	} {
		if _, err := auditStore.Append(ctx, event); err != nil {
			t.Fatalf("seed audit: %v", err)
		}
	}
	h := newHarness(t, server.Options{Audit: auditStore})
	status, body := h.request(http.MethodGet, "/admin/audit?limit=10", nil, "", "", nil)
	h.expect(status, body, http.StatusOK)
	var view struct {
		Events []struct {
			Kind     string         `json:"kind"`
			Attempts int            `json:"attempts"`
			Detail   map[string]any `json:"detail"`
		} `json:"events"`
		Count int `json:"count"`
	}
	if err := json.Unmarshal(body, &view); err != nil {
		t.Fatalf("decode: %v (%s)", err, body)
	}
	if view.Count != 2 || len(view.Events) != 2 {
		t.Fatalf("audit view = %+v", view)
	}
	if view.Events[0].Kind != audit.KindOutboxRetry {
		t.Fatalf("audit isn't newest first: %+v", view.Events)
	}
	if view.Events[1].Detail["code"] != "rate_limited" || view.Events[1].Attempts != 3 {
		t.Fatalf("audit detail = %+v", view.Events[1])
	}
	if strings.Contains(string(body), `"tenant"`) {
		t.Fatalf("audit exposed the tenant field: %s", body)
	}

	deny := server.AuthorizerFunc(func(_ context.Context, _ string, _ protocol.Actor, channel string, permission protocol.Permission) error {
		if permission == protocol.PermissionManage && channel == "*" {
			return protocol.Forbidden("no manage here")
		}
		return nil
	})
	denied := newHarness(t, server.Options{Audit: auditStore, Authorize: deny})
	status, body = denied.request(http.MethodGet, "/admin/audit", nil, "", "", nil)
	denied.expect(status, body, http.StatusForbidden)

	empty := newHarness(t)
	status, body = empty.request(http.MethodGet, "/admin/audit", nil, "", "", nil)
	empty.expect(status, body, http.StatusNotImplemented)
	var failure struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &failure); err != nil || failure.Error.Code != string(protocol.CodeUnavailable) {
		t.Fatalf("missing-audit error = %s", body)
	}
}

// failingAudit is a store whose every call fails, to prove the audit never
// changes a request's result.
type failingAudit struct{}

func (failingAudit) Append(context.Context, audit.Event) (audit.Event, error) {
	return audit.Event{}, errors.New("audit is down")
}

func (failingAudit) Recent(context.Context, string, int) ([]audit.Event, error) {
	return nil, errors.New("audit is down")
}

func TestAuditFailureDoesNotChangeTheRequest(t *testing.T) {
	h := newHarness(t, server.Options{Audit: failingAudit{}})
	status, data := h.request(http.MethodPost, "/channels", map[string]any{"path": "work"}, "", "", nil)
	h.expect(status, data, http.StatusCreated)
	status, data = h.request(http.MethodPost, "/messages", map[string]any{"channel": "work", "text": "still lands"},
		"", "", nil)
	h.expect(status, data, http.StatusCreated)
}
