package server_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jixoai/keel-board/audit"
	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/metrics"
	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/server"
	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/syncstore"
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
	h := newSyncHarness(t, syncHarnessOptions{
		mirrorChannels: true, metrics: registry, audit: auditStore,
		bridgeSecret: func(tenant string, key boardsync.Key) (boardsync.Secret, bool) {
			if tenant == "t1" && key == "slack" {
				return "smoke-secret", true
			}
			return "", false
		},
	})
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

	// An unsigned bridge request is refused after the entry and registration
	// are confirmed: a counted bridge error and one durable audit row.
	status, _ = h.request(http.MethodPost, server.APIPrefix+"/sync/bridge/slack/events", map[string]any{}, nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("unsigned bridge = %d, want 401", status)
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

// TestAuditFailureDoesNotChangeAResolvedRequest proves an audit outage cannot
// fail an accepted webhook or a manual retry whose store change committed.
func TestAuditFailureDoesNotChangeAResolvedRequest(t *testing.T) {
	h := newSyncHarness(t, syncHarnessOptions{
		mirrorChannels: true, metrics: metrics.NewRegistry(), audit: failingAudit{},
	})
	event := syncEvent("m1", "C_UNROUTED:1727.9")
	event.Channel = "C_UNROUTED"
	h.provider.ReceiveFunc = func(context.Context, boardsync.Key, *boardsync.WebhookRequest) (boardsync.Intake, error) {
		return boardsync.Intake{
			Records:  []boardsync.Record{{Dedupe: "d1", Event: event}},
			Response: boardsync.OK(),
		}, nil
	}
	status, body := h.request(http.MethodPost, "/webhooks/slack", map[string]any{"event": "message"}, nil)
	if status != http.StatusOK {
		t.Fatalf("accepted webhook with a failing audit = %d %s", status, body)
	}
	depth, err := h.store.InboxDepth(t.Context(), "t1", "slack")
	if err != nil || depth.Pending != 0 {
		t.Fatalf("an unrouted record should store nothing: %+v, %v", depth, err)
	}

	// A dead letter can still be retried when the audit write fails.
	if _, err := h.store.PushInbox(t.Context(), "t1", "slack",
		[]boardsync.Record{{Dedupe: "dead-1", Event: syncEvent("m2", "C0123:1728.1")}}); err != nil {
		t.Fatalf("push dead candidate: %v", err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		items, err := h.store.LeaseInbox(t.Context(), "t1", "slack", 10, "test")
		if err != nil {
			t.Fatalf("lease %d: %v", attempt, err)
		}
		found := false
		for _, item := range items {
			if item.ID != "m2" {
				continue
			}
			found = true
			if _, err := h.store.AckInbox(t.Context(), "t1", "slack", item.ID, item.Lease, errors.New("import failed")); err != nil {
				t.Fatalf("ack %d: %v", attempt, err)
			}
		}
		if !found {
			t.Fatalf("lease %d didn't return the dead candidate: %+v", attempt, items)
		}
	}
	dead, err := h.store.DeadInbox(t.Context(), "t1", "slack", 10)
	if err != nil || len(dead) != 1 {
		t.Fatalf("dead rows = %+v, %v", dead, err)
	}
	status, body = h.request(http.MethodPost, server.APIPrefix+"/sync/inbox/m2/retry?key=slack", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("retry with a failing audit = %d %s", status, body)
	}
	depth, err = h.store.InboxDepth(t.Context(), "t1", "slack")
	if err != nil || depth.Dead != 0 || depth.Pending != 1 {
		t.Fatalf("retry didn't reach the store: %+v, %v", depth, err)
	}
}

// TestBridgeUnknownKeysNeverBecomeLabels proves an unauthenticated bridge
// caller can't mint metric labels: unknown keys land on one fixed series, and
// only a configured, registered connection contributes its own key.
func TestBridgeUnknownKeysNeverBecomeLabels(t *testing.T) {
	registry := metrics.NewRegistry()
	auditStore := audit.NewMemory()
	h := newSyncHarness(t, syncHarnessOptions{mirrorChannels: true, metrics: registry, audit: auditStore})
	attackers := []string{"attacker-one", "attacker-two", "attacker-three"}
	for _, key := range attackers {
		status, _ := h.request(http.MethodPost, server.APIPrefix+"/sync/bridge/"+key+"/events",
			map[string]any{}, nil)
		if status != http.StatusNotFound {
			t.Fatalf("unknown bridge key %q = %d, want 404", key, status)
		}
	}
	text := metricsAt(t, h.http.URL)
	for _, key := range attackers {
		if strings.Contains(text, `key="`+key+`"`) {
			t.Fatalf("an unauthenticated key became a label:\n%s", text)
		}
	}
	if !strings.Contains(text, `board_sync_bridge_errors_total{key="unknown"} 3`) {
		t.Fatalf("unknown bridge keys weren't counted on the fixed series:\n%s", text)
	}
	rows, err := auditStore.Recent(t.Context(), "t1", 100)
	if err != nil || len(rows) != 0 {
		t.Fatalf("unauthenticated bridge probes wrote audit rows: %+v, %v", rows, err)
	}

	// A configured, registered key does contribute its own series (the harness
	// has no bridge secret, so the request fails closed after resolution).
	status, _ := h.request(http.MethodPost, server.APIPrefix+"/sync/bridge/slack/events", map[string]any{}, nil)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("known key without a secret = %d, want 503", status)
	}
	waitForMetricAt(t, h.http.URL, `board_sync_bridge_errors_total{key="slack"} 1`)
	rows, err = auditStore.Recent(t.Context(), "t1", 100)
	if err != nil || len(rows) != 0 {
		t.Fatalf("a pre-signature failure wrote an audit row: %+v, %v", rows, err)
	}
}

// TestUnauthenticatedRequestsProduceBoundedAudit proves a flood of
// unauthenticated requests grows board_audit only within the per-key bound:
// pre-authentication probes never write, and verified failures are capped.
func TestUnauthenticatedRequestsProduceBoundedAudit(t *testing.T) {
	const attempts = 60
	const perKeyLimit = 10

	t.Run("webhook", func(t *testing.T) {
		registry := metrics.NewRegistry()
		auditStore := audit.NewMemory()
		h := newSyncHarness(t, syncHarnessOptions{mirrorChannels: true, metrics: registry, audit: auditStore})
		h.provider.ReceiveFunc = func(context.Context, boardsync.Key, *boardsync.WebhookRequest) (boardsync.Intake, error) {
			return boardsync.Intake{}, boardsync.ErrUnauthorized
		}
		for i := 0; i < attempts; i++ {
			status, _ := h.request(http.MethodPost, "/webhooks/slack", map[string]any{}, nil)
			if status != http.StatusUnauthorized {
				t.Fatalf("rejected webhook %d = %d, want 401", i, status)
			}
		}
		rows, err := auditStore.Recent(t.Context(), "t1", 500)
		if err != nil {
			t.Fatalf("audit recent: %v", err)
		}
		if len(rows) != perKeyLimit {
			t.Fatalf("verified-failure audit rows = %d, want the per-key bound %d", len(rows), perKeyLimit)
		}
		waitForMetricAt(t, h.http.URL,
			fmt.Sprintf(`board_sync_webhook_duration_ms_count{key="slack",platform="slack"} %d`, attempts))

		// Unknown keys are pre-auth probes: counted on the fixed series, never
		// durable.
		for i := 0; i < 20; i++ {
			status, _ := h.request(http.MethodPost, "/webhooks/attacker-"+string(rune('a'+i%26)), map[string]any{}, nil)
			if status != http.StatusOK {
				t.Fatalf("unknown webhook key = %d, want 200", status)
			}
		}
		rows, err = auditStore.Recent(t.Context(), "t1", 500)
		if err != nil || len(rows) != perKeyLimit {
			t.Fatalf("unknown webhook probes grew the audit: %d rows, %v", len(rows), err)
		}
		// A disabled entry is a pre-auth outcome too: 200, counted on the
		// fixed series, never durable.
		if _, err := h.store.SetEntryEnabled(t.Context(), "t1", "slack", false); err != nil {
			t.Fatalf("disable entry: %v", err)
		}
		for i := 0; i < 20; i++ {
			status, _ := h.request(http.MethodPost, "/webhooks/slack", map[string]any{}, nil)
			if status != http.StatusOK {
				t.Fatalf("disabled webhook = %d, want 200", status)
			}
		}
		rows, err = auditStore.Recent(t.Context(), "t1", 500)
		if err != nil || len(rows) != perKeyLimit {
			t.Fatalf("disabled webhook probes grew the audit: %d rows, %v", len(rows), err)
		}
		text := metricsAt(t, h.http.URL)
		if !strings.Contains(text, `board_sync_webhook_duration_ms_count{key="unknown",platform="unknown"} 40`) {
			t.Fatalf("pre-auth webhook probes weren't counted on the fixed series:\n%s", text)
		}
		if strings.Contains(text, `key="attacker-`) {
			t.Fatalf("an unauthenticated webhook key became a label:\n%s", text)
		}
	})

	t.Run("bridge", func(t *testing.T) {
		registry := metrics.NewRegistry()
		auditStore := audit.NewMemory()
		h := newSyncHarness(t, syncHarnessOptions{
			mirrorChannels: true, metrics: registry, audit: auditStore,
			bridgeSecret: func(tenant string, key boardsync.Key) (boardsync.Secret, bool) {
				return "smoke-secret", tenant == "t1" && key == "slack"
			},
		})
		for i := 0; i < attempts; i++ {
			status, _ := h.request(http.MethodPost, server.APIPrefix+"/sync/bridge/slack/events",
				map[string]any{}, nil)
			if status != http.StatusUnauthorized {
				t.Fatalf("unsigned bridge %d = %d, want 401", i, status)
			}
		}
		rows, err := auditStore.Recent(t.Context(), "t1", 500)
		if err != nil {
			t.Fatalf("audit recent: %v", err)
		}
		if len(rows) != perKeyLimit {
			t.Fatalf("verified bridge-failure audit rows = %d, want the per-key bound %d", len(rows), perKeyLimit)
		}
		waitForMetricAt(t, h.http.URL,
			fmt.Sprintf(`board_sync_bridge_errors_total{key="slack"} %d`, attempts))
		// A disabled entry is a silent 200: no metric, no audit.
		if _, err := h.store.SetEntryEnabled(t.Context(), "t1", "slack", false); err != nil {
			t.Fatalf("disable entry: %v", err)
		}
		for i := 0; i < 20; i++ {
			status, _ := h.request(http.MethodPost, server.APIPrefix+"/sync/bridge/slack/events",
				map[string]any{}, nil)
			if status != http.StatusOK {
				t.Fatalf("disabled bridge = %d, want 200", status)
			}
		}
		text := metricsAt(t, h.http.URL)
		if !strings.Contains(text, fmt.Sprintf(`board_sync_bridge_errors_total{key="slack"} %d`, attempts)) {
			t.Fatalf("disabled bridge attempts were counted as errors:\n%s", text)
		}
		rows, err = auditStore.Recent(t.Context(), "t1", 500)
		if err != nil || len(rows) != perKeyLimit {
			t.Fatalf("disabled bridge probes grew the audit: %d rows, %v", len(rows), err)
		}
	})
}

// failingDepthStore reports the optional per-key outbox depth as unavailable.
type failingDepthStore struct{ *syncstore.Memory }

func (failingDepthStore) OutboxDepthByKey(context.Context, string) (map[boardsync.Key]syncstore.OutboxDepth, error) {
	return nil, errors.New("depth read failed")
}

// TestSyncStatusSurvivesOptionalDepthFailure proves the per-key metric read is
// best-effort: the status answer still carries the tenant-wide depth.
func TestSyncStatusSurvivesOptionalDepthFailure(t *testing.T) {
	registry := metrics.NewRegistry()
	h := newSyncHarness(t, syncHarnessOptions{
		mirrorChannels: true, metrics: registry,
		wrapStore: func(store syncstore.Store) syncstore.Store {
			return failingDepthStore{store.(*syncstore.Memory)}
		},
	})
	status, body := h.request(http.MethodGet, server.APIPrefix+"/sync/status", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("sync status with a failing optional depth = %d %s", status, body)
	}
	var view struct {
		Outbox struct {
			Pending int `json:"pending"`
		} `json:"outbox"`
	}
	if err := json.Unmarshal(body, &view); err != nil {
		t.Fatalf("decode status: %v (%s)", err, body)
	}
}

// TestOutboxDeadKeepsRowWhenTargetIsUnbounded proves a platform target with an
// unusual shape omits the target field instead of losing the whole dead-letter
// audit row.
func TestOutboxDeadKeepsRowWhenTargetIsUnbounded(t *testing.T) {
	auditStore := audit.NewMemory()
	api := server.New(server.Options{
		Board:  log.New(log.Options{}),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Audit:  auditStore,
	})
	hooks := api.SyncWorkerHooks()
	hooks.OutboxDead("t1", "slack", "slack", "delivery-1", "slack:C%1:T1", 3, "failed")
	events, err := auditStore.Recent(t.Context(), "t1", 10)
	if err != nil {
		t.Fatalf("recent: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("audit rows = %d, want 1: %+v", len(events), events)
	}
	event := events[0]
	if event.Kind != audit.KindOutboxDead || event.Target != "" || event.DeliveryID != "delivery-1" ||
		event.Status != audit.StatusDead || event.Attempts != 3 {
		t.Fatalf("outbox dead audit = %+v", event)
	}
}
