package server_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jixoai/keel-board/automation"
	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/server"
)

type automationWatcherBody struct {
	Watcher struct {
		ID      string `json:"id"`
		Channel string `json:"channel"`
		Action  string `json:"action"`
		Enabled bool   `json:"enabled"`
		Cursor  string `json:"cursor"`
	} `json:"watcher"`
}

type automationScheduleBody struct {
	Schedule struct {
		ID           string  `json:"id"`
		Channel      string  `json:"channel"`
		Re           *string `json:"re"`
		Status       string  `json:"status"`
		Sent         int     `json:"sent"`
		Creator      string  `json:"creator"`
		CancelReason string  `json:"cancelReason"`
	} `json:"schedule"`
}

func newAutomationHarness(t *testing.T, options ...server.Options) (*harness, *automation.Memory) {
	t.Helper()
	memory, err := automation.NewMemory(automation.Config{
		Retry:    automation.RetryPolicy{MaxAttempts: 2},
		LeaseTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := automation.NewRegistryWith(automation.PostAction{})
	if err != nil {
		t.Fatal(err)
	}
	if len(options) == 0 {
		options = []server.Options{{}}
	}
	options[0].Automation = &server.AutomationOptions{Store: memory, Actions: registry}
	return newHarness(t, options...), memory
}

func TestWatcherManagementAPI(t *testing.T) {
	h, store := newAutomationHarness(t)
	const alice = "local:user:alice"

	status, data := h.request(http.MethodPost, "/watchers", map[string]any{
		"id": "alerts", "channel": "work",
		"filter": map[string]any{"types": []string{"message.posted"}, "labels": map[string]string{"kind": "alert"}},
		"action": map[string]any{"name": "post", "params": map[string]string{"text": "secret-param"}},
		"bot":    "local:bot:alerts",
	}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	if strings.Contains(string(data), "secret-param") {
		t.Fatalf("the response leaked action parameters: %s", data)
	}
	created := decode[automationWatcherBody](t, data).Watcher
	if created.ID != "alerts" || created.Channel != "work" || created.Action != "post" || !created.Enabled {
		t.Fatalf("created watcher = %+v", created)
	}
	status, data = h.request(http.MethodGet, "/watchers/alerts", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if got := decode[automationWatcherBody](t, data).Watcher.ID; got != "alerts" {
		t.Fatalf("get returned %q", got)
	}
	status, data = h.request(http.MethodGet, "/watchers", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	var list struct {
		Watchers []struct {
			ID string `json:"id"`
		} `json:"watchers"`
	}
	if err := jsonUnmarshal(data, &list); err != nil || len(list.Watchers) != 1 {
		t.Fatalf("list = %s, %v", data, err)
	}

	// The ID is per tenant; another tenant sees a 404, not another board's
	// watcher.
	status, data = h.request(http.MethodGet, "/watchers/alerts", nil, alice, "other", nil)
	if status != http.StatusNotFound {
		t.Fatalf("cross-tenant get = %d: %s", status, data)
	}
	status, data = h.request(http.MethodGet, "/watchers", nil, alice, "other", nil)
	h.expect(status, data, http.StatusOK)
	if err := jsonUnmarshal(data, &list); err != nil || len(list.Watchers) != 0 {
		t.Fatalf("cross-tenant list = %s", data)
	}

	// A duplicate ID is a conflict; an unknown action and a URL parameter are
	// refused before storage.
	status, data = h.request(http.MethodPost, "/watchers", map[string]any{
		"id": "alerts", "channel": "work", "action": map[string]any{"name": "post", "params": map[string]string{"text": "x"}},
	}, alice, "", nil)
	if status != http.StatusConflict {
		t.Fatalf("duplicate create = %d: %s", status, data)
	}
	status, data = h.request(http.MethodPost, "/watchers", map[string]any{
		"id": "unknown", "channel": "work", "action": map[string]any{"name": "missing"},
	}, alice, "", nil)
	if status != http.StatusBadRequest {
		t.Fatalf("unknown action = %d: %s", status, data)
	}
	status, data = h.request(http.MethodPost, "/watchers", map[string]any{
		"id": "url", "channel": "work",
		"action": map[string]any{"name": "post", "params": map[string]string{"url": "https://example.com/hook"}},
	}, alice, "", nil)
	if status != http.StatusBadRequest {
		t.Fatalf("URL action = %d: %s", status, data)
	}

	// Update replaces declarative fields and preserves the enabled flag.
	status, data = h.request(http.MethodPost, "/watchers/alerts", map[string]any{
		"channel": "work/", "filter": map[string]any{"types": []string{"labels.changed"}},
		"action": map[string]any{"name": "post", "params": map[string]string{"text": "secret-param-2"}},
	}, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if strings.Contains(string(data), "secret-param-2") {
		t.Fatalf("update response leaked parameters: %s", data)
	}
	updated := decode[automationWatcherBody](t, data).Watcher
	if updated.Channel != "work/" || !updated.Enabled {
		t.Fatalf("updated watcher = %+v", updated)
	}
	status, data = h.request(http.MethodPost, "/watchers/alerts/disable", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if decode[automationWatcherBody](t, data).Watcher.Enabled {
		t.Fatal("disable did not disable")
	}
	status, data = h.request(http.MethodPost, "/watchers/alerts/enable", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	enabled := decode[automationWatcherBody](t, data).Watcher
	if !enabled.Enabled {
		t.Fatal("enable did not enable")
	}

	// Deleting removes it; a second delete is a 404.
	status, data = h.request(http.MethodDelete, "/watchers/alerts", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	status, data = h.request(http.MethodGet, "/watchers/alerts", nil, alice, "", nil)
	if status != http.StatusNotFound {
		t.Fatalf("after delete = %d: %s", status, data)
	}
	_ = store
}

func TestWatcherManagementPermissions(t *testing.T) {
	deny := server.AuthorizerFunc(func(_ context.Context, _ string, actor protocol.Actor, channel string, permission protocol.Permission) error {
		if actor.ID == "local:user:bob" && permission == protocol.PermissionManage {
			return protocol.Forbidden("bob may not manage")
		}
		if actor.ID == "local:user:alice" && permission == protocol.PermissionPost && channel == "work" {
			return protocol.Forbidden("alice may not post in work")
		}
		return nil
	})
	h, _ := newAutomationHarness(t, server.Options{Authorize: deny})
	const alice = "local:user:alice"
	const bob = "local:user:bob"

	status, data := h.request(http.MethodGet, "/watchers", nil, bob, "", nil)
	if status != http.StatusForbidden {
		t.Fatalf("bob list = %d: %s", status, data)
	}
	status, data = h.request(http.MethodPost, "/watchers", map[string]any{
		"channel": "work", "action": map[string]any{"name": "post", "params": map[string]string{"text": "x"}},
	}, alice, "", nil)
	if status != http.StatusForbidden {
		t.Fatalf("alice create without channel post = %d: %s", status, data)
	}
	// A subtree declaration is managed at *; its deliveries are still gated
	// by the bot's channel relation when they run.
	status, data = h.request(http.MethodPost, "/watchers", map[string]any{
		"channel": "work/", "action": map[string]any{"name": "post", "params": map[string]string{"text": "x"}},
	}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	status, data = h.request(http.MethodPost, "/schedules", map[string]any{
		"channel": "work", "text": "x",
	}, alice, "", nil)
	if status != http.StatusForbidden {
		t.Fatalf("schedule create without channel post = %d: %s", status, data)
	}
}

func TestWatcherDeliveryManagementAPI(t *testing.T) {
	h, store := newAutomationHarness(t)
	const alice = "local:user:alice"
	status, data := h.request(http.MethodPost, "/watchers", map[string]any{
		"id": "alerts", "channel": "work", "action": map[string]any{"name": "post", "params": map[string]string{"text": "x"}},
	}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	ctx := t.Context()
	if _, err := store.AdvanceWatcher(ctx, "default", "alerts", 5, []uint64{1, 2}); err != nil {
		t.Fatalf("advance: %v", err)
	}
	leased, err := store.LeaseDeliveries(ctx, "default", 10, "test")
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease = %+v, %v", leased, err)
	}
	if _, err := store.AckDelivery(ctx, "default", "alerts", leased[0].EventSeq, leased[0].Lease,
		automation.Permanent(errors.New("nope"))); err != nil {
		t.Fatalf("ack: %v", err)
	}

	status, data = h.request(http.MethodGet, "/watchers/alerts/deliveries?state=dead", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if strings.Contains(string(data), "secret") {
		t.Fatalf("delivery response leaked content: %s", data)
	}
	if !strings.Contains(string(data), `"event":"1"`) || !strings.Contains(string(data), `"dead":1`) {
		t.Fatalf("deliveries = %s", data)
	}
	status, data = h.request(http.MethodPost, "/watchers/alerts/deliveries/1/retry", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	// It is pending again, so clearing dead letters does nothing.
	status, data = h.request(http.MethodPost, "/watchers/alerts/deliveries/clear", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if !strings.Contains(string(data), `"cleared":0`) {
		t.Fatalf("clear = %s", data)
	}
	// A dead row is removable.
	leased, err = store.LeaseDeliveries(ctx, "default", 10, "test")
	if err != nil || len(leased) != 1 {
		t.Fatalf("relase = %+v, %v", leased, err)
	}
	if _, err := store.AckDelivery(ctx, "default", "alerts", leased[0].EventSeq, leased[0].Lease,
		automation.Permanent(errors.New("again"))); err != nil {
		t.Fatalf("ack again: %v", err)
	}
	status, data = h.request(http.MethodPost, "/watchers/alerts/deliveries/clear", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if !strings.Contains(string(data), `"cleared":1`) {
		t.Fatalf("clear = %s", data)
	}
}

func TestScheduleManagementAPI(t *testing.T) {
	h, _ := newAutomationHarness(t)
	const alice = "local:user:alice"

	status, data := h.request(http.MethodPost, "/schedules", map[string]any{
		"channel": "work", "text": "secret-body", "labels": map[string]string{"kind": "reminder"},
		"everySeconds": 3600, "times": 2,
		"nextRunAt": time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
	}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	if strings.Contains(string(data), "secret-body") {
		t.Fatalf("the schedule response leaked its text: %s", data)
	}
	created := decode[automationScheduleBody](t, data).Schedule
	if created.Status != automation.SchedulePending || created.Creator != alice {
		t.Fatalf("created schedule = %+v", created)
	}
	status, data = h.request(http.MethodGet, "/schedules/"+created.ID, nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if strings.Contains(string(data), "secret-body") {
		t.Fatalf("get leaked text: %s", data)
	}
	status, data = h.request(http.MethodGet, "/schedules", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if strings.Contains(string(data), "secret-body") {
		t.Fatalf("list leaked text: %s", data)
	}
	status, data = h.request(http.MethodGet, "/schedules/"+created.ID, nil, alice, "other", nil)
	if status != http.StatusNotFound {
		t.Fatalf("cross-tenant get = %d: %s", status, data)
	}

	// Update is partial and keeps the body out of the response.
	status, data = h.request(http.MethodPost, "/schedules/"+created.ID, map[string]any{
		"text": "updated-secret", "times": 5,
	}, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if strings.Contains(string(data), "updated-secret") {
		t.Fatalf("update leaked text: %s", data)
	}
	status, data = h.request(http.MethodPost, "/schedules/"+created.ID+"/disable", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if got := decode[automationScheduleBody](t, data).Schedule.Status; got != automation.ScheduleDisabled {
		t.Fatalf("disabled status = %q", got)
	}
	status, data = h.request(http.MethodPost, "/schedules/"+created.ID+"/enable", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if got := decode[automationScheduleBody](t, data).Schedule.Status; got != automation.SchedulePending {
		t.Fatalf("enabled status = %q", got)
	}
	status, data = h.request(http.MethodPost, "/schedules/"+created.ID+"/retry", nil, alice, "", nil)
	if status != http.StatusNotFound {
		t.Fatalf("retry a pending schedule = %d: %s", status, data)
	}
	status, data = h.request(http.MethodPost, "/schedules/"+created.ID+"/clear", nil, alice, "", nil)
	if status != http.StatusNotFound {
		t.Fatalf("clear a pending schedule = %d: %s", status, data)
	}
	status, data = h.request(http.MethodDelete, "/schedules/"+created.ID, nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	status, data = h.request(http.MethodGet, "/schedules/"+created.ID, nil, alice, "", nil)
	if status != http.StatusNotFound {
		t.Fatalf("after delete = %d: %s", status, data)
	}
}

func TestScheduleReplyTargetAPI(t *testing.T) {
	h, _ := newAutomationHarness(t)
	const alice = "local:user:alice"
	root := h.postMessage(alice, "work", "root")
	status, data := h.request(http.MethodPost, "/schedules", map[string]any{
		"re": root.ID, "text": "reply later",
	}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	created := decode[automationScheduleBody](t, data).Schedule
	if created.Channel != "work" || created.Re == nil || *created.Re != root.ID {
		t.Fatalf("reply schedule = %+v", created)
	}
	status, data = h.request(http.MethodPost, "/schedules", map[string]any{
		"channel": "work", "re": root.ID, "text": "both",
	}, alice, "", nil)
	if status != http.StatusBadRequest {
		t.Fatalf("both target = %d: %s", status, data)
	}
}

// jsonUnmarshal is a small indirection so this file's imports stay minimal.
func jsonUnmarshal(data []byte, into any) error {
	return json.Unmarshal(data, into)
}

type automationScheduleCreateBody struct {
	Schedule struct {
		ID        string `json:"id"`
		UpdatedAt string `json:"updatedAt"`
	} `json:"schedule"`
	Replayed bool `json:"replayed"`
}

func TestScheduleIdempotentCreateAndVersion(t *testing.T) {
	h, store := newAutomationHarness(t)
	const alice = "local:user:alice"
	body := map[string]any{
		"channel": "work", "text": "secret-body", "idempotencyKey": "create-1",
	}
	status, data := h.request(http.MethodPost, "/schedules", body, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	first := decode[automationScheduleCreateBody](t, data)
	status, data = h.request(http.MethodPost, "/schedules", body, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	replayed := decode[automationScheduleCreateBody](t, data)
	if !replayed.Replayed || replayed.Schedule.ID != first.Schedule.ID {
		t.Fatalf("idempotent create = %+v, want the first schedule", replayed)
	}
	if list, err := store.Schedules(t.Context(), "default"); err != nil || len(list) != 1 {
		t.Fatalf("schedules = %+v, %v; want one", list, err)
	}
	body["text"] = "different-body"
	status, data = h.request(http.MethodPost, "/schedules", body, alice, "", nil)
	if status != http.StatusConflict {
		t.Fatalf("same key different digest = %d: %s", status, data)
	}

	time.Sleep(2 * time.Millisecond)
	status, data = h.request(http.MethodPost, "/schedules/"+first.Schedule.ID, map[string]any{
		"text": "changed", "ifUpdatedAt": first.Schedule.UpdatedAt,
	}, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	updated := decode[automationScheduleBody](t, data)
	status, data = h.request(http.MethodPost, "/schedules/"+first.Schedule.ID, map[string]any{
		"text": "stale", "ifUpdatedAt": first.Schedule.UpdatedAt,
	}, alice, "", nil)
	if status != http.StatusConflict {
		t.Fatalf("stale update = %d: %s", status, data)
	}
	if code := errorCode(t, data); code != protocol.CodeConflict {
		t.Fatalf("stale update code = %s, want conflict", code)
	}
	_ = updated
}

func TestScheduleUpdateWhileLeasedIs409(t *testing.T) {
	h, store := newAutomationHarness(t)
	const alice = "local:user:alice"
	id, _ := automation.NewScheduleID()
	if _, err := store.CreateSchedule(t.Context(), automation.Schedule{
		Tenant: "default", ID: id, Channel: "work", Text: "due",
		Creator: alice, CreatorKind: string(protocol.ActorPerson),
		NextRunAt: time.Now().Add(-time.Second),
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	leases, err := store.LeaseSchedules(t.Context(), "default", 10, "test")
	if err != nil || len(leases) != 1 {
		t.Fatalf("lease = %+v, %v", leases, err)
	}
	status, data := h.request(http.MethodPost, "/schedules/"+id, map[string]any{"text": "new"}, alice, "", nil)
	if status != http.StatusConflict {
		t.Fatalf("leased update = %d: %s", status, data)
	}
	if code := errorCode(t, data); code != protocol.CodeConflict {
		t.Fatalf("leased update code = %s, want conflict", code)
	}
}

func TestWatcherBotAllowlistAPI(t *testing.T) {
	memory, err := automation.NewMemory(automation.Config{})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := automation.NewRegistryWith(automation.PostAction{})
	if err != nil {
		t.Fatal(err)
	}
	h := newHarness(t, server.Options{Automation: &server.AutomationOptions{
		Store: memory, Actions: registry, Bots: []string{"local:bot:allowed"},
	}})
	const alice = "local:user:alice"
	status, data := h.request(http.MethodPost, "/watchers", map[string]any{
		"id": "blocked", "channel": "work", "bot": "local:bot:blocked",
		"action": map[string]any{"name": "post", "params": map[string]string{"text": "x"}},
	}, alice, "", nil)
	if status != http.StatusBadRequest {
		t.Fatalf("blocked bot = %d: %s", status, data)
	}
	status, data = h.request(http.MethodPost, "/watchers", map[string]any{
		"id": "allowed", "channel": "work", "bot": "local:bot:allowed",
		"action": map[string]any{"name": "post", "params": map[string]string{"text": "x"}},
	}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
}

func TestAutomationCrossTenantWritesAre404(t *testing.T) {
	h, _ := newAutomationHarness(t)
	const alice = "local:user:alice"
	status, data := h.request(http.MethodPost, "/watchers", map[string]any{
		"id": "alerts", "channel": "work",
		"action": map[string]any{"name": "post", "params": map[string]string{"text": "x"}},
	}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	for _, request := range []struct {
		method string
		path   string
		body   any
	}{
		{http.MethodPost, "/watchers/alerts", map[string]any{"channel": "other"}},
		{http.MethodDelete, "/watchers/alerts", nil},
		{http.MethodPost, "/watchers/alerts/enable", nil},
		{http.MethodPost, "/watchers/alerts/disable", nil},
		{http.MethodPost, "/watchers/alerts/deliveries/1/retry", nil},
		{http.MethodPost, "/watchers/alerts/deliveries/clear", nil},
	} {
		status, data := h.request(request.method, request.path, request.body, alice, "other", nil)
		if status != http.StatusNotFound {
			t.Fatalf("%s %s cross-tenant = %d: %s", request.method, request.path, status, data)
		}
	}

	status, data = h.request(http.MethodPost, "/schedules", map[string]any{
		"channel": "work", "text": "later", "nextRunAt": time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
	}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	scheduleID := decode[automationScheduleBody](t, data).Schedule.ID
	for _, request := range []struct {
		method string
		path   string
		body   any
	}{
		{http.MethodPost, "/schedules/" + scheduleID, map[string]any{"text": "other"}},
		{http.MethodDelete, "/schedules/" + scheduleID, nil},
		{http.MethodPost, "/schedules/" + scheduleID + "/enable", nil},
		{http.MethodPost, "/schedules/" + scheduleID + "/disable", nil},
		{http.MethodPost, "/schedules/" + scheduleID + "/retry", nil},
		{http.MethodPost, "/schedules/" + scheduleID + "/clear", nil},
	} {
		status, data := h.request(request.method, request.path, request.body, alice, "other", nil)
		if status != http.StatusNotFound {
			t.Fatalf("%s %s cross-tenant = %d: %s", request.method, request.path, status, data)
		}
	}
}
