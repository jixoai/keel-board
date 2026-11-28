package server_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/server"
	boardsync "github.com/jixoai/keel-board/sync"
)

type healthStatus struct {
	Keys []struct {
		Key     string                `json:"key"`
		Inbox   struct{ Pending int } `json:"inbox"`
		Outbox  struct{ Pending int } `json:"outbox"`
		Targets []struct {
			Target      string `json:"target"`
			Root        string `json:"root"`
			Status      string `json:"status"`
			Pending     int    `json:"pending"`
			Dead        int    `json:"dead"`
			Undelivered int    `json:"undelivered"`
		} `json:"targets"`
		Lag struct {
			Inbox *struct {
				Outstanding int    `json:"outstanding"`
				OldestAt    string `json:"oldestAt"`
				AgeSeconds  int64  `json:"ageSeconds"`
			} `json:"inbox"`
			Outbox *struct {
				Outstanding int    `json:"outstanding"`
				OldestAt    string `json:"oldestAt"`
				AgeSeconds  int64  `json:"ageSeconds"`
			} `json:"outbox"`
		} `json:"lag"`
		Failures24h int `json:"failures24h"`
	} `json:"keys"`
	Lag struct {
		Inbox  *struct{ Outstanding int } `json:"inbox"`
		Outbox *struct{ Outstanding int } `json:"outbox"`
	} `json:"lag"`
	Failures24h int `json:"failures24h"`
}

func TestSyncHealthStatusAndTargets(t *testing.T) {
	h := newSyncHarness(t, syncHarnessOptions{})
	root := h.postMessage(map[string]any{"channel": "unmapped", "text": "root"})
	rootID, err := strconv.ParseUint(root.ID, 10, 64)
	if err != nil {
		t.Fatalf("root id %q: %v", root.ID, err)
	}
	if code, body := h.request(http.MethodPost, server.APIPrefix+"/sync/threads", map[string]any{
		"key": "slack", "thread": root.ID, "target": "C0123:1727.9",
	}, nil); code != http.StatusOK {
		t.Fatalf("attach = %d %s", code, body)
	}
	if _, err := h.store.PushInbox(t.Context(), "t1", "slack", []boardsync.Record{{
		Dedupe: "d1",
		Event:  syncEvent("m1", "C0123:1727.9"),
	}}); err != nil {
		t.Fatalf("push inbox: %v", err)
	}
	target := boardsync.ResolvedTarget{
		Key: "slack", Platform: "slack", Channel: "C0123",
		Thread: "C0123:1727.9", Kind: boardsync.TargetThread,
	}
	if _, err := h.store.EnqueueOutbox(t.Context(), "t1", "slack", boardsync.BoardEvent{
		ID: 1, Type: boardsync.BoardMessagePosted, Thread: rootID,
		Message: &boardsync.Message{ID: rootID},
	}, boardsync.Message{ID: rootID, Thread: rootID, Channel: "unmapped"},
		[]boardsync.ResolvedTarget{target}); err != nil {
		t.Fatalf("enqueue outbox: %v", err)
	}

	status, body := h.request(http.MethodGet, server.APIPrefix+"/sync/status", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("status = %d %s", status, body)
	}
	var health healthStatus
	if err := json.Unmarshal(body, &health); err != nil {
		t.Fatalf("decode status: %v (%s)", err, body)
	}
	if len(health.Keys) != 1 || health.Keys[0].Key != "slack" {
		t.Fatalf("keys = %+v", health.Keys)
	}
	key := health.Keys[0]
	if key.Inbox.Pending != 1 || key.Outbox.Pending != 1 {
		t.Fatalf("depths = %+v", key)
	}
	if len(key.Targets) != 1 {
		t.Fatalf("targets = %+v", key.Targets)
	}
	if key.Targets[0].Target != "slack:C0123:1727.9" || key.Targets[0].Root != root.ID ||
		key.Targets[0].Status != "pending" || key.Targets[0].Undelivered != 1 {
		t.Fatalf("target = %+v, root = %s", key.Targets[0], root.ID)
	}
	if key.Lag.Inbox == nil || key.Lag.Inbox.Outstanding != 1 || key.Lag.Inbox.AgeSeconds < 0 {
		t.Fatalf("inbox lag = %+v", key.Lag.Inbox)
	}
	if key.Lag.Outbox == nil || key.Lag.Outbox.Outstanding != 1 || key.Lag.Outbox.AgeSeconds < 0 {
		t.Fatalf("outbox lag = %+v", key.Lag.Outbox)
	}
	if key.Failures24h != 0 || health.Failures24h != 0 {
		t.Fatalf("failures24h = %d / %d", key.Failures24h, health.Failures24h)
	}
	if health.Lag.Inbox == nil || health.Lag.Inbox.Outstanding != 1 ||
		health.Lag.Outbox == nil || health.Lag.Outbox.Outstanding != 1 {
		t.Fatalf("tenant lag = %+v", health.Lag)
	}
	if body := string(body); !containsAll(body, `"targets"`, `"lag"`, `"failures24h"`) {
		t.Fatalf("status body lacks health fields: %s", body)
	}

	targetsStatus, targetsBody := h.request(http.MethodGet, server.APIPrefix+"/sync/entries/slack/targets", nil, nil)
	if targetsStatus != http.StatusOK {
		t.Fatalf("targets = %d %s", targetsStatus, targetsBody)
	}
	var targets struct {
		Targets []struct {
			Target string `json:"target"`
			Root   string `json:"root"`
			Status string `json:"status"`
		} `json:"targets"`
	}
	if err := json.Unmarshal(targetsBody, &targets); err != nil {
		t.Fatalf("decode targets: %v", err)
	}
	if len(targets.Targets) != 1 || targets.Targets[0].Target != "slack:C0123:1727.9" ||
		targets.Targets[0].Root != root.ID {
		t.Fatalf("targets = %+v", targets.Targets)
	}

	// Another tenant can't read this entry or its targets.
	if status, _ := h.request(http.MethodGet, server.APIPrefix+"/sync/entries/slack/targets", nil,
		map[string]string{"X-Board-Tenant": "t2"}); status != http.StatusNotFound {
		t.Fatalf("cross-tenant targets = %d, want 404", status)
	}
}

func TestSyncHealthEmptyLagIsNull(t *testing.T) {
	h := newSyncHarness(t, syncHarnessOptions{})
	status, body := h.request(http.MethodGet, server.APIPrefix+"/sync/status", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("status = %d %s", status, body)
	}
	var health struct {
		Keys []struct {
			Lag struct {
				Inbox  *struct{} `json:"inbox"`
				Outbox *struct{} `json:"outbox"`
			} `json:"lag"`
			Targets []struct{} `json:"targets"`
		} `json:"keys"`
		Lag struct {
			Inbox  *struct{} `json:"inbox"`
			Outbox *struct{} `json:"outbox"`
		} `json:"lag"`
	}
	if err := json.Unmarshal(body, &health); err != nil {
		t.Fatalf("decode: %v (%s)", err, body)
	}
	if len(health.Keys) != 1 {
		t.Fatalf("keys = %+v", health.Keys)
	}
	if health.Keys[0].Lag.Inbox != nil || health.Keys[0].Lag.Outbox != nil {
		t.Fatalf("empty entry lag = %+v, want both null", health.Keys[0].Lag)
	}
	if health.Lag.Inbox != nil || health.Lag.Outbox != nil {
		t.Fatalf("empty tenant lag = %+v, want both null", health.Lag)
	}
	if health.Keys[0].Targets == nil {
		t.Fatal("targets is null; an empty target list is []")
	}
}

func TestSyncHealthRequiresManage(t *testing.T) {
	h := newSyncHarness(t, syncHarnessOptions{
		authorize: server.AuthorizerFunc(func(_ context.Context, _ string, _ protocol.Actor, _ string, permission protocol.Permission) error {
			if permission == protocol.PermissionManage {
				return protocol.Forbidden("no manage")
			}
			return nil
		}),
	})
	if status, body := h.request(http.MethodGet, server.APIPrefix+"/sync/status", nil, nil); status != http.StatusForbidden {
		t.Fatalf("status without manage = %d %s", status, body)
	}
	if status, body := h.request(http.MethodGet, server.APIPrefix+"/sync/entries/slack/targets", nil, nil); status != http.StatusForbidden {
		t.Fatalf("targets without manage = %d %s", status, body)
	}
}

func containsAll(haystack string, needles ...string) bool {
	for _, needle := range needles {
		if !strings.Contains(haystack, needle) {
			return false
		}
	}
	return true
}
