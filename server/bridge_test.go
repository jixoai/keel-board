package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/server"
	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/chatsdk"
	"github.com/jixoai/keel-board/syncstore"
)

const bridgeTestSecret = boardsync.Secret("telegram-bridge-secret")

type bridgeHarness struct {
	t        *testing.T
	http     *httptest.Server
	store    syncstore.Store
	registry *boardsync.Registry
	server   *server.Server
	logs     *bytes.Buffer
}

func newBridgeHarness(t *testing.T) *bridgeHarness {
	t.Helper()
	store, err := syncstore.NewMemory(syncstore.Config{})
	if err != nil {
		t.Fatal(err)
	}
	return newBridgeHarnessWithStore(t, store)
}

func newBridgeHarnessWithStore(t *testing.T, store syncstore.Store) *bridgeHarness {
	t.Helper()
	board := log.New(log.Options{})
	adapter := &chatsdk.FuncAdapter{PlatformName: "telegram"}
	provider, err := chatsdk.New(chatsdk.Config{Key: "telegram", Adapter: adapter, SelfIDs: []string{"telegram:user:bot"}})
	if err != nil {
		t.Fatal(err)
	}
	registry := boardsync.NewRegistry()
	if err := registry.Add(boardsync.Entry{
		Tenant: "t1", Key: "telegram", Platform: "telegram", Provider: provider,
		Sync: boardsync.Noop,
		Routes: func(_ context.Context, channel string) (bool, error) {
			return channel == "-100", nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertEntry(t.Context(), syncstore.Entry{
		Tenant: "t1", Key: "telegram", Platform: "telegram", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	logs := &bytes.Buffer{}
	h := &bridgeHarness{t: t, store: store, registry: registry, logs: logs}
	srv := server.New(server.Options{
		Board:  board,
		Tenant: func(*http.Request) (string, error) { return "t1", nil },
		Logger: slog.New(slog.NewTextHandler(logs, nil)),
		Sync: &server.SyncOptions{
			Store: store, Registry: registry,
			Tenant:       func(*http.Request) (string, error) { return "t1", nil },
			BridgeSecret: func(tenant string, key boardsync.Key) (boardsync.Secret, bool) { return bridgeTestSecret, true },
			Logger:       slog.New(slog.NewTextHandler(logs, nil)),
		},
	})
	h.server = srv
	h.http = httptest.NewServer(srv)
	t.Cleanup(h.http.Close)
	return h
}

func (h *bridgeHarness) url(key string) string {
	return h.http.URL + server.APIPrefix + "/sync/bridge/" + key + "/events"
}

func bridgeEvent(dedupe, channel, text string) chatsdk.InboundBatch {
	return chatsdk.InboundBatch{
		Version: "1",
		Events: []chatsdk.InboundEvent{{
			Dedupe: dedupe,
			Event: boardsync.Event{
				Kind: boardsync.KindMessage, Type: boardsync.TypeMessage,
				Channel: channel, Thread: channel + ":42", Message: "42", IsRoot: true,
				Text:   text,
				Author: boardsync.Author{ID: "telegram:user:7", Name: "Ada"},
			},
		}},
	}
}

func signedBridgeRequest(t *testing.T, url string, batch chatsdk.InboundBatch, secret boardsync.Secret, timestamp int64, nonce string) *http.Request {
	t.Helper()
	body, err := json.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	return signedBridgeBody(t, url, body, secret, timestamp, nonce)
}

func signedBridgeBody(t *testing.T, url string, body []byte, secret boardsync.Secret, timestamp int64, nonce string) *http.Request {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(chatsdk.HeaderTimestamp, strconv.FormatInt(timestamp, 10))
	request.Header.Set(chatsdk.HeaderNonce, nonce)
	request.Header.Set(chatsdk.HeaderSignature, chatsdk.SignV1(secret, timestamp, nonce, body))
	return request
}

func doBridge(t *testing.T, request *http.Request) (int, map[string]int) {
	t.Helper()
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(response.Body)
	var body map[string]int
	_ = json.Unmarshal(data, &body)
	return response.StatusCode, body
}

func TestBridgeEventsStoresNormalizedEvent(t *testing.T) {
	h := newBridgeHarness(t)
	now := time.Now().Unix()
	nonce := "nonce-0123456789"
	status, body := doBridge(t, signedBridgeRequest(t, h.url("telegram"), bridgeEvent("m1", "-100", "hello"), bridgeTestSecret, now, nonce))
	if status != http.StatusOK || body["stored"] != 1 {
		t.Fatalf("status/body = %d %+v", status, body)
	}
	depth, err := h.store.InboxDepth(t.Context(), "t1", "telegram")
	if err != nil || depth.Pending != 1 {
		t.Fatalf("inbox depth = %+v, %v", depth, err)
	}
	items, err := h.store.LeaseInbox(t.Context(), "t1", "telegram", 1, "test")
	if err != nil || len(items) != 1 {
		t.Fatalf("lease = %+v, %v", items, err)
	}
	event := items[0].Event
	if event.Key != "telegram" || event.Platform != "telegram" || event.ID != "m1" || event.Text != "hello" {
		t.Fatalf("stored event = %+v", event)
	}
}

func TestBridgeEventsDropsSelfAuthoredEvent(t *testing.T) {
	h := newBridgeHarness(t)
	batch := bridgeEvent("bot-1", "-100", "our own post")
	batch.Events[0].Event.Author = boardsync.Author{ID: "telegram:user:bot", Handle: "bridge", Name: "bridge", Bot: true}
	now := time.Now().Unix()
	status, body := doBridge(t, signedBridgeRequest(t, h.url("telegram"), batch, bridgeTestSecret, now, "nonce-0123456789"))
	if status != http.StatusOK || body["stored"] != 0 {
		t.Fatalf("status/body = %d %+v", status, body)
	}
	depth, err := h.store.InboxDepth(t.Context(), "t1", "telegram")
	if err != nil || depth.Pending != 0 {
		t.Fatalf("self-authored event reached the inbox: %+v, %v", depth, err)
	}
}

func TestBridgeEventsDedupesAndReplays(t *testing.T) {
	h := newBridgeHarness(t)
	now := time.Now().Unix()
	batch := bridgeEvent("m1", "-100", "hello")
	nonce := "nonce-0123456789"
	if status, body := doBridge(t, signedBridgeRequest(t, h.url("telegram"), batch, bridgeTestSecret, now, nonce)); status != http.StatusOK || body["stored"] != 1 {
		t.Fatalf("first = %d %+v", status, body)
	}
	// A new nonce with the same dedupe is a legitimate redelivery: the inbox
	// keeps one row and reports zero stored.
	if status, body := doBridge(t, signedBridgeRequest(t, h.url("telegram"), batch, bridgeTestSecret, now, "nonce-0123456780")); status != http.StatusOK || body["stored"] != 0 {
		t.Fatalf("dedupe = %d %+v", status, body)
	}
	// The same nonce with a fresh event is a replay.
	status, _ := doBridge(t, signedBridgeRequest(t, h.url("telegram"), bridgeEvent("m2", "-100", "again"), bridgeTestSecret, now, nonce))
	if status != http.StatusConflict {
		t.Fatalf("replay status = %d", status)
	}
	depth, err := h.store.InboxDepth(t.Context(), "t1", "telegram")
	if err != nil || depth.Pending != 1 {
		t.Fatalf("inbox depth after replay = %+v, %v", depth, err)
	}
}

func TestBridgeEventsRefusesBadAuthenticationAndSize(t *testing.T) {
	h := newBridgeHarness(t)
	now := time.Now().Unix()
	batch := bridgeEvent("m1", "-100", "hello")
	cases := []struct {
		name string
		req  *http.Request
		want int
	}{
		{
			name: "bad signature",
			req: func() *http.Request {
				request := signedBridgeRequest(t, h.url("telegram"), batch, bridgeTestSecret, now, "nonce-0123456789")
				request.Header.Set(chatsdk.HeaderSignature, "v1=deadbeef")
				return request
			}(),
			want: http.StatusUnauthorized,
		},
		{
			name: "missing signature",
			req: func() *http.Request {
				request := signedBridgeRequest(t, h.url("telegram"), batch, bridgeTestSecret, now, "nonce-0123456780")
				request.Header.Del(chatsdk.HeaderSignature)
				return request
			}(),
			want: http.StatusUnauthorized,
		},
		{
			name: "stale timestamp",
			req:  signedBridgeRequest(t, h.url("telegram"), batch, bridgeTestSecret, now-3600, "nonce-0123456781"),
			want: http.StatusUnauthorized,
		},
		{
			name: "wrong secret",
			req:  signedBridgeRequest(t, h.url("telegram"), batch, "other-secret", now, "nonce-0123456782"),
			want: http.StatusUnauthorized,
		},
		{
			// Verification happens before parsing: a bad signature on a
			// non-JSON body is 401, never 400.
			name: "bad signature before parse",
			req:  signedBridgeBody(t, h.url("telegram"), []byte("not-json"), "other-secret", now, "nonce-0123456784"),
			want: http.StatusUnauthorized,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, _ := doBridge(t, tc.req)
			if status != tc.want {
				t.Fatalf("status = %d, want %d", status, tc.want)
			}
		})
	}
	if depth, _ := h.store.InboxDepth(t.Context(), "t1", "telegram"); depth.Pending != 0 {
		t.Fatalf("refused requests reached the inbox: %+v", depth)
	}

	oversized := bytes.Repeat([]byte("x"), chatsdk.MaxBodyBytes+1)
	request := signedBridgeBody(t, h.url("telegram"), oversized, bridgeTestSecret, now, "nonce-0123456783")
	if status, _ := doBridge(t, request); status != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized status = %d", status)
	}
	// A valid signature on malformed JSON reaches the decoder and is 400.
	if status, _ := doBridge(t, signedBridgeBody(t, h.url("telegram"), []byte("not-json"), bridgeTestSecret, now, "nonce-0123456785")); status != http.StatusBadRequest {
		t.Fatalf("malformed body status = %d", status)
	}
}

func TestBridgeEventsRefusesUnknownDisabledAndUnmapped(t *testing.T) {
	h := newBridgeHarness(t)
	now := time.Now().Unix()
	if status, _ := doBridge(t, signedBridgeRequest(t, h.url("missing"), bridgeEvent("m1", "-100", "hi"), bridgeTestSecret, now, "nonce-0123456789")); status != http.StatusNotFound {
		t.Fatalf("unknown key = %d", status)
	}
	if _, err := h.store.SetEntryEnabled(t.Context(), "t1", "telegram", false); err != nil {
		t.Fatal(err)
	}
	if status, body := doBridge(t, signedBridgeRequest(t, h.url("telegram"), bridgeEvent("m1", "-100", "hi"), bridgeTestSecret, now, "nonce-0123456780")); status != http.StatusOK || body != nil {
		t.Fatalf("disabled = %d %+v", status, body)
	}
	if _, err := h.store.SetEntryEnabled(t.Context(), "t1", "telegram", true); err != nil {
		t.Fatal(err)
	}
	// A channel the entry doesn't route is accepted and dropped, not an error.
	status, body := doBridge(t, signedBridgeRequest(t, h.url("telegram"), bridgeEvent("m1", "-200", "hi"), bridgeTestSecret, now, "nonce-0123456781"))
	if status != http.StatusOK || body["stored"] != 0 {
		t.Fatalf("unmapped = %d %+v", status, body)
	}
	if depth, _ := h.store.InboxDepth(t.Context(), "t1", "telegram"); depth.Pending != 0 {
		t.Fatalf("unmapped event reached the inbox: %+v", depth)
	}
}

func TestBridgeEventsNeverLogsSecretOrBody(t *testing.T) {
	h := newBridgeHarness(t)
	now := time.Now().Unix()
	secretText := "do-not-log-me-secret"
	request := signedBridgeRequest(t, h.url("telegram"), bridgeEvent("m1", "-100", "body-text-"+secretText), boardsync.Secret(secretText), now, "nonce-0123456789")
	if status, _ := doBridge(t, request); status != http.StatusUnauthorized {
		t.Fatalf("status = %d", status)
	}
	if strings.Contains(h.logs.String(), secretText) {
		t.Fatalf("log contains the secret or body: %s", h.logs.String())
	}
}

func TestBridgeEventsMissingSecretFailsClosed(t *testing.T) {
	board := log.New(log.Options{})
	store, err := syncstore.NewMemory(syncstore.Config{})
	if err != nil {
		t.Fatal(err)
	}
	provider, err := chatsdk.New(chatsdk.Config{Key: "telegram", Adapter: &chatsdk.FuncAdapter{PlatformName: "telegram"}, SelfIDs: []string{"telegram:user:bot"}})
	if err != nil {
		t.Fatal(err)
	}
	registry := boardsync.NewRegistry()
	if err := registry.Add(boardsync.Entry{
		Tenant: "t1", Key: "telegram", Platform: "telegram", Provider: provider, Sync: boardsync.Noop,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertEntry(t.Context(), syncstore.Entry{Tenant: "t1", Key: "telegram", Platform: "telegram", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	srv := server.New(server.Options{
		Board: board,
		Sync: &server.SyncOptions{
			Store: store, Registry: registry,
			Tenant: func(*http.Request) (string, error) { return "t1", nil },
		},
	})
	httpServer := httptest.NewServer(srv)
	t.Cleanup(httpServer.Close)
	now := time.Now().Unix()
	request := signedBridgeRequest(t, httpServer.URL+server.APIPrefix+"/sync/bridge/telegram/events",
		bridgeEvent("m1", "-100", "hi"), bridgeTestSecret, now, "nonce-0123456789")
	if status, _ := doBridge(t, request); status != http.StatusServiceUnavailable {
		t.Fatalf("missing secret = %d", status)
	}
}

func TestBridgeTenantIsolation(t *testing.T) {
	board := log.New(log.Options{})
	store, err := syncstore.NewMemory(syncstore.Config{})
	if err != nil {
		t.Fatal(err)
	}
	provider, err := chatsdk.New(chatsdk.Config{Key: "telegram", Adapter: &chatsdk.FuncAdapter{PlatformName: "telegram"}, SelfIDs: []string{"telegram:user:bot"}})
	if err != nil {
		t.Fatal(err)
	}
	registry := boardsync.NewRegistry()
	for _, tenant := range []string{"a", "b"} {
		if err := registry.Add(boardsync.Entry{
			Tenant: tenant, Key: "telegram", Platform: "telegram", Provider: provider, Sync: boardsync.Noop,
			Routes: func(_ context.Context, channel string) (bool, error) { return channel == "-100", nil },
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.UpsertEntry(t.Context(), syncstore.Entry{Tenant: tenant, Key: "telegram", Platform: "telegram", Enabled: true}); err != nil {
			t.Fatal(err)
		}
	}
	secrets := map[string]boardsync.Secret{"a": "secret-a-012345", "b": "secret-b-012345"}
	srv := server.New(server.Options{
		Board: board,
		Sync: &server.SyncOptions{
			Store: store, Registry: registry,
			Tenant: func(r *http.Request) (string, error) {
				return strings.TrimSpace(r.Header.Get("X-Board-Tenant")), nil
			},
			BridgeSecret: func(tenant string, key boardsync.Key) (boardsync.Secret, bool) { return secrets[tenant], true },
		},
	})
	httpServer := httptest.NewServer(srv)
	t.Cleanup(httpServer.Close)
	url := httpServer.URL + server.APIPrefix + "/sync/bridge/telegram/events"
	now := time.Now().Unix()
	request := func(tenant string, secret boardsync.Secret, nonce string) *http.Request {
		r := signedBridgeRequest(t, url, bridgeEvent("m1", "-100", "hi"), secret, now, nonce)
		r.Header.Set("X-Board-Tenant", tenant)
		return r
	}
	// Each tenant's secret only authenticates its own tenant header.
	if status, _ := doBridge(t, request("a", secrets["b"], "nonce-0123456789")); status != http.StatusUnauthorized {
		t.Fatalf("tenant A with B's secret = %d", status)
	}
	if status, _ := doBridge(t, request("b", secrets["a"], "nonce-0123456788")); status != http.StatusUnauthorized {
		t.Fatalf("tenant B with A's secret = %d", status)
	}
	if status, _ := doBridge(t, request("a", secrets["a"], "nonce-0123456780")); status != http.StatusOK {
		t.Fatalf("tenant A with its secret = %d", status)
	}
	if status, _ := doBridge(t, request("b", secrets["b"], "nonce-0123456781")); status != http.StatusOK {
		t.Fatalf("tenant B with its secret = %d", status)
	}
	if depth, _ := store.InboxDepth(t.Context(), "a", "telegram"); depth.Pending != 1 {
		t.Fatalf("tenant A row = %+v", depth)
	}
	if depth, _ := store.InboxDepth(t.Context(), "b", "telegram"); depth.Pending != 1 {
		t.Fatalf("tenant B row = %+v", depth)
	}
}

type bridgeFailStore struct {
	syncstore.Store
	pushErr    error
	nonceErr   error
	pushCalls  atomic.Int64
	nonceCalls atomic.Int64
}

func (s *bridgeFailStore) PushInbox(ctx context.Context, tenant string, key boardsync.Key, records []boardsync.Record) (syncstore.InboxPush, error) {
	s.pushCalls.Add(1)
	if s.pushErr != nil {
		return syncstore.InboxPush{}, s.pushErr
	}
	return s.Store.PushInbox(ctx, tenant, key, records)
}

func (s *bridgeFailStore) ClaimNonce(ctx context.Context, tenant string, key boardsync.Key, nonce string, ttl time.Duration) (bool, error) {
	s.nonceCalls.Add(1)
	if s.nonceErr != nil {
		return false, s.nonceErr
	}
	return s.Store.ClaimNonce(ctx, tenant, key, nonce, ttl)
}

func TestBridgeEventsStoreFailureIs500(t *testing.T) {
	memory, err := syncstore.NewMemory(syncstore.Config{})
	if err != nil {
		t.Fatal(err)
	}
	store := &bridgeFailStore{Store: memory, pushErr: errors.New("inbox is down")}
	h := newBridgeHarnessWithStore(t, store)
	now := time.Now().Unix()
	status, _ := doBridge(t, signedBridgeRequest(t, h.url("telegram"), bridgeEvent("m1", "-100", "hi"), bridgeTestSecret, now, "nonce-0123456789"))
	if status != http.StatusInternalServerError {
		t.Fatalf("store failure = %d, want 500", status)
	}
	if store.pushCalls.Load() != 1 {
		t.Fatalf("push calls = %d", store.pushCalls.Load())
	}
	depth, err := memory.InboxDepth(t.Context(), "t1", "telegram")
	if err != nil || depth.Pending != 0 || depth.Done != 0 {
		t.Fatalf("failed store wrote a row: %+v, %v", depth, err)
	}
}

func TestBridgeEventsNonceFailureIs500AndSkipsInbox(t *testing.T) {
	memory, err := syncstore.NewMemory(syncstore.Config{})
	if err != nil {
		t.Fatal(err)
	}
	store := &bridgeFailStore{Store: memory, nonceErr: errors.New("nonce table is down")}
	h := newBridgeHarnessWithStore(t, store)
	now := time.Now().Unix()
	status, _ := doBridge(t, signedBridgeRequest(t, h.url("telegram"), bridgeEvent("m1", "-100", "hi"), bridgeTestSecret, now, "nonce-0123456789"))
	if status != http.StatusInternalServerError {
		t.Fatalf("nonce failure = %d, want 500", status)
	}
	if store.nonceCalls.Load() != 1 {
		t.Fatalf("nonce calls = %d", store.nonceCalls.Load())
	}
	if store.pushCalls.Load() != 0 {
		t.Fatalf("inbox was pushed after a nonce failure: %d", store.pushCalls.Load())
	}
}

func TestBridgeEventsPlatformMismatchIs500(t *testing.T) {
	h := newBridgeHarness(t)
	if _, err := h.store.UpsertEntry(t.Context(), syncstore.Entry{
		Tenant: "t1", Key: "telegram", Platform: "discord", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	status, _ := doBridge(t, signedBridgeRequest(t, h.url("telegram"), bridgeEvent("m1", "-100", "hi"), bridgeTestSecret, now, "nonce-0123456789"))
	if status != http.StatusInternalServerError {
		t.Fatalf("platform mismatch = %d, want 500", status)
	}
}
