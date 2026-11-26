package syncstore_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/fake"
	"github.com/jixoai/keel-board/syncstore"
)

func testWebhookProvider(t *testing.T, registry *boardsync.Registry) *fake.Provider {
	t.Helper()
	provider := fake.New("slack", boardsync.Capabilities{})
	if err := registry.Add(boardsync.Entry{Tenant: "t1", Key: "slack", Platform: "slack", Provider: provider, Sync: boardsync.Mirror("support")}); err != nil {
		t.Fatalf("register entry: %v", err)
	}
	return provider
}

func newWebhookHandler(t *testing.T, store syncstore.Store, registry *boardsync.Registry, tenant syncstore.TenantFunc, maxBody int64) *syncstore.WebhookHandler {
	t.Helper()
	handler, err := syncstore.NewWebhookHandler(syncstore.WebhookConfig{
		Registry: registry, Store: store, Tenant: tenant, MaxBodyBytes: maxBody,
	})
	if err != nil {
		t.Fatalf("new webhook handler: %v", err)
	}
	return handler
}

func webhookRequest(key, body string) *http.Request {
	request := httptest.NewRequest(http.MethodPost, "/webhooks/"+key, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	return request
}

func TestWebhookStoresBeforeAckAndImportsOnce(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		seedEntry(t, store, "t1", "slack", true)
		registry := boardsync.NewRegistry()
		provider := testWebhookProvider(t, registry)
		provider.ReceiveFunc = func(context.Context, boardsync.Key, *boardsync.WebhookRequest) (boardsync.Intake, error) {
			return boardsync.Intake{
				Response: boardsync.OK(),
				Records:  []boardsync.Record{testRecord("m1", "C0123:T1")},
			}, nil
		}
		handler := newWebhookHandler(t, store, registry, func(*http.Request) (string, error) { return "t1", nil }, 1<<20)
		for i := 0; i < 2; i++ {
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, webhookRequest("slack", "{}"))
			if recorder.Code != http.StatusOK {
				t.Fatalf("webhook %d = %d", i, recorder.Code)
			}
		}
		depth, err := store.InboxDepth(ctx, "t1", "slack")
		if err != nil || depth.Pending != 1 {
			t.Fatalf("depth after duplicate webhooks = %+v, %v", depth, err)
		}
		host := fake.NewHost()
		worker, err := syncstore.NewInboundWorker(syncstore.InboundWorkerConfig{
			Store: store, Registry: registry, Runner: boardsync.NewRunner(host),
			Tenant: "t1", Key: "slack", Owner: "worker", Limit: 10, PollInterval: time.Millisecond,
		})
		if err != nil {
			t.Fatalf("new inbound worker: %v", err)
		}
		result, err := worker.Process(ctx)
		if err != nil || result.Acked != 1 {
			t.Fatalf("worker pass = %+v, %v", result, err)
		}
		if imports := host.Imports(); len(imports) != 1 || imports[0].EventID != "m1" {
			t.Fatalf("imports = %+v", imports)
		}
		if result, err := worker.Process(ctx); err != nil || result.Leased != 0 {
			t.Fatalf("second worker pass = %+v, %v", result, err)
		}
		// A platform retry after the import landed is still one board message:
		// the durable dedupe row stays, so it is neither stored nor imported.
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, webhookRequest("slack", "{}"))
		if recorder.Code != http.StatusOK {
			t.Fatalf("post-import webhook = %d", recorder.Code)
		}
		if result, err := worker.Process(ctx); err != nil || result.Leased != 0 {
			t.Fatalf("import after redelivery = %+v, %v", result, err)
		}
		if imports := host.Imports(); len(imports) != 1 {
			t.Fatalf("redelivery imported again: %+v", imports)
		}
		depth, err = store.InboxDepth(ctx, "t1", "slack")
		if err != nil || depth.Done != 1 || depth.Pending != 0 {
			t.Fatalf("final depth = %+v, %v", depth, err)
		}
	})
}

func TestWebhookHandlerStatuses(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		seedEntry(t, store, "t1", "slack", true)
		seedEntry(t, store, "t1", "disabled", true)
		seedEntry(t, store, "t1", "pending", true)
		if changed, err := store.SetEntryEnabled(t.Context(), "t1", "disabled", false); err != nil || !changed {
			t.Fatalf("disable entry: %v, %v", changed, err)
		}
		registry := boardsync.NewRegistry()
		provider := fake.New("slack", boardsync.Capabilities{})
		for _, key := range []boardsync.Key{"slack", "disabled", "mismatch"} {
			if err := registry.Add(boardsync.Entry{Tenant: "t1", Key: key, Platform: "slack", Provider: provider, Sync: boardsync.Mirror("support")}); err != nil {
				t.Fatalf("register %s: %v", key, err)
			}
		}
		if _, err := store.UpsertEntry(t.Context(), syncstore.Entry{
			Tenant: "t1", Key: "mismatch", Platform: "github", Enabled: true,
		}); err != nil {
			t.Fatalf("seed mismatched entry: %v", err)
		}
		provider.ReceiveFunc = func(context.Context, boardsync.Key, *boardsync.WebhookRequest) (boardsync.Intake, error) {
			return boardsync.Intake{Response: boardsync.OK()}, nil
		}
		tenant := func(*http.Request) (string, error) { return "t1", nil }
		handler := newWebhookHandler(t, store, registry, tenant, 1<<20)

		for _, test := range []struct {
			name    string
			request func() *http.Request
			status  int
		}{
			{"unknown key", func() *http.Request { return webhookRequest("other", "{}") }, http.StatusOK},
			{"disabled", func() *http.Request { return webhookRequest("disabled", "{}") }, http.StatusOK},
			{"unregistered", func() *http.Request { return webhookRequest("pending", "{}") }, http.StatusServiceUnavailable},
			{"platform mismatch", func() *http.Request { return webhookRequest("mismatch", "{}") }, http.StatusInternalServerError},
			{"get", func() *http.Request { return httptest.NewRequest(http.MethodGet, "/webhooks/slack", nil) }, http.StatusMethodNotAllowed},
			{"bad key", func() *http.Request { return webhookRequest("BadKey", "{}") }, http.StatusNotFound},
			{"oversized", func() *http.Request { return webhookRequest("slack", strings.Repeat("x", 64)) }, http.StatusRequestEntityTooLarge},
		} {
			t.Run(test.name, func(t *testing.T) {
				limited := handler
				if test.name == "oversized" {
					limited = newWebhookHandler(t, store, registry, tenant, 8)
				}
				recorder := httptest.NewRecorder()
				limited.ServeHTTP(recorder, test.request())
				if recorder.Code != test.status {
					t.Fatalf("status = %d, want %d", recorder.Code, test.status)
				}
			})
		}
		if receives := provider.Receives(); len(receives) != 0 {
			t.Fatalf("a refused request reached the provider: %d calls", len(receives))
		}

		unauthorized := newWebhookHandler(t, store, registry, func(*http.Request) (string, error) {
			return "", errors.New("not signed in")
		}, 1<<20)
		recorder := httptest.NewRecorder()
		unauthorized.ServeHTTP(recorder, webhookRequest("slack", "{}"))
		if recorder.Code != http.StatusUnauthorized {
			t.Fatalf("tenant refusal = %d", recorder.Code)
		}

		// A second tenant can't reach a key it doesn't own.
		otherTenant := newWebhookHandler(t, store, registry, func(*http.Request) (string, error) { return "t2", nil }, 1<<20)
		recorder = httptest.NewRecorder()
		otherTenant.ServeHTTP(recorder, webhookRequest("slack", "{}"))
		if recorder.Code != http.StatusOK {
			t.Fatalf("cross-tenant webhook = %d, want a silent 200", recorder.Code)
		}
	})
}

func TestWebhookStoreFailureIsFiveHundred(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		seedEntry(t, store, "t1", "slack", true)
		registry := boardsync.NewRegistry()
		provider := testWebhookProvider(t, registry)
		provider.ReceiveFunc = func(context.Context, boardsync.Key, *boardsync.WebhookRequest) (boardsync.Intake, error) {
			return boardsync.Intake{Records: []boardsync.Record{testRecord("m1", "C0123:T1")}}, nil
		}
		failing := failingStore{Store: store, err: errors.New("database is down")}
		handler := newWebhookHandler(t, failing, registry, func(*http.Request) (string, error) { return "t1", nil }, 1<<20)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, webhookRequest("slack", "{}"))
		if recorder.Code != http.StatusInternalServerError {
			t.Fatalf("store failure status = %d", recorder.Code)
		}
		if depth, err := store.InboxDepth(t.Context(), "t1", "slack"); err != nil || depth.Pending != 0 {
			t.Fatalf("a failed store call stored something: %+v, %v", depth, err)
		}
	})
}

func TestWebhookChallengePassesThrough(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		seedEntry(t, store, "t1", "slack", true)
		registry := boardsync.NewRegistry()
		provider := testWebhookProvider(t, registry)
		provider.ReceiveFunc = func(context.Context, boardsync.Key, *boardsync.WebhookRequest) (boardsync.Intake, error) {
			return boardsync.Intake{Response: boardsync.WebhookResponse{
				Status: http.StatusOK, Body: []byte("challenge-token"), ContentType: "text/plain",
			}}, nil
		}
		handler := newWebhookHandler(t, store, registry, func(*http.Request) (string, error) { return "t1", nil }, 1<<20)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, webhookRequest("slack", `{"type":"url_verification"}`))
		if recorder.Code != http.StatusOK || recorder.Body.String() != "challenge-token" {
			t.Fatalf("challenge = %d %q", recorder.Code, recorder.Body.String())
		}
	})
}

// failingStore delegates to a real store except at PushInbox, which is how the
// handler's store-before-ack failure path is exercised.
type failingStore struct {
	syncstore.Store
	err error
}

func (f failingStore) PushInbox(context.Context, string, boardsync.Key, []boardsync.Record) (syncstore.InboxPush, error) {
	return syncstore.InboxPush{}, f.err
}
