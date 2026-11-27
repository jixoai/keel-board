package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jixoai/keel-board/audit"
	"github.com/jixoai/keel-board/blob"
	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/metrics"
	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/server"
	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/fake"
	"github.com/jixoai/keel-board/syncstore"
)

// syncHarness is a server with sync mounted over a memory store.
type syncHarness struct {
	t        *testing.T
	http     *httptest.Server
	board    *log.Board
	store    *syncstore.Memory
	registry *boardsync.Registry
	provider *fake.Provider
	server   *server.Server
	wrapped  syncstore.Store
	blobs    blob.Store
	posts    atomic.Int64
	cancel   context.CancelFunc
	workers  sync.WaitGroup
}

func newSyncHarness(t *testing.T, opts syncHarnessOptions) *syncHarness {
	t.Helper()
	board := log.New(log.Options{})
	store, err := syncstore.NewMemory(syncstore.Config{
		InboundRetry:  boardsync.RetryPolicy{MaxAttempts: 2, BaseDelay: 0, MaxDelay: 0, Jitter: 0},
		OutboundRetry: boardsync.RetryPolicy{MaxAttempts: 2, BaseDelay: 0, MaxDelay: 0, Jitter: 0},
		// A stopped worker's lease must expire quickly in a test; production
		// uses the one-minute default this overrides.
		LeaseTTL: 200 * time.Millisecond,
	})
	serverStore := syncstore.Store(store)
	if opts.wrapStore != nil {
		serverStore = opts.wrapStore(store)
	}
	if err != nil {
		t.Fatalf("new memory store: %v", err)
	}
	provider := fake.New("slack", boardsync.Capabilities{
		StartThreads: true, Edits: true, Deletes: true, Reactions: true,
	})
	h := &syncHarness{t: t, board: board, store: store, provider: provider}
	blobs := opts.blobs
	if blobs == nil {
		blobs = blob.NewMemory()
	}
	h.blobs = blobs
	provider.ResolveTargetFunc = func(_ context.Context, key boardsync.Key, target string) (boardsync.ResolvedTarget, error) {
		if channel, _, ok := strings.Cut(target, ":"); ok {
			return boardsync.ResolvedTarget{Key: key, Platform: "slack", Channel: channel, Thread: target, Kind: boardsync.TargetThread}, nil
		}
		return boardsync.ResolvedTarget{Key: key, Platform: "slack", Channel: target, Kind: boardsync.TargetChannel}, nil
	}
	provider.PostFunc = func(_ context.Context, key boardsync.Key, target boardsync.ResolvedTarget, msg boardsync.OutboundMessage) (boardsync.Posted, error) {
		n := h.posts.Add(1)
		thread := target.Thread
		if target.Kind == boardsync.TargetChannel {
			thread = target.Channel + ":1"
		}
		return boardsync.Posted{Thread: thread, Message: fmt.Sprintf("m%d", n)}, nil
	}
	entry := boardsync.Entry{
		Tenant: "t1", Key: "slack", Platform: "slack", Provider: provider,
		Sync: boardsync.Mirror("support"),
	}
	if opts.mirrorChannels {
		fn, route := boardsync.Channels(map[string]boardsync.SyncFunc{"C0123": boardsync.Mirror("support")})
		entry.Sync = fn
		entry.Routes = route
	}
	registry := boardsync.NewRegistry()
	if err := registry.Add(entry); err != nil {
		t.Fatalf("add entry: %v", err)
	}
	h.registry = registry
	if _, err := store.UpsertEntry(t.Context(), syncstore.Entry{
		Tenant: "t1", Key: "slack", Platform: "slack", Enabled: true,
	}); err != nil {
		t.Fatalf("upsert entry: %v", err)
	}
	tenant := opts.tenant
	if tenant == nil {
		// A deployment-fixed tenant: a header can't move the webhook to
		// another board.
		tenant = func(*http.Request) (string, error) { return "t1", nil }
	}
	logger := opts.logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	srv := server.New(server.Options{
		Board:     board,
		Authorize: opts.authorize,
		Tenant: func(r *http.Request) (string, error) {
			if tenant := strings.TrimSpace(r.Header.Get("X-Board-Tenant")); tenant != "" {
				return tenant, nil
			}
			return "t1", nil
		},
		Logger:  logger,
		Blobs:   blobs,
		Metrics: opts.metrics,
		Audit:   opts.audit,
		Sync: &server.SyncOptions{
			Store: serverStore, Registry: registry, Tenant: tenant,
			Owned:         []server.SyncKey{{Tenant: "t1", Key: "slack"}},
			StartChannels: map[server.SyncKey][]string{{Tenant: "t1", Key: "slack"}: {"C0123"}},
			FileHosts:     []string{"files.example.test"},
			Logger:        logger,
		},
	})
	h.server = srv
	h.wrapped = serverStore
	httpServer := httptest.NewServer(srv)
	h.http = httpServer
	t.Cleanup(httpServer.Close)
	if opts.startWorkers {
		h.startWorkers(t)
	}
	return h
}

// serverStore is the store the server and workers use (the wrapped one when a
// test injects failures); h.store is always the concrete memory store.
func (h *syncHarness) serverStore() syncstore.Store {
	if h.wrapped != nil {
		return h.wrapped
	}
	return h.store
}

// startWorkers starts the process's consumers: one inbound worker, one
// outbound worker, and the board subscription.
func (h *syncHarness) startWorkers(t *testing.T) {
	t.Helper()
	if h.cancel != nil {
		return
	}
	if err := h.server.PrepareSync(t.Context()); err != nil {
		t.Fatalf("prepare sync: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	runner, ok := h.server.SyncRunnerFor()
	if !ok {
		t.Fatal("sync runner missing")
	}
	worker, err := syncstore.NewOutboundWorker(syncstore.OutboundWorkerConfig{
		Store: h.serverStore(), Registry: h.registry, Runner: runner, Host: h.server.SyncHost(),
		Files:  boardsync.FileLoader(h.server.SyncHost().LoadFiles),
		Tenant: "t1", Keys: []boardsync.Key{"slack"},
		PollInterval: 10 * time.Millisecond,
		Hooks:        h.server.SyncWorkerHooks(),
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("new outbound worker: %v", err)
	}
	h.workers.Add(1)
	go func() { defer h.workers.Done(); _ = worker.Run(ctx) }()
	inbound, err := syncstore.NewInboundWorker(syncstore.InboundWorkerConfig{
		Store: h.serverStore(), Registry: h.registry, Runner: runner, Tenant: "t1", Key: "slack",
		PollInterval: 10 * time.Millisecond,
		Hooks:        h.server.SyncWorkerHooks(),
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("new inbound worker: %v", err)
	}
	h.workers.Add(1)
	go func() { defer h.workers.Done(); _ = inbound.Run(ctx) }()
	h.workers.Add(1)
	go func() { defer h.workers.Done(); _ = h.server.RunSync(ctx) }()
	t.Cleanup(cancel)
}

// stopWorkers cancels the consumers and waits for their loops to notice.
func (h *syncHarness) stopWorkers() {
	if h.cancel == nil {
		return
	}
	h.cancel()
	h.cancel = nil
	h.workers.Wait()
}

type syncHarnessOptions struct {
	mirrorChannels bool
	startWorkers   bool
	tenant         syncstore.TenantFunc
	authorize      server.Authorizer
	// wrapStore lets a test inject one failure into the server's store while
	// the harness keeps the concrete memory store for assertions.
	wrapStore func(syncstore.Store) syncstore.Store
	// blobs replaces the harness's memory store for attachment tests.
	blobs blob.Store
	// logger receives the server's and sync's logs; nil discards them.
	logger *slog.Logger
	// metrics and audit are the server's observability seams.
	metrics *metrics.Registry
	audit   audit.Store
}

func (h *syncHarness) request(method, path string, body any, extra map[string]string) (int, []byte) {
	h.t.Helper()
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			h.t.Fatal(err)
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, h.http.URL+path, reader)
	if err != nil {
		h.t.Fatal(err)
	}
	if body != nil {
		request.Header.Set("content-type", "application/json")
	}
	for key, value := range extra {
		request.Header.Set(key, value)
	}
	response, err := h.http.Client().Do(request)
	if err != nil {
		h.t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		h.t.Fatal(err)
	}
	return response.StatusCode, data
}

func (h *syncHarness) postMessage(body map[string]any) protocol.MessageView {
	h.t.Helper()
	status, data := h.request(http.MethodPost, server.APIPrefix+"/messages", body, nil)
	if status != http.StatusCreated {
		h.t.Fatalf("post message: %d %s", status, data)
	}
	var response struct {
		Message protocol.MessageView `json:"message"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		h.t.Fatalf("decode post: %v (%s)", err, data)
	}
	return response.Message
}

func (h *syncHarness) waitPosts(want int64) {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if h.posts.Load() >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.t.Fatalf("posts = %d, want %d", h.posts.Load(), want)
}

func TestSyncWebhookTenantAndKeySemantics(t *testing.T) {
	// A provider that returns one record in a mapped channel.
	received := atomic.Int64{}
	h := newSyncHarness(t, syncHarnessOptions{mirrorChannels: true})
	h.provider.ReceiveFunc = func(context.Context, boardsync.Key, *boardsync.WebhookRequest) (boardsync.Intake, error) {
		received.Add(1)
		event := syncEvent("m1", "C0123:1727.9")
		return boardsync.Intake{
			Records:  []boardsync.Record{{Dedupe: "d1", Event: event}},
			Response: boardsync.OK(),
		}, nil
	}
	// A forged X-Board-Tenant header doesn't move the webhook: the fixed
	// resolver wins.
	status, body := h.request(http.MethodPost, "/webhooks/slack", map[string]any{"event": "message"}, map[string]string{"X-Board-Tenant": "t2"})
	if status != http.StatusOK {
		t.Fatalf("webhook = %d %s", status, body)
	}
	if received.Load() != 1 {
		t.Fatalf("provider receives = %d", received.Load())
	}
	depth, err := h.store.InboxDepth(t.Context(), "t1", "slack")
	if err != nil || depth.Pending != 1 {
		t.Fatalf("tenant t1 inbox = %+v, %v", depth, err)
	}
	if other, _ := h.store.InboxDepth(t.Context(), "t2", "slack"); other.Pending != 0 {
		t.Fatalf("forged tenant got the event: %+v", other)
	}

	// A key this tenant doesn't own is a silent 200.
	status, _ = h.request(http.MethodPost, "/webhooks/other", map[string]any{}, nil)
	if status != http.StatusOK {
		t.Fatalf("unowned key = %d", status)
	}
	// A disabled entry is also a silent 200, before the body is read.
	if _, err := h.store.SetEntryEnabled(t.Context(), "t1", "slack", false); err != nil {
		t.Fatal(err)
	}
	status, _ = h.request(http.MethodPost, "/webhooks/slack", map[string]any{}, nil)
	if status != http.StatusOK {
		t.Fatalf("disabled key = %d", status)
	}
	if got := received.Load(); got != 1 {
		t.Fatalf("disabled key read the body: receives = %d", got)
	}
}

func TestSyncWebhook503WhenEntryIsNotRegistered(t *testing.T) {
	h := newSyncHarness(t, syncHarnessOptions{mirrorChannels: true})
	empty := boardsync.NewRegistry()
	store, err := syncstore.NewMemory(syncstore.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertEntry(t.Context(), syncstore.Entry{
		Tenant: "t1", Key: "slack", Platform: "slack", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	handler, err := syncstore.NewWebhookHandler(syncstore.WebhookConfig{
		Registry: empty, Store: store,
		Tenant: func(*http.Request) (string, error) { return "t1", nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/webhooks/slack", strings.NewReader("{}")))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("unregistered enabled key = %d, want 503", recorder.Code)
	}
	_ = h
}

func TestSyncMirrorPostsRootAndReplyThenSuppressesEcho(t *testing.T) {
	h := newSyncHarness(t, syncHarnessOptions{startWorkers: true})
	root := h.postMessage(map[string]any{"channel": "support", "text": "ship it"})
	h.waitPosts(1)
	posts := h.provider.Posts()
	if len(posts) != 1 || posts[0].Target.Kind != boardsync.TargetChannel || posts[0].Msg.Markdown != "**dev**: ship it" {
		t.Fatalf("root posts = %+v", posts)
	}
	reply := h.postMessage(map[string]any{"re": root.ID, "text": "done"})
	h.waitPosts(2)
	posts = h.provider.Posts()
	if len(posts) != 2 || posts[1].Target.Kind != boardsync.TargetThread || posts[1].Target.Thread != "C0123:1" {
		t.Fatalf("reply posts = %+v", posts)
	}
	if posts[1].Msg.Markdown != "**dev**: done" {
		t.Fatalf("reply markdown = %q", posts[1].Msg.Markdown)
	}

	// An imported Slack reply lands on the board and must not flow back to
	// Slack (From == key). The outbox may enqueue, but the runner skips it.
	before := len(posts)
	h.provider.ReceiveFunc = func(context.Context, boardsync.Key, *boardsync.WebhookRequest) (boardsync.Intake, error) {
		event := syncEvent("s1", "C0123:1")
		event.Message, event.IsRoot, event.Text = "s1", false, "from slack"
		return boardsync.Intake{
			Records:  []boardsync.Record{{Dedupe: "s1", Event: event}},
			Response: boardsync.OK(),
		}, nil
	}
	if status, body := h.request(http.MethodPost, "/webhooks/slack", map[string]any{}, nil); status != http.StatusOK {
		t.Fatalf("webhook = %d %s", status, body)
	}
	// Wait for the import, then make sure no platform echo appears.
	var imported bool
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_ = h.board.Read("t1", func(snapshot *log.Snapshot) error {
			for _, message := range snapshot.MessagesInChannel("support") {
				if message.Text == "from slack" {
					imported = true
				}
			}
			return nil
		})
		if imported {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !imported {
		t.Fatal("slack reply was not imported")
	}
	status, body := h.request(http.MethodGet, server.APIPrefix+"/messages/"+root.ID, nil, nil)
	if status != http.StatusOK {
		t.Fatalf("read thread = %d %s", status, body)
	}
	var thread protocol.ThreadView
	if err := json.Unmarshal(body, &thread); err != nil {
		t.Fatal(err)
	}
	var fromSlack bool
	for _, reply := range thread.Replies {
		if reply.From != nil && *reply.From == "slack" {
			fromSlack = true
			if reply.Origin == nil {
				t.Fatalf("imported reply has no origin: %+v", reply)
			}
			if reply.Undelivered == nil {
				t.Fatalf("imported reply has no undelivered map: %+v", reply)
			}
		}
	}
	if !fromSlack {
		t.Fatalf("imported reply missing from the thread: %s", body)
	}
	time.Sleep(200 * time.Millisecond)
	if got := len(h.provider.Posts()); got != before {
		t.Fatalf("echo posts = %d, want %d: %+v", got, before, h.provider.Posts())
	}
	_ = reply
}

func TestSyncAttachChannelPostsRootAndBackfills(t *testing.T) {
	h := newSyncHarness(t, syncHarnessOptions{startWorkers: true, mirrorChannels: true})
	// A board thread with a reply, not yet mirrored.
	h.provider.ReceiveFunc = nil
	root := h.postMessage(map[string]any{"channel": "unmapped", "text": "hello"})
	reply := h.postMessage(map[string]any{"re": root.ID, "text": "earlier reply"})
	h.postMessage(map[string]any{"re": root.ID, "text": "private reply", "noSync": true})
	// A board channel that no entry maps: attach explicitly.
	if code, body := h.request(http.MethodPost, server.APIPrefix+"/sync/threads", map[string]any{
		"key": "slack", "thread": root.ID, "target": "C0123",
	}, nil); code != http.StatusOK {
		t.Fatalf("attach = %d %s", code, body)
	}
	h.waitPosts(2)
	posts := h.provider.Posts()
	if len(posts) != 2 {
		t.Fatalf("attach posts = %+v", posts)
	}
	if posts[0].Target.Kind != boardsync.TargetChannel || posts[0].Msg.Markdown != "**dev**: hello" {
		t.Fatalf("attach root = %+v", posts[0])
	}
	if posts[1].Target.Kind != boardsync.TargetThread || posts[1].Target.Thread != "C0123:1" {
		t.Fatalf("backfill target = %+v", posts[1])
	}
	if posts[1].Msg.Markdown != "**dev**: earlier reply" {
		t.Fatalf("backfill text = %q", posts[1].Msg.Markdown)
	}
	for _, post := range posts {
		if strings.Contains(post.Msg.Markdown, "private reply") {
			t.Fatalf("a noSync reply was backfilled: %+v", post)
		}
	}
	// Duplicate attach is idempotent: no new post.
	if code, body := h.request(http.MethodPost, server.APIPrefix+"/sync/threads", map[string]any{
		"key": "slack", "thread": root.ID, "target": "C0123",
	}, nil); code != http.StatusOK {
		t.Fatalf("duplicate attach = %d %s", code, body)
	}
	time.Sleep(200 * time.Millisecond)
	if got := h.posts.Load(); got != 2 {
		t.Fatalf("duplicate attach posted again: %d", got)
	}
	_ = reply
}

func TestSyncAttachThreadTargetOnlyFuture(t *testing.T) {
	h := newSyncHarness(t, syncHarnessOptions{startWorkers: true, mirrorChannels: true})
	root := h.postMessage(map[string]any{"channel": "unmapped", "text": "hello"})
	old := h.postMessage(map[string]any{"re": root.ID, "text": "before attach"})
	code, body := h.request(http.MethodPost, server.APIPrefix+"/sync/threads", map[string]any{
		"key": "slack", "thread": root.ID, "target": "C0123:1727.9",
	}, nil)
	if code != http.StatusOK {
		t.Fatalf("attach thread = %d %s", code, body)
	}
	future := h.postMessage(map[string]any{"re": root.ID, "text": "after attach"})
	h.waitPosts(1)
	posts := h.provider.Posts()
	if len(posts) != 1 || posts[0].Target.Kind != boardsync.TargetThread || posts[0].Target.Thread != "C0123:1727.9" {
		t.Fatalf("thread attach posts = %+v", posts)
	}
	if posts[0].Msg.Markdown != "**dev**: after attach" {
		t.Fatalf("thread attach text = %q", posts[0].Msg.Markdown)
	}
	_ = old
	_ = future
}

func TestSyncAttachCrossTenantAndPermission(t *testing.T) {
	h := newSyncHarness(t, syncHarnessOptions{mirrorChannels: true})
	root := h.postMessage(map[string]any{"channel": "support", "text": "mine"})
	// The same thread ID in another tenant is a 404: the caller can't attach
	// another board's thread even though the key name exists for it.
	status, body := h.request(http.MethodPost, server.APIPrefix+"/sync/threads", map[string]any{
		"key": "slack", "thread": root.ID, "target": "C0123",
	}, map[string]string{"X-Board-Tenant": "t2"})
	if status != http.StatusNotFound {
		t.Fatalf("cross-tenant attach = %d %s", status, body)
	}
	// The tenant running the key can attach it.
	status, body = h.request(http.MethodPost, server.APIPrefix+"/sync/threads", map[string]any{
		"key": "slack", "thread": root.ID, "target": "C0123",
	}, nil)
	if status != http.StatusOK {
		t.Fatalf("own attach = %d %s", status, body)
	}
}

func TestSyncManagementAPIDoesNotLeakContent(t *testing.T) {
	h := newSyncHarness(t, syncHarnessOptions{mirrorChannels: true})
	// Force a dead inbox item: push a record, lease it, and dead-letter it
	// with a content-bearing error.
	if _, err := h.store.PushInbox(t.Context(), "t1", "slack", []boardsync.Record{{
		Dedupe: "d1",
		Event:  syncEvent("m1", "C0123:T1"),
	}}); err != nil {
		t.Fatal(err)
	}
	items, err := h.store.LeaseInbox(t.Context(), "t1", "slack", 1, "test")
	if err != nil || len(items) != 1 {
		t.Fatalf("lease inbox = %+v, %v", items, err)
	}
	// Two failed attempts dead-letter the item in this harness's policy.
	for i := 0; i < 4; i++ {
		ack, err := h.store.AckInbox(t.Context(), "t1", "slack", items[0].ID, items[0].Lease, fmt.Errorf("secret platform body"))
		if err != nil {
			t.Fatalf("ack inbox: %v", err)
		}
		if ack.Dead {
			break
		}
		items, err = h.store.LeaseInbox(t.Context(), "t1", "slack", 1, fmt.Sprintf("test-%d", i))
		if err != nil || len(items) == 0 {
			t.Fatalf("lease retry = %+v, %v", items, err)
		}
	}
	status, body := h.request(http.MethodGet, server.APIPrefix+"/sync/inbox?key=slack", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("inbox = %d %s", status, body)
	}
	if strings.Contains(string(body), "secret platform body") || strings.Contains(string(body), "hello m1") {
		t.Fatalf("inbox leaked content: %s", body)
	}
	status, body = h.request(http.MethodGet, server.APIPrefix+"/sync/status", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("status = %d %s", status, body)
	}
	if !strings.Contains(string(body), "\"dead\"") {
		t.Fatalf("status has no depth: %s", body)
	}
	status, body = h.request(http.MethodGet, server.APIPrefix+"/sync/entries", nil, nil)
	if status != http.StatusOK || !strings.Contains(string(body), "\"slack\"") {
		t.Fatalf("entries = %d %s", status, body)
	}
	// Retry the dead letter through the API.
	var dead struct {
		Dead []struct {
			ID string `json:"id"`
		} `json:"dead"`
	}
	if err := json.Unmarshal(body, &dead); err != nil {
		// The previous body was entries; fetch inbox again.
	}
	_, inboxBody := h.request(http.MethodGet, server.APIPrefix+"/sync/inbox?key=slack", nil, nil)
	if err := json.Unmarshal(inboxBody, &dead); err != nil {
		t.Fatalf("decode inbox: %v (%s)", err, inboxBody)
	}
	if len(dead.Dead) == 0 {
		t.Fatalf("no dead letter: %s", inboxBody)
	}
	status, body = h.request(http.MethodPost, server.APIPrefix+"/sync/inbox/"+dead.Dead[0].ID+"/retry?key=slack", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("retry = %d %s", status, body)
	}
}

func TestSyncDeliveryFailureBecomesUndelivered(t *testing.T) {
	h := newSyncHarness(t, syncHarnessOptions{startWorkers: true})
	root := h.postMessage(map[string]any{"channel": "unmapped", "text": "root"})
	if code, body := h.request(http.MethodPost, server.APIPrefix+"/sync/threads", map[string]any{
		"key": "slack", "thread": root.ID, "target": "C0123:1727.9",
	}, nil); code != http.StatusOK {
		t.Fatalf("attach = %d %s", code, body)
	}
	// A permanent platform refusal dead-letters the delivery.
	h.provider.PostFunc = func(context.Context, boardsync.Key, boardsync.ResolvedTarget, boardsync.OutboundMessage) (boardsync.Posted, error) {
		return boardsync.Posted{}, boardsync.Permanent(fmt.Errorf("not_in_channel"))
	}
	reply := h.postMessage(map[string]any{"re": root.ID, "text": "doomed"})
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status, body := h.request(http.MethodGet, server.APIPrefix+"/messages/"+reply.ID, nil, nil)
		if status != http.StatusOK {
			t.Fatalf("read message = %d %s", status, body)
		}
		var view protocol.ThreadView
		if err := json.Unmarshal(body, &view); err != nil {
			t.Fatal(err)
		}
		var doomed *protocol.MessageView
		for i := range view.Replies {
			if view.Replies[i].ID == reply.ID {
				doomed = &view.Replies[i]
			}
		}
		if doomed != nil && doomed.Undelivered["slack:C0123:1727.9"] != "" {
			// The failure receipt lands.
			status, failures := h.request(http.MethodGet, server.APIPrefix+"/sync/outbox/failures?key=slack", nil, nil)
			if status != http.StatusOK || !strings.Contains(string(failures), reply.ID) {
				t.Fatalf("failures = %d %s", status, failures)
			}
			if strings.Contains(string(failures), "doomed") {
				t.Fatalf("failure list leaked text: %s", failures)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	depth, _ := h.store.OutboxDepth(t.Context(), "t1")
	t.Fatalf("delivery never dead-lettered into undelivered: depth=%+v posts=%d", depth, h.posts.Load())
}

func TestSyncNoSyncNeverMirrors(t *testing.T) {
	h := newSyncHarness(t, syncHarnessOptions{startWorkers: true})
	h.postMessage(map[string]any{"channel": "support", "text": "private", "noSync": true})
	time.Sleep(250 * time.Millisecond)
	if got := h.posts.Load(); got != 0 {
		t.Fatalf("a noSync root was mirrored: %d", got)
	}
	// A root that syncs, then a noSync reply: only the root reaches Slack.
	root := h.postMessage(map[string]any{"channel": "support", "text": "public"})
	h.waitPosts(1)
	h.postMessage(map[string]any{"re": root.ID, "text": "private reply", "noSync": true})
	time.Sleep(250 * time.Millisecond)
	if got := h.posts.Load(); got != 1 {
		t.Fatalf("a noSync reply was mirrored: %d", got)
	}
}

func TestSyncRestartRecoversPendingDelivery(t *testing.T) {
	h := newSyncHarness(t, syncHarnessOptions{startWorkers: true})
	root := h.postMessage(map[string]any{"channel": "support", "text": "before restart"})
	h.waitPosts(1)
	// Let the worker finish its receipt and ack before the simulated crash;
	// otherwise the stop leaves a live lease that must expire first.
	waitFor(t, func() bool {
		depth, err := h.store.OutboxDepth(t.Context(), "t1")
		return err == nil && depth.Done >= 1 && depth.Leased == 0
	})
	h.stopWorkers()
	// A reply lands while nothing runs; the subscription's durable cursor and
	// the outbox's pending row must deliver it after a restart.
	h.postMessage(map[string]any{"re": root.ID, "text": "after restart"})
	h.startWorkers(t)
	h.waitPosts(2)
	posts := h.provider.Posts()
	if len(posts) != 2 || posts[1].Msg.Markdown != "**dev**: after restart" {
		t.Fatalf("restart posts = %+v", posts)
	}
}

func TestSyncManagementRequiresManage(t *testing.T) {
	h := newSyncHarness(t, syncHarnessOptions{mirrorChannels: true})
	// A deny-all authorizer is installed by rebuilding the server; use the
	// access seam's shape directly.
	_ = h
	deny := server.AuthorizerFunc(func(context.Context, string, protocol.Actor, string, protocol.Permission) error {
		return protocol.Forbidden("no manage here")
	})
	board := log.New(log.Options{})
	store, err := syncstore.NewMemory(syncstore.Config{})
	if err != nil {
		t.Fatal(err)
	}
	registry := boardsync.NewRegistry()
	provider := fake.New("slack", boardsync.Capabilities{})
	if err := registry.Add(boardsync.Entry{
		Tenant: "t1", Key: "slack", Platform: "slack", Provider: provider, Sync: boardsync.Noop,
	}); err != nil {
		t.Fatal(err)
	}
	srv := server.New(server.Options{
		Board:     board,
		Authorize: deny,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		Sync: &server.SyncOptions{
			Store: store, Registry: registry,
			Tenant: func(*http.Request) (string, error) { return "t1", nil },
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	})
	httpServer := httptest.NewServer(srv)
	defer httpServer.Close()
	response, err := http.Get(httpServer.URL + server.APIPrefix + "/sync/entries")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("denied manage = %d", response.StatusCode)
	}
}

func TestSyncMirrorEditsDeletesReactions(t *testing.T) {
	h := newSyncHarness(t, syncHarnessOptions{startWorkers: true})
	root := h.postMessage(map[string]any{"channel": "support", "text": "first"})
	h.waitPosts(1)
	if code, body := h.request(http.MethodPost, server.APIPrefix+"/messages/"+root.ID+"/edit", map[string]any{"text": "second"}, nil); code != http.StatusOK {
		t.Fatalf("edit = %d %s", code, body)
	}
	waitFor(t, func() bool { return len(h.provider.Edits()) == 1 })
	edit := h.provider.Edits()[0]
	if edit.Message != "m1" || edit.Msg.Markdown != "**dev**: second" {
		t.Fatalf("edit call = %+v", edit)
	}
	if code, body := h.request(http.MethodPost, server.APIPrefix+"/messages/"+root.ID+"/reactions", map[string]any{"emoji": "eyes"}, nil); code != http.StatusOK {
		t.Fatalf("react = %d %s", code, body)
	}
	waitFor(t, func() bool { return len(h.provider.Reactions()) == 1 })
	reaction := h.provider.Reactions()[0]
	if reaction.Emoji != "eyes" || !reaction.Added || reaction.Message != "m1" {
		t.Fatalf("reaction call = %+v", reaction)
	}
	if code, body := h.request(http.MethodPost, server.APIPrefix+"/messages/"+root.ID+"/delete", map[string]any{}, nil); code != http.StatusOK {
		t.Fatalf("delete = %d %s", code, body)
	}
	waitFor(t, func() bool { return len(h.provider.Deletes()) == 1 })
	if call := h.provider.Deletes()[0]; call.Message != "m1" {
		t.Fatalf("delete call = %+v", call)
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition never held")
}

type failSecondSetCursorStore struct {
	syncstore.Store
	calls atomic.Int32
}

func (s *failSecondSetCursorStore) SetCursor(ctx context.Context, tenant string, key boardsync.Key, source, cursor string) error {
	if s.calls.Add(1) == 2 {
		return errors.New("injected cursor failure")
	}
	return s.Store.SetCursor(ctx, tenant, key, source, cursor)
}

func TestSyncThreadTargetFutureOnlyAcrossRestart(t *testing.T) {
	h := newSyncHarness(t, syncHarnessOptions{startWorkers: true, mirrorChannels: true})
	root := h.postMessage(map[string]any{"channel": "unmapped", "text": "old root"})
	old := h.postMessage(map[string]any{"re": root.ID, "text": "old reply"})
	waitFor(t, func() bool {
		depth, err := h.store.OutboxDepth(t.Context(), "t1")
		return err == nil && depth.Pending == 0 && depth.Leased == 0
	})
	h.stopWorkers()
	code, body := h.request(http.MethodPost, server.APIPrefix+"/sync/threads", map[string]any{
		"key": "slack", "thread": root.ID, "target": "C0123:1727.9",
	}, nil)
	if code != http.StatusOK {
		t.Fatalf("attach thread = %d %s", code, body)
	}
	thread, found, err := h.store.ThreadByTarget(t.Context(), "t1", "slack", boardsync.ResolvedTarget{
		Key: "slack", Platform: "slack", Channel: "C0123", Thread: "C0123:1727.9", Kind: boardsync.TargetThread,
	})
	if err != nil || !found || thread.Since == 0 {
		t.Fatalf("thread mapping = %+v, %v, %v", thread, found, err)
	}
	// Rewind the subscription so it replays the old messages: the persisted
	// since is what keeps them from being delivered.
	if err := h.store.SetCursor(t.Context(), "t1", "slack", "board-events", "0"); err != nil {
		t.Fatal(err)
	}
	h.startWorkers(t)
	time.Sleep(300 * time.Millisecond)
	if got := h.posts.Load(); got != 0 {
		t.Fatalf("a message before attach was delivered: %d", got)
	}
	h.postMessage(map[string]any{"re": root.ID, "text": "new reply"})
	h.waitPosts(1)
	posts := h.provider.Posts()
	if len(posts) != 1 || posts[0].Msg.Markdown != "**dev**: new reply" {
		t.Fatalf("future-only posts = %+v", posts)
	}
	_ = old
}

func TestSyncDeletePropagatesToAllChannelCopies(t *testing.T) {
	h := newSyncHarness(t, syncHarnessOptions{startWorkers: true, mirrorChannels: true})
	root := h.postMessage(map[string]any{"channel": "unmapped", "text": "root"})
	for _, channel := range []string{"C1", "C2"} {
		code, body := h.request(http.MethodPost, server.APIPrefix+"/sync/threads", map[string]any{
			"key": "slack", "thread": root.ID, "target": channel,
		}, nil)
		if code != http.StatusOK {
			t.Fatalf("attach %s = %d %s", channel, code, body)
		}
	}
	h.waitPosts(2)
	if code, body := h.request(http.MethodPost, server.APIPrefix+"/messages/"+root.ID+"/edit", map[string]any{"text": "edited root"}, nil); code != http.StatusOK {
		t.Fatalf("edit = %d %s", code, body)
	}
	waitFor(t, func() bool { return len(h.provider.Edits()) == 2 })
	if code, body := h.request(http.MethodPost, server.APIPrefix+"/messages/"+root.ID+"/reactions", map[string]any{"emoji": "eyes"}, nil); code != http.StatusOK {
		t.Fatalf("react = %d %s", code, body)
	}
	waitFor(t, func() bool { return len(h.provider.Reactions()) == 2 })
	if code, body := h.request(http.MethodPost, server.APIPrefix+"/messages/"+root.ID+"/delete", map[string]any{}, nil); code != http.StatusOK {
		t.Fatalf("delete = %d %s", code, body)
	}
	waitFor(t, func() bool { return len(h.provider.Deletes()) >= 2 })
	deletes := h.provider.Deletes()
	if len(deletes) != 2 {
		t.Fatalf("deletes = %+v, want one per channel copy", deletes)
	}
	copies, err := h.store.CopyForMessage(t.Context(), "t1", "slack", mustRef(t, root.ID))
	if err != nil || len(copies) != 0 {
		t.Fatalf("copies after delete = %+v, %v", copies, err)
	}
}

func TestSyncSubscriptionRestartsAfterStoreError(t *testing.T) {
	flaky := &failSecondSetCursorStore{}
	h := newSyncHarness(t, syncHarnessOptions{
		startWorkers: true, mirrorChannels: true,
		wrapStore: func(store syncstore.Store) syncstore.Store {
			flaky.Store = store
			return flaky
		},
	})
	root := h.postMessage(map[string]any{"channel": "support", "text": "survives"})
	h.waitPosts(1)
	// The first cursor write after PrepareSync fails; one-shot supervision
	// would end here. A reply must still be delivered after the restart.
	h.postMessage(map[string]any{"re": root.ID, "text": "after the failure"})
	h.waitPosts(2)
	if flaky.calls.Load() < 3 {
		t.Fatalf("the subscription wasn't restarted: cursor writes = %d", flaky.calls.Load())
	}
}

func TestSyncSelfAuthoredWebhookProducesNoRecord(t *testing.T) {
	h := newSyncHarness(t, syncHarnessOptions{startWorkers: true, mirrorChannels: true})
	h.provider.SelfIDsValue = []string{"slack:user:UAPP"}
	h.provider.ReceiveFunc = func(context.Context, boardsync.Key, *boardsync.WebhookRequest) (boardsync.Intake, error) {
		event := syncEvent("self", "C0123:1727.9")
		event.Author = boardsync.Author{ID: "slack:user:UAPP", Handle: "app", Name: "app", Bot: true}
		return boardsync.Intake{
			Records:  []boardsync.Record{{Dedupe: "self", Event: event}},
			Response: boardsync.OK(),
		}, nil
	}
	if code, body := h.request(http.MethodPost, "/webhooks/slack", map[string]any{}, nil); code != http.StatusOK {
		t.Fatalf("webhook = %d %s", code, body)
	}
	time.Sleep(200 * time.Millisecond)
	if err := h.board.Read("t1", func(snapshot *log.Snapshot) error {
		if messages := snapshot.MessagesInChannel("support"); len(messages) != 0 {
			t.Fatalf("self message imported: %+v", messages)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := h.posts.Load(); got != 0 {
		t.Fatalf("self message posted: %d", got)
	}
}

func TestSyncAttachRequiresManage(t *testing.T) {
	h := newSyncHarness(t, syncHarnessOptions{
		mirrorChannels: true,
		authorize: server.AuthorizerFunc(func(_ context.Context, _ string, _ protocol.Actor, _ string, permission protocol.Permission) error {
			if permission == protocol.PermissionManage {
				return protocol.Forbidden("no manage")
			}
			return nil
		}),
	})
	root := h.postMessage(map[string]any{"channel": "unmapped", "text": "root"})
	code, body := h.request(http.MethodPost, server.APIPrefix+"/sync/threads", map[string]any{
		"key": "slack", "thread": root.ID, "target": "C1",
	}, nil)
	if code != http.StatusForbidden {
		t.Fatalf("attach without manage = %d %s", code, body)
	}
	threads, err := h.store.Threads(t.Context(), "t1", mustRef(t, root.ID))
	if err != nil || len(threads) != 0 {
		t.Fatalf("a refused attach wrote a mapping: %+v, %v", threads, err)
	}
	events, err := h.board.History("t1", 1, ^uint64(0))
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.Type == log.EventThreadSynced {
			t.Fatalf("a refused attach wrote thread.synced: %+v", event)
		}
	}
}

func mustRef(t *testing.T, value string) uint64 {
	t.Helper()
	var id uint64
	if _, err := fmt.Sscanf(value, "%d", &id); err != nil {
		t.Fatalf("ref %q: %v", value, err)
	}
	return id
}

func TestSyncAttachThreadConcurrentConflict(t *testing.T) {
	h := newSyncHarness(t, syncHarnessOptions{mirrorChannels: true})
	first := h.postMessage(map[string]any{"channel": "support", "text": "one"})
	second := h.postMessage(map[string]any{"channel": "support", "text": "two"})
	type outcome struct {
		status int
		body   []byte
		err    error
	}
	outcomes := make(chan outcome, 2)
	start := make(chan struct{})
	for _, root := range []protocol.MessageView{first, second} {
		root := root
		go func() {
			select {
			case <-start:
			case <-time.After(time.Second):
			}
			status, body, err := rawRequest(h.http.URL+server.APIPrefix+"/sync/threads", http.MethodPost, map[string]any{
				"key": "slack", "thread": root.ID, "target": "C0123:1727.9",
			})
			outcomes <- outcome{status: status, body: body, err: err}
		}()
	}
	close(start)
	okCount, conflictCount := 0, 0
	for i := 0; i < 2; i++ {
		got := <-outcomes
		if got.err != nil {
			t.Fatalf("attach request: %v", got.err)
		}
		switch got.status {
		case http.StatusOK:
			okCount++
		case http.StatusConflict:
			conflictCount++
		default:
			t.Fatalf("attach = %d %s", got.status, got.body)
		}
	}
	if okCount != 1 || conflictCount != 1 {
		t.Fatalf("concurrent attach = %d ok, %d conflict", okCount, conflictCount)
	}
	threads, err := h.store.Threads(t.Context(), "t1", mustRef(t, first.ID))
	if err != nil {
		t.Fatal(err)
	}
	other, err := h.store.Threads(t.Context(), "t1", mustRef(t, second.ID))
	if err != nil {
		t.Fatal(err)
	}
	if len(threads)+len(other) != 1 {
		t.Fatalf("mappings = %d + %d, want one", len(threads), len(other))
	}
}

func rawRequest(url, method string, body any) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, url, reader)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		request.Header.Set("content-type", "application/json")
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	return response.StatusCode, data, err
}
