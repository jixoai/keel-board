package server_test

import (
	"bytes"
	"context"
	"encoding/json"
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

	"github.com/jixoai/keel-board/log"
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
	})
	if err != nil {
		t.Fatalf("new memory store: %v", err)
	}
	provider := fake.New("slack", boardsync.Capabilities{
		StartThreads: true, Edits: true, Deletes: true, Reactions: true,
	})
	h := &syncHarness{t: t, board: board, store: store, provider: provider}
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
	srv := server.New(server.Options{
		Board: board,
		Tenant: func(r *http.Request) (string, error) {
			if tenant := strings.TrimSpace(r.Header.Get("X-Board-Tenant")); tenant != "" {
				return tenant, nil
			}
			return "t1", nil
		},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Sync: &server.SyncOptions{
			Store: store, Registry: registry, Tenant: tenant,
			Owned:         []server.SyncKey{{Tenant: "t1", Key: "slack"}},
			StartChannels: map[boardsync.Key][]string{"slack": {"C0123"}},
			FileHosts:     []string{"files.example.test"},
			Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	})
	h.server = srv
	httpServer := httptest.NewServer(srv)
	h.http = httpServer
	t.Cleanup(httpServer.Close)
	if opts.startWorkers {
		h.startWorkers(t)
	}
	return h
}

// startWorkers starts the process's consumers: one inbound worker, one
// outbound worker, and the board subscription.
func (h *syncHarness) startWorkers(t *testing.T) {
	t.Helper()
	if h.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	runner, ok := h.server.SyncRunnerFor()
	if !ok {
		t.Fatal("sync runner missing")
	}
	worker, err := syncstore.NewOutboundWorker(syncstore.OutboundWorkerConfig{
		Store: h.store, Registry: h.registry, Runner: runner, Host: h.server.SyncHost(),
		Tenant: "t1", Keys: []boardsync.Key{"slack"},
		PollInterval: 10 * time.Millisecond, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("new outbound worker: %v", err)
	}
	h.workers.Add(1)
	go func() { defer h.workers.Done(); _ = worker.Run(ctx) }()
	inbound, err := syncstore.NewInboundWorker(syncstore.InboundWorkerConfig{
		Store: h.store, Registry: h.registry, Runner: runner, Tenant: "t1", Key: "slack",
		PollInterval: 10 * time.Millisecond, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
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

func TestSyncRestartRecoversPendingDelivery(t *testing.T) {
	h := newSyncHarness(t, syncHarnessOptions{startWorkers: true})
	root := h.postMessage(map[string]any{"channel": "support", "text": "before restart"})
	h.waitPosts(1)
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
		Board: board, Authorize: deny,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
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
