package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/server"
	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/fake"
	"github.com/jixoai/keel-board/syncstore"
)

// providerWithoutSource implements only boardsync.Provider. Embedding the
// interface (not the concrete adapter) hides the adapter's ReadSource and
// JoinSource, which is exactly how a platform that can't read sources looks.
type providerWithoutSource struct {
	boardsync.Provider
}

type sourceErrorBody struct {
	Error protocol.ErrorDetail `json:"error"`
}

type entryStatusBody struct {
	Keys []struct {
		Key      string `json:"key"`
		Platform string `json:"platform"`
		Enabled  bool   `json:"enabled"`
		Owned    bool   `json:"owned"`
		Inbox    struct {
			Pending int `json:"pending"`
			Leased  int `json:"leased"`
			Done    int `json:"done"`
			Dead    int `json:"dead"`
		} `json:"inbox"`
	} `json:"keys"`
	Outbox struct {
		Pending int `json:"pending"`
		Leased  int `json:"leased"`
		Done    int `json:"done"`
		Dead    int `json:"dead"`
	} `json:"outbox"`
}

// TestSourceSurfaceRequiresManage proves read, join, and entry status use the
// same board.manage gate as the rest of sync management, before any platform
// call is made.
func TestSourceSurfaceRequiresManage(t *testing.T) {
	h := newSyncHarness(t, syncHarnessOptions{
		authorize: server.AuthorizerFunc(func(_ context.Context, _ string, _ protocol.Actor, _ string, permission protocol.Permission) error {
			if permission == protocol.PermissionManage {
				return protocol.Forbidden("no manage")
			}
			return nil
		}),
	})
	cases := []struct {
		method string
		path   string
		body   any
	}{
		{http.MethodGet, "/v1/board/sources/slack/read?target=C_SUPPORT", nil},
		{http.MethodPost, "/v1/board/sources/slack/join", map[string]any{"target": "C_SUPPORT"}},
		{http.MethodGet, "/v1/board/sync/entries/slack/status", nil},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			status, body := h.request(tc.method, tc.path, tc.body, nil)
			if status != http.StatusForbidden {
				t.Fatalf("status = %d, want 403: %s", status, body)
			}
			var failure sourceErrorBody
			if err := json.Unmarshal(body, &failure); err != nil {
				t.Fatalf("decode: %v (%s)", err, body)
			}
			if failure.Error.Code != protocol.CodeForbidden {
				t.Fatalf("code = %q, want forbidden: %s", failure.Error.Code, body)
			}
		})
	}
	if reads := h.provider.SourceReads(); len(reads) != 0 {
		t.Fatalf("a refused read reached the provider: %+v", reads)
	}
	if joins := h.provider.SourceJoins(); len(joins) != 0 {
		t.Fatalf("a refused join reached the provider: %+v", joins)
	}
}

// TestSourceReadNormalizesAndDoesNotWrite proves a read returns the shared
// normalized shape, forwards limit/after, and appends no board event.
func TestSourceReadNormalizesAndDoesNotWrite(t *testing.T) {
	h := newSyncHarness(t, syncHarnessOptions{})
	h.provider.ReadSourceFunc = func(_ context.Context, key boardsync.Key, target string, opts boardsync.ReadOptions) (boardsync.SourceRead, error) {
		if key != "slack" || target != "C_SUPPORT" {
			return boardsync.SourceRead{}, fmt.Errorf("unexpected %q %q", key, target)
		}
		return boardsync.SourceRead{
			Kind: "channel", Channel: "C_SUPPORT", Name: "support",
			Messages: []boardsync.SourceMessage{{
				ID: "1700000000.000100", Thread: "C_SUPPORT:1700000000.000100",
				Author: "Ada", AuthorID: "slack:user:U1", Text: "hello", CreatedAt: "2026-01-02T03:04:05Z",
				Replies: 2, Files: []boardsync.File{{Name: "a.log", URL: "https://files.slack.com/a.log"}},
			}},
			More: true, Next: "cursor-2",
		}, nil
	}
	before, err := h.board.Head("t1")
	if err != nil {
		t.Fatal(err)
	}
	status, body := h.request(http.MethodGet,
		"/v1/board/sources/slack/read?target=C_SUPPORT&limit=7&after=cursor-1", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("read = %d %s", status, body)
	}
	var read boardsync.SourceRead
	if err := json.Unmarshal(body, &read); err != nil {
		t.Fatalf("decode read: %v (%s)", err, body)
	}
	if read.Kind != "channel" || read.Channel != "C_SUPPORT" || read.Name != "support" || !read.More || read.Next != "cursor-2" {
		t.Fatalf("read = %+v", read)
	}
	if len(read.Messages) != 1 {
		t.Fatalf("messages = %+v", read.Messages)
	}
	message := read.Messages[0]
	if message.ID != "1700000000.000100" || message.Thread != "C_SUPPORT:1700000000.000100" ||
		message.Author != "Ada" || message.AuthorID != "slack:user:U1" ||
		message.Text != "hello" || message.Replies != 2 || len(message.Files) != 1 ||
		message.Files[0].Name != "a.log" || message.Files[0].URL != "https://files.slack.com/a.log" {
		t.Fatalf("message = %+v", message)
	}
	calls := h.provider.SourceReads()
	if len(calls) != 1 || calls[0].Opts.Limit != 7 || calls[0].Opts.After != "cursor-1" {
		t.Fatalf("provider calls = %+v", calls)
	}
	after, err := h.board.Head("t1")
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("a source read wrote the board: head %d -> %d", before, after)
	}

	// The default limit is the shared one, and an empty read is [] not null.
	h.provider.ReadSourceFunc = func(context.Context, boardsync.Key, string, boardsync.ReadOptions) (boardsync.SourceRead, error) {
		return boardsync.SourceRead{Kind: "channel", Channel: "C_SUPPORT"}, nil
	}
	status, body = h.request(http.MethodGet, "/v1/board/sources/slack/read?target=C_SUPPORT", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("default-limit read = %d %s", status, body)
	}
	if !strings.Contains(string(body), `"messages":[]`) {
		t.Fatalf("empty messages isn't []: %s", body)
	}
	if calls := h.provider.SourceReads(); len(calls) != 2 || calls[1].Opts.Limit != boardsync.DefaultSourceLimit {
		t.Fatalf("default limit calls = %+v", calls)
	}
}

// TestSourceReadAndJoinRejections proves unknown keys are 404 (before any
// existence detail), required fields are 400, and a provider refusal or a raw
// platform error never becomes a 500 with platform text.
func TestSourceReadAndJoinRejections(t *testing.T) {
	h := newSyncHarness(t, syncHarnessOptions{})
	// Unknown and malformed keys answer the same 404.
	for _, key := range []string{"missing", "BAD"} {
		status, body := h.request(http.MethodGet, "/v1/board/sources/"+key+"/read?target=C_SUPPORT", nil, nil)
		if status != http.StatusNotFound {
			t.Fatalf("read key %q = %d %s", key, status, body)
		}
		var failure sourceErrorBody
		if err := json.Unmarshal(body, &failure); err != nil || failure.Error.Code != protocol.CodeNotFound {
			t.Fatalf("read key %q code: %v %s", key, err, body)
		}
		status, body = h.request(http.MethodPost, "/v1/board/sources/"+key+"/join", map[string]any{"target": "C_SUPPORT"}, nil)
		if status != http.StatusNotFound {
			t.Fatalf("join key %q = %d %s", key, status, body)
		}
	}
	if len(h.provider.SourceReads()) != 0 || len(h.provider.SourceJoins()) != 0 {
		t.Fatal("an unknown key reached the provider")
	}

	// A read needs a target and accepts a limit of at most 200.
	if status, body := h.request(http.MethodGet, "/v1/board/sources/slack/read", nil, nil); status != http.StatusBadRequest {
		t.Fatalf("missing target = %d %s", status, body)
	}
	if status, body := h.request(http.MethodGet, "/v1/board/sources/slack/read?target=C_SUPPORT&limit=201", nil, nil); status != http.StatusBadRequest {
		t.Fatalf("limit 201 = %d %s", status, body)
	}
	// A join needs a well-formed body and a target.
	if status, body := h.request(http.MethodPost, "/v1/board/sources/slack/join", map[string]any{}, nil); status != http.StatusBadRequest {
		t.Fatalf("join without target = %d %s", status, body)
	}
	if status, body := h.request(http.MethodPost, "/v1/board/sources/slack/join", map[string]any{"target": "C_SUPPORT", "extra": true}, nil); status != http.StatusBadRequest {
		t.Fatalf("join with an unknown field = %d %s", status, body)
	}

	// A raw platform error is classified, never copied into the response.
	h.provider.ReadSourceFunc = func(context.Context, boardsync.Key, string, boardsync.ReadOptions) (boardsync.SourceRead, error) {
		return boardsync.SourceRead{}, fmt.Errorf("platform said: RAW_PLATFORM_BODY for secret-target")
	}
	status, body := h.request(http.MethodGet, "/v1/board/sources/slack/read?target=secret-target", nil, nil)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("raw platform failure = %d %s", status, body)
	}
	if strings.Contains(string(body), "RAW_PLATFORM_BODY") || strings.Contains(string(body), "secret-target") {
		t.Fatalf("raw platform text leaked: %s", body)
	}
	var failure sourceErrorBody
	if err := json.Unmarshal(body, &failure); err != nil || failure.Error.Code != protocol.CodeUnavailable {
		t.Fatalf("raw failure code: %v %s", err, body)
	}

	// An opt-out join is a stable 501, not a 500.
	h.provider.JoinSourceFunc = func(context.Context, boardsync.Key, string) (boardsync.SourceJoin, error) {
		return boardsync.SourceJoin{}, fmt.Errorf("%w: join_disabled: this board doesn't join channels", boardsync.ErrUnsupported)
	}
	status, body = h.request(http.MethodPost, "/v1/board/sources/slack/join", map[string]any{"target": "C_SUPPORT"}, nil)
	if status != http.StatusNotImplemented {
		t.Fatalf("disabled join = %d %s", status, body)
	}
	if err := json.Unmarshal(body, &failure); err != nil || failure.Error.Code != protocol.CodeUnsupported {
		t.Fatalf("disabled join code: %v %s", err, body)
	}
	if strings.Contains(string(body), "join_disabled") {
		t.Fatalf("provider detail leaked: %s", body)
	}
}

// TestSourceJoinSuccess proves a configured join returns the provider's
// answer and forwards the target.
func TestSourceJoinSuccess(t *testing.T) {
	h := newSyncHarness(t, syncHarnessOptions{})
	h.provider.JoinSourceFunc = func(_ context.Context, key boardsync.Key, target string) (boardsync.SourceJoin, error) {
		if key != "slack" || target != "C_SUPPORT" {
			return boardsync.SourceJoin{}, fmt.Errorf("unexpected %q %q", key, target)
		}
		return boardsync.SourceJoin{Channel: "C_SUPPORT", Name: "support", Joined: true}, nil
	}
	status, body := h.request(http.MethodPost, "/v1/board/sources/slack/join", map[string]any{"target": "C_SUPPORT"}, nil)
	if status != http.StatusOK {
		t.Fatalf("join = %d %s", status, body)
	}
	var join boardsync.SourceJoin
	if err := json.Unmarshal(body, &join); err != nil {
		t.Fatalf("decode join: %v (%s)", err, body)
	}
	if join.Channel != "C_SUPPORT" || join.Name != "support" || !join.Joined {
		t.Fatalf("join = %+v", join)
	}
	if calls := h.provider.SourceJoins(); len(calls) != 1 || calls[0].Target != "C_SUPPORT" {
		t.Fatalf("join calls = %+v", calls)
	}
}

// TestSourceProviderWithoutReaderOrJoiner proves an adapter that doesn't
// implement the optional surfaces answers 501, never 500.
func TestSourceProviderWithoutReaderOrJoiner(t *testing.T) {
	h := newSyncHarness(t, syncHarnessOptions{})
	bare := fake.New("slack", boardsync.Capabilities{})
	if err := h.registry.Add(boardsync.Entry{
		Tenant: "t1", Key: "bare", Platform: "slack",
		Provider: providerWithoutSource{Provider: bare}, Sync: boardsync.Noop,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.UpsertEntry(t.Context(), syncstore.Entry{
		Tenant: "t1", Key: "bare", Platform: "slack", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	status, body := h.request(http.MethodGet, "/v1/board/sources/bare/read?target=C_SUPPORT", nil, nil)
	if status != http.StatusNotImplemented {
		t.Fatalf("read without a reader = %d %s", status, body)
	}
	var failure sourceErrorBody
	if err := json.Unmarshal(body, &failure); err != nil || failure.Error.Code != protocol.CodeUnsupported {
		t.Fatalf("read without a reader code: %v %s", err, body)
	}
	status, body = h.request(http.MethodPost, "/v1/board/sources/bare/join", map[string]any{"target": "C_SUPPORT"}, nil)
	if status != http.StatusNotImplemented {
		t.Fatalf("join without a joiner = %d %s", status, body)
	}
	if err := json.Unmarshal(body, &failure); err != nil || failure.Error.Code != protocol.CodeUnsupported {
		t.Fatalf("join without a joiner code: %v %s", err, body)
	}
	// A provider without source support still has an entry status.
	if status, body := h.request(http.MethodGet, "/v1/board/sync/entries/bare/status", nil, nil); status != http.StatusOK {
		t.Fatalf("bare status = %d %s", status, body)
	}
}

// TestSyncEntryStatus proves the single-entry status view reuses the tenant
// status shape, filtered to one connection, and keeps unknown keys at 404.
func TestSyncEntryStatus(t *testing.T) {
	h := newSyncHarness(t, syncHarnessOptions{})
	status, body := h.request(http.MethodGet, "/v1/board/sync/entries/slack/status", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("status = %d %s", status, body)
	}
	var view entryStatusBody
	if err := json.Unmarshal(body, &view); err != nil {
		t.Fatalf("decode status: %v (%s)", err, body)
	}
	if len(view.Keys) != 1 {
		t.Fatalf("keys = %+v", view.Keys)
	}
	key := view.Keys[0]
	if key.Key != "slack" || key.Platform != "slack" || !key.Enabled || !key.Owned {
		t.Fatalf("entry = %+v", key)
	}
	if status, body := h.request(http.MethodGet, "/v1/board/sync/entries/missing/status", nil, nil); status != http.StatusNotFound {
		t.Fatalf("unknown status = %d %s", status, body)
	}
}

// TestSourceLogsAreBounded proves the audit line names the connection,
// platform, action and status but never the target or message text, including
// when the provider returns a raw platform error.
func TestSourceLogsAreBounded(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	h := newSyncHarness(t, syncHarnessOptions{logger: logger})
	h.provider.ReadSourceFunc = func(context.Context, boardsync.Key, string, boardsync.ReadOptions) (boardsync.SourceRead, error) {
		return boardsync.SourceRead{
			Kind: "channel", Channel: "C_SUPPORT",
			Messages: []boardsync.SourceMessage{{ID: "1", Thread: "C_SUPPORT:1", Text: "BODY_SECRET"}},
		}, nil
	}
	if status, body := h.request(http.MethodGet,
		"/v1/board/sources/slack/read?target=SECRET_TARGET", nil, nil); status != http.StatusOK {
		t.Fatalf("read = %d %s", status, body)
	}
	h.provider.JoinSourceFunc = func(context.Context, boardsync.Key, string) (boardsync.SourceJoin, error) {
		return boardsync.SourceJoin{Channel: "C_SUPPORT"}, nil
	}
	if status, body := h.request(http.MethodPost,
		"/v1/board/sources/slack/join", map[string]any{"target": "SECRET_TARGET"}, nil); status != http.StatusOK {
		t.Fatalf("join = %d %s", status, body)
	}
	h.provider.ReadSourceFunc = func(context.Context, boardsync.Key, string, boardsync.ReadOptions) (boardsync.SourceRead, error) {
		return boardsync.SourceRead{}, fmt.Errorf("RAW_PLATFORM_BODY on SECRET_TARGET")
	}
	if status, body := h.request(http.MethodGet,
		"/v1/board/sources/slack/read?target=SECRET_TARGET", nil, nil); status != http.StatusServiceUnavailable {
		t.Fatalf("failed read = %d %s", status, body)
	}
	text := logs.String()
	for _, want := range []string{"action=read", "action=join", "key=slack", "platform=slack", "status=200", "status=503"} {
		if !strings.Contains(text, want) {
			t.Fatalf("logs lack %q:\n%s", want, text)
		}
	}
	for _, secret := range []string{"SECRET_TARGET", "BODY_SECRET", "RAW_PLATFORM_BODY"} {
		if strings.Contains(text, secret) {
			t.Fatalf("logs leaked %q:\n%s", secret, text)
		}
	}
}
