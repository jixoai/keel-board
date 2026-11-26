package slack_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/slack"
)

func newProvider(t *testing.T, baseURL string, client *http.Client, mutate func(*slack.Config)) *slack.Provider {
	t.Helper()
	cfg := slack.Config{
		Key: "slack", SigningSecret: testSecret, BotToken: testToken, BotUserID: "UAPP123",
		APIBaseURL: baseURL, HTTPClient: client, Now: now,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	p, err := slack.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func postTarget(thread bool) boardsync.ResolvedTarget {
	target := boardsync.ResolvedTarget{
		Key: "slack", Platform: "slack", Channel: "C0123ABC", Kind: boardsync.TargetChannel,
	}
	if thread {
		target.Kind = boardsync.TargetThread
		target.Thread = "C0123ABC:1727000000.123456"
	}
	return target
}

func TestPostCustomizeDegradesWithoutScope(t *testing.T) {
	var got url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth.test":
			_, _ = w.Write([]byte(`{"ok":true,"user_id":"UAPP123","response_metadata":{"scopes":["chat:write"]}}`))
		case "/chat.postMessage":
			_ = r.ParseForm()
			got = r.PostForm
			_, _ = w.Write([]byte(`{"ok":true,"ts":"1727000009.000100"}`))
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	posted, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{
		Markdown: "**Ada**: ship it",
		Sender:   &boardsync.Sender{Name: "Ada", Avatar: "https://example.com/a.png"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if posted.Message != "1727000009.000100" || posted.Thread != "C0123ABC:1727000009.000100" {
		t.Fatalf("posted = %+v", posted)
	}
	if got.Get("username") != "" || got.Get("icon_url") != "" {
		t.Fatalf("customized without the scope: %v", got)
	}
	if got.Get("text") != "**Ada**: ship it" {
		t.Fatalf("label was dropped without a sender: %q", got.Get("text"))
	}
}

func TestPostCustomizeRefusalFallsBack(t *testing.T) {
	var calls atomic.Int64
	var last url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth.test":
			_, _ = w.Write([]byte(`{"ok":true,"response_metadata":{"scopes":["chat:write","chat:write.customize"]}}`))
		case "/chat.postMessage":
			_ = r.ParseForm()
			calls.Add(1)
			last = r.PostForm
			if r.PostForm.Get("username") != "" {
				_, _ = w.Write([]byte(`{"ok":false,"error":"missing_scope"}`))
				return
			}
			_, _ = w.Write([]byte(`{"ok":true,"ts":"1727000009.000100"}`))
		}
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	_, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{
		Markdown: "**Ada**: ship it", Sender: &boardsync.Sender{Name: "Ada"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || last.Get("username") != "" || last.Get("text") != "**Ada**: ship it" {
		t.Fatalf("calls=%d form=%v", calls.Load(), last)
	}
}

func TestPostCustomizeUsernameAndIcon(t *testing.T) {
	var got url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth.test":
			_, _ = w.Write([]byte(`{"ok":true,"response_metadata":{"scopes":["chat:write.customize"]}}`))
		case "/chat.postMessage":
			_ = r.ParseForm()
			got = r.PostForm
			_, _ = w.Write([]byte(`{"ok":true,"ts":"1727000009.000100"}`))
		}
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	long := strings.Repeat("a", 100)
	_, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{
		Markdown: "**" + long + "**: ship it",
		Sender:   &boardsync.Sender{Name: long, Attribution: ": via support", Avatar: "https://example.com/a.svg"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Get("icon_url") != "" {
		t.Fatalf("Slack can't show an SVG icon: %q", got.Get("icon_url"))
	}
	username := got.Get("username")
	if runes := len([]rune(username)); runes != 80 {
		t.Fatalf("username is %d runes: %q", runes, username)
	}
	if !strings.HasSuffix(username, "…: via support") {
		t.Fatalf("attribution was cut: %q", username)
	}
	if strings.HasPrefix(got.Get("text"), "**") {
		t.Fatalf("label stayed under a named copy: %q", got.Get("text"))
	}
}

func TestPostThreadReply(t *testing.T) {
	var got url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat.postMessage" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		_ = r.ParseForm()
		got = r.PostForm
		_, _ = w.Write([]byte(`{"ok":true,"ts":"1727000009.000100"}`))
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	posted, err := p.Post(context.Background(), "slack", postTarget(true), boardsync.OutboundMessage{Markdown: "reply"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Get("thread_ts") != "1727000000.123456" {
		t.Fatalf("form = %v", got)
	}
	if posted.Thread != "C0123ABC:1727000000.123456" || posted.Message != "1727000009.000100" || posted.Files != nil {
		t.Fatalf("posted = %+v", posted)
	}
}

func TestPostFileRoot(t *testing.T) {
	var uploads, completes int
	var uploadForm *multipartForm
	var completeForm, postForm url.Values
	var gotUploadAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/chat.postMessage":
			_ = r.ParseForm()
			postForm = r.PostForm
			_, _ = w.Write([]byte(`{"ok":true,"ts":"1727000009.000100"}`))
		case "/files.getUploadURLExternal":
			_ = r.ParseForm()
			if uploads == 0 {
				if r.PostForm.Get("filename") != "run.log.txt" {
					t.Fatalf("filename = %q", r.PostForm.Get("filename"))
				}
				if r.PostForm.Get("length") != "7" {
					t.Fatalf("length = %q", r.PostForm.Get("length"))
				}
			}
			uploads++
			_, _ = w.Write([]byte(`{"ok":true,"upload_url":"https://files.slack.com/upload/v1/abc","file_id":"F1"}`))
		case "/upload/v1/abc":
			gotUploadAuth = r.Header.Get("Authorization")
			form := &multipartForm{}
			if err := r.ParseMultipartForm(1 << 20); err != nil {
				t.Fatal(err)
			}
			file, header, err := r.FormFile("file")
			if err != nil {
				t.Fatal(err)
			}
			data, _ := io.ReadAll(file)
			form.filename = header.Filename
			form.data = string(data)
			if uploadForm == nil {
				uploadForm = form
			}
			_, _ = w.Write([]byte(`OK`))
		case "/files.completeUploadExternal":
			completes++
			_ = r.ParseForm()
			completeForm = r.PostForm
			_, _ = w.Write([]byte(`{"ok":true,"files":[{"id":"F1"},{"id":"F2"}]}`))
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	defer server.Close()
	p := newProvider(t, server.URL, &http.Client{Transport: rewriteTransport{target: server.URL}}, func(c *slack.Config) {
		c.Sleep = func(context.Context, time.Duration) error { return nil }
	})
	files := []boardsync.OutboundFile{
		{Data: []byte("content"), Filename: "run.log", MIMEType: "text/plain"},
		{Data: []byte("content"), Filename: "shot.png", MIMEType: "image/png"},
	}
	posted, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{
		Markdown: "see attached", Files: files, BoardMessage: 812,
	})
	if err != nil {
		t.Fatal(err)
	}
	if uploads != 2 || completes != 1 {
		t.Fatalf("uploads=%d completes=%d", uploads, completes)
	}
	if uploadForm.filename != "run.log.txt" || uploadForm.data != "content" {
		t.Fatalf("upload = %+v", uploadForm)
	}
	if gotUploadAuth != "" {
		t.Fatalf("an upload URL got the bot token: %q", gotUploadAuth)
	}
	if postForm.Get("text") != "see attached" {
		t.Fatalf("text = %q", postForm.Get("text"))
	}
	// Files follow the text in its thread; the receipt is the text message.
	if completeForm.Get("channel_id") != "C0123ABC" || completeForm.Get("thread_ts") != "1727000009.000100" {
		t.Fatalf("complete form = %v", completeForm)
	}
	if completeForm.Get("initial_comment") != "" {
		t.Fatalf("comment = %q", completeForm.Get("initial_comment"))
	}
	if !strings.Contains(completeForm.Get("files"), `"id":"F1"`) {
		t.Fatalf("files = %q", completeForm.Get("files"))
	}
	if posted.Thread != "C0123ABC:1727000009.000100" || posted.Message != "1727000009.000100" {
		t.Fatalf("posted = %+v", posted)
	}
	if len(posted.Files) != 2 || posted.Files[0] != "F1" {
		t.Fatalf("posted files = %+v", posted.Files)
	}
}

type multipartForm struct {
	filename string
	data     string
}

func TestPostFileReply(t *testing.T) {
	var completeForm, postForm url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/chat.postMessage":
			_ = r.ParseForm()
			postForm = r.PostForm
			_, _ = w.Write([]byte(`{"ok":true,"ts":"1727000011.000300"}`))
		case "/files.getUploadURLExternal":
			_, _ = w.Write([]byte(`{"ok":true,"upload_url":"https://files.slack.com/upload/v1/abc","file_id":"F1"}`))
		case "/upload/v1/abc":
			_, _ = w.Write([]byte(`OK`))
		case "/files.completeUploadExternal":
			_ = r.ParseForm()
			completeForm = r.PostForm
			_, _ = w.Write([]byte(`{"ok":true,"files":[{"id":"F1"}]}`))
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	defer server.Close()
	p := newProvider(t, server.URL, &http.Client{Transport: rewriteTransport{target: server.URL}}, func(c *slack.Config) {
		c.Sleep = func(context.Context, time.Duration) error { return nil }
	})
	posted, err := p.Post(context.Background(), "slack", postTarget(true), boardsync.OutboundMessage{
		Markdown: "see attached",
		Files:    []boardsync.OutboundFile{{Data: []byte("x"), Filename: "a.png", MIMEType: "image/png"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if postForm.Get("thread_ts") != "1727000000.123456" {
		t.Fatalf("post form = %v", postForm)
	}
	if completeForm.Get("thread_ts") != "1727000000.123456" {
		t.Fatalf("complete form = %v", completeForm)
	}
	// The receipt is the real text message ts, never the thread root or a
	// file ID: an edit/delete must not hit the root.
	if posted.Message != "1727000011.000300" || posted.Message == "1727000000.123456" {
		t.Fatalf("posted = %+v", posted)
	}
	if posted.Thread != "C0123ABC:1727000000.123456" {
		t.Fatalf("posted thread = %+v", posted)
	}
}

func TestPostFileRefusedFallsBackToText(t *testing.T) {
	var postForm, updateForm url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/files.getUploadURLExternal":
			_, _ = w.Write([]byte(`{"ok":false,"error":"file_uploads_disabled"}`))
		case "/chat.postMessage":
			_ = r.ParseForm()
			postForm = r.PostForm
			_, _ = w.Write([]byte(`{"ok":true,"ts":"1727000009.000100"}`))
		case "/chat.update":
			_ = r.ParseForm()
			updateForm = r.PostForm
			_, _ = w.Write([]byte(`{"ok":true}`))
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	posted, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{
		Markdown: "see attached", BoardMessage: 812,
		Files: []boardsync.OutboundFile{{Data: []byte("x"), Filename: "a.png", MIMEType: "image/png"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if posted.Files == nil || len(posted.Files) != 0 {
		t.Fatalf("posted files = %#v, want an empty non-nil list", posted.Files)
	}
	if posted.Message != "1727000009.000100" {
		t.Fatalf("posted = %+v", posted)
	}
	if postForm.Get("text") != "see attached" {
		t.Fatalf("initial text = %q", postForm.Get("text"))
	}
	if !strings.Contains(updateForm.Get("text"), "📎 a.png") || !strings.Contains(updateForm.Get("text"), "on the board: #812") {
		t.Fatalf("edited text = %q", updateForm.Get("text"))
	}
}

func TestPostFileSizeAndCountLimits(t *testing.T) {
	var uploads int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/files.getUploadURLExternal":
			uploads++
			_, _ = w.Write([]byte(`{"ok":true,"upload_url":"https://files.slack.com/upload/v1/abc","file_id":"F1"}`))
		case "/upload/v1/abc":
			_, _ = w.Write([]byte(`OK`))
		case "/files.completeUploadExternal":
			_, _ = w.Write([]byte(`{"ok":true,"files":[{"id":"F1"}]}`))
		case "/chat.postMessage":
			_ = r.ParseForm()
			if r.PostForm.Get("text") == "" || !strings.Contains(r.PostForm.Get("text"), "📎") {
				t.Fatalf("notes missing from text: %q", r.PostForm.Get("text"))
			}
			_, _ = w.Write([]byte(`{"ok":true,"ts":"1727000009.000100"}`))
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	defer server.Close()
	p := newProvider(t, server.URL, &http.Client{Transport: rewriteTransport{target: server.URL}}, func(c *slack.Config) {
		c.MaxFiles = 1
		c.MaxFileBytes = 4
		c.Sleep = func(context.Context, time.Duration) error { return nil }
	})
	posted, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{
		Markdown: "see attached",
		Files: []boardsync.OutboundFile{
			{Data: []byte("ok"), Filename: "one.txt", MIMEType: "text/plain"},
			{Data: []byte("toolarge"), Filename: "two.txt", MIMEType: "text/plain"},
			{Data: []byte("excess"), Filename: "three.txt", MIMEType: "text/plain"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if uploads != 1 {
		t.Fatalf("uploads = %d, want only the first file", uploads)
	}
	if len(posted.Files) != 1 || posted.Files[0] != "F1" || posted.Message != "1727000009.000100" {
		t.Fatalf("posted = %+v", posted)
	}
}

func TestPostNoTextNoFiles(t *testing.T) {
	p := newProvider(t, "http://127.0.0.1:1", nil, nil)
	if _, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{}); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("Post = %v", err)
	}
}

func TestEditLabelFollowsTheDeliveredCopy(t *testing.T) {
	cases := []struct {
		name     string
		username string
		want     string
	}{
		{"named", "Ada", "new text"},
		{"app", "", "**Ada**: new text"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got url.Values
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/auth.test":
					_, _ = w.Write([]byte(`{"ok":true,"response_metadata":{"scopes":["chat:write.customize"]}}`))
				case "/conversations.replies":
					_, _ = w.Write([]byte(`{"ok":true,"messages":[{"ts":"1727000000.123456","username":"` + tc.username + `"}]}`))
				case "/chat.update":
					_ = r.ParseForm()
					got = r.PostForm
					_, _ = w.Write([]byte(`{"ok":true}`))
				default:
					t.Fatalf("path = %s", r.URL.Path)
				}
			}))
			defer server.Close()
			p := newProvider(t, server.URL, nil, nil)
			thread := boardsync.ThreadRef{Key: "slack", Platform: "slack", Channel: "C0123ABC", Thread: "C0123ABC:1727000000.123456"}
			if err := p.Edit(context.Background(), "slack", thread, "1727000000.123456", boardsync.OutboundMessage{
				Markdown: "**Ada**: new text", Sender: &boardsync.Sender{Name: "Ada"},
			}); err != nil {
				t.Fatal(err)
			}
			if got.Get("text") != tc.want {
				t.Fatalf("text = %q, want %q", got.Get("text"), tc.want)
			}
		})
	}
}

func TestEditRefusesFiles(t *testing.T) {
	p := newProvider(t, "http://127.0.0.1:1", nil, nil)
	thread := boardsync.ThreadRef{Key: "slack", Platform: "slack", Channel: "C0123ABC", Thread: "C0123ABC:1727000000.123456"}
	err := p.Edit(context.Background(), "slack", thread, "1727000000.123456", boardsync.OutboundMessage{
		Markdown: "x", Files: []boardsync.OutboundFile{{Data: []byte("x"), Filename: "a"}},
	})
	if !errors.Is(err, boardsync.ErrUnsupported) {
		t.Fatalf("Edit = %v", err)
	}
}

func TestReactionValidationAndPath(t *testing.T) {
	var forms []url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		forms = append(forms, r.PostForm)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	thread := boardsync.ThreadRef{Key: "slack", Platform: "slack", Channel: "C0123ABC", Thread: "C0123ABC:1727000000.123456"}
	if err := p.React(context.Background(), "slack", thread, "1727000000.123456", ":thumbsup:", true); err != nil {
		t.Fatal(err)
	}
	if err := p.React(context.Background(), "slack", thread, "1727000000.123456", "thumbsup::skin-tone-2", false); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"", "two words", "<!channel>", strings.Repeat("a", 101)} {
		if err := p.React(context.Background(), "slack", thread, "1727000000.123456", bad, true); !errors.Is(err, boardsync.ErrBadRequest) {
			t.Fatalf("React(%q) = %v", bad, err)
		}
	}
	if len(forms) != 2 || forms[0].Get("name") != "thumbsup" || forms[1].Get("name") != "thumbsup::skin-tone-2" {
		t.Fatalf("forms = %v", forms)
	}
}

func TestTyping(t *testing.T) {
	var got url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/assistant.threads.setStatus" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		_ = r.ParseForm()
		got = r.PostForm
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	thread := boardsync.ThreadRef{Key: "slack", Platform: "slack", Channel: "C0123ABC", Thread: "C0123ABC:1727000000.123456"}
	long := strings.Repeat("é", 100)
	if err := p.Typing(context.Background(), "slack", thread, "1727000000.123456", long); err != nil {
		t.Fatal(err)
	}
	if got.Get("channel_id") != "C0123ABC" || got.Get("thread_ts") != "1727000000.123456" {
		t.Fatalf("form = %v", got)
	}
	if runes := len([]rune(got.Get("status"))); runes != 50 {
		t.Fatalf("status is %d runes", runes)
	}
	if got.Get("loading_messages") == "" {
		t.Fatal("loading_messages is missing")
	}
	// A channel target has no status surface; it is a no-op.
	channelThread := boardsync.ThreadRef{Key: "slack", Platform: "slack", Channel: "C0123ABC"}
	if err := p.Typing(context.Background(), "slack", channelThread, "", "typing"); err != nil {
		t.Fatal(err)
	}
}

func TestTypingUnavailableIsUnsupported(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ok":false,"error":"missing_scope"}`))
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	thread := boardsync.ThreadRef{Key: "slack", Platform: "slack", Channel: "C0123ABC", Thread: "C0123ABC:1727000000.123456"}
	if err := p.Typing(context.Background(), "slack", thread, "", "typing"); !errors.Is(err, boardsync.ErrUnsupported) {
		t.Fatalf("Typing = %v", err)
	}
}

func TestPermalinkRejectsForeignDomain(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true,"permalink":"https://evil.example/archives/C0123ABC/p1727000000123456"}`))
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	if _, err := p.Permalink(context.Background(), "slack", boardsync.Event{
		Kind: boardsync.KindMessage, Channel: "C0123ABC", Message: "1727000000.123456",
	}); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("Permalink = %v", err)
	}
}

func TestFetchFile(t *testing.T) {
	var gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_, _ = w.Write([]byte("file-bytes"))
	}))
	defer server.Close()
	p := newProvider(t, server.URL, &http.Client{Transport: rewriteTransport{target: server.URL}}, func(c *slack.Config) {
		c.MaxFileBytes = 32
	})
	data, err := p.FetchFile(context.Background(), "slack", boardsync.File{
		Name: "a.txt", Size: 10, URL: "https://files.slack.com/files-pri/T0123ABC-F0123ABC/a.txt",
	})
	if err != nil || string(data) != "file-bytes" {
		t.Fatalf("FetchFile = %q, %v", data, err)
	}
	if gotAuth != "Bearer "+testToken {
		t.Fatalf("authorization = %q", gotAuth)
	}
	// A non-Slack URL is never fetched, even to test whether it resolves.
	if _, err := p.FetchFile(context.Background(), "slack", boardsync.File{URL: "http://169.254.169.254/latest/meta-data/"}); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("unsafe URL = %v", err)
	}
	if _, err := p.FetchFile(context.Background(), "slack", boardsync.File{
		Size: 100, URL: "https://files.slack.com/files-pri/T-F/big.bin",
	}); !errors.Is(err, slack.ErrFileTooLarge) {
		t.Fatalf("oversize = %v", err)
	}
}

func TestOutboundErrorClassification(t *testing.T) {
	cases := []struct {
		name      string
		status    int
		header    http.Header
		body      string
		want      error
		permanent bool
	}{
		{"rate limit", 429, http.Header{"Retry-After": {"7"}}, ``, boardsync.ErrRateLimited, false},
		{"server", 503, nil, ``, nil, false},
		{"bad request", 400, nil, ``, boardsync.ErrPermanent, true},
		{"auth", 401, nil, ``, boardsync.ErrUnauthorized, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				for name, values := range tc.header {
					for _, value := range values {
						w.Header().Add(name, value)
					}
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			p := newProvider(t, server.URL, nil, nil)
			_, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{Markdown: "x"})
			if err == nil {
				t.Fatal("Post succeeded")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if boardsync.IsPermanent(err) != tc.permanent {
				t.Fatalf("permanent = %v: %v", boardsync.IsPermanent(err), err)
			}
			if tc.status == 429 && !strings.Contains(err.Error(), "7s") {
				t.Fatalf("Retry-After was lost: %v", err)
			}
		})
	}
}

func TestFileRefusalClassification(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ok":false,"error":"missing_scope"}`))
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	thread := boardsync.ThreadRef{Key: "slack", Platform: "slack", Channel: "C0123ABC", Thread: "C0123ABC:1727000000.123456"}
	_, err := p.UploadFiles(context.Background(), "slack", thread, []boardsync.OutboundFile{{Data: []byte("x"), Filename: "a.png"}})
	if !errors.Is(err, slack.ErrFileRefused) || !errors.Is(err, boardsync.ErrUnauthorized) {
		t.Fatalf("UploadFiles = %v", err)
	}
	if !boardsync.IsPermanent(err) {
		t.Fatalf("file refusal isn't permanent: %v", err)
	}
}

func TestSSRFRefusals(t *testing.T) {
	// An upload URL off Slack's domain is refused before any bytes move.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true,"upload_url":"https://evil.example/upload","file_id":"F1"}`))
	}))
	defer server.Close()
	client := slack.NewClient(server.URL, nil, testToken)
	if _, _, err := client.GetUploadURLExternal(context.Background(), "a.txt", 1); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("upload URL = %v", err)
	}

	// A file download that redirects off Slack's domain is refused.
	var fetches atomic.Int64
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetches.Add(1)
		http.Redirect(w, r, "http://169.254.169.254/latest/meta-data/", http.StatusFound)
	}))
	defer redirect.Close()
	p := newProvider(t, redirect.URL, &http.Client{Transport: rewriteTransport{target: redirect.URL}}, nil)
	_, err := p.FetchFile(context.Background(), "slack", boardsync.File{URL: "https://files.slack.com/files-pri/T-F/a.txt"})
	if err == nil || fetches.Load() != 1 {
		t.Fatalf("redirect = %v, fetches = %d", err, fetches.Load())
	}
}

func TestUploadFiles(t *testing.T) {
	var completeForm url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/files.getUploadURLExternal":
			_, _ = w.Write([]byte(`{"ok":true,"upload_url":"https://files.slack.com/upload/v1/abc","file_id":"F1"}`))
		case "/upload/v1/abc":
			_, _ = w.Write([]byte(`OK`))
		case "/files.completeUploadExternal":
			_ = r.ParseForm()
			completeForm = r.PostForm
			_, _ = w.Write([]byte(`{"ok":true,"files":[{"id":"F1"}]}`))
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	defer server.Close()
	p := newProvider(t, server.URL, &http.Client{Transport: rewriteTransport{target: server.URL}}, nil)
	thread := boardsync.ThreadRef{Key: "slack", Platform: "slack", Channel: "C0123ABC", Thread: "C0123ABC:1727000000.123456"}
	ids, err := p.UploadFiles(context.Background(), "slack", thread, []boardsync.OutboundFile{
		{Data: []byte("x"), Filename: "a.png", MIMEType: "image/png"},
	})
	if err != nil || len(ids) != 1 || ids[0] != "F1" {
		t.Fatalf("UploadFiles = %v, %v", ids, err)
	}
	if completeForm.Get("thread_ts") != "1727000000.123456" {
		t.Fatalf("form = %v", completeForm)
	}
}

func TestPostValidatesTarget(t *testing.T) {
	p := newProvider(t, "http://127.0.0.1:1", nil, nil)
	target := postTarget(false)
	target.Key = "other"
	if _, err := p.Post(context.Background(), "slack", target, boardsync.OutboundMessage{Markdown: "x"}); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("Post = %v", err)
	}
	target = postTarget(false)
	target.Platform = "github"
	if _, err := p.Post(context.Background(), "slack", target, boardsync.OutboundMessage{Markdown: "x"}); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("Post = %v", err)
	}
}

// rewriteTransport sends every host to the test server, so a request to
// files.slack.com reaches httptest while the adapter still validates the URL
// Slack returned.
type rewriteTransport struct {
	target string
}

func (t rewriteTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	base, err := url.Parse(t.target)
	if err != nil {
		return nil, err
	}
	clone := request.Clone(request.Context())
	clone.URL.Scheme = base.Scheme
	clone.URL.Host = base.Host
	return http.DefaultTransport.RoundTrip(clone)
}
