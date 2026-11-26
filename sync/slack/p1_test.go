package slack_test

import (
	"context"
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

func capturePostServer(t *testing.T, got *url.Values) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/chat.postMessage":
			_ = r.ParseForm()
			*got = r.PostForm
			_, _ = w.Write([]byte(`{"ok":true,"ts":"1727000009.000100"}`))
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
}

func TestExternalTextIsInert(t *testing.T) {
	cases := []struct {
		name   string
		text   string
		want   []string
		absent []string
	}{
		{
			name: "broadcast", text: "<!channel> and <!here>",
			want: []string{"&lt;!channel&gt;", "&lt;!here&gt;"}, absent: []string{"<!channel>", "<!here>"},
		},
		{
			name: "raw mention", text: "hi <@U0456ABC>",
			want: []string{"&lt;@U0456ABC&gt;"}, absent: []string{"<@U0456ABC>"},
		},
		{
			name: "disguised link", text: "<https://evil.example|click me>",
			want: []string{"&lt;https://evil.example|click me&gt;"}, absent: []string{"<https://evil.example|click me>"},
		},
		{
			name: "entities", text: "a & b < c > d",
			want: []string{"a &amp; b &lt; c &gt; d"},
		},
		{
			name: "explicit mention", text: "hi @[slack:user:U0456ABC]",
			want: []string{"<@U0456ABC>"}, absent: []string{"&lt;@U0456ABC&gt;"},
		},
		{
			name: "other workspace mention", text: "hi @[acme:user:U0456ABC]",
			want: []string{"@[acme:user:U0456ABC]"}, absent: []string{"<@U0456ABC>"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got url.Values
			server := capturePostServer(t, &got)
			defer server.Close()
			p := newProvider(t, server.URL, nil, nil)
			if _, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{
				Markdown: "**Ada**: " + tc.text, External: true,
			}); err != nil {
				t.Fatal(err)
			}
			text := got.Get("text")
			for _, want := range tc.want {
				if !strings.Contains(text, want) {
					t.Fatalf("text = %q, missing %q", text, want)
				}
			}
			for _, absent := range tc.absent {
				if strings.Contains(text, absent) {
					t.Fatalf("text = %q, kept %q", text, absent)
				}
			}
		})
	}
}

func TestFileNamesAreInert(t *testing.T) {
	var got url.Values
	server := capturePostServer(t, &got)
	defer server.Close()
	p := newProvider(t, server.URL, nil, func(c *slack.Config) { c.MaxFileBytes = 1 })
	posted, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{
		Markdown: "file:",
		Files:    []boardsync.OutboundFile{{Data: []byte("too large"), Filename: "<@U999> <!channel>"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if posted.Files == nil || len(posted.Files) != 0 {
		t.Fatalf("posted = %+v", posted)
	}
	text := got.Get("text")
	if !strings.Contains(text, "&lt;@U999&gt; &lt;!channel&gt;") {
		t.Fatalf("text = %q", text)
	}
	if strings.Contains(text, "<@U999>") || strings.Contains(text, "<!channel>") {
		t.Fatalf("raw Slack markup in notes: %q", text)
	}
}

func TestCanonicalReactions(t *testing.T) {
	var names []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		names = append(names, r.PostForm.Get("name"))
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	thread := boardsync.ThreadRef{Key: "slack", Platform: "slack", Channel: "C0123ABC", Thread: "C0123ABC:1727000000.123456"}
	cases := map[string]string{
		"thumbs_up":              "thumbsup",
		"thumbs_down":            "thumbsdown",
		"check":                  "white_check_mark",
		"thumbs_up::skin-tone-2": "thumbsup::skin-tone-2",
		"thumbsup":               "thumbsup",
		"white_check_mark":       "white_check_mark",
	}
	for canonical, want := range cases {
		if err := p.React(context.Background(), "slack", thread, "1727000000.123456", canonical, true); err != nil {
			t.Fatalf("%s: %v", canonical, err)
		}
		if names[len(names)-1] != want {
			t.Fatalf("%s sent %q, want %q", canonical, names[len(names)-1], want)
		}
	}
}

type fileCapture struct {
	post    url.Values
	update  url.Values
	posts   atomic.Int64
	updates atomic.Int64
	deletes atomic.Int64
}

func fileServer(t *testing.T, handle func(w http.ResponseWriter, r *http.Request, cap *fileCapture)) (*slack.Provider, *fileCapture) {
	t.Helper()
	cap := &fileCapture{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/chat.postMessage":
			cap.posts.Add(1)
			_ = r.ParseForm()
			cap.post = r.PostForm
			_, _ = w.Write([]byte(`{"ok":true,"ts":"1727000009.000100"}`))
			return
		case "/chat.update":
			cap.updates.Add(1)
			_ = r.ParseForm()
			cap.update = r.PostForm
			_, _ = w.Write([]byte(`{"ok":true}`))
			return
		case "/files.delete":
			cap.deletes.Add(1)
			_, _ = w.Write([]byte(`{"ok":true}`))
			return
		}
		handle(w, r, cap)
	}))
	t.Cleanup(server.Close)
	p := newProvider(t, server.URL, &http.Client{Transport: rewriteTransport{target: server.URL}}, func(c *slack.Config) {
		c.Sleep = func(context.Context, time.Duration) error { return nil }
	})
	return p, cap
}

func TestFileFailureEditsNotes(t *testing.T) {
	cases := []struct {
		name   string
		status int
	}{
		{"413", http.StatusRequestEntityTooLarge},
		{"400", http.StatusBadRequest},
		{"401", http.StatusUnauthorized},
		{"403", http.StatusForbidden},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, cap := fileServer(t, func(w http.ResponseWriter, r *http.Request, _ *fileCapture) {
				if r.URL.Path != "/files.getUploadURLExternal" {
					t.Fatalf("path = %s", r.URL.Path)
				}
				w.WriteHeader(tc.status)
			})
			posted, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{
				Markdown: "see attached", BoardMessage: 812,
				Files: []boardsync.OutboundFile{{Data: []byte("x"), Filename: "a.png"}},
			})
			if err != nil {
				t.Fatalf("Post = %v", err)
			}
			if posted.Message != "1727000009.000100" {
				t.Fatalf("posted = %+v", posted)
			}
			if posted.Files == nil || len(posted.Files) != 0 {
				t.Fatalf("posted files = %+v", posted.Files)
			}
			if cap.posts.Load() != 1 || cap.updates.Load() != 1 {
				t.Fatalf("posts=%d updates=%d", cap.posts.Load(), cap.updates.Load())
			}
			if !strings.Contains(cap.update.Get("text"), "📎 a.png") || !strings.Contains(cap.update.Get("text"), "on the board: #812") {
				t.Fatalf("text = %q", cap.update.Get("text"))
			}
		})
	}
}

func TestFileUploadRetriesThenSucceeds(t *testing.T) {
	var gets atomic.Int64
	p, cap := fileServer(t, func(w http.ResponseWriter, r *http.Request, _ *fileCapture) {
		switch r.URL.Path {
		case "/files.getUploadURLExternal":
			if gets.Add(1) <= 2 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write([]byte(`{"ok":true,"upload_url":"https://files.slack.com/upload/v1/abc","file_id":"F1"}`))
		case "/upload/v1/abc":
			_, _ = w.Write([]byte(`OK`))
		case "/files.completeUploadExternal":
			_, _ = w.Write([]byte(`{"ok":true,"files":[{"id":"F1"}]}`))
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	})
	posted, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{
		Markdown: "see attached",
		Files:    []boardsync.OutboundFile{{Data: []byte("x"), Filename: "a.png"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if gets.Load() != 3 || cap.posts.Load() != 1 || cap.updates.Load() != 0 {
		t.Fatalf("gets=%d posts=%d updates=%d", gets.Load(), cap.posts.Load(), cap.updates.Load())
	}
	if len(posted.Files) != 1 || posted.Message != "1727000009.000100" {
		t.Fatalf("posted = %+v", posted)
	}
}

func TestFileUploadRetriesExhaustedKeepsText(t *testing.T) {
	var gets atomic.Int64
	p, cap := fileServer(t, func(w http.ResponseWriter, r *http.Request, _ *fileCapture) {
		if r.URL.Path != "/files.getUploadURLExternal" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		gets.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	posted, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{
		Markdown: "see attached", BoardMessage: 812,
		Files: []boardsync.OutboundFile{{Data: []byte("x"), Filename: "a.png"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if gets.Load() != 3 || cap.posts.Load() != 1 || cap.updates.Load() != 1 || len(posted.Files) != 0 || posted.Message != "1727000009.000100" {
		t.Fatalf("gets=%d posts=%d updates=%d posted=%+v", gets.Load(), cap.posts.Load(), cap.updates.Load(), posted)
	}
}

func TestFilePartialUploadCleansOrphanAndKeepsText(t *testing.T) {
	var gets, completes atomic.Int64
	p, cap := fileServer(t, func(w http.ResponseWriter, r *http.Request, _ *fileCapture) {
		switch r.URL.Path {
		case "/files.getUploadURLExternal":
			if gets.Add(1) == 1 {
				_, _ = w.Write([]byte(`{"ok":true,"upload_url":"https://files.slack.com/upload/v1/abc","file_id":"F1"}`))
				return
			}
			w.WriteHeader(http.StatusRequestEntityTooLarge)
		case "/upload/v1/abc":
			_, _ = w.Write([]byte(`OK`))
		case "/files.completeUploadExternal":
			completes.Add(1)
			_, _ = w.Write([]byte(`{"ok":true,"files":[{"id":"F1"}]}`))
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	})
	posted, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{
		Markdown: "see attached", BoardMessage: 812,
		Files: []boardsync.OutboundFile{
			{Data: []byte("one"), Filename: "one.png"},
			{Data: []byte("two"), Filename: "two.png"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if gets.Load() != 2 || completes.Load() != 0 || cap.posts.Load() != 1 || cap.updates.Load() != 1 || cap.deletes.Load() != 1 {
		t.Fatalf("gets=%d completes=%d posts=%d updates=%d deletes=%d", gets.Load(), completes.Load(), cap.posts.Load(), cap.updates.Load(), cap.deletes.Load())
	}
	if len(posted.Files) != 0 || posted.Message != "1727000009.000100" {
		t.Fatalf("posted = %+v", posted)
	}
}

func TestFileCompleteFailureDegrades(t *testing.T) {
	p, cap := fileServer(t, func(w http.ResponseWriter, r *http.Request, _ *fileCapture) {
		switch r.URL.Path {
		case "/files.getUploadURLExternal":
			_, _ = w.Write([]byte(`{"ok":true,"upload_url":"https://files.slack.com/upload/v1/abc","file_id":"F1"}`))
		case "/upload/v1/abc":
			_, _ = w.Write([]byte(`OK`))
		case "/files.completeUploadExternal":
			_, _ = w.Write([]byte(`{"ok":false,"error":"invalid_arguments"}`))
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	})
	posted, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{
		Markdown: "see attached",
		Files:    []boardsync.OutboundFile{{Data: []byte("x"), Filename: "a.png"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if cap.posts.Load() != 1 || cap.updates.Load() != 1 || cap.deletes.Load() != 1 || len(posted.Files) != 0 || posted.Message != "1727000009.000100" {
		t.Fatalf("posts=%d updates=%d deletes=%d posted=%+v", cap.posts.Load(), cap.updates.Load(), cap.deletes.Load(), posted)
	}
	if !strings.Contains(cap.update.Get("text"), "📎 a.png") {
		t.Fatalf("text = %q", cap.update.Get("text"))
	}
}

func TestFilePostNeverCallsFileInfo(t *testing.T) {
	var infoCalls atomic.Int64
	p, cap := fileServer(t, func(w http.ResponseWriter, r *http.Request, _ *fileCapture) {
		switch r.URL.Path {
		case "/files.getUploadURLExternal":
			_, _ = w.Write([]byte(`{"ok":true,"upload_url":"https://files.slack.com/upload/v1/abc","file_id":"F1"}`))
		case "/upload/v1/abc":
			_, _ = w.Write([]byte(`OK`))
		case "/files.completeUploadExternal":
			_, _ = w.Write([]byte(`{"ok":true,"files":[{"id":"F1"}]}`))
		case "/files.info":
			infoCalls.Add(1)
			_, _ = w.Write([]byte(`{"ok":true,"file":{"shares":{}}}`))
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	})
	posted, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{
		Markdown: "see attached",
		Files:    []boardsync.OutboundFile{{Data: []byte("x"), Filename: "a.png"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if infoCalls.Load() != 0 || cap.posts.Load() != 1 || posted.Message != "1727000009.000100" || len(posted.Files) != 1 {
		t.Fatalf("info=%d posts=%d posted=%+v", infoCalls.Load(), cap.posts.Load(), posted)
	}
}

func TestFileReplyReceiptIsTextTS(t *testing.T) {
	var completeForm url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/chat.postMessage":
			_ = r.ParseForm()
			if r.PostForm.Get("thread_ts") != "1727000000.123456" {
				t.Fatalf("post form = %v", r.PostForm)
			}
			_, _ = w.Write([]byte(`{"ok":true,"ts":"1727000011.000300"}`))
		case "/files.getUploadURLExternal":
			_, _ = w.Write([]byte(`{"ok":true,"upload_url":"https://files.slack.com/upload/v1/abc","file_id":"F1"}`))
		case "/upload/v1/abc":
			_, _ = w.Write([]byte(`OK`))
		case "/files.completeUploadExternal":
			_ = r.ParseForm()
			completeForm = r.PostForm
			_, _ = w.Write([]byte(`{"ok":true,"files":[{"id":"F1"}]}`))
		case "/files.info":
			t.Fatal("the adapter must not guess a file message ID")
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
		Files:    []boardsync.OutboundFile{{Data: []byte("x"), Filename: "a.png"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if posted.Message != "1727000011.000300" || posted.Message == "1727000000.123456" {
		t.Fatalf("posted = %+v", posted)
	}
	if posted.Thread != "C0123ABC:1727000000.123456" || completeForm.Get("thread_ts") != "1727000000.123456" {
		t.Fatalf("posted=%+v complete=%v", posted, completeForm)
	}
}

func TestTextThenFilesFailureIsPartialSuccess(t *testing.T) {
	p, cap := fileServer(t, func(w http.ResponseWriter, r *http.Request, _ *fileCapture) {
		if r.URL.Path != "/files.getUploadURLExternal" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusForbidden)
	})
	posted, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{
		Markdown: "see attached", BoardMessage: 812,
		Files: []boardsync.OutboundFile{{Data: []byte("x"), Filename: "a.png"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if cap.posts.Load() != 1 || cap.updates.Load() != 1 || len(posted.Files) != 0 || posted.Message != "1727000009.000100" {
		t.Fatalf("posts=%d updates=%d posted=%+v", cap.posts.Load(), cap.updates.Load(), posted)
	}
	if !strings.Contains(cap.update.Get("text"), "📎 a.png") || !strings.Contains(cap.update.Get("text"), "on the board: #812") {
		t.Fatalf("text = %q", cap.update.Get("text"))
	}
}

func TestOutboxDoesNotRetryFilePartialSuccess(t *testing.T) {
	var gets atomic.Int64
	p, cap := fileServer(t, func(w http.ResponseWriter, r *http.Request, _ *fileCapture) {
		if r.URL.Path != "/files.getUploadURLExternal" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		gets.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	outbox := boardsync.NewOutbox(boardsync.RetryPolicy{MaxAttempts: 3, BaseDelay: 0}, 8)
	if err := outbox.Enqueue("slack:C0123ABC:1727000000.123456", func(ctx context.Context) error {
		_, err := p.Post(ctx, "slack", postTarget(false), boardsync.OutboundMessage{
			Markdown: "see attached",
			Files:    []boardsync.OutboundFile{{Data: []byte("x"), Filename: "a.png"}},
		})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	result := outbox.Drain(context.Background())
	if result.Done != 1 || result.Retried != 0 || result.Dead != 0 {
		t.Fatalf("drain = %+v", result)
	}
	if cap.posts.Load() != 1 || cap.updates.Load() != 1 || gets.Load() != 3 {
		t.Fatalf("posts=%d updates=%d gets=%d", cap.posts.Load(), cap.updates.Load(), gets.Load())
	}
}
