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

type fileScenario struct {
	getStatus    int
	getBody      string
	getCalls     int
	completeBody string
	completeCall int
	uploadCalls  int
}

func fileProvider(t *testing.T, scenario *fileScenario) (*slack.Provider, *httptest.Server, *url.Values, *atomic.Int64) {
	t.Helper()
	var postForm url.Values
	var posts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth.test":
			_, _ = w.Write([]byte(`{"ok":true,"response_metadata":{"scopes":["files:read","files:write"]}}`))
		case "/files.getUploadURLExternal":
			scenario.getCalls++
			if scenario.getStatus != 0 {
				w.WriteHeader(scenario.getStatus)
				_, _ = w.Write([]byte(scenario.getBody))
				return
			}
			_, _ = w.Write([]byte(`{"ok":true,"upload_url":"https://files.slack.com/upload/v1/abc","file_id":"F1"}`))
		case "/upload/v1/abc":
			scenario.uploadCalls++
			_, _ = w.Write([]byte(`OK`))
		case "/files.completeUploadExternal":
			scenario.completeCall++
			if scenario.completeBody != "" {
				_, _ = w.Write([]byte(scenario.completeBody))
				return
			}
			_, _ = w.Write([]byte(`{"ok":true,"files":[{"id":"F1"}]}`))
		case "/files.info":
			_, _ = w.Write([]byte(`{"ok":true,"file":{"shares":{"public":{"C0123ABC":[{"ts":"1727000010.000200"}]}}}}`))
		case "/chat.postMessage":
			posts.Add(1)
			_ = r.ParseForm()
			postForm = r.PostForm
			_, _ = w.Write([]byte(`{"ok":true,"ts":"1727000009.000100"}`))
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)
	p := newProvider(t, server.URL, &http.Client{Transport: rewriteTransport{target: server.URL}}, func(c *slack.Config) {
		c.Sleep = func(context.Context, time.Duration) error { return nil }
	})
	return p, server, &postForm, &posts
}

func TestFileFailureDegradesAnyPreShareError(t *testing.T) {
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
			scenario := &fileScenario{getStatus: tc.status}
			p, _, form, posts := fileProvider(t, scenario)
			posted, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{
				Markdown: "see attached", BoardMessage: 812,
				Files: []boardsync.OutboundFile{{Data: []byte("x"), Filename: "a.png"}},
			})
			if err != nil {
				t.Fatalf("Post = %v", err)
			}
			if posts.Load() != 1 || !strings.Contains(form.Get("text"), "📎 a.png") || !strings.Contains(form.Get("text"), "on the board: #812") {
				t.Fatalf("posts=%d text=%q", posts.Load(), form.Get("text"))
			}
			if posted.Files == nil || len(posted.Files) != 0 {
				t.Fatalf("posted = %+v", posted)
			}
		})
	}
}

func TestFileUploadRetriesThenSucceeds(t *testing.T) {
	// The first two calls fail; later calls succeed.
	var calls atomic.Int64
	var postForm url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth.test":
			_, _ = w.Write([]byte(`{"ok":true,"response_metadata":{"scopes":["files:read","files:write"]}}`))
		case "/files.getUploadURLExternal":
			if calls.Add(1) <= 2 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write([]byte(`{"ok":true,"upload_url":"https://files.slack.com/upload/v1/abc","file_id":"F1"}`))
		case "/upload/v1/abc":
			_, _ = w.Write([]byte(`OK`))
		case "/files.completeUploadExternal":
			_, _ = w.Write([]byte(`{"ok":true,"files":[{"id":"F1","shares":{"public":{"C0123ABC":[{"ts":"1727000010.000200"}]}}}]}`))
		case "/chat.postMessage":
			_ = r.ParseForm()
			postForm = r.PostForm
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	defer server.Close()
	p := newProvider(t, server.URL, &http.Client{Transport: rewriteTransport{target: server.URL}}, func(c *slack.Config) {
		c.Sleep = func(context.Context, time.Duration) error { return nil }
	})
	posted, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{
		Markdown: "see attached",
		Files:    []boardsync.OutboundFile{{Data: []byte("x"), Filename: "a.png"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 || postForm.Get("text") != "" {
		t.Fatalf("calls=%d post=%v", calls.Load(), postForm)
	}
	if len(posted.Files) != 1 || posted.Message != "1727000010.000200" {
		t.Fatalf("posted = %+v", posted)
	}
}

func TestFileUploadRetriesExhaustedKeepsText(t *testing.T) {
	var gets atomic.Int64
	var posts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth.test":
			_, _ = w.Write([]byte(`{"ok":true,"response_metadata":{"scopes":["files:read","files:write"]}}`))
		case "/files.getUploadURLExternal":
			gets.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
		case "/chat.postMessage":
			posts.Add(1)
			_, _ = w.Write([]byte(`{"ok":true,"ts":"1727000009.000100"}`))
		}
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, func(c *slack.Config) {
		c.Sleep = func(context.Context, time.Duration) error { return nil }
	})
	posted, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{
		Markdown: "see attached", BoardMessage: 812,
		Files: []boardsync.OutboundFile{{Data: []byte("x"), Filename: "a.png"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if gets.Load() != 3 || posts.Load() != 1 || len(posted.Files) != 0 {
		t.Fatalf("gets=%d posts=%d posted=%+v", gets.Load(), posts.Load(), posted)
	}
}

func TestFilePartialUploadDegradesWithoutDoubleText(t *testing.T) {
	var gets, completes, posts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth.test":
			_, _ = w.Write([]byte(`{"ok":true,"response_metadata":{"scopes":["files:read","files:write"]}}`))
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
		case "/chat.postMessage":
			posts.Add(1)
			_, _ = w.Write([]byte(`{"ok":true,"ts":"1727000009.000100"}`))
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	defer server.Close()
	p := newProvider(t, server.URL, &http.Client{Transport: rewriteTransport{target: server.URL}}, func(c *slack.Config) {
		c.Sleep = func(context.Context, time.Duration) error { return nil }
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
	if gets.Load() != 2 || completes.Load() != 0 || posts.Load() != 1 || len(posted.Files) != 0 {
		t.Fatalf("gets=%d completes=%d posts=%d posted=%+v", gets.Load(), completes.Load(), posts.Load(), posted)
	}
}

func TestFileCompleteFailureDegrades(t *testing.T) {
	scenario := &fileScenario{completeBody: `{"ok":false,"error":"invalid_arguments"}`}
	p, _, form, posts := fileProvider(t, scenario)
	posted, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{
		Markdown: "see attached",
		Files:    []boardsync.OutboundFile{{Data: []byte("x"), Filename: "a.png"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if scenario.completeCall != 1 || posts.Load() != 1 || len(posted.Files) != 0 || !strings.Contains(form.Get("text"), "📎 a.png") {
		t.Fatalf("complete=%d posts=%d posted=%+v text=%q", scenario.completeCall, posts.Load(), posted, form.Get("text"))
	}
}

func TestFileSharesReusedWithoutFileInfo(t *testing.T) {
	var infoCalls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth.test":
			_, _ = w.Write([]byte(`{"ok":true,"response_metadata":{"scopes":["files:read","files:write"]}}`))
		case "/files.getUploadURLExternal":
			_, _ = w.Write([]byte(`{"ok":true,"upload_url":"https://files.slack.com/upload/v1/abc","file_id":"F1"}`))
		case "/upload/v1/abc":
			_, _ = w.Write([]byte(`OK`))
		case "/files.completeUploadExternal":
			_, _ = w.Write([]byte(`{"ok":true,"files":[{"id":"F1","shares":{"public":{"C0123ABC":[{"ts":"1727000010.000200"}]}}}]}`))
		case "/files.info":
			infoCalls.Add(1)
			_, _ = w.Write([]byte(`{"ok":true,"file":{"shares":{}}}`))
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	defer server.Close()
	p := newProvider(t, server.URL, &http.Client{Transport: rewriteTransport{target: server.URL}}, func(c *slack.Config) {
		c.Sleep = func(context.Context, time.Duration) error { return nil }
	})
	posted, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{
		Markdown: "see attached",
		Files:    []boardsync.OutboundFile{{Data: []byte("x"), Filename: "a.png"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if infoCalls.Load() != 0 || posted.Message != "1727000010.000200" {
		t.Fatalf("info=%d posted=%+v", infoCalls.Load(), posted)
	}
}

func TestFindShareFailureIsPartialSuccess(t *testing.T) {
	var posts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth.test":
			_, _ = w.Write([]byte(`{"ok":true,"response_metadata":{"scopes":["files:read","files:write"]}}`))
		case "/files.getUploadURLExternal":
			_, _ = w.Write([]byte(`{"ok":true,"upload_url":"https://files.slack.com/upload/v1/abc","file_id":"F1"}`))
		case "/upload/v1/abc":
			_, _ = w.Write([]byte(`OK`))
		case "/files.completeUploadExternal":
			_, _ = w.Write([]byte(`{"ok":true,"files":[{"id":"F1"}]}`))
		case "/files.info":
			w.WriteHeader(http.StatusInternalServerError)
		case "/chat.postMessage":
			posts.Add(1)
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	defer server.Close()
	p := newProvider(t, server.URL, &http.Client{Transport: rewriteTransport{target: server.URL}}, func(c *slack.Config) {
		c.Sleep = func(context.Context, time.Duration) error { return nil }
	})
	posted, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{
		Markdown: "see attached",
		Files:    []boardsync.OutboundFile{{Data: []byte("x"), Filename: "a.png"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if posts.Load() != 0 || len(posted.Files) != 1 || posted.Message == "" {
		t.Fatalf("posts=%d posted=%+v", posts.Load(), posted)
	}
}

func TestTextThenFilesFailureIsPartialSuccess(t *testing.T) {
	var posts, updates atomic.Int64
	var updateForm url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth.test":
			_, _ = w.Write([]byte(`{"ok":true,"response_metadata":{"scopes":["chat:write"]}}`))
		case "/chat.postMessage":
			posts.Add(1)
			_, _ = w.Write([]byte(`{"ok":true,"ts":"1727000009.000100"}`))
		case "/files.getUploadURLExternal":
			w.WriteHeader(http.StatusForbidden)
		case "/chat.update":
			updates.Add(1)
			_ = r.ParseForm()
			updateForm = r.PostForm
			_, _ = w.Write([]byte(`{"ok":true}`))
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, func(c *slack.Config) {
		c.Sleep = func(context.Context, time.Duration) error { return nil }
	})
	posted, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{
		Markdown: "see attached", BoardMessage: 812,
		Files: []boardsync.OutboundFile{{Data: []byte("x"), Filename: "a.png"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if posts.Load() != 1 || updates.Load() != 1 || len(posted.Files) != 0 {
		t.Fatalf("posts=%d updates=%d posted=%+v", posts.Load(), updates.Load(), posted)
	}
	if !strings.Contains(updateForm.Get("text"), "📎 a.png") || !strings.Contains(updateForm.Get("text"), "on the board: #812") {
		t.Fatalf("text = %q", updateForm.Get("text"))
	}
}

func TestOutboxDoesNotRetryPartialFileSuccess(t *testing.T) {
	var posts, gets atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth.test":
			_, _ = w.Write([]byte(`{"ok":true,"response_metadata":{"scopes":["files:read","files:write"]}}`))
		case "/chat.postMessage":
			posts.Add(1)
			_, _ = w.Write([]byte(`{"ok":true,"ts":"1727000009.000100"}`))
		case "/files.getUploadURLExternal":
			gets.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, func(c *slack.Config) {
		c.Sleep = func(context.Context, time.Duration) error { return nil }
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
	if posts.Load() != 1 || gets.Load() != 3 {
		t.Fatalf("posts=%d gets=%d", posts.Load(), gets.Load())
	}
}
