package slack_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/slack"
)

func TestInternalPostAndEditUseMarkdownOnly(t *testing.T) {
	var post, update url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/chat.postMessage":
			_ = r.ParseForm()
			post = r.PostForm
			_, _ = w.Write([]byte(`{"ok":true,"ts":"1727000009.000100"}`))
		case "/chat.update":
			_ = r.ParseForm()
			update = r.PostForm
			_, _ = w.Write([]byte(`{"ok":true}`))
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	if _, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{Markdown: "**Ada**: ship it"}); err != nil {
		t.Fatal(err)
	}
	if post.Get("text") != "" || post.Get("markdown_text") != "**Ada**: ship it" {
		t.Fatalf("post form = %v", post)
	}
	thread := boardsync.ThreadRef{Key: "slack", Platform: "slack", Channel: "C0123ABC", Thread: "C0123ABC:1727000009.000100"}
	if err := p.Edit(context.Background(), "slack", thread, "1727000009.000100", boardsync.OutboundMessage{Markdown: "**Ada**: edited"}); err != nil {
		t.Fatal(err)
	}
	if update.Get("text") != "" || update.Get("markdown_text") != "**Ada**: edited" {
		t.Fatalf("update form = %v", update)
	}
}

func TestExternalPostUsesTextOnly(t *testing.T) {
	var post url.Values
	server := capturePostServer(t, &post)
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	if _, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{
		Markdown: "<!channel>", External: true,
	}); err != nil {
		t.Fatal(err)
	}
	if post.Get("markdown_text") != "" || post.Get("text") != "&lt;!channel&gt;" {
		t.Fatalf("post form = %v", post)
	}
}

func TestInternalPostAndEditClampTo12000Characters(t *testing.T) {
	var post, update url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/chat.postMessage":
			_ = r.ParseForm()
			post = r.PostForm
			_, _ = w.Write([]byte(`{"ok":true,"ts":"1727000009.000100"}`))
		case "/chat.update":
			_ = r.ParseForm()
			update = r.PostForm
			_, _ = w.Write([]byte(`{"ok":true}`))
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	long := strings.Repeat("é", 13000)
	if _, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{Markdown: long}); err != nil {
		t.Fatal(err)
	}
	if got := len([]rune(post.Get("markdown_text"))); got != 12000 {
		t.Fatalf("post markdown_text has %d characters", got)
	}
	if post.Get("text") != "" {
		t.Fatalf("post sent text too: %v", post)
	}
	thread := boardsync.ThreadRef{Key: "slack", Platform: "slack", Channel: "C0123ABC", Thread: "C0123ABC:1727000009.000100"}
	if err := p.Edit(context.Background(), "slack", thread, "1727000009.000100", boardsync.OutboundMessage{Markdown: long}); err != nil {
		t.Fatal(err)
	}
	if got := len([]rune(update.Get("markdown_text"))); got != 12000 {
		t.Fatalf("update markdown_text has %d characters", got)
	}
	if update.Get("text") != "" {
		t.Fatalf("update sent text too: %v", update)
	}
}

func TestTruncationDoesNotSplitMention(t *testing.T) {
	var post url.Values
	server := capturePostServer(t, &post)
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	markdown := strings.Repeat("a", 11990) + " @[slack:user:U0456ABC]"
	if _, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{Markdown: markdown}); err != nil {
		t.Fatal(err)
	}
	body := post.Get("markdown_text")
	if len([]rune(body)) > 12000 {
		t.Fatalf("body has %d characters", len([]rune(body)))
	}
	if strings.Contains(body, "<@") || strings.Contains(body, "@[slack:user:U0456") {
		t.Fatalf("truncation left part of a mention: %q", body[len(body)-40:])
	}
}

func TestInternalPostOverLimitFakeSlack(t *testing.T) {
	fake := newFakeSlack(t)
	p := fake.provider(t)
	posted, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{
		Markdown: strings.Repeat("x", 13000),
	})
	if err != nil {
		t.Fatalf("Post = %v", err)
	}
	fake.mu.Lock()
	message := fake.find("C0123ABC", posted.Message)
	fake.mu.Unlock()
	if message == nil {
		t.Fatal("message wasn't stored")
	}
	if got := len([]rune(message.Text)); got != 12000 {
		t.Fatalf("fake stored %d characters", got)
	}
}

func TestUpdateWarningLogsWithoutBody(t *testing.T) {
	var logs []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/chat.postMessage":
			_, _ = w.Write([]byte(`{"ok":true,"ts":"1727000009.000100"}`))
		case "/files.getUploadURLExternal":
			w.WriteHeader(http.StatusForbidden)
		case "/chat.update":
			_, _ = w.Write([]byte(`{"ok":false,"error":"message_not_found"}`))
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	defer server.Close()
	p, err := slack.New(slack.Config{
		Key: "slack", SigningSecret: testSecret, BotToken: testToken, BotUserID: "UAPP123",
		APIBaseURL: server.URL, Now: now, Sleep: func(context.Context, time.Duration) error { return nil },
		Logf: func(format string, args ...any) { logs = append(logs, format) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{
		Markdown: "secret board body",
		Files:    []boardsync.OutboundFile{{Data: []byte("x"), Filename: "a.png"}},
	}); err != nil {
		t.Fatal(err)
	}
	if len(logs) != 1 || strings.Contains(logs[0], "secret board body") {
		t.Fatalf("logs = %v", logs)
	}
}
