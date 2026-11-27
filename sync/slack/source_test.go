package slack_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/slack"
)

func TestReadSourceChannel(t *testing.T) {
	var historyForm url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/conversations.info":
			_, _ = w.Write([]byte(`{"ok":true,"channel":{"id":"C0123ABC","name":"general","is_member":true}}`))
		case "/conversations.history":
			_ = r.ParseForm()
			historyForm = r.PostForm
			_, _ = w.Write([]byte(`{"ok":true,"has_more":true,"response_metadata":{"next_cursor":"cur-2"},"messages":[
				{"ts":"1727000005.000100","user":"U0999XYZ","text":"second <@U0456ABC>","thread_ts":"1727000000.123456"},
				{"ts":"1727000000.123456","user":"U0456ABC","text":"first","reply_count":1,
				 "files":[{"id":"F1","name":"a.log","size":2,"mimetype":"text/plain","url_private_download":"https://files.slack.com/f/a.log"},
				          {"id":"F2","name":"b.bin","size":2,"url_private_download":"https://evil.example/b.bin"}]}
			]}`))
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	read, err := p.ReadSource(context.Background(), "slack", "C0123ABC", boardsync.ReadOptions{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if historyForm.Get("limit") != "1" {
		t.Fatalf("limit = %q", historyForm.Get("limit"))
	}
	if read.Kind != "channel" || read.Channel != "C0123ABC" || read.Name != "general" || read.Thread != "" || !read.More {
		t.Fatalf("read = %+v", read)
	}
	// A trimmed page must not offer the platform cursor: it would resume past
	// the trimmed message.
	if read.Next != "" {
		t.Fatalf("next = %q", read.Next)
	}
	if len(read.Messages) != 1 {
		t.Fatalf("messages = %+v", read.Messages)
	}
	message := read.Messages[0]
	// History is newest first in Slack; a reader sees oldest first.
	if message.ID != "1727000000.123456" || message.Author != "U0456ABC" || message.AuthorID != "slack:user:U0456ABC" {
		t.Fatalf("message = %+v", message)
	}
	if message.Thread != "C0123ABC:1727000000.123456" || message.Replies != 1 || message.CreatedAt != "2024-09-22T10:13:20Z" {
		t.Fatalf("message = %+v", message)
	}
	if len(message.Files) != 2 || message.Files[0].URL != "https://files.slack.com/f/a.log" || message.Files[1].URL != "" {
		t.Fatalf("files = %+v", message.Files)
	}
}

func TestReadSourceThread(t *testing.T) {
	var replyTS string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/conversations.info":
			_, _ = w.Write([]byte(`{"ok":true,"channel":{"id":"C0123ABC","name":"general","is_member":true}}`))
		case "/conversations.replies":
			_ = r.ParseForm()
			replyTS = r.PostForm.Get("ts")
			_, _ = w.Write([]byte(`{"ok":true,"messages":[
				{"ts":"1727000000.123456","user":"U0456ABC","text":"root"},
				{"ts":"1727000005.000100","user":"U0999XYZ","text":"reply","thread_ts":"1727000000.123456"}
			]}`))
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	read, err := p.ReadSource(context.Background(), "slack", "C0123ABC:1727000000.123456", boardsync.ReadOptions{Limit: 50})
	if err != nil {
		t.Fatal(err)
	}
	if replyTS != "1727000000.123456" {
		t.Fatalf("read ts = %q", replyTS)
	}
	if read.Thread != "C0123ABC:1727000000.123456" || len(read.Messages) != 2 {
		t.Fatalf("read = %+v", read)
	}
	if read.Messages[1].Thread != "C0123ABC:1727000000.123456" || read.Messages[1].Author != "U0999XYZ" {
		t.Fatalf("reply = %+v", read.Messages[1])
	}
}

func TestReadSourceReplyTSReadsItsThread(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/conversations.info":
			_, _ = w.Write([]byte(`{"ok":true,"channel":{"id":"C0123ABC","name":"general","is_member":true}}`))
		case "/conversations.replies":
			_ = r.ParseForm()
			calls.Add(1)
			if r.PostForm.Get("ts") == "1727000005.000100" {
				_, _ = w.Write([]byte(`{"ok":true,"messages":[{"ts":"1727000005.000100","user":"U0999XYZ","text":"reply","thread_ts":"1727000000.123456"}]}`))
				return
			}
			_, _ = w.Write([]byte(`{"ok":true,"messages":[{"ts":"1727000000.123456","user":"U0456ABC","text":"root"}]}`))
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	read, err := p.ReadSource(context.Background(), "slack", "C0123ABC:1727000005.000100", boardsync.ReadOptions{Limit: 50})
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || read.Thread != "C0123ABC:1727000000.123456" || read.Messages[0].ID != "1727000000.123456" {
		t.Fatalf("calls=%d read=%+v", calls.Load(), read)
	}
}

func TestReadSourceChannelPaginates(t *testing.T) {
	var historyForm url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/conversations.info":
			_, _ = w.Write([]byte(`{"ok":true,"channel":{"id":"C0123ABC","name":"general","is_member":true}}`))
		case "/conversations.history":
			_ = r.ParseForm()
			historyForm = r.PostForm
			_, _ = w.Write([]byte(`{"ok":true,"has_more":true,"response_metadata":{"next_cursor":"cur-2"},
				"messages":[{"ts":"1727000010.000100","user":"U0456ABC","text":"next page"}]}`))
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	read, err := p.ReadSource(context.Background(), "slack", "C0123ABC",
		boardsync.ReadOptions{Limit: 10, After: "cur-1"})
	if err != nil {
		t.Fatal(err)
	}
	if historyForm.Get("cursor") != "cur-1" {
		t.Fatalf("cursor = %q", historyForm.Get("cursor"))
	}
	if read.Next != "cur-2" || !read.More || read.Kind != "channel" {
		t.Fatalf("read = %+v", read)
	}
}

func TestReadSourcePermissions(t *testing.T) {
	cases := []struct {
		name string
		body string
		want error
	}{
		{"private", `{"ok":true,"channel":{"id":"C0123ABC","name":"secret","is_member":true,"is_private":true}}`, boardsync.ErrUnsupported},
		{"dm", `{"ok":true,"channel":{"id":"C0123ABC","is_member":true,"is_im":true}}`, boardsync.ErrUnsupported},
		{"shared", `{"ok":true,"channel":{"id":"C0123ABC","name":"ext","is_member":true,"is_ext_shared":true}}`, boardsync.ErrUnsupported},
		{"not a member", `{"ok":true,"channel":{"id":"C0123ABC","name":"general"}}`, boardsync.ErrUnsupported},
		{"unknown", `{"ok":false,"error":"channel_not_found"}`, boardsync.ErrNotFound},
		{"missing scope", `{"ok":false,"error":"missing_scope"}`, boardsync.ErrUnauthorized},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			p := newProvider(t, server.URL, nil, nil)
			if _, err := p.ReadSource(context.Background(), "slack", "C0123ABC", boardsync.ReadOptions{Limit: 10}); !errors.Is(err, tc.want) {
				t.Fatalf("ReadSource = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestJoinSourceOptIn(t *testing.T) {
	var joined atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/conversations.info":
			_, _ = w.Write([]byte(`{"ok":true,"channel":{"id":"C0123ABC","name":"general"}}`))
		case "/conversations.join":
			_ = r.ParseForm()
			if r.PostForm.Get("channel") != "C0123ABC" {
				t.Fatalf("form = %v", r.PostForm)
			}
			joined.Add(1)
			_, _ = w.Write([]byte(`{"ok":true}`))
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, func(c *slack.Config) { c.JoinChannels = true })
	result, err := p.JoinSource(context.Background(), "slack", "C0123ABC")
	if err != nil {
		t.Fatal(err)
	}
	if !result.Joined || result.Channel != "C0123ABC" || result.Name != "general" || joined.Load() != 1 {
		t.Fatalf("result = %+v, joins = %d", result, joined.Load())
	}
}

func TestJoinSourceNeedsOptIn(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	if _, err := p.JoinSource(context.Background(), "slack", "C0123ABC"); !errors.Is(err, boardsync.ErrUnsupported) || !strings.Contains(err.Error(), "join_disabled") {
		t.Fatalf("JoinSource = %v", err)
	}
	if calls.Load() != 0 {
		t.Fatal("a disabled join touched the network")
	}
}

func TestJoinSourceRefusals(t *testing.T) {
	cases := []struct {
		name string
		body string
		want error
	}{
		{"private", `{"ok":true,"channel":{"id":"C0123ABC","name":"secret","is_private":true}}`, boardsync.ErrUnsupported},
		{"shared", `{"ok":true,"channel":{"id":"C0123ABC","name":"ext","is_ext_shared":true}}`, boardsync.ErrUnsupported},
		{"unknown", `{"ok":false,"error":"channel_not_found"}`, boardsync.ErrNotFound},
		{"scope", `{"ok":false,"error":"missing_scope"}`, boardsync.ErrUnauthorized},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			p := newProvider(t, server.URL, nil, func(c *slack.Config) { c.JoinChannels = true })
			if _, err := p.JoinSource(context.Background(), "slack", "C0123ABC"); !errors.Is(err, tc.want) {
				t.Fatalf("JoinSource = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestJoinSourceAlreadyMember(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/conversations.info" {
			t.Fatalf("unexpected %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"ok":true,"channel":{"id":"C0123ABC","name":"general","is_member":true}}`))
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, func(c *slack.Config) { c.JoinChannels = true })
	result, err := p.JoinSource(context.Background(), "slack", "C0123ABC")
	if err != nil {
		t.Fatal(err)
	}
	if result.Joined || calls.Load() != 1 {
		t.Fatalf("result = %+v, calls = %d", result, calls.Load())
	}
}

func TestResolveChannelFallsBackToPublicListing(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/conversations.list" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		_ = r.ParseForm()
		calls.Add(1)
		if strings.Contains(r.PostForm.Get("types"), "private_channel") {
			_, _ = w.Write([]byte(`{"ok":false,"error":"missing_scope"}`))
			return
		}
		_, _ = w.Write([]byte(`{"ok":true,"channels":[{"id":"C0123ABC","name":"general","is_member":true}]}`))
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	id, err := p.ResolveChannel(context.Background(), "slack", "#general")
	if err != nil || id != "C0123ABC" {
		t.Fatalf("ResolveChannel = %q, %v", id, err)
	}
	if _, err := p.ResolveChannel(context.Background(), "slack", "#secret"); !errors.Is(err, boardsync.ErrNotFound) {
		t.Fatalf("private by name = %v", err)
	}
	if calls.Load() < 3 {
		t.Fatalf("calls = %d", calls.Load())
	}
}

func TestResolveChannelPrivateNameWithGroupsRead(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true,"channels":[
			{"id":"C0123ABC","name":"general","is_member":true},
			{"id":"G0123ABC","name":"secret","is_member":true,"is_private":true}
		]}`))
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	id, err := p.ResolveChannel(context.Background(), "slack", "#secret")
	if err != nil || id != "G0123ABC" {
		t.Fatalf("ResolveChannel = %q, %v", id, err)
	}
}

func TestResolveTargetRejectsForeignPermalinks(t *testing.T) {
	p := newProvider(t, "http://127.0.0.1:1", nil, nil)
	for _, target := range []string{
		"https://evil.example/archives/C0123ABC/p1727000000123456",
		"https://acme.slack.com.evil.example/archives/C0123ABC/p1727000000123456",
		"C0123ABC:not-a-ts",
		"C0123ABC:1727000000.123456:extra",
	} {
		if _, err := p.ResolveTarget(context.Background(), "slack", target); !errors.Is(err, boardsync.ErrBadRequest) {
			t.Fatalf("ResolveTarget(%q) = %v", target, err)
		}
	}
}

func TestLocateRejectsForeignDomain(t *testing.T) {
	p := newProvider(t, "http://127.0.0.1:1", nil, nil)
	if _, _, ok := p.Locate("https://evil.example/archives/C0123ABC/p1727000000123456"); ok {
		t.Fatal("Locate accepted a foreign URL")
	}
	thread, message, ok := p.Locate("https://acme.enterprise.slack.com/archives/C0123ABC/p1727000000123456?thread_ts=1727000000.123456")
	if !ok || thread != "C0123ABC:1727000000.123456" || message != "1727000000.123456" {
		t.Fatalf("Locate = %q, %q, %v", thread, message, ok)
	}
}

func TestPermalinkReturnsSlackURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat.getPermalink" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"ok":true,"permalink":"https://acme.slack.com/archives/C0123ABC/p1727000000123456"}`))
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	link, err := p.Permalink(context.Background(), "slack", boardsync.Event{
		Kind: boardsync.KindMessage, Channel: "C0123ABC", Message: "1727000000.123456",
	})
	if err != nil || link != "https://acme.slack.com/archives/C0123ABC/p1727000000123456" {
		t.Fatalf("Permalink = %q, %v", link, err)
	}
	if link, err := p.Permalink(context.Background(), "slack", boardsync.Event{Kind: boardsync.KindDeleted}); err != nil || link != "" {
		t.Fatalf("deleted Permalink = %q, %v", link, err)
	}
}
