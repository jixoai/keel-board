package github_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/github"
)

// sourceFixture serves the issue/PR/comment reads a source read makes and
// records every request URL.
type sourceFixture struct {
	t        *testing.T
	requests []string
}

func newSourceProvider(t *testing.T) (*github.Provider, *sourceFixture) {
	t.Helper()
	fixture := &sourceFixture{t: t}
	server := httptest.NewServer(http.HandlerFunc(fixture.serve))
	t.Cleanup(server.Close)
	provider, err := github.New(github.Config{
		Key: "github", WebhookSecret: "secret", Token: "ghs_test",
		BotUserID: 7000, Repos: []string{"acme/www"}, APIBaseURL: server.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	return provider, fixture
}

func (f *sourceFixture) serve(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer ghs_test" {
		f.t.Errorf("authorization = %q", r.Header.Get("Authorization"))
	}
	f.requests = append(f.requests, r.URL.RequestURI())
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.URL.Path == "/repos/acme/www/issues/45":
		_, _ = w.Write([]byte(`{
			"number":45,"title":"Checkout retry","body":"Ping @ada","state":"open",
			"html_url":"https://github.com/acme/www/issues/45",
			"user":{"login":"ada","id":1001},"comments":2,
			"created_at":"2026-01-02T03:04:05Z"
		}`))
	case r.URL.Path == "/repos/acme/www/issues/45/comments":
		if r.URL.Query().Get("page") == "1" {
			w.Header().Set("Link", `<https://api.github.com/repos/acme/www/issues/45/comments?per_page=2&page=2>; rel="next"`)
			_, _ = w.Write([]byte(`[
				{"id":9001,"body":"first comment","user":{"login":"julian","id":1002},"created_at":"2026-01-02T05:00:00Z"},
				{"id":9002,"body":"second comment","user":{"login":"ada","id":1001},"created_at":"2026-01-02T06:00:00Z"}
			]`))
			return
		}
		_, _ = w.Write([]byte(`[
			{"id":9003,"body":"third comment","user":{"login":"grace","id":1003},"created_at":"2026-01-02T07:00:00Z"}
		]`))
	case r.URL.Path == "/repos/acme/www/issues/9":
		_, _ = w.Write([]byte(`{
			"number":9,"title":"Checkout retry PR","body":"PR body","state":"open",
			"html_url":"https://github.com/acme/www/pull/9",
			"user":{"login":"ada","id":1001},"comments":1,
			"pull_request":{"url":"https://api.github.com/repos/acme/www/pulls/9"},
			"created_at":"2026-01-02T03:04:05Z"
		}`))
	case r.URL.Path == "/repos/acme/www/pulls/9":
		_, _ = w.Write([]byte(`{
			"number":9,"title":"Checkout retry PR","body":"PR body","state":"open",
			"html_url":"https://github.com/acme/www/pull/9",
			"user":{"login":"ada","id":1001},"comments":1,
			"created_at":"2026-01-02T03:04:05Z"
		}`))
	case r.URL.Path == "/repos/acme/www/issues/9/comments":
		_, _ = w.Write([]byte(`[
			{"id":9100,"body":"PR review note","user":{"login":"julian","id":1002},"created_at":"2026-01-02T05:00:00Z"}
		]`))
	default:
		f.t.Errorf("unexpected request %s", r.URL.String())
		w.WriteHeader(http.StatusNotFound)
	}
}

func TestReadSourceIssueByShorthand(t *testing.T) {
	p, fixture := newSourceProvider(t)
	read, err := p.ReadSource(context.Background(), "github", "acme/www#45", boardsync.ReadOptions{Limit: 2})
	if err != nil {
		t.Fatalf("ReadSource: %v", err)
	}
	if read.Kind != "issue" || read.Channel != "acme/www:issues" ||
		read.Thread != "acme/www:issue:45" || read.Name != "Checkout retry" {
		t.Fatalf("read = %+v", read)
	}
	if !read.More || read.Next != "2" {
		t.Fatalf("more/next = %t/%q", read.More, read.Next)
	}
	if len(read.Messages) != 3 {
		t.Fatalf("messages = %+v", read.Messages)
	}
	root := read.Messages[0]
	if root.ID != "acme/www#45" || root.Author != "ada" || root.AuthorID != "github:user:1001" ||
		root.Replies != 2 || !strings.Contains(root.Text, "Ping @[github:user:ada]") {
		t.Fatalf("root = %+v", root)
	}
	if read.Messages[1].ID != "9001" || read.Messages[1].AuthorID != "github:user:1002" ||
		read.Messages[1].Thread != "acme/www:issue:45" {
		t.Fatalf("comment = %+v", read.Messages[1])
	}
	want := []string{"/repos/acme/www/issues/45", "/repos/acme/www/issues/45/comments?per_page=2&page=1"}
	if fmt.Sprint(fixture.requests) != fmt.Sprint(want) {
		t.Fatalf("requests = %v", fixture.requests)
	}
}

func TestReadSourceIssuePageTwo(t *testing.T) {
	p, fixture := newSourceProvider(t)
	read, err := p.ReadSource(context.Background(), "github", "acme/www#45", boardsync.ReadOptions{Limit: 2, After: "2"})
	if err != nil {
		t.Fatalf("ReadSource: %v", err)
	}
	if read.More || read.Next != "" {
		t.Fatalf("more/next = %t/%q", read.More, read.Next)
	}
	if len(read.Messages) != 1 || read.Messages[0].ID != "9003" {
		t.Fatalf("messages = %+v", read.Messages)
	}
	if !strings.Contains(fmt.Sprint(fixture.requests), "/repos/acme/www/issues/45/comments?per_page=2&page=2") {
		t.Fatalf("requests = %v", fixture.requests)
	}
}

func TestReadSourcePullNativeAndURL(t *testing.T) {
	for _, target := range []string{"acme/www:9", "https://github.com/acme/www/pull/9"} {
		t.Run(target, func(t *testing.T) {
			p, _ := newSourceProvider(t)
			read, err := p.ReadSource(context.Background(), "github", target, boardsync.ReadOptions{})
			if err != nil {
				t.Fatalf("ReadSource: %v", err)
			}
			if read.Kind != "pull_request" || read.Channel != "acme/www:pulls" ||
				read.Thread != "acme/www:9" || read.Name != "Checkout retry PR" {
				t.Fatalf("read = %+v", read)
			}
			if len(read.Messages) != 2 {
				t.Fatalf("messages = %+v", read.Messages)
			}
			if read.Messages[0].ID != "acme/www#9" || read.Messages[0].AuthorID != "github:user:1001" {
				t.Fatalf("root = %+v", read.Messages[0])
			}
			if read.Messages[1].ID != "9100" || read.Messages[1].AuthorID != "github:user:1002" {
				t.Fatalf("comment = %+v", read.Messages[1])
			}
		})
	}
}

func TestReadSourceRejectsOutsideAllowlist(t *testing.T) {
	p, fixture := newSourceProvider(t)
	_, err := p.ReadSource(context.Background(), "github", "other/repo#1", boardsync.ReadOptions{})
	if !errors.Is(err, boardsync.ErrNotFound) {
		t.Fatalf("outside allowlist = %v", err)
	}
	if len(fixture.requests) != 0 {
		t.Fatalf("an unlisted repo reached GitHub: %v", fixture.requests)
	}
}

func TestReadSourceRejectsBadCursorAndKey(t *testing.T) {
	p, fixture := newSourceProvider(t)
	for _, after := range []string{"abc", "0", "-1", "1000001"} {
		if _, err := p.ReadSource(context.Background(), "github", "acme/www#45",
			boardsync.ReadOptions{After: after}); !errors.Is(err, boardsync.ErrBadRequest) {
			t.Fatalf("cursor %q = %v, want ErrBadRequest", after, err)
		}
	}
	if _, err := p.ReadSource(context.Background(), "other", "acme/www#45", boardsync.ReadOptions{}); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("key mismatch = %v", err)
	}
	if len(fixture.requests) != 0 {
		t.Fatalf("a rejected read reached GitHub: %v", fixture.requests)
	}
}

func TestReadSourceLimitCapsAtHundred(t *testing.T) {
	p, fixture := newSourceProvider(t)
	if _, err := p.ReadSource(context.Background(), "github", "acme/www#45", boardsync.ReadOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ReadSource(context.Background(), "github", "acme/www#45", boardsync.ReadOptions{Limit: 500}); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(fixture.requests, "\n")
	if !strings.Contains(joined, "per_page=50&page=1") || !strings.Contains(joined, "per_page=100&page=1") {
		t.Fatalf("requests = %v", fixture.requests)
	}
}
