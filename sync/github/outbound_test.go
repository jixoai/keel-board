package github_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/github"
)

func writeProvider(t *testing.T, baseURL string, mutate func(*github.Config)) *github.Provider {
	t.Helper()
	return providerWith(t, func(c *github.Config) {
		c.WriteRepos = []string{"acme/www"}
		c.APIBaseURL = baseURL
		if mutate != nil {
			mutate(c)
		}
	})
}

func prTarget() boardsync.ResolvedTarget {
	return boardsync.ResolvedTarget{
		Key: "github", Platform: "github", Channel: "acme/www:pulls",
		Thread: "acme/www:123", Kind: boardsync.TargetThread,
	}
}

func TestViewerIsResolvedOnceAndSkipsOwnComments(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/user" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		calls++
		if r.Header.Get("Authorization") != "Bearer ghp_test_token" {
			t.Fatalf("authorization = %q", r.Header.Get("Authorization"))
		}
		_, _ = w.Write([]byte(`{"login":"keel-board[bot]","id":4242,"type":"Bot"}`))
	}))
	defer server.Close()

	p := writeProvider(t, server.URL, func(c *github.Config) { c.DetectViewer = true })
	author, err := p.Viewer(context.Background())
	if err != nil {
		t.Fatalf("Viewer: %v", err)
	}
	if author.ID != "github:user:4242" || author.Handle != "keel-board[bot]" || !author.Bot {
		t.Fatalf("viewer = %+v", author)
	}
	if _, err := p.Viewer(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("viewer calls = %d, want one cached call", calls)
	}

	body := []byte(strings.ReplaceAll(string(fixture(t, "pr_comment.json")),
		`"login": "ada", "id": 1001, "type": "User"`,
		`"login": "keel-board[bot]", "id": 4242, "type": "Bot"`))
	intake, err := receiveRequest(t, p, "issue_comment", "viewer-echo", body)
	if err != nil {
		t.Fatal(err)
	}
	if len(intake.Records) != 0 {
		t.Fatalf("viewer's own comment imported: %+v", intake.Records)
	}
	if calls != 1 {
		t.Fatalf("viewer calls after webhook = %d, want the cached identity", calls)
	}
}

func TestPostClampsCommentAndKeepsMarker(t *testing.T) {
	var body map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("User-Agent") != "keel-board-sync" {
			t.Fatalf("user agent = %q", r.Header.Get("User-Agent"))
		}
		if r.Header.Get("X-GitHub-Api-Version") != "2022-11-28" {
			t.Fatalf("api version = %q", r.Header.Get("X-GitHub-Api-Version"))
		}
		data, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(data, &body); err != nil {
			t.Fatal(err)
		}
		_, _ = w.Write([]byte(`{"id":9400}`))
	}))
	defer server.Close()

	p := writeProvider(t, server.URL, nil)
	text := "nul\x00 and a long tail " + strings.Repeat("a", github.MaxCommentLength+100)
	if _, err := p.Post(context.Background(), "github", prTarget(), boardsync.OutboundMessage{Markdown: text, BoardMessage: 812}); err != nil {
		t.Fatalf("Post: %v", err)
	}
	comment := body["body"]
	if strings.ContainsRune(comment, 0) {
		t.Fatal("comment still carries a NUL")
	}
	if count := utf8.RuneCountInString(comment); count > github.MaxCommentLength {
		t.Fatalf("comment length = %d, want <= %d", count, github.MaxCommentLength)
	}
	if !strings.Contains(comment, "keel-board:sync key=github message=812 thread=github:acme/www:123") {
		t.Fatalf("marker missing from clamped comment: %q", comment)
	}
	if !strings.Contains(comment, "…") {
		t.Fatalf("clamped comment has no ellipsis: %q", comment[:120])
	}
}

func TestPostRejectsFilesEmptyTextAndForeignSubthread(t *testing.T) {
	p := writeProvider(t, "http://127.0.0.1:1", nil)
	ctx := context.Background()
	if _, err := p.Post(ctx, "github", prTarget(), boardsync.OutboundMessage{
		Markdown: "x", Files: []boardsync.OutboundFile{{Filename: "a.txt"}},
	}); !errors.Is(err, boardsync.ErrUnsupported) {
		t.Fatalf("files = %v", err)
	}
	if _, err := p.Post(ctx, "github", prTarget(), boardsync.OutboundMessage{Markdown: "  "}); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("empty text = %v", err)
	}
	foreign := prTarget()
	foreign.Subthread = "acme/www:456:rc:300"
	if _, err := p.Post(ctx, "github", foreign, boardsync.OutboundMessage{Markdown: "x"}); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("foreign subthread = %v", err)
	}
	mismatched := prTarget()
	mismatched.Channel = "acme/www:issues"
	if _, err := p.Post(ctx, "github", mismatched, boardsync.OutboundMessage{Markdown: "x"}); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("mismatched channel = %v", err)
	}
}

func TestDisabledMarkerWithoutIdentityRefusesToSend(t *testing.T) {
	p := writeProvider(t, "http://127.0.0.1:1", func(c *github.Config) {
		c.DisableMarker = true
	})
	if _, err := p.Post(context.Background(), "github", prTarget(), boardsync.OutboundMessage{Markdown: "x"}); !errors.Is(err, boardsync.ErrUnsupported) {
		t.Fatalf("Post without any echo guard = %v", err)
	}
	thread := boardsync.ThreadRef{Key: "github", Platform: "github", Channel: "acme/www:pulls", Thread: "acme/www:123"}
	if err := p.Edit(context.Background(), "github", thread, "9001", boardsync.OutboundMessage{Markdown: "x"}); !errors.Is(err, boardsync.ErrUnsupported) {
		t.Fatalf("Edit without any echo guard = %v", err)
	}
	if err := p.Delete(context.Background(), "github", thread, "9001"); !errors.Is(err, boardsync.ErrUnsupported) {
		t.Fatalf("Delete without any echo guard = %v", err)
	}
	if err := p.React(context.Background(), "github", thread, "9001", "rocket", true); !errors.Is(err, boardsync.ErrUnsupported) {
		t.Fatalf("React without any echo guard = %v", err)
	}
}

func TestDisabledMarkerRefusesWhenViewerCannotBeResolved(t *testing.T) {
	comments := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/user" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		comments++
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":9500}`))
	}))
	defer server.Close()
	p := writeProvider(t, server.URL, func(c *github.Config) {
		c.DisableMarker = true
		c.DetectViewer = true
	})
	if _, err := p.Post(context.Background(), "github", prTarget(), boardsync.OutboundMessage{Markdown: "x"}); err == nil {
		t.Fatal("Post succeeded without a resolved viewer and without a marker")
	}
	if comments != 0 {
		t.Fatalf("comment endpoint reached %d times", comments)
	}
}

func TestViewerFailureIsNotRetriedOnEveryWebhook(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	p := writeProvider(t, server.URL, func(c *github.Config) {
		c.DetectViewer = true
		c.Now = func() time.Time { return now }
	})
	body := fixture(t, "pr_comment.json")
	if intake, err := receiveRequest(t, p, "issue_comment", "v-1", body); err != nil || len(intake.Records) != 1 {
		t.Fatalf("first webhook = %+v, %v", intake.Records, err)
	}
	if _, err := receiveRequest(t, p, "issue_comment", "v-2", body); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("viewer calls = %d, want one failed lookup inside the cooldown", calls)
	}
}

func TestCommentIDsMustBePositive(t *testing.T) {
	p := writeProvider(t, "http://127.0.0.1:1", nil)
	thread := boardsync.ThreadRef{Key: "github", Platform: "github", Channel: "acme/www:pulls", Thread: "acme/www:123"}
	for _, id := range []string{"0", "-1"} {
		if err := p.Edit(context.Background(), "github", thread, id, boardsync.OutboundMessage{Markdown: "x"}); !errors.Is(err, boardsync.ErrBadRequest) {
			t.Fatalf("Edit comment ID %q = %v", id, err)
		}
		if err := p.Delete(context.Background(), "github", thread, id); !errors.Is(err, boardsync.ErrBadRequest) {
			t.Fatalf("Delete comment ID %q = %v", id, err)
		}
	}
}

func TestDeleteNotFoundDependsOnEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"Not Found"}`))
	}))
	defer server.Close()
	p := writeProvider(t, server.URL, nil)
	review := boardsync.ThreadRef{
		Key: "github", Platform: "github", Channel: "acme/www:pulls",
		Thread: "acme/www:123", Subthread: "acme/www:123:rc:300",
	}
	if err := p.Delete(context.Background(), "github", review, "9001"); err != nil {
		t.Fatalf("review comment already gone = %v", err)
	}
	issue := boardsync.ThreadRef{Key: "github", Platform: "github", Channel: "acme/www:pulls", Thread: "acme/www:123"}
	if err := p.Delete(context.Background(), "github", issue, "9001"); !errors.Is(err, boardsync.ErrNotFound) {
		t.Fatalf("bare thread delete = %v, want ErrNotFound (endpoint is ambiguous)", err)
	}
	if err := p.Edit(context.Background(), "github", issue, "9001", boardsync.OutboundMessage{Markdown: "new"}); !errors.Is(err, boardsync.ErrNotFound) {
		t.Fatalf("Edit missing comment = %v", err)
	}
}

func TestRESTErrorClassification(t *testing.T) {
	cases := []struct {
		name       string
		status     int
		header     map[string]string
		body       string
		want       error
		wantRate   bool
		retryAfter string
		permanent  bool
	}{
		{name: "rate 429", status: 429, header: map[string]string{"Retry-After": "2"}, body: "slow down", want: boardsync.ErrRateLimited, wantRate: true, retryAfter: "2s"},
		{name: "rate 403 remaining", status: 403, header: map[string]string{"X-RateLimit-Remaining": "0"}, body: "forbidden", want: boardsync.ErrRateLimited, wantRate: true},
		{name: "rate 403 retry-after", status: 403, header: map[string]string{"Retry-After": "7"}, body: "forbidden", want: boardsync.ErrRateLimited, wantRate: true, retryAfter: "7s"},
		{name: "rate 403 secondary", status: 403, body: "You have exceeded a secondary rate limit.", want: boardsync.ErrRateLimited, wantRate: true},
		{name: "permission 403", status: 403, body: "Resource not accessible by integration", want: boardsync.ErrUnauthorized, permanent: true},
		{name: "auth 401", status: 401, body: "Bad credentials", want: boardsync.ErrUnauthorized, permanent: true},
		{name: "not found", status: 404, body: "Not Found", want: boardsync.ErrNotFound, permanent: true},
		{name: "unprocessable", status: 422, body: "Validation Failed", want: boardsync.ErrBadRequest, permanent: true},
		{name: "bad request", status: 400, body: "Bad Request", want: boardsync.ErrBadRequest, permanent: true},
		{name: "server 500", status: 500, body: "oops"},
		{name: "server 502", status: 502, body: "bad gateway"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				for name, value := range tc.header {
					w.Header().Set(name, value)
				}
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			p := writeProvider(t, server.URL, nil)
			_, err := p.Post(context.Background(), "github", prTarget(), boardsync.OutboundMessage{Markdown: "x"})
			if err == nil {
				t.Fatal("Post succeeded")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if boardsync.IsPermanent(err) != tc.permanent {
				t.Fatalf("permanent = %v, want %v (%v)", boardsync.IsPermanent(err), tc.permanent, err)
			}
			if tc.wantRate {
				var rate *github.RateLimitError
				if !errors.As(err, &rate) {
					t.Fatalf("err %v isn't a RateLimitError", err)
				}
				if tc.retryAfter != "" && rate.RetryAfter.String() != tc.retryAfter {
					t.Fatalf("retry after = %s, want %s", rate.RetryAfter, tc.retryAfter)
				}
			}
			if strings.Contains(err.Error(), tc.body) {
				t.Fatalf("error quotes the response body: %v", err)
			}
		})
	}
}

func TestTransportErrorRedactsToken(t *testing.T) {
	const token = "ghs_super_secret"
	p, err := github.New(github.Config{
		Key: "github", WebhookSecret: testSecret, Token: token,
		Repos: []string{"acme/www"}, WriteRepos: []string{"acme/www"},
		Doer: github.DoerFunc(func(*http.Request) (*http.Response, error) {
			return nil, fmt.Errorf("dial failed for %s", token)
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Post(context.Background(), "github", prTarget(), boardsync.OutboundMessage{Markdown: "x"})
	if err == nil {
		t.Fatal("Post succeeded")
	}
	if strings.Contains(err.Error(), token) {
		t.Fatalf("error leaks the token: %v", err)
	}
	if !strings.Contains(err.Error(), boardsync.Redacted) {
		t.Fatalf("error wasn't redacted: %v", err)
	}
}

func TestMarkerDisabledStillSkipsByBotID(t *testing.T) {
	p := providerWith(t, func(c *github.Config) {
		c.BotUserID = 4242
		c.DisableMarker = true
	})
	body := []byte(strings.ReplaceAll(string(fixture(t, "pr_comment.json")),
		`"login": "ada", "id": 1001, "type": "User"`,
		`"login": "keel-board[bot]", "id": 4242, "type": "Bot"`))
	if intake, err := receiveRequest(t, p, "issue_comment", "no-marker", body); err != nil || len(intake.Records) != 0 {
		t.Fatalf("bot comment without marker = %+v, %v", intake.Records, err)
	}
}
