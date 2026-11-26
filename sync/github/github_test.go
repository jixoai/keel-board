package github_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/github"
)

const testSecret = "github-webhook-secret"

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func request(t *testing.T, event, delivery string, body []byte) *boardsync.WebhookRequest {
	t.Helper()
	header := http.Header{}
	header.Set(github.EventHeader, event)
	header.Set(github.DeliveryHeader, delivery)
	header.Set(github.SignatureHeader, github.Sign([]byte(testSecret), body))
	header.Set("Content-Type", "application/json")
	return &boardsync.WebhookRequest{
		Method: http.MethodPost,
		Path:   "/webhooks/github",
		Header: header,
		Body:   body,
	}
}

func newProvider(t *testing.T, write bool) *github.Provider {
	t.Helper()
	cfg := github.Config{
		Key:           "github",
		WebhookSecret: testSecret,
		Token:         "ghs_test_token",
		BotUserID:     4242,
		BotLogin:      "keel-board[bot]",
		Repos:         []string{"acme/www"},
	}
	if write {
		cfg.WriteRepos = []string{"acme/www"}
	}
	p, err := github.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func receive(t *testing.T, p *github.Provider, event string, body []byte) []boardsync.Record {
	t.Helper()
	intake, err := p.Receive(context.Background(), "github", request(t, event, "delivery-1", body))
	if err != nil {
		t.Fatalf("Receive(%s): %v", event, err)
	}
	return intake.Records
}

func TestSignature(t *testing.T) {
	body := []byte(`{"action":"opened"}`)
	signature := github.Sign([]byte(testSecret), body)
	if err := github.VerifySignature([]byte(testSecret), signature, body); err != nil {
		t.Fatalf("valid: %v", err)
	}
	mac := hmac.New(sha256.New, []byte(testSecret))
	mac.Write(body)
	want := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	if signature != want {
		t.Fatalf("Sign = %q, want %q", signature, want)
	}
	if err := github.VerifySignature([]byte("wrong"), signature, body); !errors.Is(err, boardsync.ErrUnauthorized) {
		t.Fatalf("wrong secret = %v", err)
	}
	if err := github.VerifySignature([]byte(testSecret), "sha1=deadbeef", body); !errors.Is(err, boardsync.ErrUnauthorized) {
		t.Fatalf("sha1 = %v", err)
	}
	if err := github.VerifySignature(nil, signature, body); !errors.Is(err, boardsync.ErrUnauthorized) {
		t.Fatalf("missing secret = %v", err)
	}
}

func TestReceiveIssueComment(t *testing.T) {
	records := receive(t, newProvider(t, false), "issue_comment", fixture(t, "issue_comment.json"))
	if len(records) != 1 {
		t.Fatalf("records = %d", len(records))
	}
	event := records[0].Event
	if event.Kind != boardsync.KindMessage || event.Type != boardsync.TypeComment {
		t.Fatalf("event = %+v", event)
	}
	if event.Channel != "acme/www:issues" || event.Thread != "acme/www:issue:45" || event.Subthread != "" {
		t.Fatalf("location = %s / %s / %s", event.Channel, event.Thread, event.Subthread)
	}
	if event.Message != "9001" || event.Text != "confirmed on staging" {
		t.Fatalf("message = %s / %q", event.Message, event.Text)
	}
	if event.Author.ID != "github:user:1002" || event.Author.Handle != "julian" {
		t.Fatalf("author = %+v", event.Author)
	}
	if err := event.Validate(); err != nil {
		t.Fatalf("event: %v", err)
	}
}

func TestReceivePRComment(t *testing.T) {
	event := receive(t, newProvider(t, false), "issue_comment", fixture(t, "pr_comment.json"))[0].Event
	if event.Channel != "acme/www:pulls" || event.Thread != "acme/www:123" {
		t.Fatalf("location = %s / %s", event.Channel, event.Thread)
	}
}

func TestReceiveReviewComment(t *testing.T) {
	event := receive(t, newProvider(t, false), "pull_request_review_comment", fixture(t, "review_comment.json"))[0].Event
	if event.Thread != "acme/www:123" || event.Subthread != "acme/www:123:rc:300" {
		t.Fatalf("thread = %s, subthread = %s", event.Thread, event.Subthread)
	}
	if event.Context != "src/auth.ts:42" {
		t.Fatalf("context = %q", event.Context)
	}
	if event.Message != "345" {
		t.Fatalf("message = %s", event.Message)
	}
}

func TestReceivePROpened(t *testing.T) {
	record := receive(t, newProvider(t, false), "pull_request", fixture(t, "pr_opened.json"))[0]
	event := record.Event
	if event.Kind != boardsync.KindLifecycle || event.Type != "pr.opened" {
		t.Fatalf("event = %+v", event)
	}
	if event.Thread != "acme/www:123" || event.Channel != "acme/www:pulls" {
		t.Fatalf("location = %s / %s", event.Channel, event.Thread)
	}
	lc := event.Lifecycle
	if lc == nil || !lc.CreateRoot || lc.Reply != "" {
		t.Fatalf("lifecycle = %+v", lc)
	}
	if lc.Labels["pr"] != "acme/www#123" || lc.Labels["state"] != "open" || lc.Labels["head"] != "a1b2c3d4e5f6" {
		t.Fatalf("labels = %+v", lc.Labels)
	}
	if lc.Labels["tag.bug"] != "bug" || lc.Labels["tag.good-first-issue"] != "good first issue" || lc.Labels["assignee.julian"] != "julian" {
		t.Fatalf("tag labels = %+v", lc.Labels)
	}
	owns := strings.Join(lc.Owns, ",")
	if !strings.Contains(owns, "tag.") || !strings.Contains(owns, "assignee.") {
		t.Fatalf("owns = %v", lc.Owns)
	}
	if !strings.HasPrefix(record.Dedupe, "lifecycle:github:") {
		t.Fatalf("dedupe = %q", record.Dedupe)
	}
}

func TestReceivePRClosed(t *testing.T) {
	event := receive(t, newProvider(t, false), "pull_request", fixture(t, "pr_closed.json"))[0].Event
	if event.Type != "pr.closed" || event.Lifecycle.Reply != "merged by julian" {
		t.Fatalf("event = %+v", event.Lifecycle)
	}
	if event.Lifecycle.Labels["state"] != "merged" {
		t.Fatalf("labels = %+v", event.Lifecycle.Labels)
	}
}

func TestReceiveReviewApproved(t *testing.T) {
	event := receive(t, newProvider(t, false), "pull_request_review", fixture(t, "review_approved.json"))[0].Event
	if event.Type != "review.submitted" {
		t.Fatalf("type = %s", event.Type)
	}
	if event.Lifecycle.Labels["review"] != "approved" || !strings.HasPrefix(event.Lifecycle.Reply, "approved by julian") {
		t.Fatalf("lifecycle = %+v", event.Lifecycle)
	}
	if !strings.Contains(event.Lifecycle.Reply, "Looks good.") {
		t.Fatalf("reply = %q", event.Lifecycle.Reply)
	}
}

func TestReceiveWorkflowRun(t *testing.T) {
	event := receive(t, newProvider(t, false), "workflow_run", fixture(t, "workflow_run.json"))[0].Event
	if event.Type != "ci.failed" || event.Thread != "acme/www:123" {
		t.Fatalf("event = %+v", event)
	}
	if event.Lifecycle.Labels["ci.1001-123.check"] != "failure" {
		t.Fatalf("labels = %+v", event.Lifecycle.Labels)
	}
	if !strings.Contains(event.Lifecycle.Reply, "check failure on www#123") {
		t.Fatalf("reply = %q", event.Lifecycle.Reply)
	}
	if event.Author.ID != "github:user:41898282" || !event.Author.Bot {
		t.Fatalf("author = %+v", event.Author)
	}
}

func TestRepoAllowlist(t *testing.T) {
	body := []byte(strings.ReplaceAll(string(fixture(t, "issue_comment.json")), "acme/www", "other/repo"))
	if records := receive(t, newProvider(t, false), "issue_comment", body); len(records) != 0 {
		t.Fatalf("records = %+v", records)
	}
}

func TestOwnCommentSkipped(t *testing.T) {
	if records := receive(t, newProvider(t, false), "issue_comment", fixture(t, "own_comment.json")); len(records) != 0 {
		t.Fatalf("marker comment imported: %+v", records)
	}
}

func TestPing(t *testing.T) {
	if records := receive(t, newProvider(t, false), "ping", fixture(t, "ping.json")); len(records) != 0 {
		t.Fatalf("records = %+v", records)
	}
}

func TestResolveTarget(t *testing.T) {
	p := newProvider(t, false)
	cases := []struct {
		target  string
		channel string
		thread  string
		kind    boardsync.TargetKind
	}{
		{"acme/www", "acme/www:pulls", "", boardsync.TargetChannel},
		{"acme/www:123", "acme/www:pulls", "acme/www:123", boardsync.TargetThread},
		{"acme/www:issue:45", "acme/www:issues", "acme/www:issue:45", boardsync.TargetThread},
	}
	for _, tc := range cases {
		got, err := p.ResolveTarget(context.Background(), "github", tc.target)
		if err != nil {
			t.Fatalf("%s: %v", tc.target, err)
		}
		if got.Channel != tc.channel || got.Thread != tc.thread || got.Kind != tc.kind {
			t.Fatalf("%s = %+v", tc.target, got)
		}
	}
	if _, err := p.ResolveTarget(context.Background(), "github", "acme/www:123:rc:345"); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("review target = %v", err)
	}
}

func TestPostComment(t *testing.T) {
	var path, body string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		data, _ := io.ReadAll(r.Body)
		body = string(data)
		if r.Header.Get("Authorization") != "Bearer ghs_test_token" {
			t.Fatalf("authorization = %q", r.Header.Get("Authorization"))
		}
		_, _ = w.Write([]byte(`{"id":9300,"html_url":"https://github.com/acme/www/pull/123#issuecomment-9300"}`))
	}))
	defer server.Close()
	cfg := github.Config{
		Key: "github", WebhookSecret: testSecret, Token: "ghs_test_token",
		Repos: []string{"acme/www"}, WriteRepos: []string{"acme/www"},
		APIBaseURL: server.URL,
	}
	p, err := github.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	target := boardsync.ResolvedTarget{Key: "github", Platform: "github", Channel: "acme/www:pulls", Thread: "acme/www:123", Kind: boardsync.TargetThread}
	posted, err := p.Post(context.Background(), "github", target, boardsync.OutboundMessage{Markdown: "**Ada**: ship it", BoardMessage: 812})
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	if path != "/repos/acme/www/issues/123/comments" {
		t.Fatalf("path = %q", path)
	}
	if !strings.Contains(body, "**Ada**: ship it") || !strings.Contains(body, "keel-board:sync key=github message=812") {
		t.Fatalf("body = %q", body)
	}
	if posted.Message != "9300" || posted.Thread != "acme/www:123" {
		t.Fatalf("posted = %+v", posted)
	}
}

func TestPostReviewReply(t *testing.T) {
	var path string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		_, _ = w.Write([]byte(`{"id":9301}`))
	}))
	defer server.Close()
	cfg := github.Config{
		Key: "github", WebhookSecret: testSecret, Token: "t",
		Repos: []string{"acme/www"}, WriteRepos: []string{"acme/www"}, APIBaseURL: server.URL,
	}
	p, err := github.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	target := boardsync.ResolvedTarget{
		Key: "github", Platform: "github", Channel: "acme/www:pulls",
		Thread: "acme/www:123", Subthread: "acme/www:123:rc:300", Kind: boardsync.TargetThread,
	}
	if _, err := p.Post(context.Background(), "github", target, boardsync.OutboundMessage{Markdown: "fixed"}); err != nil {
		t.Fatal(err)
	}
	if path != "/repos/acme/www/pulls/123/comments/300/replies" {
		t.Fatalf("path = %q", path)
	}
}

func TestWritesDeniedByDefault(t *testing.T) {
	p := newProvider(t, false)
	target := boardsync.ResolvedTarget{Key: "github", Platform: "github", Channel: "acme/www:pulls", Thread: "acme/www:123", Kind: boardsync.TargetThread}
	if _, err := p.Post(context.Background(), "github", target, boardsync.OutboundMessage{Markdown: "x"}); !errors.Is(err, boardsync.ErrUnsupported) {
		t.Fatalf("Post = %v, want ErrUnsupported", err)
	}
}

func TestEditDeleteReact(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		_, _ = w.Write([]byte(`{"id":1}`))
	}))
	defer server.Close()
	cfg := github.Config{
		Key: "github", WebhookSecret: testSecret, Token: "t",
		Repos: []string{"acme/www"}, WriteRepos: []string{"acme/www"}, APIBaseURL: server.URL,
	}
	p, err := github.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	thread := boardsync.ThreadRef{Key: "github", Platform: "github", Channel: "acme/www:pulls", Thread: "acme/www:123"}
	ctx := context.Background()
	if err := p.Edit(ctx, "github", thread, "9001", boardsync.OutboundMessage{Markdown: "new"}); err != nil {
		t.Fatal(err)
	}
	if err := p.Delete(ctx, "github", thread, "9001"); err != nil {
		t.Fatal(err)
	}
	if err := p.React(ctx, "github", thread, "9001", "thumbsup", true); err != nil {
		t.Fatal(err)
	}
	if err := p.React(ctx, "github", thread, "root:1", "rocket", true); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"/repos/acme/www/issues/comments/9001",
		"/repos/acme/www/issues/comments/9001",
		"/repos/acme/www/issues/comments/9001/reactions",
		"/repos/acme/www/issues/123/reactions",
	}
	if len(paths) != len(want) {
		t.Fatalf("paths = %v", paths)
	}
	for i := range want {
		if paths[i] != want[i] {
			t.Fatalf("paths = %v, want %v", paths, want)
		}
	}
	if err := p.React(ctx, "github", thread, "9001", "thumbsup", false); !errors.Is(err, boardsync.ErrUnsupported) {
		t.Fatalf("reaction removal = %v", err)
	}
}

func TestFetchRoot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/acme/www/pulls/123" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{
			"number":123,"title":"checkout retry","body":"Retries.","html_url":"https://github.com/acme/www/pull/123",
			"state":"open","draft":false,"merged":false,
			"user":{"login":"ada","id":1001,"type":"User"},
			"labels":[{"name":"bug"}],"assignees":[{"login":"julian","id":1002}],
			"head":{"sha":"a1b2c3d4"}
		}`))
	}))
	defer server.Close()
	cfg := github.Config{
		Key: "github", WebhookSecret: testSecret, Token: "t",
		Repos: []string{"acme/www"}, APIBaseURL: server.URL,
	}
	p, err := github.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	draft, err := p.FetchRoot(context.Background(), "github", "acme/www:123")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(draft.Text, "PR #123: checkout retry") {
		t.Fatalf("text = %q", draft.Text)
	}
	if draft.Labels["pr"] != "acme/www#123" || draft.Labels["state"] != "open" {
		t.Fatalf("labels = %+v", draft.Labels)
	}
}

func TestLocateAndURL(t *testing.T) {
	p := newProvider(t, false)
	thread, message, ok := p.Locate("https://github.com/acme/www/pull/123#discussion_r345")
	if !ok || thread != "acme/www:123" || message != "345" {
		t.Fatalf("Locate = %q, %q, %v", thread, message, ok)
	}
	thread, message, ok = p.Locate("https://github.com/acme/www/issues/45#issuecomment-9001")
	if !ok || thread != "acme/www:issue:45" || message != "9001" {
		t.Fatalf("Locate issue = %q, %q, %v", thread, message, ok)
	}
	link, err := p.URL("acme/www:123", "345")
	if err != nil || link != "https://github.com/acme/www/pull/123#issuecomment-345" {
		t.Fatalf("URL = %q, %v", link, err)
	}
}
