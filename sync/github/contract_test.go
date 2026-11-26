package github_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/fake"
	"github.com/jixoai/keel-board/sync/github"
)

// fakeGitHub is a loopback GitHub REST API. It performs no network I/O beyond
// httptest and needs no credential.
type fakeGitHub struct {
	server *httptest.Server

	mu              sync.Mutex
	nextID          int64
	bodies          map[int64]string
	comments        int
	reactions       int
	reviewEdits     int
	reviewDeletes   int
	reviewReactions int
}

func newFakeGitHub(t *testing.T) *fakeGitHub {
	t.Helper()
	f := &fakeGitHub{nextID: 9300, bodies: make(map[int64]string)}
	f.server = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeGitHub) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		switch r.URL.Path {
		case "/user":
			_, _ = w.Write([]byte(`{"login":"keel-board[bot]","id":4242,"type":"Bot"}`))
			return
		case "/repos/acme/www/pulls/123":
			_, _ = w.Write([]byte(`{
				"number":123,"title":"checkout retry","body":"Retries.","html_url":"https://github.com/acme/www/pull/123",
				"state":"open","draft":false,"merged":false,
				"user":{"login":"ada","id":1001,"type":"User"},
				"labels":[{"name":"bug"}],"assignees":[{"login":"julian","id":1002}],
				"head":{"sha":"a1b2c3d4e5f6"}
			}`))
			return
		case "/repos/acme/www/issues/45":
			_, _ = w.Write([]byte(`{
				"number":45,"title":"blank export page","body":"Blank.","html_url":"https://github.com/acme/www/issues/45",
				"state":"open","user":{"login":"ada","id":1001,"type":"User"},
				"labels":[{"name":"bug"}],"assignees":[]
			}`))
			return
		}
	}

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	// repos/{owner}/{repo}/...
	if len(parts) < 5 || parts[0] != "repos" {
		http.NotFound(w, r)
		return
	}
	kind, id := parts[3], parts[4]

	switch {
	case r.Method == http.MethodPost && kind == "issues" && len(parts) == 6 && parts[5] == "comments":
		f.createComment(w, r)
		return
	case r.Method == http.MethodPost && kind == "pulls" && len(parts) == 8 && parts[5] == "comments" && parts[7] == "replies":
		f.createComment(w, r)
		return
	case (r.Method == http.MethodPatch || r.Method == http.MethodDelete) && kind == "issues" && len(parts) == 6 && parts[4] == "comments":
		f.changeComment(w, r, false)
		return
	case (r.Method == http.MethodPatch || r.Method == http.MethodDelete) && kind == "pulls" && len(parts) == 6 && parts[4] == "comments":
		f.changeComment(w, r, true)
		return
	case r.Method == http.MethodPost && len(parts) >= 6 && parts[len(parts)-1] == "reactions":
		f.mu.Lock()
		f.reactions++
		if kind == "pulls" && len(parts) >= 6 && parts[4] == "comments" {
			f.reviewReactions++
		}
		f.mu.Unlock()
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":1,"content":"+1"}`))
		return
	}
	if id != "" {
		http.NotFound(w, r)
		return
	}
	http.NotFound(w, r)
}

func (f *fakeGitHub) createComment(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Body string `json:"body"`
	}
	data, _ := io.ReadAll(r.Body)
	if err := json.Unmarshal(data, &body); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	id := f.nextID
	f.nextID++
	f.bodies[id] = body.Body
	f.comments++
	f.mu.Unlock()
	_, _ = fmt.Fprintf(w, `{"id":%d,"html_url":"https://github.com/acme/www/pull/123#issuecomment-%d"}`, id, id)
}

func (f *fakeGitHub) changeComment(w http.ResponseWriter, r *http.Request, review bool) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	id, _ := strconv.ParseInt(parts[5], 10, 64)
	switch r.Method {
	case http.MethodPatch:
		var body struct {
			Body string `json:"body"`
		}
		data, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(data, &body)
		f.mu.Lock()
		f.bodies[id] = body.Body
		if review {
			f.reviewEdits++
		}
		f.mu.Unlock()
		_, _ = fmt.Fprintf(w, `{"id":%d}`, id)
	case http.MethodDelete:
		f.mu.Lock()
		delete(f.bodies, id)
		if review {
			f.reviewDeletes++
		}
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}
}

func (f *fakeGitHub) bodyOf(id int64) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.bodies[id]
}

func (f *fakeGitHub) counts() (comments, reactions int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.comments, f.reactions
}

func (f *fakeGitHub) reviewChanges() (edits, deletes, reactions int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reviewEdits, f.reviewDeletes, f.reviewReactions
}

func contractProvider(t *testing.T, serverURL string) *github.Provider {
	t.Helper()
	return providerWith(t, func(c *github.Config) {
		c.APIBaseURL = serverURL
		c.WriteRepos = []string{"acme/www"}
		c.BotUserID = 4242
		c.DetectViewer = true
	})
}

func TestGitHubContractMatrix(t *testing.T) {
	p := newProvider(t, false)
	cases := []struct {
		event    string
		file     string
		kind     boardsync.EventKind
		typ      string
		channel  string
		thread   string
		sub      string
		message  string
		labels   map[string]string
		unset    []string
		create   bool
		reply    string
		authorID string
	}{
		{event: "issue_comment", file: "issue_comment.json", kind: boardsync.KindMessage, typ: boardsync.TypeComment, channel: "acme/www:issues", thread: "acme/www:issue:45", message: "9001", authorID: "github:user:1002"},
		{event: "issue_comment", file: "pr_comment.json", kind: boardsync.KindMessage, typ: boardsync.TypeComment, channel: "acme/www:pulls", thread: "acme/www:123", message: "9100", authorID: "github:user:1001"},
		{event: "issue_comment", file: "issue_comment_edited.json", kind: boardsync.KindEdited, typ: boardsync.TypeMessageEdited, channel: "acme/www:issues", thread: "acme/www:issue:45", message: "9001", authorID: "github:user:1002"},
		{event: "issue_comment", file: "issue_comment_deleted.json", kind: boardsync.KindDeleted, typ: boardsync.TypeMessageDeleted, channel: "acme/www:issues", thread: "acme/www:issue:45", message: "9001"},
		{event: "pull_request_review_comment", file: "review_comment_root.json", kind: boardsync.KindMessage, typ: boardsync.TypeComment, channel: "acme/www:pulls", thread: "acme/www:123", sub: "acme/www:123:rc:345", message: "345", authorID: "github:user:1001"},
		{event: "pull_request_review_comment", file: "review_comment.json", kind: boardsync.KindMessage, typ: boardsync.TypeComment, channel: "acme/www:pulls", thread: "acme/www:123", sub: "acme/www:123:rc:300", message: "345", authorID: "github:user:1001"},
		{event: "pull_request", file: "pr_opened.json", kind: boardsync.KindLifecycle, typ: "pr.opened", channel: "acme/www:pulls", thread: "acme/www:123", labels: map[string]string{"state": "open", "head": "a1b2c3d4e5f6", "tag.bug": "bug", "assignee.julian": "julian"}, create: true, authorID: "github:user:1001"},
		{event: "pull_request", file: "pr_synchronize.json", kind: boardsync.KindLifecycle, typ: "pr.synchronized", channel: "acme/www:pulls", thread: "acme/www:123", labels: map[string]string{"head": "b2c3d4e5f6a7"}},
		{event: "pull_request", file: "pr_reopened.json", kind: boardsync.KindLifecycle, typ: "pr.reopened", channel: "acme/www:pulls", thread: "acme/www:123", labels: map[string]string{"state": "open"}, create: true, reply: "reopened by julian", authorID: "github:user:1002"},
		{event: "pull_request", file: "pr_closed.json", kind: boardsync.KindLifecycle, typ: "pr.closed", channel: "acme/www:pulls", thread: "acme/www:123", labels: map[string]string{"state": "merged"}, reply: "merged by julian", authorID: "github:user:1002"},
		{event: "issues", file: "issue_opened.json", kind: boardsync.KindLifecycle, typ: "issue.opened", channel: "acme/www:issues", thread: "acme/www:issue:45", labels: map[string]string{"state": "open", "issue": "acme/www#45"}, create: true, authorID: "github:user:1001"},
		{event: "issues", file: "issue_closed.json", kind: boardsync.KindLifecycle, typ: "issue.closed", channel: "acme/www:issues", thread: "acme/www:issue:45", labels: map[string]string{"state": "closed"}, reply: "closed by julian", authorID: "github:user:1002"},
		{event: "issues", file: "issue_reopened.json", kind: boardsync.KindLifecycle, typ: "issue.reopened", channel: "acme/www:issues", thread: "acme/www:issue:45", labels: map[string]string{"state": "open"}, create: true, reply: "reopened by julian", authorID: "github:user:1002"},
		{event: "pull_request_review", file: "review_approved.json", kind: boardsync.KindLifecycle, typ: "review.submitted", channel: "acme/www:pulls", thread: "acme/www:123", labels: map[string]string{"review": "approved"}, reply: "approved by julian\n\nLooks good.", authorID: "github:user:1002"},
		{event: "pull_request_review", file: "review_dismissed.json", kind: boardsync.KindLifecycle, typ: "review.dismissed", channel: "acme/www:pulls", thread: "acme/www:123", unset: []string{"review"}, authorID: "github:user:1002"},
		{event: "workflow_run", file: "workflow_run.json", kind: boardsync.KindLifecycle, typ: "ci.failed", channel: "acme/www:pulls", thread: "acme/www:123", labels: map[string]string{"ci.1001-123.check": "failure"}, reply: "check failure on www#123 at a1b2c3d: https://github.com/acme/www/actions/runs/5001", authorID: "github:user:41898282"},
		{event: "workflow_run", file: "workflow_success.json", kind: boardsync.KindLifecycle, typ: "ci.passed", channel: "acme/www:pulls", thread: "acme/www:123", unset: []string{"ci.1001-123.check"}, authorID: "github:user:41898282"},
	}
	for _, tc := range cases {
		t.Run(tc.file+"/"+tc.typ, func(t *testing.T) {
			records := receive(t, p, tc.event, fixture(t, tc.file))
			if len(records) != 1 {
				t.Fatalf("records = %d", len(records))
			}
			event := records[0].Event
			if event.Kind != tc.kind || event.Type != tc.typ {
				t.Fatalf("kind/type = %s/%s, want %s/%s", event.Kind, event.Type, tc.kind, tc.typ)
			}
			if event.Channel != tc.channel || event.Thread != tc.thread || event.Subthread != tc.sub {
				t.Fatalf("location = %s / %s / %s", event.Channel, event.Thread, event.Subthread)
			}
			if tc.message != "" && event.Message != tc.message {
				t.Fatalf("message = %q, want %q", event.Message, tc.message)
			}
			if tc.authorID != "" && event.Author.ID != tc.authorID {
				t.Fatalf("author = %+v, want %s", event.Author, tc.authorID)
			}
			if tc.kind == boardsync.KindLifecycle {
				lc := event.Lifecycle
				if lc == nil {
					t.Fatal("lifecycle is nil")
				}
				if lc.CreateRoot != tc.create || lc.Reply != tc.reply {
					t.Fatalf("lifecycle = %+v", lc)
				}
				for key, want := range tc.labels {
					if lc.Labels[key] != want {
						t.Fatalf("label %s = %q, want %q (%+v)", key, lc.Labels[key], want, lc.Labels)
					}
				}
				for _, key := range tc.unset {
					if !contains(lc.Unset, key) {
						t.Fatalf("unset = %v, want %s", lc.Unset, key)
					}
				}
			}
			if err := event.Validate(); err != nil {
				t.Fatalf("event validate: %v", err)
			}
		})
	}
}

func TestEndToEndExistingThreadAndEchoSuppression(t *testing.T) {
	f := newFakeGitHub(t)
	p := contractProvider(t, f.server.URL)
	entry := boardsync.Entry{Tenant: "t1", Key: "github", Platform: "github", Provider: p, Sync: boardsync.Mirror("prs")}
	registry := boardsync.NewRegistry()
	if err := registry.Add(entry); err != nil {
		t.Fatal(err)
	}
	// The repo's pulls channel isn't mapped; the board already syncs this PR,
	// so its comment still arrives.
	gate := fake.NewGate(nil, []string{"github:acme/www:123"})
	inbox := boardsync.NewMemoryInbox(boardsync.InboundRetry, 0)
	ctx := context.Background()
	response, err := registry.HandleWebhook(ctx, "t1", "github", request(t, "issue_comment", "in-1", fixture(t, "pr_comment.json")), gate, inbox)
	if err != nil || response.Status != 200 {
		t.Fatalf("HandleWebhook = %+v, %v", response, err)
	}
	if pending := inbox.Pending("github"); pending != 1 {
		t.Fatalf("pending = %d", pending)
	}
	items, err := inbox.Lease(ctx, "github", 10)
	if err != nil || len(items) != 1 {
		t.Fatalf("lease = %+v, %v", items, err)
	}
	host := fake.NewHost()
	runner := boardsync.NewRunner(host)
	if err := runner.Inbound(ctx, entry, items[0].Event); err != nil {
		t.Fatalf("Inbound: %v", err)
	}
	if imports := host.Imports(); len(imports) != 1 || imports[0].Channel != "prs" {
		t.Fatalf("imports = %+v", imports)
	}
	if err := inbox.Ack(ctx, "github", items[0].ID, items[0].Lease, nil); err != nil {
		t.Fatal(err)
	}

	// Board reply goes out as a comment with the hidden marker.
	message := boardsync.Message{ID: 812, Thread: 812, Channel: "prs", Text: "ship it", Actor: "local:user:ada", ActorName: "Ada"}
	target := prTarget()
	receipt, err := runner.Outbound(ctx, entry, boardsync.BoardEvent{Type: boardsync.BoardMessagePosted, Thread: 812}, message, &target, "")
	if err != nil {
		t.Fatalf("Outbound: %v", err)
	}
	if receipt.Message != "9300" || receipt.Thread.External() != "github:acme/www:123" {
		t.Fatalf("receipt = %+v", receipt)
	}
	if records := host.Records(); len(records) != 1 || records[0].Type != boardsync.BoardMessageSynced {
		t.Fatalf("records = %+v", records)
	}
	posted := f.bodyOf(9300)
	if !strings.Contains(posted, "**Ada**: ship it") || !strings.Contains(posted, "keel-board:sync key=github message=812 thread=github:acme/www:123") {
		t.Fatalf("posted body = %q", posted)
	}

	// The platform echoes the comment back. The marker drops it even though the
	// author isn't the app's bot account.
	echo := commentWebhook(t, 9300, posted, 1001, "ada", true, "echo-1")
	if response, err := registry.HandleWebhook(ctx, "t1", "github", echo, gate, inbox); err != nil || response.Status != 200 {
		t.Fatalf("echo = %+v, %v", response, err)
	}
	if pending := inbox.Pending("github"); pending != 0 {
		t.Fatalf("echo imported; pending = %d", pending)
	}

	// A bot comment with no marker is dropped by the bot identity.
	botEcho := commentWebhook(t, 9400, "no marker here", 4242, "keel-board[bot]", true, "echo-2")
	if response, err := registry.HandleWebhook(ctx, "t1", "github", botEcho, gate, inbox); err != nil || response.Status != 200 {
		t.Fatalf("bot echo = %+v, %v", response, err)
	}
	if pending := inbox.Pending("github"); pending != 0 {
		t.Fatalf("bot echo imported; pending = %d", pending)
	}
	comments, _ := f.counts()
	if comments != 1 {
		t.Fatalf("comments posted = %d, want one", comments)
	}
}

func TestEndToEndIssueReviewAndCI(t *testing.T) {
	f := newFakeGitHub(t)
	p := contractProvider(t, f.server.URL)
	entry := boardsync.Entry{Tenant: "t1", Key: "github", Platform: "github", Provider: p, Sync: boardsync.Mirror("board")}
	registry := boardsync.NewRegistry()
	if err := registry.Add(entry); err != nil {
		t.Fatal(err)
	}
	gate := fake.NewGate(nil, []string{"github:acme/www:issue:45", "github:acme/www:123"})
	inbox := boardsync.NewMemoryInbox(boardsync.InboundRetry, 0)
	ctx := context.Background()

	// An issue comment imports on the existing issue thread.
	response, err := registry.HandleWebhook(ctx, "t1", "github", request(t, "issue_comment", "issue-1", fixture(t, "issue_comment.json")), gate, inbox)
	if err != nil || response.Status != 200 {
		t.Fatalf("issue comment = %+v, %v", response, err)
	}
	// A review reply imports under the PR's review sub-thread.
	if response, err := registry.HandleWebhook(ctx, "t1", "github", request(t, "pull_request_review_comment", "review-1", fixture(t, "review_comment.json")), gate, inbox); err != nil || response.Status != 200 {
		t.Fatalf("review comment = %+v, %v", response, err)
	}
	// CI failure imports as a lifecycle record on the PR thread.
	if response, err := registry.HandleWebhook(ctx, "t1", "github", request(t, "workflow_run", "ci-1", fixture(t, "workflow_run.json")), gate, inbox); err != nil || response.Status != 200 {
		t.Fatalf("ci = %+v, %v", response, err)
	}
	if pending := inbox.Pending("github"); pending != 3 {
		t.Fatalf("pending = %d, want three lanes", pending)
	}

	host := fake.NewHost()
	runner := boardsync.NewRunner(host)
	items, err := inbox.Lease(ctx, "github", 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range items {
		if err := runner.Inbound(ctx, entry, item.Event); err != nil {
			t.Fatalf("Inbound(%s): %v", item.Event.Type, err)
		}
		if err := inbox.Ack(ctx, "github", item.ID, item.Lease, nil); err != nil {
			t.Fatal(err)
		}
	}
	if imports := host.Imports(); len(imports) != 3 {
		t.Fatalf("imports = %d, want three", len(imports))
	}

	// Reply to an issue and to a review thread.
	issueTarget := boardsync.ResolvedTarget{Key: "github", Platform: "github", Channel: "acme/www:issues", Thread: "acme/www:issue:45", Kind: boardsync.TargetThread}
	message := boardsync.Message{ID: 900, Thread: 900, Channel: "board", Text: "on it", Actor: "local:user:ada", ActorName: "Ada"}
	if _, err := runner.Outbound(ctx, entry, boardsync.BoardEvent{Type: boardsync.BoardMessagePosted, Thread: 900}, message, &issueTarget, ""); err != nil {
		t.Fatalf("issue reply: %v", err)
	}
	reviewTarget := prTarget()
	reviewTarget.Subthread = "acme/www:123:rc:300"
	message = boardsync.Message{ID: 901, Thread: 901, Channel: "board", Text: "fixed", Actor: "local:user:ada", ActorName: "Ada"}
	receipt, err := runner.Outbound(ctx, entry, boardsync.BoardEvent{Type: boardsync.BoardMessagePosted, Thread: 901}, message, &reviewTarget, "")
	if err != nil {
		t.Fatalf("review reply: %v", err)
	}
	comments, _ := f.counts()
	if comments != 2 {
		t.Fatalf("comments posted = %d, want two", comments)
	}
	if body := f.bodyOf(9300); !strings.Contains(body, "on it") {
		t.Fatalf("issue reply body = %q", body)
	}
	if body := f.bodyOf(9301); !strings.Contains(body, "fixed") || !strings.Contains(body, "thread=github:acme/www:123") {
		t.Fatalf("review reply body = %q", body)
	}
	// A host rebuilds the copy target from the receipt. The sub-thread must
	// survive, or edit/delete/react would hit the issue-comment endpoints.
	if receipt.Thread.Thread != "acme/www:123" || receipt.Thread.Subthread != "acme/www:123:rc:300" {
		t.Fatalf("review receipt lost its sub-thread: %+v", receipt.Thread)
	}
	copyTarget := boardsync.ResolvedTarget{
		Key: receipt.Thread.Key, Platform: receipt.Thread.Platform, Channel: receipt.Thread.Channel,
		Thread: receipt.Thread.Thread, Subthread: receipt.Thread.Subthread, Kind: boardsync.TargetThread,
	}
	edited := boardsync.Message{ID: 901, Thread: 901, Channel: "board", Text: "fixed again", Actor: "local:user:ada", ActorName: "Ada"}
	if _, err := runner.Outbound(ctx, entry, boardsync.BoardEvent{Type: boardsync.BoardMessageEdited, Thread: 901}, edited, &copyTarget, receipt.Message); err != nil {
		t.Fatalf("review edit: %v", err)
	}
	if _, err := runner.Outbound(ctx, entry, boardsync.BoardEvent{Type: boardsync.BoardReactionAdded, Thread: 901, Emoji: "rocket"}, edited, &copyTarget, receipt.Message); err != nil {
		t.Fatalf("review reaction: %v", err)
	}
	if _, err := runner.Outbound(ctx, entry, boardsync.BoardEvent{Type: boardsync.BoardMessageDeleted, Thread: 901}, edited, &copyTarget, receipt.Message); err != nil {
		t.Fatalf("review delete: %v", err)
	}
	edits, deletes, reactions := f.reviewChanges()
	if edits != 1 || deletes != 1 || reactions != 1 {
		t.Fatalf("review endpoints = edit %d, delete %d, reaction %d; want 1/1/1", edits, deletes, reactions)
	}
}

func TestBoardRootDoesNotCreateGitHubIssue(t *testing.T) {
	f := newFakeGitHub(t)
	p := contractProvider(t, f.server.URL)
	if p.Capabilities().StartThreads {
		t.Fatal("github must not claim startThreads")
	}
	entry := boardsync.Entry{Tenant: "t1", Key: "github", Platform: "github", Provider: p, Sync: boardsync.Mirror("prs")}
	runner := boardsync.NewRunner(fake.NewHost())
	message := boardsync.Message{ID: 7, Thread: 7, Channel: "prs", Text: "new root", Actor: "local:user:ada"}
	err := runner.OfferRoot(context.Background(), entry, boardsync.BoardEvent{Type: boardsync.BoardMessagePosted, Thread: 7}, message, "acme/www:pulls")
	if err == nil {
		t.Fatal("OfferRoot should refuse: GitHub cannot start a thread")
	}
	if !strings.Contains(err.Error(), "can't start threads") {
		t.Fatalf("error = %v", err)
	}
	if comments, reactions := f.counts(); comments != 0 || reactions != 0 {
		t.Fatalf("github received %d comments and %d reactions", comments, reactions)
	}
	if target := prTarget(); target.Kind == boardsync.TargetThread {
		target.Kind = boardsync.TargetChannel
		target.Thread = ""
		if _, err := p.Post(context.Background(), "github", target, boardsync.OutboundMessage{Markdown: "root"}); err == nil {
			t.Fatal("Post on a channel target succeeded")
		}
	}
}

func commentWebhook(t *testing.T, id int64, body string, userID int64, login string, isPR bool, delivery string) *boardsync.WebhookRequest {
	t.Helper()
	issue := map[string]any{
		"number": 123, "title": "checkout retry", "html_url": "https://github.com/acme/www/pull/123",
		"state": "open", "labels": []any{}, "assignees": []any{},
	}
	if isPR {
		issue["number"] = 123
		issue["pull_request"] = map[string]any{}
	}
	payload := map[string]any{
		"action": "created",
		"issue":  issue,
		"comment": map[string]any{
			"id": id, "body": body,
			"user":       map[string]any{"login": login, "id": userID, "type": "User"},
			"html_url":   "https://github.com/acme/www/pull/123#issuecomment-" + strconv.FormatInt(id, 10),
			"created_at": "2026-01-02T03:04:05Z",
			"updated_at": "2026-01-02T03:04:05Z",
		},
		"repository": map[string]any{"id": 1001, "full_name": "acme/www", "name": "www", "private": false, "visibility": "public", "owner": map[string]any{"login": "acme"}},
		"sender":     map[string]any{"login": login, "id": userID, "type": "User"},
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return request(t, "issue_comment", delivery, data)
}
