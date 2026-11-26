package github_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/fake"
	"github.com/jixoai/keel-board/sync/github"
)

func providerWith(t *testing.T, mutate func(*github.Config)) *github.Provider {
	t.Helper()
	cfg := github.Config{
		Key:           "github",
		WebhookSecret: testSecret,
		Token:         "ghs_test_token",
		Repos:         []string{"acme/www"},
	}
	if mutate != nil {
		mutate(&cfg)
	}
	p, err := github.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// withInstallation adds (or replaces) the GitHub App installation object.
func withInstallation(t *testing.T, body []byte, id int64) []byte {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	payload["installation"] = map[string]any{"id": id}
	out, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func receiveRequest(t *testing.T, p *github.Provider, event, delivery string, body []byte) (boardsync.Intake, error) {
	t.Helper()
	return p.Receive(context.Background(), "github", request(t, event, delivery, body))
}

func TestReceiveRejectsUnverifiedOversizedAndHeaderless(t *testing.T) {
	p := newProvider(t, false)
	body := fixture(t, "issue_comment.json")

	missing := request(t, "issue_comment", "d-1", body)
	delete(missing.Header, github.SignatureHeader)
	if _, err := p.Receive(context.Background(), "github", missing); !errors.Is(err, boardsync.ErrUnauthorized) {
		t.Fatalf("missing signature = %v", err)
	}

	sha1 := request(t, "issue_comment", "d-1", body)
	sha1.Header.Set(github.SignatureHeader, "sha1=deadbeef")
	if _, err := p.Receive(context.Background(), "github", sha1); !errors.Is(err, boardsync.ErrUnauthorized) {
		t.Fatalf("sha1 signature = %v", err)
	}

	noEvent := request(t, "issue_comment", "d-1", body)
	noEvent.Header.Del(github.EventHeader)
	if _, err := p.Receive(context.Background(), "github", noEvent); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("missing event header = %v", err)
	}

	small := providerWith(t, func(c *github.Config) { c.MaxBodyBytes = 64 })
	if _, err := small.Receive(context.Background(), "github", request(t, "issue_comment", "d-1", body)); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("oversized body = %v", err)
	}
}

func TestReceiveRequiresUsableDeliveryID(t *testing.T) {
	p := newProvider(t, false)
	body := fixture(t, "issue_comment.json")
	for _, delivery := range []string{"", "not a delivery", strings.Repeat("a", 129)} {
		if _, err := receiveRequest(t, p, "issue_comment", delivery, body); !errors.Is(err, boardsync.ErrBadRequest) {
			t.Fatalf("delivery %q = %v, want ErrBadRequest", delivery, err)
		}
	}
	if _, err := receiveRequest(t, p, "issue_comment", "9f6c1a4e-6a1b-4f0e-9d6a-2f6a1b4f0e9d", body); err != nil {
		t.Fatalf("valid delivery: %v", err)
	}
}

func TestDuplicateDeliveryImportsOnce(t *testing.T) {
	p := newProvider(t, false)
	entry := boardsync.Entry{Key: "github", Provider: p, Sync: boardsync.Into("prs")}
	registry := boardsync.NewRegistry()
	if err := registry.Add(entry); err != nil {
		t.Fatal(err)
	}
	inbox := boardsync.NewMemoryInbox(boardsync.InboundRetry, 0)
	gate := fake.NewGate([]string{"acme/www:pulls"}, nil)
	body := fixture(t, "pr_comment.json")
	ctx := context.Background()

	for attempt := 0; attempt < 2; attempt++ {
		response, err := registry.HandleWebhook(ctx, "github", request(t, "issue_comment", "same-delivery", body), gate, inbox)
		if err != nil || response.Status != 200 {
			t.Fatalf("attempt %d = %+v, %v", attempt, response, err)
		}
	}
	if pending := inbox.Pending("github"); pending != 1 {
		t.Fatalf("pending = %d, want one deduped event", pending)
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
	if err := inbox.Ack(ctx, "github", items[0].ID, items[0].Lease, nil); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if imports := host.Imports(); len(imports) != 1 {
		t.Fatalf("imports = %d, want one", len(imports))
	}
}

func TestUnknownRepoAndInstallationFailClosed(t *testing.T) {
	p := providerWith(t, func(c *github.Config) { c.InstallationID = 777 })
	base := fixture(t, "issue_comment.json")

	if records := receive(t, p, "issue_comment", base); len(records) != 0 {
		t.Fatalf("missing installation imported: %+v", records)
	}
	if records := receive(t, p, "issue_comment", withInstallation(t, base, 888)); len(records) != 0 {
		t.Fatalf("unknown installation imported: %+v", records)
	}
	records := receive(t, p, "issue_comment", withInstallation(t, base, 777))
	if len(records) != 1 {
		t.Fatalf("matching installation records = %d", len(records))
	}
	if records := receive(t, p, "issue_comment", []byte(strings.ReplaceAll(string(base), "acme/www", "other/repo"))); len(records) != 0 {
		t.Fatalf("unknown repo imported: %+v", records)
	}
	invalidRepo := []byte(strings.ReplaceAll(string(base), `"full_name": "acme/www"`, `"full_name": "../../etc/passwd"`))
	if records := receive(t, p, "issue_comment", invalidRepo); len(records) != 0 {
		t.Fatalf("invalid repo imported: %+v", records)
	}
}

func TestIssueLifecycle(t *testing.T) {
	p := newProvider(t, false)
	cases := []struct {
		file       string
		eventType  string
		createRoot bool
		state      string
		reply      string
	}{
		{"issue_opened.json", "issue.opened", true, "open", ""},
		{"issue_closed.json", "issue.closed", false, "closed", "closed by julian"},
		{"issue_reopened.json", "issue.reopened", true, "open", "reopened by julian"},
	}
	for _, tc := range cases {
		t.Run(tc.eventType, func(t *testing.T) {
			event := receive(t, p, "issues", fixture(t, tc.file))[0].Event
			if event.Kind != boardsync.KindLifecycle || event.Type != tc.eventType {
				t.Fatalf("event = %+v", event)
			}
			if event.Channel != "acme/www:issues" || event.Thread != "acme/www:issue:45" {
				t.Fatalf("location = %s / %s", event.Channel, event.Thread)
			}
			lc := event.Lifecycle
			if lc.CreateRoot != tc.createRoot || lc.Reply != tc.reply {
				t.Fatalf("lifecycle = %+v", lc)
			}
			if lc.Labels["state"] != tc.state || lc.Labels["issue"] != "acme/www#45" {
				t.Fatalf("labels = %+v", lc.Labels)
			}
			if !strings.HasPrefix(recordsDedupe(t, p, "issues", tc.file), "lifecycle:github:") {
				t.Fatal("issue lifecycle has no lifecycle dedupe")
			}
		})
	}
}

func recordsDedupe(t *testing.T, p *github.Provider, event, file string) string {
	t.Helper()
	return receive(t, p, event, fixture(t, file))[0].Dedupe
}

func TestPullRequestLifecycle(t *testing.T) {
	p := newProvider(t, false)

	opened := receive(t, p, "pull_request", fixture(t, "pr_opened.json"))[0].Event
	if opened.Type != "pr.opened" || !opened.Lifecycle.CreateRoot {
		t.Fatalf("opened = %+v", opened.Lifecycle)
	}
	if opened.Lifecycle.Labels["state"] != "open" || opened.Lifecycle.Labels["head"] != "a1b2c3d4e5f6" {
		t.Fatalf("opened labels = %+v", opened.Lifecycle.Labels)
	}

	synchronized := receive(t, p, "pull_request", fixture(t, "pr_synchronize.json"))[0].Event
	if synchronized.Type != "pr.synchronized" {
		t.Fatalf("synchronize type = %q", synchronized.Type)
	}
	if synchronized.Lifecycle.Labels["head"] != "b2c3d4e5f6a7" {
		t.Fatalf("head = %+v", synchronized.Lifecycle.Labels)
	}
	if !contains(synchronized.Lifecycle.Owns, "ci.1001-123.") {
		t.Fatalf("owns = %v", synchronized.Lifecycle.Owns)
	}

	reopened := receive(t, p, "pull_request", fixture(t, "pr_reopened.json"))[0].Event
	if reopened.Type != "pr.reopened" || !reopened.Lifecycle.CreateRoot || reopened.Lifecycle.Reply != "reopened by julian" {
		t.Fatalf("reopened = %+v", reopened.Lifecycle)
	}
	if reopened.Lifecycle.Labels["state"] != "open" {
		t.Fatalf("reopened state = %+v", reopened.Lifecycle.Labels)
	}

	closed := receive(t, p, "pull_request", fixture(t, "pr_closed.json"))[0].Event
	if closed.Type != "pr.closed" || closed.Lifecycle.Reply != "merged by julian" || closed.Lifecycle.Labels["state"] != "merged" {
		t.Fatalf("closed = %+v", closed.Lifecycle)
	}
}

func TestIssueViewOfPullRequestOnlyReconcilesLabelsAndAssignees(t *testing.T) {
	p := newProvider(t, false)
	base := string(fixture(t, "pr_comment.json"))

	// A PR's `issues` closed event must not fight the `pull_request` event,
	// which alone knows whether the close was a merge.
	closed := strings.ReplaceAll(base, `"action": "created"`, `"action": "closed"`)
	closed = strings.ReplaceAll(closed, `"state": "open"`, `"state": "closed"`)
	if records := receive(t, p, "issues", []byte(closed)); len(records) != 0 {
		t.Fatalf("PR issue-view close imported: %+v", records)
	}

	labeled := strings.ReplaceAll(base, `"action": "created"`, `"action": "labeled"`)
	records := receive(t, p, "issues", []byte(labeled))
	if len(records) != 1 {
		t.Fatalf("labeled records = %d", len(records))
	}
	event := records[0].Event
	if event.Type != "pr.labeled" || event.Channel != "acme/www:pulls" || event.Thread != "acme/www:123" {
		t.Fatalf("event = %+v", event)
	}
	if _, exists := event.Lifecycle.Labels["state"]; exists {
		t.Fatalf("issue-view label event carried state: %+v", event.Lifecycle.Labels)
	}
	if event.Lifecycle.Reply != "" || event.Lifecycle.CreateRoot {
		t.Fatalf("issue-view label event has lifecycle side effects: %+v", event.Lifecycle)
	}
	if event.Lifecycle.Labels["pr"] != "acme/www#123" {
		t.Fatalf("labels = %+v", event.Lifecycle.Labels)
	}
}

func TestReviewDismissedClearsReview(t *testing.T) {
	event := receive(t, newProvider(t, false), "pull_request_review", fixture(t, "review_dismissed.json"))[0].Event
	if event.Type != "review.dismissed" || len(event.Lifecycle.Unset) != 1 || event.Lifecycle.Unset[0] != "review" {
		t.Fatalf("event = %+v", event.Lifecycle)
	}
	if event.Thread != "acme/www:123" || event.Channel != "acme/www:pulls" {
		t.Fatalf("location = %s / %s", event.Channel, event.Thread)
	}
	if event.Author.Handle != "julian" {
		t.Fatalf("author = %+v", event.Author)
	}
}

func TestWorkflowRunPassUnsetsAndFailSets(t *testing.T) {
	p := newProvider(t, false)
	failed := receive(t, p, "workflow_run", fixture(t, "workflow_run.json"))[0].Event
	if failed.Type != "ci.failed" || failed.Lifecycle.Labels["ci.1001-123.check"] != "failure" {
		t.Fatalf("failed = %+v / %+v", failed, failed.Lifecycle.Labels)
	}
	if !strings.Contains(failed.Lifecycle.Reply, "check failure on www#123") {
		t.Fatalf("reply = %q", failed.Lifecycle.Reply)
	}
	passed := receive(t, p, "workflow_run", fixture(t, "workflow_success.json"))[0].Event
	if passed.Type != "ci.passed" || len(passed.Lifecycle.Unset) != 1 || passed.Lifecycle.Unset[0] != "ci.1001-123.check" {
		t.Fatalf("passed = %+v / %+v", passed, passed.Lifecycle.Unset)
	}
	if len(passed.Lifecycle.Labels) != 0 || passed.Lifecycle.Reply != "" {
		t.Fatalf("passed wrote labels or a reply: %+v", passed.Lifecycle)
	}
}

func TestWorkflowRunLongNameKeepsBoundedUniqueKey(t *testing.T) {
	longPath := ".github/workflows/" + strings.Repeat("a-very-long-workflow-name-", 5) + ".yml"
	body := []byte(strings.ReplaceAll(string(fixture(t, "workflow_run.json")), ".github/workflows/check.yml", longPath))
	event := receive(t, newProvider(t, false), "workflow_run", body)[0].Event
	if len(event.Lifecycle.Labels) != 1 {
		t.Fatalf("labels = %+v", event.Lifecycle.Labels)
	}
	for key, value := range event.Lifecycle.Labels {
		if len(key) > 64 {
			t.Fatalf("label key %q is %d bytes", key, len(key))
		}
		if !strings.HasPrefix(key, "ci.1001-123.") || value != "failure" {
			t.Fatalf("label = %s=%s", key, value)
		}
	}
	if len(event.Lifecycle.Owns) != 1 || event.Lifecycle.Owns[0] != "ci.1001-123." {
		t.Fatalf("owns = %v", event.Lifecycle.Owns)
	}
}

func TestCommentEditedAndDeleted(t *testing.T) {
	p := newProvider(t, false)
	edited := receive(t, p, "issue_comment", fixture(t, "issue_comment_edited.json"))[0].Event
	if edited.Kind != boardsync.KindEdited || edited.Type != boardsync.TypeMessageEdited {
		t.Fatalf("edited = %+v", edited)
	}
	if edited.Text != "confirmed on staging and production" {
		t.Fatalf("edited text = %q", edited.Text)
	}
	if !strings.HasPrefix(edited.ID, "edited:github:") {
		t.Fatalf("edited ID = %q", edited.ID)
	}
	deleted := receive(t, p, "issue_comment", fixture(t, "issue_comment_deleted.json"))[0].Event
	if deleted.Kind != boardsync.KindDeleted || deleted.Type != boardsync.TypeMessageDeleted {
		t.Fatalf("deleted = %+v", deleted)
	}
	if deleted.Author.ID != "" {
		t.Fatalf("deleted author = %+v", deleted.Author)
	}
	if !strings.HasPrefix(deleted.ID, "deleted:github:") {
		t.Fatalf("deleted ID = %q", deleted.ID)
	}
}

func TestReviewThreadGrouping(t *testing.T) {
	p := newProvider(t, false)
	root := receive(t, p, "pull_request_review_comment", fixture(t, "review_comment_root.json"))[0].Event
	if root.Subthread != "acme/www:123:rc:345" {
		t.Fatalf("root subthread = %q", root.Subthread)
	}
	reply := receive(t, p, "pull_request_review_comment", fixture(t, "review_comment.json"))[0].Event
	if reply.Subthread != "acme/www:123:rc:300" {
		t.Fatalf("reply subthread = %q", reply.Subthread)
	}
	if root.Thread != reply.Thread {
		t.Fatalf("review threads aren't grouped under one PR: %q / %q", root.Thread, reply.Thread)
	}
	if root.ID == reply.ID {
		t.Fatal("two review comments share a dedupe ID")
	}
}

func TestOwnCommentByBotIdentity(t *testing.T) {
	p := providerWith(t, func(c *github.Config) { c.BotUserID = 4242 })
	body := []byte(strings.ReplaceAll(string(fixture(t, "pr_comment.json")), `"id": 9100`, `"id": 9101`))
	body = []byte(strings.ReplaceAll(string(body), `"user": { "login": "ada", "id": 1001, "type": "User" }`, `"user": { "login": "keel-board[bot]", "id": 4242, "type": "Bot" }`))
	records := receive(t, p, "issue_comment", body)
	if len(records) != 0 {
		t.Fatalf("bot comment imported: %+v", records)
	}

	loginOnly := providerWith(t, func(c *github.Config) { c.BotLogin = "keel-board[bot]" })
	if records := receive(t, loginOnly, "issue_comment", body); len(records) != 0 {
		t.Fatalf("bot-login comment imported: %+v", records)
	}
}

func TestPingAndUnknownEventsAreAcknowledgedWithoutRecords(t *testing.T) {
	p := newProvider(t, false)
	if records := receive(t, p, "ping", fixture(t, "ping.json")); len(records) != 0 {
		t.Fatalf("ping = %+v", records)
	}
	if records := receive(t, p, "deployment_status", fixture(t, "ping.json")); len(records) != 0 {
		t.Fatalf("unknown event = %+v", records)
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
