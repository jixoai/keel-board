package github_test

import (
	"encoding/json"
	"strings"
	"testing"
)

// mutateFixture decodes a JSON fixture, lets the test change it, and encodes it
// again so the request signature covers the changed body.
func mutateFixture(t *testing.T, name string, mutate func(map[string]any)) []byte {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(fixture(t, name), &payload); err != nil {
		t.Fatal(err)
	}
	mutate(payload)
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestPullRequestLifecycleEdges(t *testing.T) {
	p := newProvider(t, false)

	edited := receive(t, p, "pull_request", mutateFixture(t, "pr_opened.json", func(payload map[string]any) {
		payload["action"] = "edited"
		payload["pull_request"].(map[string]any)["title"] = "checkout retry, second pass"
	}))[0].Event
	if edited.Type != "pr.edited" || !edited.Lifecycle.Retitled || edited.Lifecycle.CreateRoot {
		t.Fatalf("edited = %+v", edited.Lifecycle)
	}
	if !strings.Contains(edited.Lifecycle.Title, "second pass") {
		t.Fatalf("title = %q", edited.Lifecycle.Title)
	}

	ready := receive(t, p, "pull_request", mutateFixture(t, "pr_opened.json", func(payload map[string]any) {
		payload["action"] = "ready_for_review"
		payload["pull_request"].(map[string]any)["draft"] = false
	}))[0].Event
	if ready.Type != "pr.ready" || !ready.Lifecycle.CreateRoot || ready.Lifecycle.Labels["state"] != "open" {
		t.Fatalf("ready = %+v / %+v", ready.Lifecycle, ready.Lifecycle.Labels)
	}

	draft := receive(t, p, "pull_request", mutateFixture(t, "pr_opened.json", func(payload map[string]any) {
		payload["action"] = "converted_to_draft"
		payload["pull_request"].(map[string]any)["draft"] = true
	}))[0].Event
	if draft.Type != "pr.draft" || draft.Lifecycle.Labels["state"] != "draft" {
		t.Fatalf("draft = %+v / %+v", draft.Lifecycle, draft.Lifecycle.Labels)
	}

	labeled := receive(t, p, "pull_request", mutateFixture(t, "pr_opened.json", func(payload map[string]any) {
		payload["action"] = "labeled"
		payload["pull_request"].(map[string]any)["labels"] = []any{map[string]any{"name": "needs review"}}
		payload["pull_request"].(map[string]any)["assignees"] = []any{}
	}))[0].Event
	if labeled.Type != "pr.labeled" || labeled.Lifecycle.Reply != "" || labeled.Lifecycle.CreateRoot {
		t.Fatalf("labeled = %+v", labeled.Lifecycle)
	}
	if labeled.Lifecycle.Labels["tag.needs-review"] != "needs review" {
		t.Fatalf("labels = %+v", labeled.Lifecycle.Labels)
	}
	if !contains(labeled.Lifecycle.Owns, "assignee.") {
		t.Fatalf("unassigned PR doesn't own assignee labels: %v", labeled.Lifecycle.Owns)
	}
}

func TestIssueLifecycleLabelEdges(t *testing.T) {
	p := newProvider(t, false)

	edited := receive(t, p, "issues", mutateFixture(t, "issue_opened.json", func(payload map[string]any) {
		payload["action"] = "edited"
		payload["issue"].(map[string]any)["title"] = "blank export page in Safari"
	}))[0].Event
	if edited.Type != "issue.edited" || !edited.Lifecycle.Retitled {
		t.Fatalf("edited = %+v", edited.Lifecycle)
	}
	if !strings.Contains(edited.Lifecycle.Title, "Safari") {
		t.Fatalf("title = %q", edited.Lifecycle.Title)
	}

	unassigned := receive(t, p, "issues", mutateFixture(t, "issue_opened.json", func(payload map[string]any) {
		payload["action"] = "unassigned"
		payload["issue"].(map[string]any)["assignees"] = []any{}
	}))[0].Event
	if unassigned.Type != "issue.unassigned" {
		t.Fatalf("type = %q", unassigned.Type)
	}
	if _, exists := unassigned.Lifecycle.Labels["assignee.julian"]; exists {
		t.Fatalf("unassigned issue still names julian: %+v", unassigned.Lifecycle.Labels)
	}
	if !contains(unassigned.Lifecycle.Owns, "assignee.") {
		t.Fatalf("owns = %v", unassigned.Lifecycle.Owns)
	}
}

func TestReviewChangesRequested(t *testing.T) {
	body := mutateFixture(t, "review_approved.json", func(payload map[string]any) {
		review := payload["review"].(map[string]any)
		review["state"] = "changes_requested"
		review["body"] = "Please add a regression test."
	})
	event := receive(t, newProvider(t, false), "pull_request_review", body)[0].Event
	if event.Lifecycle.Labels["review"] != "changes_requested" {
		t.Fatalf("labels = %+v", event.Lifecycle.Labels)
	}
	if !strings.HasPrefix(event.Lifecycle.Reply, "changes requested by julian") || !strings.Contains(event.Lifecycle.Reply, "regression test") {
		t.Fatalf("reply = %q", event.Lifecycle.Reply)
	}
}

func TestCommentReviewDoesNotChangeReviewLabel(t *testing.T) {
	body := mutateFixture(t, "review_approved.json", func(payload map[string]any) {
		payload["review"].(map[string]any)["state"] = "commented"
	})
	if records := receive(t, newProvider(t, false), "pull_request_review", body); len(records) != 0 {
		t.Fatalf("commented review imported: %+v", records)
	}
}
