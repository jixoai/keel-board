package github_test

import (
	"encoding/json"
	"strings"
	"testing"

	boardsync "github.com/jixoai/keel-board/sync"
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

// applyLifecycle mirrors the host's label reconciliation: write Labels, unset
// keys under Owns that Labels doesn't carry, then apply explicit Unset.
func applyLifecycle(labels map[string]string, lifecycle *boardsync.Lifecycle) {
	for key, value := range lifecycle.Labels {
		labels[key] = value
	}
	for _, prefix := range lifecycle.Owns {
		for key := range labels {
			if strings.HasPrefix(key, prefix) {
				if _, kept := lifecycle.Labels[key]; !kept {
					delete(labels, key)
				}
			}
		}
	}
	for _, key := range lifecycle.Unset {
		delete(labels, key)
	}
}

func workflowEvent(t *testing.T, file, path string) *boardsync.Lifecycle {
	t.Helper()
	body := mutateFixture(t, file, func(payload map[string]any) {
		payload["workflow_run"].(map[string]any)["path"] = path
	})
	return receive(t, newProvider(t, false), "workflow_run", body)[0].Event.Lifecycle
}

func TestWorkflowRunsOwnOnlyTheirExactKey(t *testing.T) {
	labels := map[string]string{}
	applyLifecycle(labels, workflowEvent(t, "workflow_run.json", ".github/workflows/lint.yml"))
	applyLifecycle(labels, workflowEvent(t, "workflow_success.json", ".github/workflows/check.yml"))
	// check passing must not clear lint's failure.
	if labels["ci.1001-123.lint"] != "failure" {
		t.Fatalf("lint failure was cleared by check success: %+v", labels)
	}
	if _, exists := labels["ci.1001-123.check"]; exists {
		t.Fatalf("check success didn't clear check: %+v", labels)
	}

	labels = map[string]string{}
	applyLifecycle(labels, workflowEvent(t, "workflow_run.json", ".github/workflows/check.yml"))
	applyLifecycle(labels, workflowEvent(t, "workflow_success.json", ".github/workflows/lint.yml"))
	if labels["ci.1001-123.check"] != "failure" {
		t.Fatalf("check failure was cleared by lint success: %+v", labels)
	}
	if _, exists := labels["ci.1001-123.lint"]; exists {
		t.Fatalf("lint success didn't clear lint: %+v", labels)
	}
}

func TestLabelValuesAreSanitized(t *testing.T) {
	body := mutateFixture(t, "pr_opened.json", func(payload map[string]any) {
		payload["pull_request"].(map[string]any)["labels"] = []any{map[string]any{"name": "bad\nlabel\x00"}}
	})
	event := receive(t, newProvider(t, false), "pull_request", body)[0].Event
	value, ok := event.Lifecycle.Labels["tag.bad-label"]
	if !ok {
		t.Fatalf("labels = %+v", event.Lifecycle.Labels)
	}
	if strings.ContainsRune(value, 0) || strings.ContainsAny(value, "\n\r\t") {
		t.Fatalf("label value still has control characters: %q", value)
	}
}

func TestLongLabelKeepsBoundedKey(t *testing.T) {
	long := strings.Repeat("x", 70)
	body := mutateFixture(t, "pr_opened.json", func(payload map[string]any) {
		payload["pull_request"].(map[string]any)["labels"] = []any{map[string]any{"name": long}}
	})
	event := receive(t, newProvider(t, false), "pull_request", body)[0].Event
	found := false
	for key, value := range event.Lifecycle.Labels {
		if !strings.HasPrefix(key, "tag.") {
			continue
		}
		found = true
		if len(key) > 64 {
			t.Fatalf("tag key %q is %d bytes", key, len(key))
		}
		if value != long {
			t.Fatalf("tag value = %q", value)
		}
	}
	if !found {
		t.Fatalf("labels = %+v", event.Lifecycle.Labels)
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
