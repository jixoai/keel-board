package github_test

import (
	"testing"

	"github.com/jixoai/keel-board/sync/github"
)

func oneRecord(t *testing.T, p *github.Provider, event, delivery string, body []byte) string {
	t.Helper()
	intake, err := receiveRequest(t, p, event, delivery, body)
	if err != nil {
		t.Fatalf("Receive(%s): %v", event, err)
	}
	if len(intake.Records) != 1 {
		t.Fatalf("records = %d", len(intake.Records))
	}
	return intake.Records[0].Dedupe
}

func TestCommentDedupeDistinguishesMessagesInOneLane(t *testing.T) {
	p := newProvider(t, false)
	first := fixture(t, "issue_comment.json")
	second := mutateFixture(t, "issue_comment.json", func(payload map[string]any) {
		payload["comment"].(map[string]any)["id"] = 9002
	})
	a := oneRecord(t, p, "issue_comment", "d-1", first)
	b := oneRecord(t, p, "issue_comment", "d-2", second)
	if a == b {
		t.Fatalf("two comments in one lane share dedupe %q", a)
	}
	// A redelivery of the same comment, even with a new delivery ID, is one
	// occurrence: message identity, not the delivery, is stable here.
	if again := oneRecord(t, p, "issue_comment", "d-3", first); again != a {
		t.Fatalf("redelivery changed dedupe: %q then %q", a, again)
	}
}

func TestLifecycleDedupeIncludesDelivery(t *testing.T) {
	p := newProvider(t, false)
	body := fixture(t, "pr_closed.json")
	a := oneRecord(t, p, "pull_request", "delivery-a", body)
	b := oneRecord(t, p, "pull_request", "delivery-b", body)
	if a == b {
		t.Fatalf("two deliveries share dedupe %q", a)
	}
	if again := oneRecord(t, p, "pull_request", "delivery-a", body); again != a {
		t.Fatalf("same delivery changed dedupe: %q then %q", a, again)
	}
}

func TestEditDedupeIncludesTextAndTime(t *testing.T) {
	p := newProvider(t, false)
	edited := func(body, updatedAt string) []byte {
		return mutateFixture(t, "issue_comment.json", func(payload map[string]any) {
			payload["action"] = "edited"
			comment := payload["comment"].(map[string]any)
			comment["body"] = body
			comment["updated_at"] = updatedAt
		})
	}
	first := oneRecord(t, p, "issue_comment", "edit-1", edited("one", "2026-01-03T04:05:06Z"))
	sameText := oneRecord(t, p, "issue_comment", "edit-2", edited("one", "2026-01-03T04:05:07Z"))
	otherText := oneRecord(t, p, "issue_comment", "edit-3", edited("two", "2026-01-03T04:05:06Z"))
	if first == sameText {
		t.Fatalf("two edits with the same text share dedupe %q", first)
	}
	if first == otherText {
		t.Fatalf("two edits with different text share dedupe %q", first)
	}
	if sameText == otherText {
		t.Fatalf("different edits share dedupe %q", sameText)
	}
}
