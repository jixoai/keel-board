package linear_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/linear"
)

func issueThreadRef(key boardsync.Key) boardsync.ThreadRef {
	return boardsync.ThreadRef{Key: key, Platform: "linear", Channel: "ENG", Thread: "ENG:" + testIssueID}
}

func TestIssueUpdateQueryHasNoInputID(t *testing.T) {
	fake, server := newFakeLinear(t)
	defer server.Close()
	p := newProvider(t, server.URL)
	if err := p.Edit(context.Background(), "linear", issueThreadRef("linear"), linear.IssueRootMessage,
		boardsync.OutboundMessage{Markdown: "changed"}); err != nil {
		t.Fatalf("Edit: %v", err)
	}
	query := fake.lastQuery("mutation IssueUpdate")
	if strings.Contains(query, "input: { id:") || strings.Contains(query, "input: {id:") {
		t.Fatalf("IssueUpdateInput has no id field: %s", query)
	}
	if !strings.Contains(query, "issueUpdate(id: $id") || !strings.Contains(query, "$id: String!") {
		t.Fatalf("id argument missing: %s", query)
	}
}

func TestEditIssueWritesStatePriorityProjectLabelsAssignee(t *testing.T) {
	fake, server := newFakeLinear(t)
	defer server.Close()
	p := newProvider(t, server.URL)
	err := p.Edit(context.Background(), "linear", issueThreadRef("linear"), linear.IssueRootMessage, boardsync.OutboundMessage{
		Markdown: "new description",
		Labels: map[string]string{
			"issue": "ENG-123", "state": "completed", "priority": "high", "project": "Checkout",
			"tag.bug": "Bug", "tag.new": "New", "assignee.ada": "ada",
		},
	})
	if err != nil {
		t.Fatalf("Edit: %v", err)
	}
	variables := fake.lastVariables("mutation IssueUpdate")
	if variables["id"] != testIssueID || variables["description"] != "new description" {
		t.Fatalf("variables = %+v", variables)
	}
	if variables["stateId"] != "state-completed" || variables["priority"] != float64(2) {
		t.Fatalf("state/priority = %+v", variables)
	}
	if variables["projectId"] != testProjectID || variables["assigneeId"] != "user-1" {
		t.Fatalf("project/assignee = %+v", variables)
	}
	labelIDs, _ := variables["labelIds"].([]any)
	if len(labelIDs) != 2 || labelIDs[0] != "label-1" || labelIDs[1] != "label-new" {
		t.Fatalf("labelIds = %+v", variables["labelIds"])
	}
	if fake.callsContaining("mutation IssueLabelCreate") != 1 {
		t.Fatalf("calls = %v", fake.calls)
	}
}

func TestLabelsChangeClearsAbsentProperties(t *testing.T) {
	fake, server := newFakeLinear(t)
	defer server.Close()
	p := newProvider(t, server.URL)
	before := map[string]string{
		"state": "completed", "priority": "high", "project": "Checkout",
		"tag.bug": "Bug", "assignee.ada": "ada",
	}
	err := p.Labels(context.Background(), "linear", issueThreadRef("linear"), map[string]string{
		"state": "started", "priority": "low",
	}, before)
	if err != nil {
		t.Fatalf("Labels: %v", err)
	}
	variables := fake.lastVariables("mutation IssueUpdate")
	if variables["stateId"] != "state-started" || variables["priority"] != float64(4) {
		t.Fatalf("variables = %+v", variables)
	}
	if variables["projectId"] != nil || variables["assigneeId"] != nil {
		t.Fatalf("removed project/assignee should clear: %+v", variables)
	}
	if labelIDs, ok := variables["labelIds"].([]any); !ok || len(labelIDs) != 0 {
		t.Fatalf("removed labels should clear: %+v", variables["labelIds"])
	}
	if fake.callsContaining("mutation IssueLabelCreate") != 0 {
		t.Fatal("clearing labels created one")
	}
}

func TestLabelsChangePriorityOnlyKeepsOtherProperties(t *testing.T) {
	fake, server := newFakeLinear(t)
	defer server.Close()
	p := newProvider(t, server.URL)
	labels := map[string]string{
		"state": "started", "priority": "low", "project": "Checkout",
		"tag.bug": "Bug", "assignee.ada": "ada",
	}
	before := map[string]string{
		"state": "started", "priority": "high", "project": "Checkout",
		"tag.bug": "Bug", "assignee.ada": "ada",
	}
	if err := p.Labels(context.Background(), "linear", issueThreadRef("linear"), labels, before); err != nil {
		t.Fatalf("Labels: %v", err)
	}
	variables := fake.lastVariables("mutation IssueUpdate")
	if variables["priority"] != float64(4) {
		t.Fatalf("priority = %+v", variables)
	}
	for _, key := range []string{"stateId", "projectId", "assigneeId", "labelIds"} {
		if _, exists := variables[key]; exists {
			t.Fatalf("%s rewritten for a priority-only change: %+v", key, variables)
		}
	}
	if fake.callsContaining("query IssueByID") != 0 {
		t.Fatal("unchanged state fetched the issue")
	}
}

func TestEmptyLabelsClearProperties(t *testing.T) {
	fake, server := newFakeLinear(t)
	defer server.Close()
	p := newProvider(t, server.URL)
	before := map[string]string{
		"state": "started", "priority": "high", "project": "Checkout",
		"tag.bug": "Bug", "assignee.ada": "ada",
	}
	if err := p.Labels(context.Background(), "linear", issueThreadRef("linear"), map[string]string{}, before); err != nil {
		t.Fatalf("Labels: %v", err)
	}
	variables := fake.lastVariables("mutation IssueUpdate")
	if variables["projectId"] != nil || variables["assigneeId"] != nil {
		t.Fatalf("empty labels should clear project/assignee: %+v", variables)
	}
	if labelIDs, ok := variables["labelIds"].([]any); !ok || len(labelIDs) != 0 {
		t.Fatalf("empty labels should clear tags: %+v", variables["labelIds"])
	}
	if variables["priority"] != float64(0) {
		t.Fatalf("removed priority should clear: %+v", variables)
	}
	if _, exists := variables["stateId"]; exists {
		t.Fatalf("removed state has no stateId to write: %+v", variables)
	}
}

func TestMultipleStartedStatesPreserveCurrent(t *testing.T) {
	fake, server := newFakeLinear(t)
	defer server.Close()
	fake.setStates([]any{
		map[string]any{"id": "state-started", "name": "In Progress", "type": "started"},
		map[string]any{"id": "state-review", "name": "In Review", "type": "started"},
		map[string]any{"id": "state-completed", "name": "Done", "type": "completed"},
	})
	p := newProvider(t, server.URL)
	if err := p.Labels(context.Background(), "linear", issueThreadRef("linear"), map[string]string{
		"state": "started", "priority": "high",
	}, map[string]string{"state": "unstarted", "priority": "high"}); err != nil {
		t.Fatalf("Labels: %v", err)
	}
	variables := fake.lastVariables("mutation IssueUpdate")
	if variables["stateId"] != "state-started" {
		t.Fatalf("ambiguous started state moved the issue: %+v", variables)
	}
}

func TestEditIssueUnknownStateFailsClosed(t *testing.T) {
	fake, server := newFakeLinear(t)
	defer server.Close()
	p := newProvider(t, server.URL)
	err := p.Edit(context.Background(), "linear", issueThreadRef("linear"), linear.IssueRootMessage, boardsync.OutboundMessage{
		Markdown: "x", Labels: map[string]string{"state": "nonsense"},
	})
	if !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("unknown state = %v", err)
	}
	if fake.callsContaining("mutation IssueUpdate") != 0 {
		t.Fatal("unknown state reached issueUpdate")
	}
}

func TestPostRootCarriesLabels(t *testing.T) {
	fake, server := newFakeLinear(t)
	defer server.Close()
	p := newProvider(t, server.URL)
	target := boardsync.ResolvedTarget{Key: "linear", Platform: "linear", Channel: "ENG", Kind: boardsync.TargetChannel}
	posted, err := p.Post(context.Background(), "linear", target, boardsync.OutboundMessage{
		Markdown: "Checkout retry\n\nRetries.",
		Labels:   map[string]string{"state": "started", "priority": "urgent", "tag.bug": "Bug"},
	})
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	if posted.Message != linear.IssueRootMessage {
		t.Fatalf("posted = %+v", posted)
	}
	variables := fake.lastVariables("mutation IssueCreate")
	if variables["stateId"] != "state-started" || variables["priority"] != float64(1) {
		t.Fatalf("variables = %+v", variables)
	}
	if variables["teamId"] != testTeamID || variables["title"] != "Checkout retry" {
		t.Fatalf("variables = %+v", variables)
	}
}

func TestInboundParentCommentThenBoardReplyUsesParent(t *testing.T) {
	fake, server := newFakeLinear(t)
	defer server.Close()
	p := newProvider(t, server.URL)
	records := receive(t, p, fixture(t, "comment_reply.json"))
	if len(records) != 1 {
		t.Fatalf("records = %+v", records)
	}
	event := records[0].Event
	if event.Subthread != "ENG:"+testIssueID+":c:comment-parent" || event.Message != "comment-child" {
		t.Fatalf("event = %+v", event)
	}
	target := boardsync.ResolvedTarget{
		Key: "linear", Platform: "linear", Channel: event.Channel,
		Thread: event.Thread, Subthread: event.Subthread, Kind: boardsync.TargetThread,
	}
	posted, err := p.Post(context.Background(), "linear", target, boardsync.OutboundMessage{Markdown: "**ada**: reply"})
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	if posted.Message != "comment-9" {
		t.Fatalf("posted = %+v", posted)
	}
	variables := fake.lastVariables("mutation CommentCreate")
	if variables["parentId"] != "comment-parent" {
		t.Fatalf("parentId = %+v", variables["parentId"])
	}
	if !strings.Contains(fake.lastQuery("mutation CommentCreate"), "parentId") {
		t.Fatal("commentCreate mutation has no parentId field")
	}
}

func TestEditCommentIgnoringSubthreadParent(t *testing.T) {
	fake, server := newFakeLinear(t)
	defer server.Close()
	p := newProvider(t, server.URL)
	thread := issueThreadRef("linear")
	thread.Subthread = thread.Thread + ":c:comment-1"
	if err := p.Edit(context.Background(), "linear", thread, "comment-1", boardsync.OutboundMessage{Markdown: "fixed"}); err != nil {
		t.Fatalf("Edit: %v", err)
	}
	if fake.callsContaining("mutation CommentUpdate") != 1 {
		t.Fatalf("calls = %v", fake.calls)
	}
}
