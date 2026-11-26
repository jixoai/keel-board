package linear_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/linear"
)

const (
	testProjectID  = "project-1"
	testCommentID  = "comment-1"
	testIssueURL   = "https://linear.app/acme/issue/ENG-123/checkout-retry"
	testCommentURL = testIssueURL + "#comment-12345678"
)

// fakeLinear is a stateful GraphQL server: enough of Linear's schema for the
// adapter's reads, writes, and links. It records every call so a test can
// prove which side effects happened.
type fakeLinear struct {
	t  *testing.T
	mu sync.Mutex

	status       int
	gqlErrors    []any
	projectTeams []string

	calls     []string
	variables []map[string]any
}

func newFakeLinear(t *testing.T) (*fakeLinear, *httptest.Server) {
	t.Helper()
	fake := &fakeLinear{t: t, projectTeams: []string{"ENG"}}
	return fake, httptest.NewServer(http.HandlerFunc(fake.serve))
}

func (f *fakeLinear) serve(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer lin_test_token" {
		f.t.Errorf("authorization = %q", r.Header.Get("Authorization"))
	}
	var body struct {
		Query     string         `json:"query"`
		Variables map[string]any `json:"variables"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		f.t.Errorf("decode: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	f.calls = append(f.calls, body.Query)
	f.variables = append(f.variables, body.Variables)
	status := f.status
	gqlErrors := f.gqlErrors
	f.mu.Unlock()
	if status != 0 {
		w.WriteHeader(status)
		return
	}
	if len(gqlErrors) > 0 {
		_ = json.NewEncoder(w).Encode(map[string]any{"errors": gqlErrors})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"data": f.answer(body.Query, body.Variables)})
}

func (f *fakeLinear) answer(query string, variables map[string]any) any {
	switch {
	case strings.Contains(query, "query ReadTeam"):
		if variables["key"] == "ENG" {
			return map[string]any{"teams": map[string]any{"nodes": []any{
				map[string]any{"id": testTeamID, "key": "ENG", "name": "Engineering"},
			}}}
		}
		return map[string]any{"teams": map[string]any{"nodes": []any{}}}
	case strings.Contains(query, "query ReadIssues"):
		return map[string]any{"issues": map[string]any{
			"nodes":    f.issueNodes(variables["filter"]),
			"pageInfo": map[string]any{"hasNextPage": false, "endCursor": ""},
		}}
	case strings.Contains(query, "query ReadIssue"):
		if variables["id"] != testIssueID {
			return map[string]any{"issue": nil}
		}
		return map[string]any{"issue": f.issue(true)}
	case strings.Contains(query, "query ReadProjectSlug"):
		return map[string]any{"projects": map[string]any{"nodes": []any{f.project()}}}
	case strings.Contains(query, "query ReadProject("):
		return map[string]any{"project": f.project()}
	case strings.Contains(query, "query CommentURL"):
		if variables["id"] == testCommentID {
			return map[string]any{"comment": map[string]any{"id": testCommentID, "url": testCommentURL}}
		}
		return map[string]any{"comment": nil}
	case strings.Contains(query, "query IssueByKey"):
		if variables["team"] == "ENG" && variables["number"] == float64(123) {
			return map[string]any{"issues": map[string]any{"nodes": []any{
				map[string]any{"id": testIssueID, "identifier": "ENG-123", "team": map[string]any{"key": "ENG"}},
			}}}
		}
		return map[string]any{"issues": map[string]any{"nodes": []any{}}}
	case strings.Contains(query, "query IssueByID"):
		return map[string]any{"issue": f.issue(false)}
	case strings.Contains(query, "mutation CommentCreate"):
		return map[string]any{"commentCreate": map[string]any{"success": true, "comment": map[string]any{"id": "comment-9"}}}
	case strings.Contains(query, "mutation IssueCreate"):
		return map[string]any{"issueCreate": map[string]any{"success": true, "issue": map[string]any{
			"id": "issue-new", "identifier": "ENG-124", "team": map[string]any{"key": "ENG"},
		}}}
	case strings.Contains(query, "mutation IssueUpdate"):
		return map[string]any{"issueUpdate": map[string]any{"success": true}}
	case strings.Contains(query, "mutation CommentUpdate"):
		return map[string]any{"commentUpdate": map[string]any{"success": true}}
	case strings.Contains(query, "mutation CommentDelete"):
		return map[string]any{"commentDelete": map[string]any{"success": true}}
	case strings.Contains(query, "mutation ReactionCreate"):
		return map[string]any{"reactionCreate": map[string]any{"success": true}}
	default:
		f.t.Errorf("unexpected query %q", query)
		return nil
	}
}

func (f *fakeLinear) issueNodes(filter any) []any {
	byTeam, byProject := "", ""
	if values, ok := filter.(map[string]any); ok {
		if team, ok := values["team"].(map[string]any); ok {
			if key, ok := team["key"].(map[string]any); ok {
				byTeam, _ = key["eq"].(string)
			}
		}
		if project, ok := values["project"].(map[string]any); ok {
			if id, ok := project["id"].(map[string]any); ok {
				byProject, _ = id["eq"].(string)
			}
		}
	}
	if byTeam != "" && byTeam != "ENG" {
		return nil
	}
	if byProject != "" && byProject != testProjectID {
		return nil
	}
	return []any{f.issue(false)}
}

func (f *fakeLinear) issue(withComments bool) map[string]any {
	issue := map[string]any{
		"id": testIssueID, "identifier": "ENG-123", "title": "Checkout retry",
		"description": "Retries checkout when the provider times out.",
		"url":         testIssueURL, "createdAt": "2026-01-02T03:04:05Z",
		"team":     map[string]any{"id": testTeamID, "key": "ENG", "name": "Engineering"},
		"state":    map[string]any{"id": "state-2", "name": "In Progress", "type": "started"},
		"priority": 2, "priorityLabel": "High",
		"project":  map[string]any{"id": testProjectID, "name": "Checkout", "url": "https://linear.app/acme/project/checkout"},
		"labels":   map[string]any{"nodes": []any{map[string]any{"id": "label-1", "name": "Bug"}}},
		"assignee": map[string]any{"id": "user-1", "name": "Ada", "displayName": "ada", "url": "https://linear.app/acme/profiles/ada"},
		"creator":  map[string]any{"id": "user-1", "name": "Ada", "displayName": "ada", "url": "https://linear.app/acme/profiles/ada"},
		"comments": map[string]any{
			"nodes": []any{
				map[string]any{"id": testCommentID, "body": "I reproduced this.", "createdAt": "2026-01-02T05:00:00Z",
					"user": map[string]any{"id": "user-2", "name": "Julian", "url": "https://linear.app/acme/profiles/julian"}},
			},
			"pageInfo": map[string]any{"hasNextPage": false, "endCursor": ""},
		},
	}
	if !withComments {
		issue["comments"] = map[string]any{"nodes": []any{map[string]any{"id": testCommentID}}}
	}
	return issue
}

func (f *fakeLinear) project() map[string]any {
	f.mu.Lock()
	keys := append([]string(nil), f.projectTeams...)
	f.mu.Unlock()
	teams := make([]any, 0, len(keys))
	for _, key := range keys {
		teams = append(teams, map[string]any{"id": "team-" + key, "key": key})
	}
	return map[string]any{
		"id": testProjectID, "name": "Checkout", "url": "https://linear.app/acme/project/checkout", "slugId": "abcdef12",
		"teams": map[string]any{"nodes": teams},
	}
}

func (f *fakeLinear) setProjectTeams(keys []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.projectTeams = keys
}

func (f *fakeLinear) callsContaining(fragment string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	count := 0
	for _, call := range f.calls {
		if strings.Contains(call, fragment) {
			count++
		}
	}
	return count
}

func newFakeProvider(t *testing.T, fake *fakeLinear, server *httptest.Server) *linear.Provider {
	t.Helper()
	return newProvider(t, server.URL)
}

func TestReadSourceTeam(t *testing.T) {
	fake, server := newFakeLinear(t)
	defer server.Close()
	p := newFakeProvider(t, fake, server)
	read, err := p.ReadSource(context.Background(), "linear", "eng", linear.ReadOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ReadSource: %v", err)
	}
	if read.Kind != "team" || read.Channel != "ENG" || read.Name != "Engineering" {
		t.Fatalf("read = %+v", read)
	}
	if len(read.Messages) != 1 || read.Messages[0].ID != "ENG-123" || read.Messages[0].Thread != "ENG:"+testIssueID {
		t.Fatalf("messages = %+v", read.Messages)
	}
	if !strings.Contains(read.Messages[0].Text, "ENG-123: Checkout retry") || !strings.Contains(read.Messages[0].Text, "In Progress") {
		t.Fatalf("text = %q", read.Messages[0].Text)
	}
	if read.Messages[0].Author != "ada" || read.Messages[0].Replies != 1 {
		t.Fatalf("message = %+v", read.Messages[0])
	}
	if _, err := p.ReadSource(context.Background(), "linear", "OPS", linear.ReadOptions{}); !errors.Is(err, boardsync.ErrNotFound) {
		t.Fatalf("unlisted team = %v", err)
	}
	if calls := fake.callsContaining("query ReadTeam"); calls != 1 {
		t.Fatalf("unlisted team reached linear: %d calls", calls)
	}
}

func TestReadSourceProject(t *testing.T) {
	fake, server := newFakeLinear(t)
	defer server.Close()
	p := newFakeProvider(t, fake, server)
	read, err := p.ReadSource(context.Background(), "linear", "https://linear.app/acme/project/checkout-abcdef12", linear.ReadOptions{})
	if err != nil {
		t.Fatalf("ReadSource project url: %v", err)
	}
	if read.Kind != "project" || read.Channel != testProjectID || read.Name != "Checkout" {
		t.Fatalf("read = %+v", read)
	}
	if fake.callsContaining("query ReadProjectSlug") != 1 {
		t.Fatal("project URL should read by slug")
	}
	if _, err := p.ReadSource(context.Background(), "linear", "project:"+testProjectID, linear.ReadOptions{}); err != nil {
		t.Fatalf("ReadSource project id: %v", err)
	}
	if fake.callsContaining("query ReadProject(") != 1 {
		t.Fatal("project: should read by ID")
	}
	fake.setProjectTeams([]string{"ENG", "OPS"})
	if _, err := p.ReadSource(context.Background(), "linear", "project:"+testProjectID, linear.ReadOptions{}); !errors.Is(err, boardsync.ErrNotFound) {
		t.Fatalf("project with an unlisted team = %v", err)
	}
}

func TestReadSourceIssue(t *testing.T) {
	fake, server := newFakeLinear(t)
	defer server.Close()
	p := newFakeProvider(t, fake, server)
	read, err := p.ReadSource(context.Background(), "linear", "ENG-123", linear.ReadOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ReadSource: %v", err)
	}
	if read.Kind != "issue" || read.Channel != "ENG" || read.Thread != "ENG:"+testIssueID {
		t.Fatalf("read = %+v", read)
	}
	if len(read.Messages) != 2 {
		t.Fatalf("messages = %+v", read.Messages)
	}
	if read.Messages[0].ID != "ENG-123" || read.Messages[0].Replies != 1 {
		t.Fatalf("root = %+v", read.Messages[0])
	}
	if read.Messages[1].ID != testCommentID || read.Messages[1].Author != "julian" || read.Messages[1].Text != "I reproduced this." {
		t.Fatalf("comment = %+v", read.Messages[1])
	}
	if fake.callsContaining("query IssueByKey") != 1 || fake.callsContaining("query ReadIssue") != 1 {
		t.Fatalf("calls = %v", fake.calls)
	}
	if _, err := p.ReadSource(context.Background(), "linear", "https://linear.app/acme/issue/ENG-123/checkout-retry", linear.ReadOptions{}); err != nil {
		t.Fatalf("issue URL: %v", err)
	}
	if fake.callsContaining("query IssueByKey") != 2 {
		t.Fatalf("issue URL should resolve its identifier: %v", fake.calls)
	}
	if _, err := p.ReadSource(context.Background(), "linear", testIssueID, linear.ReadOptions{}); err != nil {
		t.Fatalf("issue UUID: %v", err)
	}
	if fake.callsContaining("query ReadIssue") != 3 {
		t.Fatalf("calls = %v", fake.calls)
	}
	if _, err := p.ReadSource(context.Background(), "linear", "OPS-1", linear.ReadOptions{}); !errors.Is(err, boardsync.ErrNotFound) {
		t.Fatalf("unlisted issue team = %v", err)
	}
	if _, err := p.ReadSource(context.Background(), "other", "ENG", linear.ReadOptions{}); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("wrong key = %v", err)
	}
}

func TestReadSourcePagination(t *testing.T) {
	fake, server := newFakeLinear(t)
	defer server.Close()
	p := newFakeProvider(t, fake, server)
	if _, err := p.ReadSource(context.Background(), "linear", "ENG-123", linear.ReadOptions{Limit: 500, After: "cursor-1"}); err != nil {
		t.Fatal(err)
	}
	if got := fake.variables[len(fake.variables)-1]; got["first"] != float64(100) || got["after"] != "cursor-1" {
		t.Fatalf("variables = %+v", got)
	}
}

func TestPermalink(t *testing.T) {
	fake, server := newFakeLinear(t)
	defer server.Close()
	p := newFakeProvider(t, fake, server)
	event := boardsync.Event{
		Key: "linear", Platform: "linear", Kind: boardsync.KindMessage,
		Channel: "ENG", Thread: "ENG:" + testIssueID, Message: testCommentID,
	}
	link, err := p.Permalink(context.Background(), "linear", event)
	if err != nil || link != testCommentURL {
		t.Fatalf("comment link = %q, %v", link, err)
	}
	lifecycle := boardsync.Event{
		Key: "linear", Platform: "linear", Kind: boardsync.KindLifecycle,
		Channel: "ENG", Thread: "ENG:" + testIssueID,
	}
	link, err = p.Permalink(context.Background(), "linear", lifecycle)
	if err != nil || link != testIssueURL {
		t.Fatalf("issue link = %q, %v", link, err)
	}
	deleted := event
	deleted.Kind = boardsync.KindDeleted
	deleted.Message = "comment-gone"
	link, err = p.Permalink(context.Background(), "linear", deleted)
	if err != nil || link != testIssueURL {
		t.Fatalf("deleted comment link = %q, %v", link, err)
	}
	if _, err := p.Permalink(context.Background(), "linear", boardsync.Event{Thread: "OPS:" + testIssueID}); !errors.Is(err, boardsync.ErrNotFound) {
		t.Fatalf("unlisted team = %v", err)
	}
	if _, err := p.Permalink(context.Background(), "other", event); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("wrong key = %v", err)
	}
}

func TestLinearEndToEnd(t *testing.T) {
	fake, server := newFakeLinear(t)
	defer server.Close()
	p := newFakeProvider(t, fake, server)
	ctx := context.Background()

	records := receive(t, p, fixture(t, "comment_created.json"))
	if len(records) != 1 || records[0].Event.Thread != "ENG:"+testIssueID {
		t.Fatalf("inbound = %+v", records)
	}

	threadTarget := boardsync.ResolvedTarget{Key: "linear", Platform: "linear", Channel: "ENG", Thread: "ENG:" + testIssueID, Kind: boardsync.TargetThread}
	posted, err := p.Post(ctx, "linear", threadTarget, boardsync.OutboundMessage{Markdown: "**ada**: reply"})
	if err != nil || posted.Message != "comment-9" || posted.Thread != "ENG:"+testIssueID {
		t.Fatalf("comment post = %+v, %v", posted, err)
	}
	thread := boardsync.ThreadRef{Key: "linear", Platform: "linear", Channel: "ENG", Thread: "ENG:" + testIssueID, Subthread: "ENG:" + testIssueID + ":c:comment-9"}
	if err := p.Edit(ctx, "linear", thread, "comment-9", boardsync.OutboundMessage{Markdown: "edited"}); err != nil {
		t.Fatalf("edit = %v", err)
	}
	if err := p.Delete(ctx, "linear", thread, "comment-9"); err != nil {
		t.Fatalf("delete = %v", err)
	}
	if err := p.React(ctx, "linear", thread, "comment-9", "thumbsup", true); err != nil {
		t.Fatalf("react = %v", err)
	}
	if err := p.React(ctx, "linear", thread, "comment-9", "thumbsup", false); !errors.Is(err, boardsync.ErrUnsupported) {
		t.Fatalf("reaction removal = %v", err)
	}

	channelTarget := boardsync.ResolvedTarget{Key: "linear", Platform: "linear", Channel: "ENG", Kind: boardsync.TargetChannel}
	root, err := p.Post(ctx, "linear", channelTarget, boardsync.OutboundMessage{Markdown: "**ada**: Checkout retry\n\nRetries."})
	if err != nil || root.Message != linear.IssueRootMessage || root.Thread != "ENG:issue-new" {
		t.Fatalf("root post = %+v, %v", root, err)
	}
	rootThread := boardsync.ThreadRef{Key: "linear", Platform: "linear", Channel: "ENG", Thread: root.Thread}
	if err := p.Edit(ctx, "linear", rootThread, linear.IssueRootMessage, boardsync.OutboundMessage{Markdown: "new description"}); err != nil {
		t.Fatalf("root edit = %v", err)
	}
	if err := p.Delete(ctx, "linear", rootThread, linear.IssueRootMessage); err != nil {
		t.Fatalf("root delete = %v", err)
	}
	if err := p.React(ctx, "linear", rootThread, linear.IssueRootMessage, "thumbsup", true); err != nil {
		t.Fatalf("root react = %v", err)
	}

	draft, err := p.FetchRoot(ctx, "linear", "ENG:"+testIssueID)
	if err != nil || draft.Labels["issue"] != "ENG-123" || draft.Labels["state"] != "started" {
		t.Fatalf("fetch root = %+v, %v", draft, err)
	}

	link, err := p.Permalink(ctx, "linear", records[0].Event)
	if err != nil || link != testCommentURL {
		t.Fatalf("permalink = %q, %v", link, err)
	}

	team, err := p.ReadSource(ctx, "linear", "ENG", linear.ReadOptions{})
	if err != nil || team.Kind != "team" || len(team.Messages) != 1 {
		t.Fatalf("team read = %+v, %v", team, err)
	}
	issueRead, err := p.ReadSource(ctx, "linear", "ENG-123", linear.ReadOptions{})
	if err != nil || issueRead.Kind != "issue" || len(issueRead.Messages) != 2 {
		t.Fatalf("issue read = %+v, %v", issueRead, err)
	}

	for _, fragment := range []string{"mutation CommentCreate", "mutation CommentUpdate", "mutation CommentDelete",
		"mutation ReactionCreate", "mutation IssueCreate", "mutation IssueUpdate", "query CommentURL", "query ReadTeam", "query ReadIssue"} {
		if fake.callsContaining(fragment) == 0 {
			t.Fatalf("end to end missed %s; calls = %v", fragment, fake.calls)
		}
	}
}
