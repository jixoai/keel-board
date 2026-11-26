package linear_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/linear"
)

const (
	testSecret  = "linear-webhook-secret"
	testTeamID  = "team-1"
	testIssueID = "11111111-1111-1111-1111-111111111111"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func request(t *testing.T, body []byte) *boardsync.WebhookRequest {
	t.Helper()
	header := http.Header{}
	header.Set(linear.SignatureHeader, linear.Sign([]byte(testSecret), body))
	header.Set(linear.DeliveryHeader, "delivery-1")
	header.Set("Content-Type", "application/json")
	return &boardsync.WebhookRequest{Method: http.MethodPost, Path: "/webhooks/linear", Header: header, Body: body}
}

func newProvider(t *testing.T, api string) *linear.Provider {
	t.Helper()
	cfg := linear.Config{
		Key:           "linear",
		WebhookSecret: testSecret,
		APIToken:      "lin_test_token",
		TeamKeys:      []string{"ENG"},
		BotUserID:     "app-1",
	}
	if api != "" {
		cfg.APIBaseURL = api
	}
	p, err := linear.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func receive(t *testing.T, p *linear.Provider, body []byte) []boardsync.Record {
	t.Helper()
	intake, err := p.Receive(context.Background(), "linear", request(t, body))
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	return intake.Records
}

func TestSignature(t *testing.T) {
	body := []byte(`{"type":"Issue"}`)
	signature := linear.Sign([]byte(testSecret), body)
	if err := linear.VerifySignature([]byte(testSecret), signature, body); err != nil {
		t.Fatalf("valid: %v", err)
	}
	if err := linear.VerifySignature([]byte("wrong"), signature, body); !errors.Is(err, boardsync.ErrUnauthorized) {
		t.Fatalf("wrong secret = %v", err)
	}
	if err := linear.VerifyTimestamp(0, time.Now(), linear.DefaultTolerance); err != nil {
		t.Fatalf("zero timestamp: %v", err)
	}
	if err := linear.VerifyTimestamp(time.Now().Add(-10*time.Minute).UnixMilli(), time.Now(), linear.DefaultTolerance); !errors.Is(err, boardsync.ErrUnauthorized) {
		t.Fatalf("stale timestamp = %v", err)
	}
}

func TestIssueCreated(t *testing.T) {
	record := receive(t, newProvider(t, ""), fixture(t, "issue_created.json"))[0]
	event := record.Event
	if event.Kind != boardsync.KindLifecycle || event.Type != "issue.created" {
		t.Fatalf("event = %+v", event)
	}
	if event.Channel != "ENG" || event.Thread != "ENG:"+testIssueID {
		t.Fatalf("location = %s / %s", event.Channel, event.Thread)
	}
	lc := event.Lifecycle
	if lc == nil || !lc.CreateRoot {
		t.Fatalf("lifecycle = %+v", lc)
	}
	if lc.Title != "ENG-123: Checkout retry" || lc.URL == "" || lc.Body == "" {
		t.Fatalf("lifecycle = %+v", lc)
	}
	want := map[string]string{
		"issue": "ENG-123", "state": "started", "priority": "high",
		"project": "Checkout", "tag.bug": "Bug", "assignee.ada": "ada",
	}
	for key, value := range want {
		if lc.Labels[key] != value {
			t.Fatalf("label %s = %q, want %q (all %+v)", key, lc.Labels[key], value, lc.Labels)
		}
	}
	if !strings.HasPrefix(record.Dedupe, "lifecycle:linear:") {
		t.Fatalf("dedupe = %q", record.Dedupe)
	}
}

func TestIssueUpdated(t *testing.T) {
	event := receive(t, newProvider(t, ""), fixture(t, "issue_updated.json"))[0].Event
	if event.Type != "issue.updated" {
		t.Fatalf("type = %s", event.Type)
	}
	if !event.Lifecycle.Retitled {
		t.Fatal("a title change should retitle the root")
	}
	if !event.Lifecycle.CreateRoot {
		t.Fatal("a person's edit brings an unseen issue in")
	}
	if event.Lifecycle.Labels["state"] != "completed" {
		t.Fatalf("labels = %+v", event.Lifecycle.Labels)
	}
	if strings.Join(event.Lifecycle.Owns, ",") == "" {
		t.Fatalf("owns = %v", event.Lifecycle.Owns)
	}
}

func TestCommentCreated(t *testing.T) {
	event := receive(t, newProvider(t, ""), fixture(t, "comment_created.json"))[0].Event
	if event.Kind != boardsync.KindMessage || event.Type != boardsync.TypeComment {
		t.Fatalf("event = %+v", event)
	}
	if event.Channel != "ENG" || event.Thread != "ENG:"+testIssueID {
		t.Fatalf("location = %s / %s", event.Channel, event.Thread)
	}
	if event.Subthread != "ENG:"+testIssueID+":c:comment-1" {
		t.Fatalf("subthread = %s", event.Subthread)
	}
	if event.Author.ID != "linear:user:user-2" || event.Author.Handle != "julian" {
		t.Fatalf("author = %+v", event.Author)
	}
	if event.Text != "I reproduced this on staging." {
		t.Fatalf("text = %q", event.Text)
	}
}

func TestCommentUpdatedAndRemoved(t *testing.T) {
	updated := receive(t, newProvider(t, ""), fixture(t, "comment_updated.json"))[0].Event
	if updated.Kind != boardsync.KindEdited || updated.Text != "I reproduced this on staging and in production." {
		t.Fatalf("updated = %+v", updated)
	}
	removed := receive(t, newProvider(t, ""), fixture(t, "comment_removed.json"))[0].Event
	if removed.Kind != boardsync.KindDeleted || removed.Message != "comment-1" {
		t.Fatalf("removed = %+v", removed)
	}
}

func TestOwnCommentSkipped(t *testing.T) {
	if records := receive(t, newProvider(t, ""), fixture(t, "own_comment.json")); len(records) != 0 {
		t.Fatalf("own comment imported: %+v", records)
	}
}

func TestTeamAllowlist(t *testing.T) {
	body := []byte(strings.ReplaceAll(string(fixture(t, "issue_created.json")), `"key": "ENG"`, `"key": "OTHER"`))
	if records := receive(t, newProvider(t, ""), body); len(records) != 0 {
		t.Fatalf("records = %+v", records)
	}
}

func graphServer(t *testing.T, answer func(query string, variables map[string]any) any) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer lin_test_token" {
			t.Fatalf("authorization = %q", r.Header.Get("Authorization"))
		}
		var body struct {
			Query     string         `json:"query"`
			Variables map[string]any `json:"variables"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		data := answer(body.Query, body.Variables)
		_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
	}))
}

func TestResolveTarget(t *testing.T) {
	server := graphServer(t, func(query string, variables map[string]any) any {
		if strings.Contains(query, "issues(filter:") {
			if variables["team"] != "ENG" || variables["number"] != float64(123) {
				t.Fatalf("variables = %+v", variables)
			}
			return map[string]any{"issues": map[string]any{"nodes": []any{
				map[string]any{"id": testIssueID, "identifier": "ENG-123", "team": map[string]any{"key": "ENG"}},
			}}}
		}
		return map[string]any{}
	})
	defer server.Close()
	p := newProvider(t, server.URL)
	channel, err := p.ResolveTarget(context.Background(), "linear", "ENG")
	if err != nil || channel.Kind != boardsync.TargetChannel || channel.Channel != "ENG" {
		t.Fatalf("team target = %+v, %v", channel, err)
	}
	thread, err := p.ResolveTarget(context.Background(), "linear", "ENG-123")
	if err != nil || thread.Kind != boardsync.TargetThread || thread.Thread != "ENG:"+testIssueID {
		t.Fatalf("identifier target = %+v, %v", thread, err)
	}
	native, err := p.ResolveTarget(context.Background(), "linear", "ENG:"+testIssueID)
	if err != nil || native.Thread != "ENG:"+testIssueID {
		t.Fatalf("native target = %+v, %v", native, err)
	}
}

func TestPostComment(t *testing.T) {
	var query string
	var variables map[string]any
	server := graphServer(t, func(q string, vars map[string]any) any {
		query, variables = q, vars
		return map[string]any{"commentCreate": map[string]any{"success": true, "comment": map[string]any{"id": "comment-9"}}}
	})
	defer server.Close()
	p := newProvider(t, server.URL)
	target := boardsync.ResolvedTarget{Key: "linear", Platform: "linear", Channel: "ENG", Thread: "ENG:" + testIssueID, Kind: boardsync.TargetThread}
	posted, err := p.Post(context.Background(), "linear", target, boardsync.OutboundMessage{Markdown: "**Ada**: ship it"})
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	if !strings.Contains(query, "commentCreate") || variables["issueId"] != testIssueID || variables["body"] != "**Ada**: ship it" {
		t.Fatalf("query = %q, variables = %+v", query, variables)
	}
	if posted.Thread != "ENG:"+testIssueID || posted.Message != "comment-9" {
		t.Fatalf("posted = %+v", posted)
	}
}

func TestPostCreateIssue(t *testing.T) {
	var queries []string
	server := graphServer(t, func(q string, vars map[string]any) any {
		queries = append(queries, q)
		switch {
		case strings.Contains(q, "teams("):
			return map[string]any{"teams": map[string]any{"nodes": []any{map[string]any{"id": testTeamID, "key": "ENG"}}}}
		case strings.Contains(q, "issueCreate"):
			if vars["teamId"] != testTeamID || vars["title"] != "Checkout retry" {
				t.Fatalf("variables = %+v", vars)
			}
			return map[string]any{"issueCreate": map[string]any{"success": true, "issue": map[string]any{
				"id": "issue-new", "identifier": "ENG-124", "team": map[string]any{"key": "ENG"},
			}}}
		default:
			t.Fatalf("unexpected query %q", q)
			return nil
		}
	})
	defer server.Close()
	p := newProvider(t, server.URL)
	target := boardsync.ResolvedTarget{Key: "linear", Platform: "linear", Channel: "ENG", Kind: boardsync.TargetChannel}
	posted, err := p.Post(context.Background(), "linear", target, boardsync.OutboundMessage{Markdown: "Checkout retry\n\nRetries when the provider times out."})
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	if posted.Thread != "ENG:issue-new" || posted.Message != "issue-new" {
		t.Fatalf("posted = %+v", posted)
	}
	if len(queries) != 2 {
		t.Fatalf("queries = %d", len(queries))
	}
}

func TestEditDeleteReact(t *testing.T) {
	var queries []string
	server := graphServer(t, func(q string, vars map[string]any) any {
		queries = append(queries, q)
		switch {
		case strings.Contains(q, "commentUpdate"):
			return map[string]any{"commentUpdate": map[string]any{"success": true}}
		case strings.Contains(q, "commentDelete"):
			return map[string]any{"commentDelete": map[string]any{"success": true}}
		case strings.Contains(q, "reactionCreate"):
			return map[string]any{"reactionCreate": map[string]any{"success": true}}
		default:
			t.Fatalf("unexpected query %q", q)
			return nil
		}
	})
	defer server.Close()
	p := newProvider(t, server.URL)
	thread := boardsync.ThreadRef{Key: "linear", Platform: "linear", Channel: "ENG", Thread: "ENG:" + testIssueID, Subthread: "ENG:" + testIssueID + ":c:comment-1"}
	ctx := context.Background()
	if err := p.Edit(ctx, "linear", thread, "comment-1", boardsync.OutboundMessage{Markdown: "fixed"}); err != nil {
		t.Fatal(err)
	}
	if err := p.Delete(ctx, "linear", thread, "comment-1"); err != nil {
		t.Fatal(err)
	}
	if err := p.React(ctx, "linear", thread, "comment-1", "thumbsup", true); err != nil {
		t.Fatal(err)
	}
	if err := p.React(ctx, "linear", thread, "comment-1", "thumbsup", false); !errors.Is(err, boardsync.ErrUnsupported) {
		t.Fatalf("reaction removal = %v", err)
	}
	if len(queries) != 3 {
		t.Fatalf("queries = %d", len(queries))
	}
}

func TestFetchRoot(t *testing.T) {
	server := graphServer(t, func(q string, vars map[string]any) any {
		if !strings.Contains(q, "issue(id:") || vars["id"] != testIssueID {
			t.Fatalf("query = %q, vars = %+v", q, vars)
		}
		return map[string]any{"issue": map[string]any{
			"id": testIssueID, "identifier": "ENG-123", "number": 123,
			"title": "Checkout retry", "description": "Retries.",
			"url":      "https://linear.app/acme/issue/ENG-123/checkout-retry",
			"team":     map[string]any{"id": testTeamID, "key": "ENG"},
			"state":    map[string]any{"name": "In Progress", "type": "started"},
			"priority": 2, "priorityLabel": "High",
			"labels":  map[string]any{"nodes": []any{map[string]any{"name": "Bug"}}},
			"creator": map[string]any{"id": "user-1", "displayName": "ada"},
		}}
	})
	defer server.Close()
	p := newProvider(t, server.URL)
	draft, err := p.FetchRoot(context.Background(), "linear", "ENG:"+testIssueID)
	if err != nil {
		t.Fatalf("FetchRoot: %v", err)
	}
	if !strings.Contains(draft.Text, "ENG-123: Checkout retry") {
		t.Fatalf("text = %q", draft.Text)
	}
	if draft.Labels["issue"] != "ENG-123" || draft.Labels["state"] != "started" || draft.Labels["tag.bug"] != "Bug" {
		t.Fatalf("labels = %+v", draft.Labels)
	}
}
