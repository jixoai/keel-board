package linear_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
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
	return requestWith(t, body, "delivery-1")
}

func requestWith(t *testing.T, body []byte, delivery string) *boardsync.WebhookRequest {
	t.Helper()
	header := http.Header{}
	header.Set(linear.SignatureHeader, linear.Sign([]byte(testSecret), body))
	if delivery != "" {
		header.Set(linear.DeliveryHeader, delivery)
	}
	header.Set("Content-Type", "application/json")
	return &boardsync.WebhookRequest{Method: http.MethodPost, Path: "/webhooks/linear", Header: header, Body: body}
}

func newProvider(t *testing.T, api string) *linear.Provider {
	t.Helper()
	return newProviderWith(t, api, nil)
}

func newProviderWith(t *testing.T, api string, mutate func(*linear.Config)) *linear.Provider {
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
	if mutate != nil {
		mutate(&cfg)
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
	if err := linear.VerifySignature([]byte(testSecret), strings.ToUpper(signature), body); err != nil {
		t.Fatalf("uppercase hex: %v", err)
	}
	if err := linear.VerifySignature([]byte("wrong"), signature, body); !errors.Is(err, boardsync.ErrUnauthorized) {
		t.Fatalf("wrong secret = %v", err)
	}
	if err := linear.VerifySignature([]byte(testSecret), "not-hex", body); !errors.Is(err, boardsync.ErrUnauthorized) {
		t.Fatalf("malformed = %v", err)
	}
	if err := linear.VerifySignature([]byte(testSecret), signature[:len(signature)-2], body); !errors.Is(err, boardsync.ErrUnauthorized) {
		t.Fatalf("truncated = %v", err)
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
	for _, prefix := range []string{"tag.", "assignee."} {
		if !contains(lc.Owns, prefix) {
			t.Fatalf("owns %v is missing %s", lc.Owns, prefix)
		}
	}
	if len(lc.Unset) != 0 {
		t.Fatalf("unset = %v", lc.Unset)
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
	if event.Lifecycle.Labels["priority"] != "high" {
		t.Fatalf("labels = %+v", event.Lifecycle.Labels)
	}
	if !contains(event.Lifecycle.Unset, "project") {
		t.Fatalf("unset = %v", event.Lifecycle.Unset)
	}
	for _, prefix := range []string{"tag.", "assignee."} {
		if !contains(event.Lifecycle.Owns, prefix) {
			t.Fatalf("owns %v is missing %s", event.Lifecycle.Owns, prefix)
		}
	}
}

func TestStateTypes(t *testing.T) {
	for _, state := range []string{"triage", "backlog", "unstarted", "started", "completed", "canceled"} {
		t.Run(state, func(t *testing.T) {
			body := issueUpdateBody(`"state":{"id":"state-1","name":"Whatever","type":"` + state + `"}`)
			event := receive(t, newProvider(t, ""), body)[0].Event
			if got := event.Lifecycle.Labels["state"]; got != state {
				t.Fatalf("state = %q, want %q", got, state)
			}
		})
	}
	// A workflow state without a type falls back to its lowercased name.
	body := issueUpdateBody(`"state":{"id":"state-1","name":"In Review"}`)
	if got := receive(t, newProvider(t, ""), body)[0].Event.Lifecycle.Labels["state"]; got != "in review" {
		t.Fatalf("state name fallback = %q", got)
	}
}

func TestUnknownPropertiesUnset(t *testing.T) {
	body := issueUpdateBody(`"state":null,"priority":0,"project":null,"labels":[],"assignee":null`)
	event := receive(t, newProvider(t, ""), body)[0].Event
	for _, key := range []string{"state", "priority", "project"} {
		if !contains(event.Lifecycle.Unset, key) {
			t.Fatalf("unset %v is missing %s", event.Lifecycle.Unset, key)
		}
		if _, exists := event.Lifecycle.Labels[key]; exists {
			t.Fatalf("labels %+v still has %s", event.Lifecycle.Labels, key)
		}
	}
}

func TestOwnIssueDoesNotCreateRoot(t *testing.T) {
	body := []byte(strings.ReplaceAll(string(fixture(t, "issue_created.json")), `"type": "user"`, `"type": "application"`))
	body = []byte(strings.ReplaceAll(string(body), `"id": "user-1"`, `"id": "app-1"`))
	records := receive(t, newProvider(t, ""), body)
	if len(records) != 1 {
		t.Fatalf("records = %+v", records)
	}
	if records[0].Event.Lifecycle.CreateRoot {
		t.Fatal("the app's own issue must not open a second board root")
	}
	if !records[0].Event.Author.Bot {
		t.Fatalf("author = %+v", records[0].Event.Author)
	}
}

func TestAutomationUpdateDoesNotCreateRoot(t *testing.T) {
	body := []byte(strings.ReplaceAll(string(fixture(t, "issue_updated.json")), `"type": "user"`, `"type": "application"`))
	if records := receive(t, newProvider(t, ""), body); records[0].Event.Lifecycle.CreateRoot {
		t.Fatal("an application's edit must not open a root")
	}
	archived := []byte(strings.ReplaceAll(string(fixture(t, "issue_updated.json")), `"archivedAt": null`, `"archivedAt": "2026-01-02T04:00:00Z"`))
	if records := receive(t, newProvider(t, ""), archived); records[0].Event.Lifecycle.CreateRoot {
		t.Fatal("an archived issue's update must not open a root")
	}
}

func TestDuplicateDelivery(t *testing.T) {
	p, _ := commentServer(t, "ENG", false)
	body := fixture(t, "comment_created.json")
	first := receive(t, p, body)
	second, err := p.Receive(context.Background(), "linear", requestWith(t, body, "delivery-2"))
	if err != nil {
		t.Fatal(err)
	}
	if first[0].Dedupe != second.Records[0].Dedupe {
		t.Fatalf("a redelivery changed identity: %q != %q", first[0].Dedupe, second.Records[0].Dedupe)
	}

	// Without Linear-Delivery, identity still comes from the payload, not the
	// subscription's webhookId: two edits of one issue are two occurrences.
	update := fixture(t, "issue_updated.json")
	one, err := p.Receive(context.Background(), "linear", requestWith(t, update, ""))
	if err != nil {
		t.Fatal(err)
	}
	two, err := p.Receive(context.Background(), "linear", requestWith(t, update, ""))
	if err != nil {
		t.Fatal(err)
	}
	if one.Records[0].Dedupe != two.Records[0].Dedupe {
		t.Fatal("the same payload without a delivery header changed identity")
	}
	changed := []byte(strings.ReplaceAll(string(update), `"title": "Checkout retry v2"`, `"title": "Checkout retry v3"`))
	three, err := p.Receive(context.Background(), "linear", requestWith(t, changed, ""))
	if err != nil {
		t.Fatal(err)
	}
	if one.Records[0].Dedupe == three.Records[0].Dedupe {
		t.Fatal("a different payload reused the fallback identity")
	}
}

func TestUnsignedMalformedBody(t *testing.T) {
	body := []byte(`{`)
	header := http.Header{}
	header.Set(linear.SignatureHeader, "deadbeef")
	req := &boardsync.WebhookRequest{Method: http.MethodPost, Header: header, Body: body}
	_, err := newProvider(t, "").Receive(context.Background(), "linear", req)
	if !errors.Is(err, boardsync.ErrUnauthorized) {
		t.Fatalf("unsigned malformed body = %v", err)
	}
}

func TestStaleTimestampRefused(t *testing.T) {
	body := []byte(strings.ReplaceAll(string(fixture(t, "issue_created.json")), `"webhookTimestamp": 0`,
		`"webhookTimestamp": `+strconv.FormatInt(time.Now().Add(-10*time.Minute).UnixMilli(), 10)))
	_, err := newProvider(t, "").Receive(context.Background(), "linear", request(t, body))
	if !errors.Is(err, boardsync.ErrUnauthorized) {
		t.Fatalf("stale webhook = %v", err)
	}
}

func TestBodyLimit(t *testing.T) {
	p := newProviderWith(t, "", func(cfg *linear.Config) { cfg.MaxBodyBytes = 16 })
	_, err := p.Receive(context.Background(), "linear", request(t, fixture(t, "issue_created.json")))
	if !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("oversized body = %v", err)
	}
}

func commentServer(t *testing.T, teamKey string, refOnly bool) (*linear.Provider, *int) {
	t.Helper()
	calls := 0
	server := graphServer(t, func(q string, vars map[string]any) any {
		calls++
		switch {
		case strings.Contains(q, "query IssueRefByID"):
			if vars["id"] != testIssueID {
				t.Fatalf("issue ref id = %+v", vars)
			}
			issue := map[string]any{"id": testIssueID}
			if !refOnly {
				issue["identifier"] = "ENG-123"
				issue["team"] = map[string]any{"key": teamKey}
			}
			return map[string]any{"issue": issue}
		case strings.Contains(q, "query IssueByID"):
			return map[string]any{"issue": map[string]any{
				"id": testIssueID, "identifier": "ENG-123",
				"team": map[string]any{"key": teamKey},
			}}
		default:
			t.Fatalf("unexpected query %q", q)
			return nil
		}
	})
	t.Cleanup(server.Close)
	return newProvider(t, server.URL), &calls
}

func TestCommentUnknownTeamDropped(t *testing.T) {
	p, _ := commentServer(t, "OTHER", false)
	if records := receive(t, p, fixture(t, "comment_created.json")); len(records) != 0 {
		t.Fatalf("records = %+v", records)
	}
}

func TestCommentParentGroupsThread(t *testing.T) {
	p, _ := commentServer(t, "ENG", false)
	body := []byte(strings.ReplaceAll(string(fixture(t, "comment_created.json")), `"id": "comment-1",`,
		`"id": "comment-2", "parentId": "comment-1",`))
	event := receive(t, p, body)[0].Event
	if event.Message != "comment-2" {
		t.Fatalf("message = %s", event.Message)
	}
	if event.Subthread != "ENG:"+testIssueID+":c:comment-1" {
		t.Fatalf("subthread = %s", event.Subthread)
	}
}

func TestCommentWithoutIssueFetchesIt(t *testing.T) {
	p, calls := commentServer(t, "ENG", false)
	event := receive(t, p, fixture(t, "comment_created.json"))[0].Event
	if event.Channel != "ENG" || event.Thread != "ENG:"+testIssueID {
		t.Fatalf("event = %+v", event)
	}
	if *calls != 1 {
		t.Fatalf("issue lookups = %d, want one", *calls)
	}
}

func TestCommentReferenceWithoutTeamRefetches(t *testing.T) {
	p, calls := commentServer(t, "ENG", true)
	event := receive(t, p, fixture(t, "comment_created.json"))[0].Event
	if event.Channel != "ENG" || event.Thread != "ENG:"+testIssueID {
		t.Fatalf("event = %+v", event)
	}
	if *calls != 2 {
		t.Fatalf("issue lookups = %d, want a ref then a full issue", *calls)
	}
}

func TestCommentOwnUserIDNeedsNoLookup(t *testing.T) {
	p, calls := commentServer(t, "ENG", false)
	if records := receive(t, p, fixture(t, "own_comment.json")); len(records) != 0 {
		t.Fatalf("own comment imported: %+v", records)
	}
	if *calls != 0 {
		t.Fatalf("own comment looked up its issue %d times", *calls)
	}
}

func TestTeamAllowlist(t *testing.T) {
	body := []byte(strings.ReplaceAll(string(fixture(t, "issue_created.json")), `"key": "ENG"`, `"key": "OTHER"`))
	if records := receive(t, newProvider(t, ""), body); len(records) != 0 {
		t.Fatalf("records = %+v", records)
	}
}

func issueUpdateBody(fields string) []byte {
	return []byte(`{"action":"update","type":"Issue","data":{"id":"` + testIssueID + `","identifier":"ENG-123","title":"Checkout retry",` +
		fields + `},"actor":{"id":"user-1","name":"ada","type":"user"},"updatedFrom":{"stateId":"state-0"},"webhookTimestamp":0}`)
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestCommentCreated(t *testing.T) {
	p, _ := commentServer(t, "ENG", false)
	event := receive(t, p, fixture(t, "comment_created.json"))[0].Event
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
	p, _ := commentServer(t, "ENG", false)
	updated := receive(t, p, fixture(t, "comment_updated.json"))[0].Event
	if updated.Kind != boardsync.KindEdited || updated.Text != "I reproduced this on staging and in production." {
		t.Fatalf("updated = %+v", updated)
	}
	removed := receive(t, p, fixture(t, "comment_removed.json"))[0].Event
	if removed.Kind != boardsync.KindDeleted || removed.Message != "comment-1" {
		t.Fatalf("removed = %+v", removed)
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
	target := boardsync.ResolvedTarget{Key: "linear", Platform: "linear", Channel: "ENG", Thread: "ENG:" + testIssueID,
		Subthread: "ENG:" + testIssueID + ":c:comment-parent", Kind: boardsync.TargetThread}
	posted, err := p.Post(context.Background(), "linear", target, boardsync.OutboundMessage{Markdown: "**Ada**: ship it"})
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	if !strings.Contains(query, "commentCreate") || variables["issueId"] != testIssueID || variables["body"] != "**Ada**: ship it" {
		t.Fatalf("query = %q, variables = %+v", query, variables)
	}
	if variables["parentId"] != "comment-parent" {
		t.Fatalf("parentId = %+v", variables["parentId"])
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
	posted, err := p.Post(context.Background(), "linear", target, boardsync.OutboundMessage{Markdown: "**Ada**: Checkout retry\n\nRetries when the provider times out."})
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	if posted.Thread != "ENG:issue-new" || posted.Message != linear.IssueRootMessage {
		t.Fatalf("posted = %+v", posted)
	}
	if len(queries) != 2 {
		t.Fatalf("queries = %d", len(queries))
	}
}

func TestIssueRootEditDeleteReact(t *testing.T) {
	var queries []string
	server := graphServer(t, func(q string, vars map[string]any) any {
		queries = append(queries, q)
		if !strings.Contains(q, "issueUpdate") {
			t.Fatalf("unexpected query %q", q)
		}
		if vars["id"] != testIssueID || vars["description"] != "new description" {
			t.Fatalf("variables = %+v", vars)
		}
		return map[string]any{"issueUpdate": map[string]any{"success": true}}
	})
	defer server.Close()
	p := newProvider(t, server.URL)
	thread := boardsync.ThreadRef{Key: "linear", Platform: "linear", Channel: "ENG", Thread: "ENG:" + testIssueID}
	ctx := context.Background()
	if err := p.Edit(ctx, "linear", thread, linear.IssueRootMessage, boardsync.OutboundMessage{Markdown: "new description"}); err != nil {
		t.Fatalf("Edit root: %v", err)
	}
	if err := p.Delete(ctx, "linear", thread, linear.IssueRootMessage); err != nil {
		t.Fatalf("Delete root: %v", err)
	}
	if err := p.React(ctx, "linear", thread, linear.IssueRootMessage, "thumbsup", true); err != nil {
		t.Fatalf("React root: %v", err)
	}
	if len(queries) != 1 {
		t.Fatalf("root delete/react must not call linear; queries = %d", len(queries))
	}
}

func TestOutboundLimits(t *testing.T) {
	p := newProvider(t, "")
	thread := boardsync.ResolvedTarget{Key: "linear", Platform: "linear", Channel: "ENG", Thread: "ENG:" + testIssueID, Kind: boardsync.TargetThread}
	tooLong := boardsync.OutboundMessage{Markdown: strings.Repeat("a", linear.MaxCommentChars+1)}
	if _, err := p.Post(context.Background(), "linear", thread, tooLong); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("long comment = %v", err)
	}
	withFile := boardsync.OutboundMessage{Markdown: "hi", Files: []boardsync.OutboundFile{{Filename: "a.txt"}}}
	if _, err := p.Post(context.Background(), "linear", thread, withFile); !errors.Is(err, boardsync.ErrUnsupported) {
		t.Fatalf("file post = %v", err)
	}
	empty := boardsync.OutboundMessage{Markdown: "  "}
	if _, err := p.Post(context.Background(), "linear", thread, empty); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("empty post = %v", err)
	}
}

func TestResolveTargetStrict(t *testing.T) {
	p := newProvider(t, "")
	ctx := context.Background()
	if target, err := p.ResolveTarget(ctx, "linear", "eng"); err != nil || target.Channel != "ENG" {
		t.Fatalf("lowercase team = %+v, %v", target, err)
	}
	if _, err := p.ResolveTarget(ctx, "linear", "ENG:"+testIssueID+":c:comment-1"); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("comment sub-thread target = %v", err)
	}
	if _, err := p.ResolveTarget(ctx, "other", "ENG"); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("wrong key = %v", err)
	}
}

func TestFailureClassification(t *testing.T) {
	for _, test := range []struct {
		status int
		want   error
	}{
		{http.StatusUnauthorized, boardsync.ErrUnauthorized},
		{http.StatusForbidden, boardsync.ErrUnauthorized},
		{http.StatusNotFound, boardsync.ErrNotFound},
		{http.StatusBadRequest, boardsync.ErrBadRequest},
		{http.StatusUnprocessableEntity, boardsync.ErrBadRequest},
		{http.StatusTooManyRequests, boardsync.ErrRateLimited},
		{http.StatusRequestTimeout, nil},
		{http.StatusConflict, nil},
		{http.StatusTooEarly, nil},
	} {
		t.Run(strconv.Itoa(test.status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(test.status)
			}))
			defer server.Close()
			_, err := newProvider(t, server.URL).ResolveTarget(context.Background(), "linear", testIssueID)
			if test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("status %d = %v, want %v", test.status, err, test.want)
			}
			permanent := test.status == http.StatusUnauthorized || test.status == http.StatusForbidden ||
				test.status == http.StatusNotFound || test.status == http.StatusBadRequest ||
				test.status == http.StatusUnprocessableEntity
			if boardsync.IsPermanent(err) != permanent {
				t.Fatalf("status %d permanent = %v", test.status, boardsync.IsPermanent(err))
			}
		})
	}
	t.Run("retry after", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Retry-After", "7")
			w.WriteHeader(http.StatusTooManyRequests)
		}))
		defer server.Close()
		_, err := newProvider(t, server.URL).ResolveTarget(context.Background(), "linear", testIssueID)
		if !errors.Is(err, boardsync.ErrRateLimited) || linear.RetryAfterSeconds(err) != 7 {
			t.Fatalf("retry-after = %v, %d", err, linear.RetryAfterSeconds(err))
		}
	})
	t.Run("server", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadGateway)
		}))
		defer server.Close()
		_, err := newProvider(t, server.URL).ResolveTarget(context.Background(), "linear", testIssueID)
		if err == nil || boardsync.IsPermanent(err) {
			t.Fatalf("5xx = %v, permanent = %v", err, boardsync.IsPermanent(err))
		}
	})
	for _, test := range []struct {
		code string
		want error
	}{
		{"AUTHENTICATION_ERROR", boardsync.ErrUnauthorized},
		{"RATELIMITED", boardsync.ErrRateLimited},
		{"INVALID_INPUT", boardsync.ErrBadRequest},
		{"GRAPHQL_VALIDATION_FAILED", boardsync.ErrBadRequest},
	} {
		t.Run(test.code, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{"errors": []any{
					map[string]any{"message": "nope", "extensions": map[string]any{"code": test.code}},
				}})
			}))
			defer server.Close()
			_, err := newProvider(t, server.URL).ResolveTarget(context.Background(), "linear", testIssueID)
			if !errors.Is(err, test.want) {
				t.Fatalf("%s = %v, want %v", test.code, err, test.want)
			}
		})
	}
	t.Run("transient graphql error is retryable", func(t *testing.T) {
		for _, code := range []string{"INTERNAL_SERVER_ERROR", "SERVICE_UNAVAILABLE", "TIMEOUT"} {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{"errors": []any{
					map[string]any{"message": "later", "extensions": map[string]any{"code": code}},
				}})
			}))
			_, err := newProvider(t, server.URL).ResolveTarget(context.Background(), "linear", testIssueID)
			server.Close()
			if err == nil || boardsync.IsPermanent(err) {
				t.Fatalf("%s = %v, permanent = %v", code, err, boardsync.IsPermanent(err))
			}
		}
	})
	t.Run("unknown graphql error is permanent", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{"errors": []any{map[string]any{"message": "bad query"}}})
		}))
		defer server.Close()
		_, err := newProvider(t, server.URL).ResolveTarget(context.Background(), "linear", testIssueID)
		if !boardsync.IsPermanent(err) {
			t.Fatalf("unknown graphql error = %v", err)
		}
	})
	t.Run("failed mutation is permanent", func(t *testing.T) {
		server := graphServer(t, func(q string, vars map[string]any) any {
			return map[string]any{"commentCreate": map[string]any{"success": false}}
		})
		defer server.Close()
		target := boardsync.ResolvedTarget{Key: "linear", Platform: "linear", Channel: "ENG", Thread: "ENG:" + testIssueID, Kind: boardsync.TargetThread}
		_, err := newProvider(t, server.URL).Post(context.Background(), "linear", target, boardsync.OutboundMessage{Markdown: "hi"})
		if !boardsync.IsPermanent(err) {
			t.Fatalf("failed mutation = %v", err)
		}
	})
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
