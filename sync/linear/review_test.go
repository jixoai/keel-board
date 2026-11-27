package linear_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/linear"
)

func TestCommentAuthorUsesEnvelopeActor(t *testing.T) {
	p, _ := commentServer(t, "ENG", false)
	body := []byte(strings.ReplaceAll(string(fixture(t, "comment_created.json")), `"name": "julian"`, `"name": "ada"`))
	event := receive(t, p, body)[0].Event
	if event.Author.Handle != "ada" {
		t.Fatalf("author = %+v", event.Author)
	}
}

func TestNonIssueCommentIgnored(t *testing.T) {
	body := []byte(`{"action":"create","type":"Comment","data":{"id":"comment-p1","body":"on a project","projectId":"project-1","userId":"user-2"},"actor":{"id":"user-2","name":"julian","type":"user"},"webhookTimestamp":0}`)
	intake, err := newProvider(t, "").Receive(context.Background(), "linear", request(t, body))
	if err != nil || len(intake.Records) != 0 {
		t.Fatalf("non-issue comment = %+v, %v", intake, err)
	}
}

func TestLegacySignatureHeader(t *testing.T) {
	body := fixture(t, "issue_created.json")
	header := http.Header{}
	header.Set(linear.LegacySignatureHeader, linear.Sign([]byte(testSecret), body))
	header.Set(linear.DeliveryHeader, "legacy-1")
	req := &boardsync.WebhookRequest{Method: http.MethodPost, Header: header, Body: body}
	intake, err := newProvider(t, "").Receive(context.Background(), "linear", req)
	if err != nil || len(intake.Records) != 1 {
		t.Fatalf("legacy header = %+v, %v", intake, err)
	}
}

func TestEmptySecretRejected(t *testing.T) {
	_, err := linear.New(linear.Config{
		Key: "linear", APIToken: "lin_test_token", TeamKeys: []string{"ENG"}, BotUserID: "app-1",
	})
	if !errors.Is(err, boardsync.ErrUnauthorized) {
		t.Fatalf("empty secret = %v", err)
	}
	if err := linear.VerifySignature(nil, "00", []byte("{}")); !errors.Is(err, boardsync.ErrUnauthorized) {
		t.Fatalf("verify empty secret = %v", err)
	}
}

func TestBotUserIDRequired(t *testing.T) {
	_, err := linear.New(linear.Config{
		Key: "linear", WebhookSecret: testSecret, APIToken: "lin_test_token", TeamKeys: []string{"ENG"},
	})
	if !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("missing bot id = %v", err)
	}
}

func TestInvalidTokenScheme(t *testing.T) {
	_, err := linear.New(linear.Config{
		Key: "linear", WebhookSecret: testSecret, APIToken: "lin_test_token",
		TeamKeys: []string{"ENG"}, BotUserID: "app-1", TokenScheme: "basic",
	})
	if !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("token scheme = %v", err)
	}
}

func requestWithHeader(t *testing.T, body []byte, name, value string) *boardsync.WebhookRequest {
	t.Helper()
	req := request(t, body)
	req.Header.Set(name, value)
	return req
}

func TestTimestampHeaderFallback(t *testing.T) {
	p := newProvider(t, "")
	body := fixture(t, "issue_created.json")
	now := strconv.FormatInt(time.Now().UnixMilli(), 10)
	if _, err := p.Receive(context.Background(), "linear", requestWithHeader(t, body, linear.TimestampHeader, now)); err != nil {
		t.Fatalf("fresh header = %v", err)
	}
	stale := strconv.FormatInt(time.Now().Add(-10*time.Minute).UnixMilli(), 10)
	if _, err := p.Receive(context.Background(), "linear", requestWithHeader(t, body, linear.TimestampHeader, stale)); !errors.Is(err, boardsync.ErrUnauthorized) {
		t.Fatalf("stale header = %v", err)
	}
	if _, err := p.Receive(context.Background(), "linear", request(t, body)); err != nil {
		t.Fatalf("zero timestamps should never expire: %v", err)
	}
}

func TestDescriptionUpdateRetitles(t *testing.T) {
	body := issueUpdateBody(`"description":"new","state":{"type":"started","name":"In Progress"}`)
	body = []byte(strings.Replace(string(body), `"updatedFrom":{"stateId":"state-0"}`,
		`"updatedFrom":{"description":"old"}`, 1))
	event := receive(t, newProvider(t, ""), body)[0].Event
	if !event.Lifecycle.Retitled {
		t.Fatal("a description change should retitle")
	}
}

func TestCommentEditDeleteDedupe(t *testing.T) {
	p, _ := commentServer(t, "ENG", false)
	first := receive(t, p, fixture(t, "comment_updated.json"))[0]
	second := receive(t, p, fixture(t, "comment_updated.json"))[0]
	if first.Dedupe != second.Dedupe {
		t.Fatalf("redelivered edit changed identity: %q != %q", first.Dedupe, second.Dedupe)
	}
	changed := []byte(strings.ReplaceAll(string(fixture(t, "comment_updated.json")),
		`"updatedAt": "2026-01-02T05:10:00Z"`, `"updatedAt": "2026-01-02T05:11:00Z"`))
	if receive(t, p, changed)[0].Dedupe == first.Dedupe {
		t.Fatal("a later edit reused identity")
	}
	one := receive(t, p, fixture(t, "comment_removed.json"))[0]
	two := receive(t, p, fixture(t, "comment_removed.json"))[0]
	if one.Dedupe != two.Dedupe {
		t.Fatal("redelivered delete changed identity")
	}
}

func TestOwnIssueCreateChecksCreator(t *testing.T) {
	body := []byte(strings.Replace(string(fixture(t, "issue_created.json")),
		`"creator": { "id": "user-1"`, `"creator": { "id": "app-1"`, 1))
	records := receive(t, newProvider(t, ""), body)
	if len(records) != 1 || records[0].Event.Lifecycle.CreateRoot {
		t.Fatalf("own creator opened a root: %+v", records)
	}
}

func TestOwnIssueUpdateDoesNotCreateRoot(t *testing.T) {
	body := []byte(strings.ReplaceAll(string(fixture(t, "issue_updated.json")), `"id": "user-1"`, `"id": "app-1"`))
	body = []byte(strings.ReplaceAll(string(body), `"type": "user"`, `"type": "application"`))
	records := receive(t, newProvider(t, ""), body)
	if len(records) != 1 || records[0].Event.Lifecycle.CreateRoot {
		t.Fatalf("own update opened a root: %+v", records)
	}
}

func TestOwnCommentUpdateSkipped(t *testing.T) {
	p, calls := commentServer(t, "ENG", false)
	body := []byte(strings.ReplaceAll(string(fixture(t, "own_comment.json")), `"action": "create"`, `"action": "update"`))
	body = []byte(strings.ReplaceAll(string(body), `"body": "the app's own comment",`, `"body": "edited",`))
	if records := receive(t, p, body); len(records) != 0 {
		t.Fatalf("own update imported: %+v", records)
	}
	if *calls != 0 {
		t.Fatalf("own update looked up its issue %d times", *calls)
	}
}

func TestCrossTeamFetchRootAndPermalinkRefused(t *testing.T) {
	server := graphServer(t, func(q string, vars map[string]any) any {
		return map[string]any{"issue": map[string]any{
			"id": testIssueID, "identifier": "OPS-1", "url": testIssueURL,
			"team": map[string]any{"key": "OPS"},
		}}
	})
	defer server.Close()
	p := newProviderWith(t, server.URL, func(cfg *linear.Config) { cfg.TeamKeys = []string{"ENG", "OPS"} })
	ctx := context.Background()
	if _, err := p.FetchRoot(ctx, "linear", "ENG:"+testIssueID); !errors.Is(err, boardsync.ErrNotFound) {
		t.Fatalf("cross-team root = %v", err)
	}
	event := boardsync.Event{Key: "linear", Platform: "linear", Kind: boardsync.KindLifecycle, Thread: "ENG:" + testIssueID}
	if _, err := p.Permalink(ctx, "linear", event); !errors.Is(err, boardsync.ErrNotFound) {
		t.Fatalf("cross-team permalink = %v", err)
	}
}

func TestMissingTeamFetchRootAndPermalinkRefused(t *testing.T) {
	server := graphServer(t, func(q string, vars map[string]any) any {
		return map[string]any{"issue": map[string]any{
			"id": testIssueID, "identifier": "", "url": testIssueURL,
		}}
	})
	defer server.Close()
	p := newProviderWith(t, server.URL, func(cfg *linear.Config) { cfg.TeamKeys = []string{"ENG", "OPS"} })
	ctx := context.Background()
	if _, err := p.FetchRoot(ctx, "linear", "ENG:"+testIssueID); !errors.Is(err, boardsync.ErrNotFound) {
		t.Fatalf("missing-team root = %v", err)
	}
	event := boardsync.Event{Key: "linear", Platform: "linear", Kind: boardsync.KindLifecycle, Thread: "ENG:" + testIssueID}
	if _, err := p.Permalink(ctx, "linear", event); !errors.Is(err, boardsync.ErrNotFound) {
		t.Fatalf("missing-team permalink = %v", err)
	}
}

func TestRawAndTextLimits(t *testing.T) {
	long := strings.Repeat("a", 300<<10)
	body := []byte(`{"action":"update","type":"Issue","data":{"id":"` + testIssueID + `","identifier":"ENG-123","title":"t","description":"` + long + `","team":{"key":"ENG"},"state":{"type":"started"}},"actor":{"id":"user-1","name":"ada","type":"user"},"webhookTimestamp":0}`)
	record := receive(t, newProvider(t, ""), body)[0]
	if len(record.Event.Raw) > linear.MaxRawBytes {
		t.Fatalf("raw = %d bytes", len(record.Event.Raw))
	}
	var note struct {
		Truncated bool `json:"truncated"`
	}
	if err := json.Unmarshal(record.Event.Raw, &note); err != nil || !note.Truncated {
		t.Fatalf("raw note = %s, %v", record.Event.Raw, err)
	}
	if got := len([]rune(record.Event.Lifecycle.Body)); got > linear.MaxTextChars {
		t.Fatalf("description = %d runes", got)
	}

	comment := []byte(`{"action":"create","type":"Comment","data":{"id":"c1","body":"` + strings.Repeat("b", linear.MaxCommentChars+50) + `","issueId":"` + testIssueID + `","userId":"user-2"},"actor":{"id":"user-2","name":"u","type":"user"},"webhookTimestamp":0}`)
	cp, _ := commentServer(t, "ENG", false)
	event := receive(t, cp, comment)[0].Event
	if got := len([]rune(event.Text)); got > linear.MaxCommentChars {
		t.Fatalf("comment = %d runes", got)
	}
}

func TestLabelKeyAndValueClamp(t *testing.T) {
	name := strings.Repeat("A", 300) + `\u0001`
	body := []byte(strings.ReplaceAll(string(fixture(t, "issue_created.json")), `"name": "Bug"`, `"name": "`+name+`"`))
	event := receive(t, newProvider(t, ""), body)[0].Event
	found := false
	for key, value := range event.Lifecycle.Labels {
		if strings.HasPrefix(key, "tag.") {
			found = true
			if len(key) > 64 || len([]rune(value)) > 256 || strings.ContainsRune(value, '\u0001') {
				t.Fatalf("label %q = %q (len %d)", key, value, len([]rune(value)))
			}
		}
	}
	if !found {
		t.Fatalf("labels = %+v", event.Lifecycle.Labels)
	}
}

func TestPlatformChecks(t *testing.T) {
	p := newProvider(t, "")
	ctx := context.Background()
	target := boardsync.ResolvedTarget{Key: "linear", Platform: "slack", Channel: "ENG", Kind: boardsync.TargetChannel}
	if _, err := p.Post(ctx, "linear", target, boardsync.OutboundMessage{Markdown: "x"}); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("post platform = %v", err)
	}
	thread := boardsync.ThreadRef{Key: "linear", Platform: "slack", Channel: "ENG", Thread: "ENG:" + testIssueID}
	if err := p.Edit(ctx, "linear", thread, "comment-1", boardsync.OutboundMessage{Markdown: "x"}); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("edit platform = %v", err)
	}
	if err := p.Delete(ctx, "linear", thread, "comment-1"); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("delete platform = %v", err)
	}
	if err := p.React(ctx, "linear", thread, "comment-1", "x", true); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("react platform = %v", err)
	}
}

func TestAuthSchemes(t *testing.T) {
	for _, test := range []struct {
		name   string
		token  string
		scheme string
		want   string
	}{
		{"auto api key", "lin_api_abc", "", "lin_api_abc"},
		{"auto bearer", "oauth-token", "", "Bearer oauth-token"},
		{"explicit api key", "abc", "api_key", "abc"},
		{"explicit bearer", "abc", "bearer", "Bearer abc"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var got string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got = r.Header.Get("Authorization")
				_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"issue": map[string]any{
					"id": testIssueID, "identifier": "ENG-123", "team": map[string]any{"key": "ENG"},
				}}})
			}))
			defer server.Close()
			p := newProviderWith(t, server.URL, func(cfg *linear.Config) {
				cfg.APIToken = boardsync.Secret(test.token)
				cfg.TokenScheme = test.scheme
			})
			if _, err := p.ResolveTarget(context.Background(), "linear", testIssueID); err != nil {
				t.Fatalf("resolve: %v", err)
			}
			if got != test.want {
				t.Fatalf("authorization = %q, want %q", got, test.want)
			}
		})
	}
}

func TestProjectUUIDReadsByID(t *testing.T) {
	fake, server := newFakeLinear(t)
	defer server.Close()
	p := newProvider(t, server.URL)
	if _, err := p.ReadSource(context.Background(), "linear", "project:"+testIssueID, linear.ReadOptions{}); err != nil {
		t.Fatalf("project by id: %v", err)
	}
	if fake.callsContaining("query ReadProject(") != 1 {
		t.Fatalf("calls = %v", fake.calls)
	}
	if fake.callsContaining("query ReadProjectSlug") != 0 {
		t.Fatalf("a UUID went through slug: %v", fake.calls)
	}
	if _, err := p.ReadSource(context.Background(), "linear", "https://linear.app/acme/project/checkout-"+testIssueID, linear.ReadOptions{}); err != nil {
		t.Fatalf("project URL by id: %v", err)
	}
	if fake.callsContaining("query ReadProject(") != 2 || fake.callsContaining("query ReadProjectSlug") != 0 {
		t.Fatalf("UUID URL calls = %v", fake.calls)
	}
}
