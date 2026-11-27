package github_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	boardsync "github.com/jixoai/keel-board/sync"
)

func TestPostRendersOwnMentionsAndKeepsForeignOnesLiteral(t *testing.T) {
	var body map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(data, &body); err != nil {
			t.Fatal(err)
		}
		_, _ = w.Write([]byte(`{"id":9400}`))
	}))
	defer server.Close()

	p := writeProvider(t, server.URL, nil)
	text := "hi @[github:user:ada], @[slack:user:U1], and `@[github:user:inert]`"
	if _, err := p.Post(context.Background(), "github", prTarget(), boardsync.OutboundMessage{Markdown: text}); err != nil {
		t.Fatalf("Post: %v", err)
	}
	comment := body["body"]
	if !strings.Contains(comment, "@ada") {
		t.Fatalf("own mention not native: %q", comment)
	}
	if !strings.Contains(comment, "@[slack:user:U1]") {
		t.Fatalf("foreign mention rewritten: %q", comment)
	}
	if !strings.Contains(comment, "`@[github:user:inert]`") {
		t.Fatalf("code mention rewritten: %q", comment)
	}
	if strings.Contains(comment, "@[github:user:ada]") {
		t.Fatalf("own token left behind: %q", comment)
	}
}

func TestReceiveTurnsLoginsIntoExactMentions(t *testing.T) {
	p := newProvider(t, false)
	raw := string(fixture(t, "issue_comment.json"))
	raw = strings.Replace(raw, `"body": "confirmed on staging"`,
		`"body": "hi @ada, @[acme:user:U1], `+"`@ada`"+`, and @acme/team"`, 1)
	intake, err := receiveRequest(t, p, "issue_comment", "mention-1", []byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if len(intake.Records) != 1 {
		t.Fatalf("records = %+v", intake.Records)
	}
	text := intake.Records[0].Event.Text
	if !strings.Contains(text, "@[github:user:ada]") {
		t.Fatalf("login not an exact mention: %q", text)
	}
	if !strings.Contains(text, "@[acme:user:U1]") {
		t.Fatalf("foreign token rewritten: %q", text)
	}
	if !strings.Contains(text, "`@ada`") {
		t.Fatalf("code span rewritten: %q", text)
	}
	if !strings.Contains(text, "@acme/team") {
		t.Fatalf("team mention rewritten: %q", text)
	}
}
