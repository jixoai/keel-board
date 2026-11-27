package slack_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	boardsync "github.com/jixoai/keel-board/sync"
)

func TestPostMentionsScopeToTheKeyAndSkipCode(t *testing.T) {
	var got url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		got = r.PostForm
		_, _ = w.Write([]byte(`{"ok":true,"ts":"1727000009.000100"}`))
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	text := "hi @[slack:user:U1], `@[slack:user:U2]`, @[acme:user:U3]"
	if _, err := p.Post(context.Background(), "slack", postTarget(true), boardsync.OutboundMessage{Markdown: text}); err != nil {
		t.Fatalf("Post: %v", err)
	}
	body := formBody(got)
	if !strings.Contains(body, "<@U1>") {
		t.Fatalf("own mention not native: %q", body)
	}
	if !strings.Contains(body, "`@[slack:user:U2]`") {
		t.Fatalf("code mention rewritten: %q", body)
	}
	if !strings.Contains(body, "@[acme:user:U3]") {
		t.Fatalf("foreign mention rewritten: %q", body)
	}
}
