package slack_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/slack"
)

const (
	testSecret  = "signing-secret"
	testToken   = "xoxb-test-token"
	testNowUnix = int64(1727000400)
)

func now() time.Time { return time.Unix(testNowUnix, 0).UTC() }

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func signedRequest(t *testing.T, body []byte) *boardsync.WebhookRequest {
	t.Helper()
	timestamp := "1727000400"
	return &boardsync.WebhookRequest{
		Method: http.MethodPost,
		Path:   "/webhooks/slack",
		Header: http.Header{
			"Content-Type":        {"application/json"},
			slack.TimestampHeader: {timestamp},
			slack.SignatureHeader: {slack.Sign([]byte(testSecret), timestamp, body)},
		},
		Body: body,
	}
}

func provider(t *testing.T) *slack.Provider {
	t.Helper()
	p, err := slack.New(slack.Config{
		Key:           "slack",
		SigningSecret: testSecret,
		BotToken:      testToken,
		BotUserID:     "UAPP123",
		Now:           now,
		Tolerance:     5 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func receive(t *testing.T, p *slack.Provider, body []byte) boardsync.Intake {
	t.Helper()
	intake, err := p.Receive(context.Background(), "slack", signedRequest(t, body))
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	return intake
}

func TestSignature(t *testing.T) {
	body := []byte(`{"type":"event_callback"}`)
	timestamp := "1727000400"
	signature := slack.Sign([]byte(testSecret), timestamp, body)
	if err := slack.VerifySignature([]byte(testSecret), timestamp, signature, body, now(), 5*time.Minute); err != nil {
		t.Fatalf("valid signature: %v", err)
	}
	if err := slack.VerifySignature([]byte("wrong"), timestamp, signature, body, now(), 5*time.Minute); !errors.Is(err, boardsync.ErrUnauthorized) {
		t.Fatalf("wrong secret = %v", err)
	}
	if err := slack.VerifySignature([]byte(testSecret), "1726990000", signature, body, now(), 5*time.Minute); !errors.Is(err, boardsync.ErrUnauthorized) {
		t.Fatalf("stale timestamp = %v", err)
	}
	if err := slack.VerifySignature(nil, timestamp, signature, body, now(), time.Minute); !errors.Is(err, boardsync.ErrUnauthorized) {
		t.Fatalf("missing secret = %v", err)
	}
	// Independent vector: HMAC-SHA256("v0:"+timestamp+":"+body).
	mac := hmac.New(sha256.New, []byte(testSecret))
	mac.Write([]byte("v0:" + timestamp + ":"))
	mac.Write(body)
	want := "v0=" + hex.EncodeToString(mac.Sum(nil))
	if signature != want {
		t.Fatalf("Sign = %q, want %q", signature, want)
	}
}

func TestReceiveChallenge(t *testing.T) {
	intake := receive(t, provider(t), fixture(t, "url_verification.json"))
	if intake.Response.Status != 200 || intake.Response.ContentType != "application/json" {
		t.Fatalf("response = %+v", intake.Response)
	}
	var body map[string]string
	if err := json.Unmarshal(intake.Response.Body, &body); err != nil {
		t.Fatal(err)
	}
	if body["challenge"] != "challenge-abc-123" {
		t.Fatalf("challenge = %q", body["challenge"])
	}
	if len(intake.Records) != 0 {
		t.Fatal("a challenge carries no records")
	}
}

func TestReceiveMessage(t *testing.T) {
	intake := receive(t, provider(t), fixture(t, "message.json"))
	if len(intake.Records) != 1 {
		t.Fatalf("records = %d", len(intake.Records))
	}
	record := intake.Records[0]
	event := record.Event
	if record.Dedupe == "" || event.ID != record.Dedupe {
		t.Fatalf("dedupe = %q, event id = %q", record.Dedupe, event.ID)
	}
	if event.Key != "slack" || event.Platform != "slack" || event.Kind != boardsync.KindMessage {
		t.Fatalf("event = %+v", event)
	}
	if event.Channel != "C0123ABC" || event.Thread != "C0123ABC:1727000000.123456" || event.Message != "1727000000.123456" {
		t.Fatalf("location = %s / %s / %s", event.Channel, event.Thread, event.Message)
	}
	if !event.IsRoot {
		t.Fatal("a message without thread_ts is a root")
	}
	if event.Author.ID != "slack:user:U0456ABC" {
		t.Fatalf("author = %+v", event.Author)
	}
	if !strings.Contains(event.Text, "@[slack:user:U0999XYZ]") || !strings.Contains(event.Text, "[the doc](https://example.com/doc)") {
		t.Fatalf("text = %q", event.Text)
	}
	if len(event.Raw) == 0 {
		t.Fatal("raw payload was not kept")
	}
	if err := event.Validate(); err != nil {
		t.Fatalf("normalized event: %v", err)
	}
}

func TestReceiveThreadReply(t *testing.T) {
	event := receive(t, provider(t), fixture(t, "message_thread_reply.json")).Records[0].Event
	if event.IsRoot {
		t.Fatal("a reply is not a root")
	}
	if event.Thread != "C0123ABC:1727000000.123456" || event.Message != "1727000005.000100" {
		t.Fatalf("thread = %q, message = %q", event.Thread, event.Message)
	}
}

func TestReceiveEdit(t *testing.T) {
	record := receive(t, provider(t), fixture(t, "message_changed.json")).Records[0]
	if record.Event.Kind != boardsync.KindEdited || record.Event.Type != boardsync.TypeMessageEdited {
		t.Fatalf("event = %+v", record.Event)
	}
	if record.Event.Message != "1727000000.123456" || record.Event.Text != "ship it today" {
		t.Fatalf("edit = %+v", record.Event)
	}
	if !strings.HasPrefix(record.Dedupe, "edited:slack:") {
		t.Fatalf("dedupe = %q", record.Dedupe)
	}
}

func TestReceiveDelete(t *testing.T) {
	record := receive(t, provider(t), fixture(t, "message_deleted.json")).Records[0]
	if record.Event.Kind != boardsync.KindDeleted || record.Event.Message != "1727000005.000100" {
		t.Fatalf("event = %+v", record.Event)
	}
	if record.Event.Thread != "C0123ABC:1727000000.123456" {
		t.Fatalf("thread = %q", record.Event.Thread)
	}
}

func TestReceiveReaction(t *testing.T) {
	record := receive(t, provider(t), fixture(t, "reaction_added.json")).Records[0]
	if record.Event.Kind != boardsync.KindReaction || !record.Event.Added || record.Event.Emoji != "thumbsup" {
		t.Fatalf("event = %+v", record.Event)
	}
	if record.Event.Message != "1727000000.123456" || record.Event.Author.ID != "slack:user:U0999XYZ" {
		t.Fatalf("event = %+v", record.Event)
	}
	if !strings.HasPrefix(record.Dedupe, "reaction:slack:") {
		t.Fatalf("dedupe = %q", record.Dedupe)
	}
	second := receive(t, provider(t), fixture(t, "reaction_added.json")).Records[0]
	if second.Dedupe != record.Dedupe {
		t.Fatal("a redelivered reaction got a new identity")
	}
}

func TestReceiveOwnMessageSkipped(t *testing.T) {
	intake := receive(t, provider(t), fixture(t, "own_message.json"))
	if len(intake.Records) != 0 {
		t.Fatalf("own message imported: %+v", intake.Records)
	}
}

func TestReceiveOtherBotMessage(t *testing.T) {
	record := receive(t, provider(t), fixture(t, "bot_message.json")).Records[0]
	if record.Event.Text != "checkout is down" || !record.Event.Author.Bot {
		t.Fatalf("event = %+v", record.Event)
	}
	if record.Event.Author.ID != "slack:user:B0999XYZ" {
		t.Fatalf("author = %+v", record.Event.Author)
	}
}

func TestReceiveBadSignature(t *testing.T) {
	body := fixture(t, "message.json")
	request := signedRequest(t, body)
	request.Header.Set(slack.SignatureHeader, "v0=deadbeef")
	if _, err := provider(t).Receive(context.Background(), "slack", request); !errors.Is(err, boardsync.ErrUnauthorized) {
		t.Fatalf("Receive = %v, want ErrUnauthorized", err)
	}
}

func TestResolveTarget(t *testing.T) {
	p := provider(t)
	cases := []struct {
		target  string
		channel string
		thread  string
		kind    boardsync.TargetKind
	}{
		{"C0123ABC", "C0123ABC", "", boardsync.TargetChannel},
		{"C0123ABC:1727000000.123456", "C0123ABC", "C0123ABC:1727000000.123456", boardsync.TargetThread},
		{"https://acme.slack.com/archives/C0123ABC/p1727000000123456", "C0123ABC", "C0123ABC:1727000000.123456", boardsync.TargetThread},
		{"https://acme.slack.com/archives/C0123ABC/p1727000005123456?thread_ts=1727000000.123456", "C0123ABC", "C0123ABC:1727000000.123456", boardsync.TargetThread},
	}
	for _, tc := range cases {
		got, err := p.ResolveTarget(context.Background(), "slack", tc.target)
		if err != nil {
			t.Fatalf("%s: %v", tc.target, err)
		}
		if got.Channel != tc.channel || got.Thread != tc.thread || got.Kind != tc.kind {
			t.Fatalf("%s = %+v", tc.target, got)
		}
	}
	if _, err := p.ResolveTarget(context.Background(), "slack", "not a channel"); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("bad target = %v", err)
	}
}

func TestResolveChannelByName(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/conversations.list" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer "+testToken {
			t.Fatalf("authorization = %q", r.Header.Get("Authorization"))
		}
		_, _ = w.Write([]byte(`{"ok":true,"channels":[{"id":"C0123ABC","name":"general","is_member":true}]}`))
	}))
	defer server.Close()
	p, err := slack.New(slack.Config{
		Key: "slack", SigningSecret: testSecret, BotToken: testToken, BotUserID: "UAPP123",
		APIBaseURL: server.URL, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	id, err := p.ResolveChannel(context.Background(), "slack", "#general")
	if err != nil || id != "C0123ABC" {
		t.Fatalf("ResolveChannel = %q, %v", id, err)
	}
	// The listing is cached for the next lookup.
	if _, err := p.ResolveChannel(context.Background(), "slack", "#general"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ResolveChannel(context.Background(), "slack", "#missing"); !errors.Is(err, boardsync.ErrNotFound) {
		t.Fatalf("missing channel = %v", err)
	}
}

func TestPostMessage(t *testing.T) {
	var got url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth.test":
			_, _ = w.Write([]byte(`{"ok":true,"user_id":"UAPP123","response_metadata":{"scopes":["chat:write","chat:write.customize"]}}`))
			return
		case "/chat.postMessage":
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		got = r.PostForm
		_, _ = w.Write([]byte(`{"ok":true,"ts":"1727000009.000100"}`))
	}))
	defer server.Close()
	p, err := slack.New(slack.Config{
		Key: "slack", SigningSecret: testSecret, BotToken: testToken, BotUserID: "UAPP123",
		APIBaseURL: server.URL, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	target := boardsync.ResolvedTarget{
		Key: "slack", Platform: "slack", Channel: "C0123ABC",
		Thread: "C0123ABC:1727000000.123456", Kind: boardsync.TargetThread,
	}
	posted, err := p.Post(context.Background(), "slack", target, boardsync.OutboundMessage{
		Markdown:     "**Ada**: ship it",
		Sender:       &boardsync.Sender{Name: "Ada", Avatar: "https://example.com/a.png"},
		BoardMessage: 812,
	})
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	if posted.Thread != "C0123ABC:1727000000.123456" || posted.Message != "1727000009.000100" {
		t.Fatalf("posted = %+v", posted)
	}
	if got.Get("channel") != "C0123ABC" || got.Get("thread_ts") != "1727000000.123456" {
		t.Fatalf("form = %v", got)
	}
	if got.Get("markdown_text") != "ship it" || got.Get("username") != "Ada" {
		t.Fatalf("form = %v", got)
	}
	if got.Get("unfurl_links") != "false" || got.Get("unfurl_media") != "false" {
		t.Fatalf("unfurl flags = %v", got)
	}
}

func TestEditDeleteReact(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	p, err := slack.New(slack.Config{
		Key: "slack", SigningSecret: testSecret, BotToken: testToken, BotUserID: "UAPP123",
		APIBaseURL: server.URL, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	thread := boardsync.ThreadRef{Key: "slack", Platform: "slack", Channel: "C0123ABC", Thread: "C0123ABC:1727000000.123456"}
	ctx := context.Background()
	if err := p.Edit(ctx, "slack", thread, "1727000000.123456", boardsync.OutboundMessage{Markdown: "new"}); err != nil {
		t.Fatal(err)
	}
	if err := p.Delete(ctx, "slack", thread, "1727000000.123456"); err != nil {
		t.Fatal(err)
	}
	if err := p.React(ctx, "slack", thread, "1727000000.123456", "thumbsup", true); err != nil {
		t.Fatal(err)
	}
	want := []string{"/chat.update", "/chat.delete", "/reactions.add"}
	if len(paths) != len(want) {
		t.Fatalf("paths = %v", paths)
	}
	for i := range want {
		if paths[i] != want[i] {
			t.Fatalf("paths = %v, want %v", paths, want)
		}
	}
}

func TestSlackAPIErrorClassification(t *testing.T) {
	cases := []struct {
		body string
		want error
	}{
		{`{"ok":false,"error":"channel_not_found"}`, boardsync.ErrNotFound},
		{`{"ok":false,"error":"invalid_auth"}`, boardsync.ErrUnauthorized},
		{`{"ok":false,"error":"ratelimited"}`, boardsync.ErrRateLimited},
		{`{"ok":false,"error":"not_in_channel"}`, boardsync.ErrPermanent},
	}
	for _, tc := range cases {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(tc.body))
		}))
		p, err := slack.New(slack.Config{
			Key: "slack", SigningSecret: testSecret, BotToken: testToken, BotUserID: "UAPP123",
			APIBaseURL: server.URL, Now: now,
		})
		if err != nil {
			t.Fatal(err)
		}
		target := boardsync.ResolvedTarget{Key: "slack", Platform: "slack", Channel: "C0123ABC", Thread: "C0123ABC:1.1", Kind: boardsync.TargetThread}
		_, err = p.Post(context.Background(), "slack", target, boardsync.OutboundMessage{Markdown: "x"})
		if !errors.Is(err, tc.want) {
			t.Fatalf("%s = %v, want %v", tc.body, err, tc.want)
		}
		server.Close()
	}
}

func TestLocate(t *testing.T) {
	p := provider(t)
	thread, message, ok := p.Locate("https://acme.slack.com/archives/C0123ABC/p1727000000123456")
	if !ok || thread != "C0123ABC:1727000000.123456" || message != "1727000000.123456" {
		t.Fatalf("Locate = %q, %q, %v", thread, message, ok)
	}
	if _, _, ok := p.Locate("https://example.com/x"); ok {
		t.Fatal("Locate accepted a foreign URL")
	}
}
