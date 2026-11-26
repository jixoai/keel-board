package slack_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/slack"
)

func TestReceiveFileShare(t *testing.T) {
	record := receive(t, provider(t), fixture(t, "message_file_share.json")).Records[0]
	event := record.Event
	if event.Kind != boardsync.KindMessage || event.Text != "see attached" {
		t.Fatalf("event = %+v", event)
	}
	if len(event.Files) != 2 {
		t.Fatalf("files = %d", len(event.Files))
	}
	safe := event.Files[0]
	if safe.Name != "run.log" || safe.Type != "file" || safe.MIMEType != "text/plain" || safe.Size != 2048 {
		t.Fatalf("file = %+v", safe)
	}
	if safe.URL != "https://files.slack.com/files-pri/T0123ABC-F0123ABC/run.log" {
		t.Fatalf("file URL = %q", safe.URL)
	}
	// A payload cannot point a fetch at a link-local address.
	if event.Files[1].URL != "" {
		t.Fatalf("unsafe URL kept: %q", event.Files[1].URL)
	}
	if err := event.Validate(); err != nil {
		t.Fatalf("event: %v", err)
	}
}

func TestReceiveFileCountLimit(t *testing.T) {
	var body map[string]any
	if err := json.Unmarshal(fixture(t, "message_file_share.json"), &body); err != nil {
		t.Fatal(err)
	}
	event := body["event"].(map[string]any)
	files := make([]any, 0, 15)
	for i := 0; i < 15; i++ {
		files = append(files, map[string]any{
			"id": fmt.Sprintf("F%04d", i), "name": "f.txt", "mimetype": "text/plain", "size": 1,
			"url_private_download": "https://files.slack.com/files-pri/T0123ABC-F/files/f.txt",
		})
	}
	event["files"] = files
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	intake := receive(t, provider(t), data)
	if len(intake.Records) != 1 || len(intake.Records[0].Event.Files) != 10 {
		t.Fatalf("files = %d, want the default cap of 10", len(intake.Records[0].Event.Files))
	}
}

func TestReceiveLinkSnapshot(t *testing.T) {
	record := receive(t, provider(t), fixture(t, "message_link.json")).Records[0]
	event := record.Event
	if len(event.Links) != 1 {
		t.Fatalf("links = %+v", event.Links)
	}
	link := event.Links[0]
	if link.Thread != "slack:C0123ABC:1727000000.123456" || link.Message != "1727000000.123456" {
		t.Fatalf("link = %+v", link)
	}
	if !link.Available || link.Snapshot == nil {
		t.Fatalf("link = %+v", link)
	}
	if link.Snapshot.Author != "Ada" || !strings.Contains(link.Snapshot.Text, "@[slack:user:U0999XYZ]") {
		t.Fatalf("snapshot = %+v", link.Snapshot)
	}
	if len(link.Snapshot.Files) != 1 || link.Snapshot.Files[0].Name != "shot.png" {
		t.Fatalf("snapshot files = %+v", link.Snapshot.Files)
	}
	if link.Snapshot.CreatedAt != "2024-09-22T10:13:20Z" {
		t.Fatalf("created at = %q", link.Snapshot.CreatedAt)
	}
}

func TestReceiveUnfurlChanged(t *testing.T) {
	record := receive(t, provider(t), fixture(t, "message_unfurl_changed.json")).Records[0]
	if record.Event.Kind != boardsync.KindEdited || len(record.Event.Links) != 1 {
		t.Fatalf("event = %+v", record.Event)
	}
	if !strings.HasPrefix(record.Dedupe, "event:slack:") || !strings.Contains(record.Dedupe, "unfurl") {
		t.Fatalf("dedupe = %q", record.Dedupe)
	}
	again := receive(t, provider(t), fixture(t, "message_unfurl_changed.json")).Records[0]
	if again.Dedupe != record.Dedupe {
		t.Fatal("a redelivered unfurl got a new identity")
	}
	// The same text with a later unfurl event is a new occurrence.
	var body map[string]any
	if err := json.Unmarshal(fixture(t, "message_unfurl_changed.json"), &body); err != nil {
		t.Fatal(err)
	}
	body["event_id"] = "Ev0203ABD"
	data, _ := json.Marshal(body)
	next := receive(t, provider(t), data).Records[0]
	if next.Dedupe == record.Dedupe {
		t.Fatal("a second unfurl collapsed into the first")
	}
}

func TestReceiveForward(t *testing.T) {
	record := receive(t, provider(t), fixture(t, "message_forward.json")).Records[0]
	event := record.Event
	if !strings.Contains(event.Text, "> **Ada**: deploys are down") || !strings.Contains(event.Text, "https://acme.slack.com/archives/C0123ABC/p1727000000123456") {
		t.Fatalf("text = %q", event.Text)
	}
	if len(event.Links) != 1 || event.Links[0].Snapshot == nil {
		t.Fatalf("links = %+v", event.Links)
	}
}

func TestReceiveReactionOccurrences(t *testing.T) {
	p := provider(t)
	added := receive(t, p, fixture(t, "reaction_added.json")).Records[0]
	removed := receive(t, p, fixture(t, "reaction_removed.json")).Records[0]
	if added.Event.Added != true || removed.Event.Added != false {
		t.Fatalf("added=%v removed=%v", added.Event.Added, removed.Event.Added)
	}
	// Add, remove, add again: three occurrences, three identities.
	var body map[string]any
	if err := json.Unmarshal(fixture(t, "reaction_added.json"), &body); err != nil {
		t.Fatal(err)
	}
	body["event_id"] = "Ev0127ABD"
	data, _ := json.Marshal(body)
	second := receive(t, p, data).Records[0]
	if added.Dedupe == removed.Dedupe || added.Dedupe == second.Dedupe {
		t.Fatalf("dedupes = %q %q %q", added.Dedupe, removed.Dedupe, second.Dedupe)
	}
	if again := receive(t, p, fixture(t, "reaction_added.json")).Records[0]; again.Dedupe != added.Dedupe {
		t.Fatal("redelivered reaction changed identity")
	}
}

func TestReceiveReactionWithoutOccurrenceRefused(t *testing.T) {
	var body map[string]any
	if err := json.Unmarshal(fixture(t, "reaction_added.json"), &body); err != nil {
		t.Fatal(err)
	}
	delete(body, "event_id")
	event := body["event"].(map[string]any)
	delete(event, "event_ts")
	data, _ := json.Marshal(body)
	if _, err := provider(t).Receive(context.Background(), "slack", signedRequest(t, data)); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("Receive = %v, want ErrBadRequest", err)
	}
}

func TestReceiveMarkerEchoSkipped(t *testing.T) {
	var body map[string]any
	if err := json.Unmarshal(fixture(t, "message.json"), &body); err != nil {
		t.Fatal(err)
	}
	event := body["event"].(map[string]any)
	event["text"] = "echo <!-- keel-board:sync key=slack message=812 -->"
	data, _ := json.Marshal(body)
	if intake := receive(t, provider(t), data); len(intake.Records) != 0 {
		t.Fatalf("marker echo imported: %+v", intake.Records)
	}
}

func TestReceiveCleansControlCharacters(t *testing.T) {
	var body map[string]any
	if err := json.Unmarshal(fixture(t, "message.json"), &body); err != nil {
		t.Fatal(err)
	}
	event := body["event"].(map[string]any)
	event["text"] = "a\u0000b\u0007c\nnext"
	data, _ := json.Marshal(body)
	text := receive(t, provider(t), data).Records[0].Event.Text
	if strings.ContainsRune(text, '\x00') || strings.ContainsRune(text, '\x07') || !strings.Contains(text, "\nnext") {
		t.Fatalf("text = %q", text)
	}
}

func TestReceiveRefusesMalformedTimestamp(t *testing.T) {
	var body map[string]any
	if err := json.Unmarshal(fixture(t, "message.json"), &body); err != nil {
		t.Fatal(err)
	}
	body["event"].(map[string]any)["ts"] = "not-a-ts"
	data, _ := json.Marshal(body)
	if _, err := provider(t).Receive(context.Background(), "slack", signedRequest(t, data)); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("Receive = %v", err)
	}
}

func TestReceiveEscapedMarkerSkipped(t *testing.T) {
	var body map[string]any
	if err := json.Unmarshal(fixture(t, "message.json"), &body); err != nil {
		t.Fatal(err)
	}
	body["event"].(map[string]any)["text"] = "&lt;!-- keel-board:sync key=slack message=812 --&gt;"
	data, _ := json.Marshal(body)
	if intake := receive(t, provider(t), data); len(intake.Records) != 0 {
		t.Fatalf("escaped marker imported: %+v", intake.Records)
	}
}

func TestReceiveBotIDOwnMessageSkipped(t *testing.T) {
	var body map[string]any
	if err := json.Unmarshal(fixture(t, "bot_message.json"), &body); err != nil {
		t.Fatal(err)
	}
	event := body["event"].(map[string]any)
	event["bot_id"] = "BAPP123"
	data, _ := json.Marshal(body)
	p, err := slack.New(slack.Config{
		Key: "slack", SigningSecret: testSecret, BotToken: testToken,
		BotID: "BAPP123", Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if intake := receive(t, p, data); len(intake.Records) != 0 {
		t.Fatalf("own bot message imported: %+v", intake.Records)
	}
}

func TestReceiveEditIdentities(t *testing.T) {
	first := receive(t, provider(t), fixture(t, "message_changed.json")).Records[0]
	if again := receive(t, provider(t), fixture(t, "message_changed.json")).Records[0]; again.Dedupe != first.Dedupe {
		t.Fatal("redelivered edit changed identity")
	}
	var body map[string]any
	if err := json.Unmarshal(fixture(t, "message_changed.json"), &body); err != nil {
		t.Fatal(err)
	}
	message := body["event"].(map[string]any)["message"].(map[string]any)
	edited := message["edited"].(map[string]any)
	edited["ts"] = "1727000101.000000"
	data, _ := json.Marshal(body)
	second := receive(t, provider(t), data).Records[0]
	if second.Dedupe == first.Dedupe {
		t.Fatal("a later edit collapsed into the first")
	}
}

func TestReceiveStableIdentities(t *testing.T) {
	cases := []string{"message.json", "message_thread_reply.json", "message_changed.json", "message_deleted.json", "reaction_added.json", "message_file_share.json", "message_link.json"}
	for _, name := range cases {
		first := receive(t, provider(t), fixture(t, name)).Records[0]
		second := receive(t, provider(t), fixture(t, name)).Records[0]
		if first.Dedupe != second.Dedupe {
			t.Fatalf("%s: dedupe %q then %q", name, first.Dedupe, second.Dedupe)
		}
		if len(first.Dedupe) > 300 {
			t.Fatalf("%s: dedupe is %d bytes", name, len(first.Dedupe))
		}
	}
}

func TestReceiveDoesNoNetworkIO(t *testing.T) {
	var calls atomic.Int64
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, errors.New("no network allowed")
	})}
	p, err := slack.New(slack.Config{
		Key: "slack", SigningSecret: testSecret, BotToken: testToken, BotUserID: "UAPP123",
		Now: now, HTTPClient: client,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"message.json", "message_file_share.json", "message_link.json", "message_unfurl_changed.json"} {
		receive(t, p, fixture(t, name))
	}
	if calls.Load() != 0 {
		t.Fatalf("Receive made %d network calls; it must answer inside 3 seconds", calls.Load())
	}
}

func TestReceiveRejectsHugeBody(t *testing.T) {
	p, err := slack.New(slack.Config{
		Key: "slack", SigningSecret: testSecret, BotToken: testToken, BotUserID: "UAPP123", Now: now, MaxBodyBytes: 256,
	})
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"type":"event_callback","event":{}}` + strings.Repeat(" ", 512))
	if _, err := p.Receive(context.Background(), "slack", signedRequest(t, body)); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("Receive = %v, want ErrBadRequest", err)
	}
}

func TestReceiveSignatureShapes(t *testing.T) {
	body := fixture(t, "message.json")
	request := signedRequest(t, body)
	request.Header.Set(slack.SignatureHeader, strings.Replace(request.Header.Get(slack.SignatureHeader), "v0=", "v1=", 1))
	if _, err := provider(t).Receive(context.Background(), "slack", request); !errors.Is(err, boardsync.ErrUnauthorized) {
		t.Fatalf("v1 signature = %v", err)
	}
	// A timestamp in the future is outside the window too.
	future := fmt.Sprintf("%d", now().Unix()+int64(10*time.Minute/time.Second))
	request = &boardsync.WebhookRequest{
		Method: http.MethodPost, Path: "/webhooks/slack",
		Header: http.Header{
			slack.TimestampHeader: {future},
			slack.SignatureHeader: {slack.Sign([]byte(testSecret), future, body)},
		},
		Body: body,
	}
	if _, err := provider(t).Receive(context.Background(), "slack", request); !errors.Is(err, boardsync.ErrUnauthorized) {
		t.Fatalf("future timestamp = %v", err)
	}
}

func TestReceiveChallengeNeedsSignature(t *testing.T) {
	body := fixture(t, "url_verification.json")
	request := signedRequest(t, body)
	request.Header.Del(slack.SignatureHeader)
	if _, err := provider(t).Receive(context.Background(), "slack", request); !errors.Is(err, boardsync.ErrUnauthorized) {
		t.Fatalf("unsigned challenge = %v", err)
	}
}

// roundTripFunc lets a test see whether an adapter touched the network.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// routeGate scopes a two-argument RouteFunc to the tenant and key the
// registry hands to a Gate. It is not a GateFuncs: those take four
// arguments and a RouteFunc cannot be used directly for them.
type routeGate struct {
	routes boardsync.RouteFunc
}

func (g routeGate) Routes(ctx context.Context, tenant string, key boardsync.Key, channel string) (bool, error) {
	if tenant != "tenant-a" || key != "slack" {
		return false, nil
	}
	return g.routes(ctx, channel)
}

func (g routeGate) ThreadSynced(context.Context, string, boardsync.Key, string) (bool, error) {
	return false, nil
}

const testTenant = "tenant-a"

func testEntry(p *slack.Provider, routes boardsync.RouteFunc) boardsync.Entry {
	return boardsync.Entry{
		Tenant: testTenant, Key: "slack", Platform: "slack",
		Provider: p, Sync: boardsync.Noop, Routes: routes,
	}
}

func TestWebhookStoresBeforeAnswering(t *testing.T) {
	p := provider(t)
	registry := boardsync.NewRegistry()
	entry := testEntry(p, func(context.Context, string) (bool, error) { return true, nil })
	if err := registry.Add(entry); err != nil {
		t.Fatal(err)
	}
	gate := routeGate{routes: entry.Routes}
	body := fixture(t, "message.json")
	inbox := &recordingInbox{}
	response, err := registry.HandleWebhook(context.Background(), testTenant, "slack", signedRequest(t, body), gate, inbox)
	if err != nil {
		t.Fatal(err)
	}
	if response.Status != 200 {
		t.Fatalf("status = %d", response.Status)
	}
	if inbox.pushes != 1 || inbox.records != 1 {
		t.Fatalf("inbox = %+v", inbox)
	}
	// A redelivery is stored before the same 200 is answered, and deduped.
	response, err = registry.HandleWebhook(context.Background(), testTenant, "slack", signedRequest(t, body), gate, inbox)
	if err != nil || response.Status != 200 {
		t.Fatalf("redelivery = %d, %v", response.Status, err)
	}
	if inbox.records != 1 {
		t.Fatalf("duplicate delivery was stored twice")
	}
	// A storage failure must answer 500 so Slack sends the event again.
	failing := &failingInbox{}
	response, err = registry.HandleWebhook(context.Background(), testTenant, "slack", signedRequest(t, body), gate, failing)
	if err == nil || response.Status != 500 {
		t.Fatalf("failing store = %d, %v", response.Status, err)
	}
}

func TestWebhookDropsUnmappedChannel(t *testing.T) {
	entry := testEntry(provider(t), func(context.Context, string) (bool, error) { return false, nil })
	registry := boardsync.NewRegistry()
	if err := registry.Add(entry); err != nil {
		t.Fatal(err)
	}
	inbox := &recordingInbox{}
	response, err := registry.HandleWebhook(context.Background(), testTenant, "slack", signedRequest(t, fixture(t, "message.json")), routeGate{routes: entry.Routes}, inbox)
	if err != nil || response.Status != 200 {
		t.Fatalf("response = %d, %v", response.Status, err)
	}
	if inbox.pushes != 0 {
		t.Fatal("an unmapped channel reached the inbox")
	}
}

type recordingInbox struct {
	pushes  int
	records int
	seen    map[string]bool
}

func (r *recordingInbox) Push(_ context.Context, _ boardsync.Key, records []boardsync.Record) (int, error) {
	r.pushes++
	if r.seen == nil {
		r.seen = make(map[string]bool)
	}
	added := 0
	for _, record := range records {
		if !r.seen[record.Dedupe] {
			r.seen[record.Dedupe] = true
			added++
		}
	}
	r.records += added
	return added, nil
}

func (r *recordingInbox) Lease(context.Context, boardsync.Key, int) ([]boardsync.InboxItem, error) {
	return nil, nil
}

func (r *recordingInbox) Ack(context.Context, boardsync.Key, string, string, error) error { return nil }

type failingInbox struct{}

func (failingInbox) Push(context.Context, boardsync.Key, []boardsync.Record) (int, error) {
	return 0, errors.New("disk is full")
}

func (failingInbox) Lease(context.Context, boardsync.Key, int) ([]boardsync.InboxItem, error) {
	return nil, nil
}

func (failingInbox) Ack(context.Context, boardsync.Key, string, string, error) error { return nil }
