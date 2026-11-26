package slack_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/slack"
)

// fakeMessage is one message the fake Slack holds.
type fakeMessage struct {
	TS        string
	Channel   string
	ThreadTS  string
	User      string
	Text      string
	Username  string
	Files     []string
	Deleted   bool
	Reactions map[string]bool
}

// fakeSlack is an httptest Slack Web API with just enough state for the
// adapter to post, change, upload, read, and echo back.
type fakeSlack struct {
	mu       sync.Mutex
	server   *httptest.Server
	scopes   []string
	messages map[string][]*fakeMessage
	files    map[string][]byte
	shares   map[string]string
	statuses []string
	next     int
	fileNext int
	joined   map[string]bool
	member   map[string]bool
}

func newFakeSlack(t *testing.T) *fakeSlack {
	t.Helper()
	fake := &fakeSlack{
		scopes:   []string{"chat:write", "chat:write.customize", "reactions:read", "reactions:write", "channels:read", "groups:read", "channels:history", "groups:history", "files:read", "files:write", "channels:join", "assistant:write"},
		messages: make(map[string][]*fakeMessage),
		files:    make(map[string][]byte),
		shares:   make(map[string]string),
		joined:   make(map[string]bool),
		member:   map[string]bool{"C0123ABC": true},
	}
	fake.server = httptest.NewServer(http.HandlerFunc(fake.handle))
	t.Cleanup(fake.server.Close)
	return fake
}

func (f *fakeSlack) provider(t *testing.T) *slack.Provider {
	t.Helper()
	return newProvider(t, f.server.URL, &http.Client{Transport: rewriteTransport{target: f.server.URL}}, func(c *slack.Config) {
		c.Sleep = func(context.Context, time.Duration) error { return nil }
	})
}

func (f *fakeSlack) nextTS() string {
	f.next++
	return fmt.Sprintf("1727001000.%06d", f.next)
}

func (f *fakeSlack) add(message *fakeMessage) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.messages[message.Channel] = append(f.messages[message.Channel], message)
}

func (f *fakeSlack) find(channel, ts string) *fakeMessage {
	for _, message := range f.messages[channel] {
		if message.TS == ts {
			return message
		}
	}
	return nil
}

func (f *fakeSlack) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/files-pri/") {
		f.mu.Lock()
		data, ok := f.files[strings.TrimPrefix(r.URL.Path, "/files-pri/")]
		f.mu.Unlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(data)
		return
	}
	if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/") {
		if err := r.ParseMultipartForm(4 << 20); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		file, _, err := r.FormFile("file")
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		data := make([]byte, 0)
		buffer := make([]byte, 1024)
		for {
			n, readErr := file.Read(buffer)
			data = append(data, buffer[:n]...)
			if readErr != nil {
				break
			}
		}
		f.mu.Lock()
		f.files[strings.TrimPrefix(r.URL.Path, "/upload/")] = data
		f.mu.Unlock()
		_, _ = w.Write([]byte("OK"))
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch r.URL.Path {
	case "/auth.test":
		payload, _ := json.Marshal(map[string]any{
			"ok": true, "user_id": "UAPP123", "team_id": "T0123ABC",
			"response_metadata": map[string]any{"scopes": f.scopes},
		})
		_, _ = w.Write(payload)
	case "/chat.postMessage":
		channel := r.PostForm.Get("channel")
		ts := f.nextTS()
		threadTS := r.PostForm.Get("thread_ts")
		f.messages[channel] = append(f.messages[channel], &fakeMessage{
			TS: ts, Channel: channel, ThreadTS: threadTS, User: "UAPP123",
			Text: r.PostForm.Get("text"), Username: r.PostForm.Get("username"),
			Reactions: map[string]bool{},
		})
		_, _ = w.Write([]byte(`{"ok":true,"ts":"` + ts + `"}`))
	case "/chat.update":
		message := f.find(r.PostForm.Get("channel"), r.PostForm.Get("ts"))
		if message == nil {
			_, _ = w.Write([]byte(`{"ok":false,"error":"message_not_found"}`))
			return
		}
		message.Text = r.PostForm.Get("text")
		_, _ = w.Write([]byte(`{"ok":true}`))
	case "/chat.delete":
		message := f.find(r.PostForm.Get("channel"), r.PostForm.Get("ts"))
		if message == nil {
			_, _ = w.Write([]byte(`{"ok":false,"error":"message_not_found"}`))
			return
		}
		message.Deleted = true
		_, _ = w.Write([]byte(`{"ok":true}`))
	case "/reactions.add", "/reactions.remove":
		message := f.find(r.PostForm.Get("channel"), r.PostForm.Get("timestamp"))
		if message == nil {
			_, _ = w.Write([]byte(`{"ok":false,"error":"message_not_found"}`))
			return
		}
		if message.Reactions == nil {
			message.Reactions = map[string]bool{}
		}
		message.Reactions[r.PostForm.Get("name")] = r.URL.Path == "/reactions.add"
		_, _ = w.Write([]byte(`{"ok":true}`))
	case "/files.getUploadURLExternal":
		f.fileNext++
		id := fmt.Sprintf("F%d", f.fileNext)
		_, _ = w.Write([]byte(`{"ok":true,"upload_url":"https://files.slack.com/upload/` + id + `","file_id":"` + id + `"}`))
	case "/files.completeUploadExternal":
		var refs []struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal([]byte(r.PostForm.Get("files")), &refs); err != nil {
			_, _ = w.Write([]byte(`{"ok":false,"error":"invalid_arguments"}`))
			return
		}
		channel := r.PostForm.Get("channel_id")
		ts := f.nextTS()
		f.messages[channel] = append(f.messages[channel], &fakeMessage{
			TS: ts, Channel: channel, ThreadTS: r.PostForm.Get("thread_ts"), User: "UAPP123",
			Text: r.PostForm.Get("initial_comment"), Reactions: map[string]bool{},
		})
		ids := make([]map[string]string, 0, len(refs))
		for _, ref := range refs {
			f.shares[ref.ID] = channel + "|" + ts
			ids = append(ids, map[string]string{"id": ref.ID})
		}
		payload, _ := json.Marshal(map[string]any{"ok": true, "files": ids})
		_, _ = w.Write(payload)
	case "/files.info":
		share := f.shares[r.PostForm.Get("file")]
		parts := strings.SplitN(share, "|", 2)
		if len(parts) != 2 {
			_, _ = w.Write([]byte(`{"ok":true,"file":{"shares":{}}}`))
			return
		}
		_, _ = w.Write([]byte(`{"ok":true,"file":{"shares":{"public":{"` + parts[0] + `":[{"ts":"` + parts[1] + `"}]}}}}`))
	case "/assistant.threads.setStatus":
		f.statuses = append(f.statuses, r.PostForm.Get("channel_id")+"|"+r.PostForm.Get("thread_ts")+"|"+r.PostForm.Get("status"))
		_, _ = w.Write([]byte(`{"ok":true}`))
	case "/chat.getPermalink":
		channel := r.PostForm.Get("channel")
		ts := strings.Replace(r.PostForm.Get("message_ts"), ".", "", 1)
		_, _ = w.Write([]byte(`{"ok":true,"permalink":"https://acme.slack.com/archives/` + channel + `/p` + ts + `"}`))
	case "/conversations.info":
		channel := r.PostForm.Get("channel")
		_, _ = w.Write([]byte(`{"ok":true,"channel":{"id":"` + channel + `","name":"general","is_member":` + strconv.FormatBool(f.member[channel]) + `}}`))
	case "/conversations.list":
		_, _ = w.Write([]byte(`{"ok":true,"channels":[{"id":"C0123ABC","name":"general","is_member":true}]}`))
	case "/conversations.replies":
		channel := r.PostForm.Get("channel")
		ts := r.PostForm.Get("ts")
		found := make([]*fakeMessage, 0)
		for _, message := range f.messages[channel] {
			root := message.ThreadTS
			if root == "" {
				root = message.TS
			}
			if root == ts || message.TS == ts {
				found = append(found, message)
			}
		}
		payload, _ := json.Marshal(map[string]any{"ok": true, "messages": fakeMessagesJSON(found)})
		_, _ = w.Write(payload)
	case "/conversations.history":
		channel := r.PostForm.Get("channel")
		found := f.messages[channel]
		ordered := make([]*fakeMessage, len(found))
		for i := range found {
			ordered[len(found)-1-i] = found[i]
		}
		payload, _ := json.Marshal(map[string]any{"ok": true, "messages": fakeMessagesJSON(ordered), "has_more": false})
		_, _ = w.Write(payload)
	case "/conversations.join":
		f.joined[r.PostForm.Get("channel")] = true
		f.member[r.PostForm.Get("channel")] = true
		_, _ = w.Write([]byte(`{"ok":true}`))
	default:
		http.NotFound(w, r)
	}
}

func fakeMessagesJSON(messages []*fakeMessage) []map[string]any {
	out := make([]map[string]any, 0, len(messages))
	for _, message := range messages {
		out = append(out, map[string]any{
			"ts": message.TS, "user": message.User, "username": message.Username,
			"text": message.Text, "thread_ts": message.ThreadTS,
		})
	}
	return out
}

func signedEvent(t *testing.T, event map[string]any, eventID string) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"type": "event_callback", "team_id": "T0123ABC", "api_app_id": "A0123ABC",
		"event_id": eventID, "event_time": 1727001000, "event": event,
	})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func receiveEvent(t *testing.T, p *slack.Provider, event map[string]any, eventID string) []boardsync.Record {
	t.Helper()
	body := signedEvent(t, event, eventID)
	intake, err := p.Receive(context.Background(), "slack", signedRequest(t, body))
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	return intake.Records
}

// TestSlackEndToEnd drives message, edit, delete, reaction, file, and typing
// through one fake Slack, in both directions.
func TestSlackEndToEnd(t *testing.T) {
	fake := newFakeSlack(t)
	p := fake.provider(t)
	ctx := context.Background()
	channel := "C0123ABC"
	thread := "C0123ABC:1727000000.123456"

	// Inbound: a person's message.
	inbound := receiveEvent(t, p, map[string]any{
		"type": "message", "channel": channel, "user": "U0456ABC",
		"text": "hello <@U0999XYZ>", "ts": "1727000000.123456", "event_ts": "1727000000.123456",
	}, "Ev100")
	if len(inbound) != 1 || inbound[0].Event.Thread != thread || inbound[0].Event.Kind != boardsync.KindMessage {
		t.Fatalf("inbound = %+v", inbound)
	}
	// The fake holds the inbound message too, so a reaction can land on it.
	fake.add(&fakeMessage{TS: "1727000000.123456", Channel: channel, User: "U0456ABC", Text: "hello", Reactions: map[string]bool{}})

	// Outbound: the board's reply goes into that thread.
	posted, err := p.Post(ctx, "slack", boardsync.ResolvedTarget{
		Key: "slack", Platform: "slack", Channel: channel, Thread: thread, Kind: boardsync.TargetThread,
	}, boardsync.OutboundMessage{Markdown: "**Board**: got it"})
	if err != nil {
		t.Fatal(err)
	}
	if posted.Message == "" || posted.Thread != thread {
		t.Fatalf("posted = %+v", posted)
	}
	// Echo: Slack sends the app's own post back; it is dropped.
	echo := receiveEvent(t, p, map[string]any{
		"type": "message", "channel": channel, "user": "UAPP123",
		"text": "**Board**: got it", "ts": posted.Message, "thread_ts": "1727000000.123456", "event_ts": posted.Message,
	}, "Ev101")
	if len(echo) != 0 {
		t.Fatalf("own post echoed back: %+v", echo)
	}

	// Edit: out and back.
	if err := p.Edit(ctx, "slack", boardsync.ThreadRef{Key: "slack", Platform: "slack", Channel: channel, Thread: thread}, posted.Message, boardsync.OutboundMessage{Markdown: "**Board**: edited"}); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	if got := fake.find(channel, posted.Message).Text; got != "**Board**: edited" {
		t.Fatalf("edited text = %q", got)
	}
	fake.mu.Unlock()
	edited := receiveEvent(t, p, map[string]any{
		"type": "message", "subtype": "message_changed", "channel": channel,
		"message": map[string]any{
			"type": "message", "user": "U0456ABC", "text": "hello again", "ts": "1727000000.123456",
			"thread_ts": "1727000000.123456", "edited": map[string]any{"user": "U0456ABC", "ts": "1727001010.000000"},
		},
	}, "Ev102")
	if len(edited) != 1 || edited[0].Event.Kind != boardsync.KindEdited || edited[0].Event.Text != "hello again" {
		t.Fatalf("edited = %+v", edited)
	}

	// Delete: out and back.
	if err := p.Delete(ctx, "slack", boardsync.ThreadRef{Key: "slack", Platform: "slack", Channel: channel, Thread: thread}, posted.Message); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	if !fake.find(channel, posted.Message).Deleted {
		t.Fatal("delete did not reach Slack")
	}
	fake.mu.Unlock()
	deleted := receiveEvent(t, p, map[string]any{
		"type": "message", "subtype": "message_deleted", "channel": channel, "deleted_ts": "1727000005.000100",
		"previous_message": map[string]any{"type": "message", "user": "U0999XYZ", "text": "gone", "ts": "1727000005.000100", "thread_ts": "1727000000.123456"},
	}, "Ev103")
	if len(deleted) != 1 || deleted[0].Event.Kind != boardsync.KindDeleted {
		t.Fatalf("deleted = %+v", deleted)
	}

	// Reaction: out, and both occurrences in.
	threadRef := boardsync.ThreadRef{Key: "slack", Platform: "slack", Channel: channel, Thread: thread}
	if err := p.React(ctx, "slack", threadRef, "1727000000.123456", "thumbsup", true); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	if !fake.find(channel, "1727000000.123456").Reactions["thumbsup"] {
		t.Fatal("reaction did not reach Slack")
	}
	fake.mu.Unlock()
	added := receiveEvent(t, p, map[string]any{
		"type": "reaction_added", "user": "U0999XYZ", "reaction": "thumbsup",
		"item":     map[string]any{"type": "message", "channel": channel, "ts": "1727000000.123456"},
		"event_ts": "1727001020.000000",
	}, "Ev104")
	removed := receiveEvent(t, p, map[string]any{
		"type": "reaction_removed", "user": "U0999XYZ", "reaction": "thumbsup",
		"item":     map[string]any{"type": "message", "channel": channel, "ts": "1727000000.123456"},
		"event_ts": "1727001021.000000",
	}, "Ev105")
	if len(added) != 1 || len(removed) != 1 || added[0].Dedupe == removed[0].Dedupe {
		t.Fatalf("reactions = %+v / %+v", added, removed)
	}

	// Files out: the text and files land in one message.
	filePost, err := p.Post(ctx, "slack", boardsync.ResolvedTarget{
		Key: "slack", Platform: "slack", Channel: channel, Thread: thread, Kind: boardsync.TargetThread,
	}, boardsync.OutboundMessage{
		Markdown: "logs attached",
		Files:    []boardsync.OutboundFile{{Data: []byte("run output"), Filename: "run.log", MIMEType: "text/plain"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(filePost.Files) != 1 {
		t.Fatalf("files = %+v", filePost.Files)
	}
	fake.mu.Lock()
	if string(fake.files[filePost.Files[0]]) != "run output" {
		t.Fatalf("uploaded bytes = %q", fake.files[filePost.Files[0]])
	}
	fake.mu.Unlock()

	// Files in: a file_share webhook, then the bytes through FetchFile.
	inboundFiles := receiveEvent(t, p, map[string]any{
		"type": "message", "subtype": "file_share", "channel": channel, "user": "U0456ABC",
		"text": "see attached", "ts": "1727001030.000100", "event_ts": "1727001030.000100",
		"files": []map[string]any{{
			"id": "F123", "name": "note.txt", "mimetype": "text/plain", "size": 10,
			"url_private_download": "https://files.slack.com/files-pri/T0123ABC-F123/note.txt",
		}},
	}, "Ev106")
	if len(inboundFiles) != 1 || len(inboundFiles[0].Event.Files) != 1 {
		t.Fatalf("inbound files = %+v", inboundFiles)
	}
	fake.mu.Lock()
	fake.files["T0123ABC-F123/note.txt"] = []byte("file bytes")
	fake.mu.Unlock()
	data, err := p.FetchFile(ctx, "slack", inboundFiles[0].Event.Files[0])
	if err != nil || string(data) != "file bytes" {
		t.Fatalf("FetchFile = %q, %v", data, err)
	}

	// Typing: the thread status reaches Slack.
	if err := p.Typing(ctx, "slack", threadRef, "1727000000.123456", "running tests"); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	if len(fake.statuses) != 1 || !strings.Contains(fake.statuses[0], "running tests") {
		t.Fatalf("statuses = %v", fake.statuses)
	}
	fake.mu.Unlock()

	// Permalink: Slack's own URL.
	link, err := p.Permalink(ctx, "slack", boardsync.Event{Kind: boardsync.KindMessage, Channel: channel, Message: "1727000000.123456"})
	if err != nil || !strings.HasPrefix(link, "https://acme.slack.com/archives/C0123ABC/p1727000000123456") {
		t.Fatalf("permalink = %q, %v", link, err)
	}

	// Source read returns the thread oldest first.
	read, err := p.ReadSource(ctx, "slack", thread, 50)
	if err != nil {
		t.Fatal(err)
	}
	if read.Thread != thread || len(read.Messages) == 0 {
		t.Fatalf("read = %+v", read)
	}

	// Join is opt-in and reaches conversations.join.
	fake.mu.Lock()
	fake.member[channel] = false
	fake.mu.Unlock()
	joinProvider := newProvider(t, fake.server.URL, &http.Client{Transport: rewriteTransport{target: fake.server.URL}}, func(c *slack.Config) {
		c.JoinChannels = true
	})
	join, err := joinProvider.JoinSource(ctx, "slack", channel)
	if err != nil || !join.Joined {
		t.Fatalf("join = %+v, %v", join, err)
	}
	fake.mu.Lock()
	if !fake.joined[channel] {
		t.Fatal("join did not reach Slack")
	}
	fake.mu.Unlock()
}
