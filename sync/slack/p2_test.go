package slack_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/slack"
)

func TestReceiveTeamAllowlist(t *testing.T) {
	p, err := slack.New(slack.Config{
		Key: "slack", SigningSecret: testSecret, BotToken: testToken,
		BotUserID: "UAPP123", TeamID: "T0123ABC", Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	body := fixture(t, "message.json")
	if intake := receive(t, p, body); len(intake.Records) != 1 {
		t.Fatalf("records = %+v", intake.Records)
	}
	var envelope map[string]any
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatal(err)
	}
	envelope["team_id"] = "T999XYZ"
	mutated, _ := json.Marshal(envelope)
	if _, err := p.Receive(context.Background(), "slack", signedRequest(t, mutated)); !errors.Is(err, boardsync.ErrUnauthorized) {
		t.Fatalf("other team = %v", err)
	}
	delete(envelope, "team_id")
	mutated, _ = json.Marshal(envelope)
	if _, err := p.Receive(context.Background(), "slack", signedRequest(t, mutated)); !errors.Is(err, boardsync.ErrUnauthorized) {
		t.Fatalf("missing team = %v", err)
	}
}

func TestReceiveKeyMismatch(t *testing.T) {
	if _, err := provider(t).Receive(context.Background(), "other", signedRequest(t, fixture(t, "message.json"))); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("Receive = %v", err)
	}
}

func TestBotIdentityRequired(t *testing.T) {
	if _, err := slack.New(slack.Config{
		Key: "slack", SigningSecret: testSecret, BotToken: testToken, Now: now,
	}); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("New = %v", err)
	}
}

func TestReceiveEditWithoutIdentity(t *testing.T) {
	var body map[string]any
	if err := json.Unmarshal(fixture(t, "message_changed.json"), &body); err != nil {
		t.Fatal(err)
	}
	delete(body, "event_id")
	message := body["event"].(map[string]any)["message"].(map[string]any)
	delete(message, "edited")
	event := body["event"].(map[string]any)
	delete(event, "event_ts")
	data, _ := json.Marshal(body)
	if _, err := provider(t).Receive(context.Background(), "slack", signedRequest(t, data)); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("Receive = %v", err)
	}
}

func TestChannelListPageCap(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{"ok":true,"channels":[],"response_metadata":{"next_cursor":"more"}}`))
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	if _, err := p.ResolveChannel(context.Background(), "slack", "#general"); err == nil {
		t.Fatal("a never-ending listing was accepted")
	}
	if calls.Load() != 64 {
		t.Fatalf("calls = %d, want 64", calls.Load())
	}
}

func TestResolveTargetRejectsDM(t *testing.T) {
	p := newProvider(t, "http://127.0.0.1:1", nil, nil)
	if _, err := p.ResolveTarget(context.Background(), "slack", "D0123ABC"); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("DM target = %v", err)
	}
}

func TestReadSourceRejectsMPIM(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true,"channel":{"id":"G0123ABC","name":"group","is_member":true,"is_mpim":true}}`))
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	if _, err := p.ReadSource(context.Background(), "slack", "C0123ABC", 10); !errors.Is(err, boardsync.ErrUnsupported) {
		t.Fatalf("ReadSource = %v", err)
	}
}

func TestResolveTargetUsesNameCache(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{"ok":true,"channels":[{"id":"C0123ABC","name":"general","is_member":true}]}`))
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	for i := 0; i < 2; i++ {
		target, err := p.ResolveTarget(context.Background(), "slack", "#general")
		if err != nil || target.Channel != "C0123ABC" || target.Kind != boardsync.TargetChannel {
			t.Fatalf("ResolveTarget = %+v, %v", target, err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("list calls = %d, want the cached first listing", calls.Load())
	}
}

func TestFetchRoot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/conversations.replies":
			_ = r.ParseForm()
			if r.PostForm.Get("ts") != "1727000000.123456" {
				t.Fatalf("ts = %q", r.PostForm.Get("ts"))
			}
			_, _ = w.Write([]byte(`{"ok":true,"messages":[{"ts":"1727000000.123456","user":"U0456ABC","text":"root <@U0999XYZ>"}]}`))
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	root, err := p.FetchRoot(context.Background(), "slack", "C0123ABC:1727000000.123456")
	if err != nil {
		t.Fatal(err)
	}
	if root.Text != "root @[slack:user:U0999XYZ]" || root.Author.ID != "slack:user:U0456ABC" {
		t.Fatalf("root = %+v", root)
	}
	if _, err := p.FetchRoot(context.Background(), "slack", "C0123ABC:bad"); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("bad root = %v", err)
	}
}

func TestAPIRejectsRedirect(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Redirect(w, r, "/elsewhere", http.StatusFound)
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	if _, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{Markdown: "x"}); err == nil {
		t.Fatal("a redirect was followed")
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d", calls.Load())
	}
}

func TestRetryAfterIsStructured(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	_, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{Markdown: "x"})
	var rate *slack.RateLimitError
	if !errors.As(err, &rate) || rate.RetryAfter != 7*time.Second || !errors.Is(err, boardsync.ErrRateLimited) {
		t.Fatalf("err = %v, rate = %+v", err, rate)
	}
}

func TestEditRejectsEmptyText(t *testing.T) {
	p := newProvider(t, "http://127.0.0.1:1", nil, nil)
	thread := boardsync.ThreadRef{Key: "slack", Platform: "slack", Channel: "C0123ABC", Thread: "C0123ABC:1727000000.123456"}
	if err := p.Edit(context.Background(), "slack", thread, "1727000000.123456", boardsync.OutboundMessage{Markdown: "   "}); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("Edit = %v", err)
	}
}

func TestFilesReadMissingPostsTextFirst(t *testing.T) {
	var order []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth.test":
			_, _ = w.Write([]byte(`{"ok":true,"response_metadata":{"scopes":["chat:write"]}}`))
		case "/chat.postMessage":
			order = append(order, "post")
			_, _ = w.Write([]byte(`{"ok":true,"ts":"1727000009.000100"}`))
		case "/files.getUploadURLExternal":
			order = append(order, "url")
			_, _ = w.Write([]byte(`{"ok":true,"upload_url":"https://files.slack.com/upload/v1/abc","file_id":"F1"}`))
		case "/upload/v1/abc":
			order = append(order, "upload")
			_, _ = w.Write([]byte(`OK`))
		case "/files.completeUploadExternal":
			order = append(order, "complete")
			_ = r.ParseForm()
			if r.PostForm.Get("thread_ts") != "1727000009.000100" {
				t.Fatalf("thread_ts = %q", r.PostForm.Get("thread_ts"))
			}
			_, _ = w.Write([]byte(`{"ok":true,"files":[{"id":"F1"}]}`))
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	defer server.Close()
	p := newProvider(t, server.URL, &http.Client{Transport: rewriteTransport{target: server.URL}}, nil)
	posted, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{
		Markdown: "see attached",
		Files:    []boardsync.OutboundFile{{Data: []byte("x"), Filename: "a.png"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(order, ",") != "post,url,upload,complete" {
		t.Fatalf("order = %v", order)
	}
	if posted.Message != "1727000009.000100" || len(posted.Files) != 1 {
		t.Fatalf("posted = %+v", posted)
	}
}

func TestReceiveCleansFormatCharacters(t *testing.T) {
	var body map[string]any
	if err := json.Unmarshal(fixture(t, "message.json"), &body); err != nil {
		t.Fatal(err)
	}
	body["event"].(map[string]any)["text"] = "a\u200bb\u202ec\u200dd"
	data, _ := json.Marshal(body)
	text := receive(t, provider(t), data).Records[0].Event.Text
	if text != "abc\u200dd" {
		t.Fatalf("text = %q", text)
	}
}

func TestExternalEditOmitsMarkdownText(t *testing.T) {
	var got url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/chat.update":
			_ = r.ParseForm()
			got = r.PostForm
			_, _ = w.Write([]byte(`{"ok":true}`))
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	thread := boardsync.ThreadRef{Key: "slack", Platform: "slack", Channel: "C0123ABC", Thread: "C0123ABC:1727000000.123456"}
	if err := p.Edit(context.Background(), "slack", thread, "1727000000.123456", boardsync.OutboundMessage{
		Markdown: "<!channel>", External: true,
	}); err != nil {
		t.Fatal(err)
	}
	if got.Get("text") != "&lt;!channel&gt;" || got.Get("markdown_text") != "" {
		t.Fatalf("form = %v", got)
	}
}

func TestEscapedTextIsClampedAfterEscaping(t *testing.T) {
	var got url.Values
	server := capturePostServer(t, &got)
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	if _, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{
		Markdown: strings.Repeat("<", 30000), External: true,
	}); err != nil {
		t.Fatal(err)
	}
	if len([]rune(got.Get("text"))) > 12000 {
		t.Fatalf("text is %d runes after escaping", len([]rune(got.Get("text"))))
	}
}

func TestFileNameIsBounded(t *testing.T) {
	var got url.Values
	server := capturePostServer(t, &got)
	defer server.Close()
	p := newProvider(t, server.URL, nil, func(c *slack.Config) { c.MaxFileBytes = 1 })
	name := strings.Repeat("x", 5000)
	if _, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{
		Markdown: "file",
		Files:    []boardsync.OutboundFile{{Data: []byte("too large"), Filename: name}},
	}); err != nil {
		t.Fatal(err)
	}
	text := formBody(got)
	if len(text) > 2000 || strings.Contains(text, name) {
		t.Fatalf("file name was not bounded: %d bytes", len(text))
	}
}

func TestReceiveRejectsDirectMessages(t *testing.T) {
	for _, channelType := range []string{"im", "mpim"} {
		var body map[string]any
		if err := json.Unmarshal(fixture(t, "message.json"), &body); err != nil {
			t.Fatal(err)
		}
		event := body["event"].(map[string]any)
		event["channel_type"] = channelType
		if channelType == "im" {
			event["channel"] = "D0123ABC"
		}
		data, _ := json.Marshal(body)
		if intake := receive(t, provider(t), data); len(intake.Records) != 0 {
			t.Fatalf("%s imported: %+v", channelType, intake.Records)
		}
	}
}

func TestURLIsUnsupported(t *testing.T) {
	p := newProvider(t, "http://127.0.0.1:1", nil, nil)
	if _, err := p.URL("C0123ABC:1727000000.123456", "1727000000.123456"); !errors.Is(err, boardsync.ErrUnsupported) {
		t.Fatalf("URL = %v", err)
	}
}

func TestUploadURLIsRedactedFromErrors(t *testing.T) {
	client := slack.NewClient("", &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return nil, errors.New("request to " + req.URL.String() + " with " + testToken)
	})}, boardsync.Secret(testToken))
	err := client.UploadExternal(context.Background(), "https://files.slack.com/upload/secret-path", "a.txt", []byte("x"))
	if err == nil {
		t.Fatal("UploadExternal succeeded")
	}
	if strings.Contains(err.Error(), "secret-path") || strings.Contains(err.Error(), testToken) {
		t.Fatalf("error leaked the upload URL or token: %v", err)
	}
}

func TestFileURLIsRedactedFromErrors(t *testing.T) {
	client := slack.NewClient("", &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return nil, errors.New("request to " + req.URL.String() + " with " + testToken)
	})}, boardsync.Secret(testToken))
	_, err := client.FetchFile(context.Background(), boardsync.File{
		URL: "https://files.slack.com/files-pri/T-F/secret.txt",
	}, 1024)
	if err == nil {
		t.Fatal("FetchFile succeeded")
	}
	if strings.Contains(err.Error(), "secret.txt") || strings.Contains(err.Error(), testToken) {
		t.Fatalf("error leaked the file URL or token: %v", err)
	}
}

func TestUploadRetryHonorsRetryAfter(t *testing.T) {
	var sleeps []time.Duration
	var gets atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/chat.postMessage":
			_, _ = w.Write([]byte(`{"ok":true,"ts":"1727000009.000100"}`))
		case "/files.getUploadURLExternal":
			if gets.Add(1) == 1 {
				w.Header().Set("Retry-After", "2")
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			_, _ = w.Write([]byte(`{"ok":true,"upload_url":"https://files.slack.com/upload/v1/abc","file_id":"F1"}`))
		case "/upload/v1/abc":
			_, _ = w.Write([]byte(`OK`))
		case "/files.completeUploadExternal":
			_, _ = w.Write([]byte(`{"ok":true,"files":[{"id":"F1"}]}`))
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	defer server.Close()
	p := newProvider(t, server.URL, &http.Client{Transport: rewriteTransport{target: server.URL}}, func(c *slack.Config) {
		c.Sleep = func(_ context.Context, d time.Duration) error {
			sleeps = append(sleeps, d)
			return nil
		}
	})
	if _, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{
		Markdown: "see attached",
		Files:    []boardsync.OutboundFile{{Data: []byte("x"), Filename: "a.png"}},
	}); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, d := range sleeps {
		if d >= 2*time.Second {
			found = true
		}
	}
	if !found {
		t.Fatalf("sleeps = %v, want a Retry-After wait", sleeps)
	}
}

func TestFetchRootCleansFileNames(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true,"messages":[{"ts":"1727000000.123456","user":"U0456ABC","text":"","files":[{"name":"a\u0000b.txt"}]}]}`))
	}))
	defer server.Close()
	p := newProvider(t, server.URL, nil, nil)
	root, err := p.FetchRoot(context.Background(), "slack", "C0123ABC:1727000000.123456")
	if err != nil {
		t.Fatal(err)
	}
	if strings.ContainsRune(root.Text, 0) || root.Text != "📎 ab.txt" {
		t.Fatalf("root text = %q", root.Text)
	}
}

func TestReceiveEscapedMarkupStaysLiteral(t *testing.T) {
	cases := []struct {
		name string
		text string
		want string
		bad  string
	}{
		{"broadcast", "&lt;!channel&gt;", "<!channel>", "@channel"},
		{"mention", "&lt;@U0456ABC&gt;", "<@U0456ABC>", "@[slack:user:"},
		{"link", "&lt;https://example.com|label&gt;", "<https://example.com|label>", "[label](https://example.com)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var body map[string]any
			if err := json.Unmarshal(fixture(t, "message.json"), &body); err != nil {
				t.Fatal(err)
			}
			body["event"].(map[string]any)["text"] = tc.text
			data, _ := json.Marshal(body)
			text := receive(t, provider(t), data).Records[0].Event.Text
			if text != tc.want {
				t.Fatalf("text = %q, want %q", text, tc.want)
			}
			if strings.Contains(text, tc.bad) {
				t.Fatalf("escaped markup was revived: %q", text)
			}
		})
	}
}

func TestMentionNamespaceFollowsEntryKey(t *testing.T) {
	p, err := slack.New(slack.Config{
		Key: "support-slack", SigningSecret: testSecret, BotToken: testToken,
		BotUserID: "UAPP123", Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(fixture(t, "message.json"), &body); err != nil {
		t.Fatal(err)
	}
	body["event"].(map[string]any)["text"] = "hi <@U0999XYZ>"
	data, _ := json.Marshal(body)
	intake, err := p.Receive(context.Background(), "support-slack", signedRequest(t, data))
	if err != nil {
		t.Fatal(err)
	}
	event := intake.Records[0].Event
	if event.Author.ID != "support-slack:user:U0456ABC" || !strings.Contains(event.Text, "@[support-slack:user:U0999XYZ]") {
		t.Fatalf("event = %+v", event)
	}
	if strings.Contains(event.Text, "@[slack:user:") {
		t.Fatalf("hardcoded platform namespace: %q", event.Text)
	}

	var got url.Values
	server := capturePostServer(t, &got)
	defer server.Close()
	out, err := slack.New(slack.Config{
		Key: "support-slack", SigningSecret: testSecret, BotToken: testToken,
		BotUserID: "UAPP123", APIBaseURL: server.URL, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := out.Post(context.Background(), "support-slack", boardsync.ResolvedTarget{
		Key: "support-slack", Platform: "slack", Channel: "C0123ABC", Kind: boardsync.TargetChannel,
	}, boardsync.OutboundMessage{
		Markdown: "@[support-slack:user:U0456ABC] and @[slack:user:U0999XYZ]",
	}); err != nil {
		t.Fatal(err)
	}
	bodyText := formBody(got)
	if !strings.Contains(bodyText, "<@U0456ABC>") || strings.Contains(bodyText, "<@U0999XYZ>") {
		t.Fatalf("mention namespace leaked: %q", bodyText)
	}
}

func TestUploadFilenameCleanedAndBounded(t *testing.T) {
	var filename string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/chat.postMessage":
			_, _ = w.Write([]byte(`{"ok":true,"ts":"1727000009.000100"}`))
		case "/files.getUploadURLExternal":
			_ = r.ParseForm()
			filename = r.PostForm.Get("filename")
			_, _ = w.Write([]byte(`{"ok":true,"upload_url":"https://files.slack.com/upload/v1/abc","file_id":"F1"}`))
		case "/upload/v1/abc":
			_, _ = w.Write([]byte(`OK`))
		case "/files.completeUploadExternal":
			_, _ = w.Write([]byte(`{"ok":true,"files":[{"id":"F1"}]}`))
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	defer server.Close()
	p := newProvider(t, server.URL, &http.Client{Transport: rewriteTransport{target: server.URL}}, nil)
	name := "a\u0000b" + strings.Repeat("x", 600)
	if _, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{
		Markdown: "file",
		Files:    []boardsync.OutboundFile{{Data: []byte("x"), Filename: name, MIMEType: "text/plain"}},
	}); err != nil {
		t.Fatal(err)
	}
	if strings.ContainsRune(filename, 0) || len([]rune(filename)) > 204 || !strings.HasSuffix(filename, ".txt") {
		t.Fatalf("filename = %q", filename)
	}
}

func TestFileFailureDoesNotRepeatAttachmentLine(t *testing.T) {
	p, cap := fileServer(t, func(w http.ResponseWriter, r *http.Request, _ *fileCapture) {
		if r.URL.Path != "/files.getUploadURLExternal" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusForbidden)
	})
	if _, err := p.Post(context.Background(), "slack", postTarget(false), boardsync.OutboundMessage{
		Files: []boardsync.OutboundFile{{Data: []byte("x"), Filename: "a.png"}},
	}); err != nil {
		t.Fatal(err)
	}
	body := formBody(cap.update)
	if got := strings.Count(body, "📎 a.png"); got != 1 {
		t.Fatalf("attachment line appears %d times: %q", got, body)
	}
}

func TestLocateRejectsDMPermalink(t *testing.T) {
	p := newProvider(t, "http://127.0.0.1:1", nil, nil)
	if _, _, ok := p.Locate("https://acme.slack.com/archives/D0123ABC/p1727000000123456"); ok {
		t.Fatal("Locate accepted a DM permalink")
	}
	if _, err := p.ResolveTarget(context.Background(), "slack", "https://acme.slack.com/archives/D0123ABC/p1727000000123456"); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("ResolveTarget = %v", err)
	}
}

func TestNonSlackKeySourceAndFetchRoot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/conversations.info":
			_, _ = w.Write([]byte(`{"ok":true,"channel":{"id":"C0123ABC","name":"support","is_member":true}}`))
		case "/conversations.history":
			_, _ = w.Write([]byte(`{"ok":true,"messages":[{"ts":"1727000000.123456","user":"U0456ABC","text":"hi <@U0999XYZ>"}]}`))
		case "/conversations.replies":
			_, _ = w.Write([]byte(`{"ok":true,"messages":[{"ts":"1727000000.123456","user":"U0456ABC","text":"root"}]}`))
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	defer server.Close()
	p, err := slack.New(slack.Config{
		Key: "support-slack", SigningSecret: testSecret, BotToken: testToken,
		BotUserID: "UAPP123", APIBaseURL: server.URL, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	read, err := p.ReadSource(context.Background(), "support-slack", "C0123ABC", 10)
	if err != nil {
		t.Fatal(err)
	}
	if read.Messages[0].AuthorID != "support-slack:user:U0456ABC" || !strings.Contains(read.Messages[0].Text, "@[support-slack:user:U0999XYZ]") {
		t.Fatalf("read = %+v", read.Messages)
	}
	if _, err := p.ReadSource(context.Background(), "slack", "C0123ABC", 10); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("wrong key ReadSource = %v", err)
	}
	root, err := p.FetchRoot(context.Background(), "support-slack", "C0123ABC:1727000000.123456")
	if err != nil {
		t.Fatal(err)
	}
	if root.Author.ID != "support-slack:user:U0456ABC" {
		t.Fatalf("root = %+v", root)
	}
	if _, err := p.FetchRoot(context.Background(), "slack", "C0123ABC:1727000000.123456"); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("wrong key FetchRoot = %v", err)
	}
}
