package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// fakePlatform is a local, TLS "internet" for the three adapters. Its
// certificate is trusted only by the client the harness injects, and every
// hostname the adapters normally dial (slack.com, files.slack.com,
// api.github.com, api.linear.app) resolves here — while the URL and its
// validation stay exactly what production sees.
type fakePlatform struct {
	listener net.Listener
	server   *http.Server
	client   *http.Client
	addr     string

	mu      sync.Mutex
	calls   []platformCall
	history []platformCall

	slack  *slackFake
	github *githubFake
	linear *linearFake
}

// platformCall is one platform-side HTTP request, kept as evidence.
type platformCall struct {
	At     time.Time `json:"at"`
	Host   string    `json:"host"`
	Method string    `json:"method"`
	Path   string    `json:"path"`
	Status int       `json:"status"`
	Detail string    `json:"detail,omitempty"`
}

// newFakePlatform starts the TLS listener and returns the platform.
func newFakePlatform() (*fakePlatform, error) {
	pool, certificate, err := fakeCertificate()
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("fake platform listener: %w", err)
	}
	platform := &fakePlatform{
		listener: listener,
		addr:     listener.Addr().String(),
		slack:    newSlackFake(),
		github:   newGitHubFake(),
		linear:   newLinearFake(),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", platform.route)
	platform.server = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	tlsListener := tls.NewListener(listener, &tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS12,
	})
	go func() { _ = platform.server.Serve(tlsListener) }()
	transport := &http.Transport{
		TLSClientConfig:   &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
		ForceAttemptHTTP2: false,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, "tcp", platform.addr)
		},
	}
	platform.client = &http.Client{Transport: transport, Timeout: 30 * time.Second}
	return platform, nil
}

// HTTPClient is what the harness passes to every adapter config. It keeps the
// URL hostname untouched and redirects only the TCP destination.
func (f *fakePlatform) HTTPClient() *http.Client { return f.client }

// Close stops the fake platform.
func (f *fakePlatform) Close() {
	if f == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = f.server.Shutdown(ctx)
	_ = f.listener.Close()
}

// fakeCertificate builds a CA and a leaf for the hostnames the adapters dial.
func fakeCertificate() (*x509.CertPool, tls.Certificate, error) {
	now := time.Now()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, tls.Certificate{}, err
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "keel-board e2e CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, tls.Certificate{}, err
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, tls.Certificate{}, err
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, tls.Certificate{}, err
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "slack.com"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames: []string{
			"slack.com", "files.slack.com", "api.github.com", "api.linear.app",
			"localhost",
		},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		return nil, tls.Certificate{}, err
	}
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	return pool, tls.Certificate{Certificate: [][]byte{leafDER}, PrivateKey: leafKey}, nil
}

// route dispatches by Host, the way the real internet would.
func (f *fakePlatform) route(w http.ResponseWriter, r *http.Request) {
	recorder := &statusRecorder{ResponseWriter: w}
	host := strings.ToLower(r.Host)
	if index := strings.IndexByte(host, ':'); index >= 0 {
		host = host[:index]
	}
	var detail string
	switch host {
	case "slack.com":
		detail = f.slack.serve(recorder, r)
	case "files.slack.com":
		detail = f.serveSlackFile(recorder, r)
	case "api.github.com":
		detail = f.github.serve(recorder, r)
	case "api.linear.app":
		detail = f.linear.serve(recorder, r)
	default:
		recorder.WriteHeader(http.StatusNotFound)
		detail = "unknown host"
	}
	status := recorder.status
	if status == 0 {
		status = http.StatusOK
	}
	call := platformCall{
		At: time.Now(), Host: host, Method: r.Method, Path: r.URL.Path,
		Status: status, Detail: detail,
	}
	f.mu.Lock()
	f.calls = append(f.calls, call)
	f.history = append(f.history, call)
	f.mu.Unlock()
}

// fakeSlackFileBytes is what the fake Slack file host serves and what the
// inbound file assertions compare against.
const fakeSlackFileBytes = "fake-slack-file-bytes"

// serveSlackFile answers the pre-signed upload URL and file downloads.
func (f *fakePlatform) serveSlackFile(w http.ResponseWriter, r *http.Request) string {
	switch r.Method {
	case http.MethodPost:
		// Slack's upload is a multipart form; record the file part's bytes,
		// not the envelope, so a test can compare the uploaded content size.
		size := 0
		if mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err == nil && strings.HasPrefix(mediaType, "multipart/") {
			if reader := multipart.NewReader(r.Body, params["boundary"]); reader != nil {
				if part, partErr := reader.NextPart(); partErr == nil {
					body, _ := io.ReadAll(io.LimitReader(part, 8<<20))
					size = len(body)
				}
			}
		} else {
			body, _ := io.ReadAll(io.LimitReader(r.Body, 8<<20))
			size = len(body)
		}
		f.slack.recordUpload(r.URL.Path, size)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
		return fmt.Sprintf("upload %d bytes", size)
	case http.MethodGet:
		_, _ = w.Write([]byte(fakeSlackFileBytes))
		return "download"
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		return "method not allowed"
	}
}

// Calls returns a snapshot of every platform request.
func (f *fakePlatform) Calls() []platformCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]platformCall(nil), f.calls...)
}

// ResetCalls clears the per-window access log (not the platform's own state
// and not the cumulative history the report writes).
func (f *fakePlatform) ResetCalls() {
	f.mu.Lock()
	f.calls = nil
	f.mu.Unlock()
}

// History returns every platform request since the harness started.
func (f *fakePlatform) History() []platformCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]platformCall(nil), f.history...)
}

// CallsTo returns calls whose host and path prefix match.
func (f *fakePlatform) CallsTo(host, prefix string) []platformCall {
	var out []platformCall
	for _, call := range f.Calls() {
		if call.Host == host && strings.HasPrefix(call.Path, prefix) {
			out = append(out, call)
		}
	}
	return out
}

// Platform accessor methods keep scenario code readable.
func (f *fakePlatform) SlackPosts() []slackPost              { return f.slack.postsSnapshot() }
func (f *fakePlatform) SlackEdits() []slackEdit              { return f.slack.editsSnapshot() }
func (f *fakePlatform) SlackDeletes() []slackDelete          { return f.slack.deletesSnapshot() }
func (f *fakePlatform) SlackReactions() []slackReaction      { return f.slack.reactionsSnapshot() }
func (f *fakePlatform) SlackTyping() []slackTyping           { return f.slack.typingSnapshot() }
func (f *fakePlatform) SlackUploads() []slackUpload          { return f.slack.uploadsSnapshot() }
func (f *fakePlatform) GitHubWrites() []githubWrite          { return f.github.writesSnapshot() }
func (f *fakePlatform) LinearWrites() []linearWrite          { return f.linear.writesSnapshot() }
func (f *fakePlatform) SlackFailText(text string, times int) { f.slack.failText(text, times) }
func (f *fakePlatform) SlackClearFailures()                  { f.slack.clearFailures() }

// ResetSlack clears the Slack record slices and the platform access log, so a
// scenario can assert "exactly what happened in this window". It keeps the
// fake's message store and failure/block rules.
func (f *fakePlatform) ResetSlack() {
	f.slack.mu.Lock()
	f.slack.posts = nil
	f.slack.edits = nil
	f.slack.deletes = nil
	f.slack.reactions = nil
	f.slack.typing = nil
	f.slack.uploads = nil
	f.slack.mu.Unlock()
	f.ResetCalls()
}

// ResetGitHub clears GitHub write records and the access log.
func (f *fakePlatform) ResetGitHub() {
	f.github.mu.Lock()
	f.github.writes = nil
	f.github.mu.Unlock()
	f.ResetCalls()
}

// ResetLinear clears Linear GraphQL records and the access log.
func (f *fakePlatform) ResetLinear() {
	f.linear.mu.Lock()
	f.linear.writes = nil
	f.linear.mu.Unlock()
	f.ResetCalls()
}

// SlackBlock blocks a chat.postMessage whose text contains text. It returns a
// channel closed when the fake has received the request and a release func.
func (f *fakePlatform) SlackBlock(text string) (<-chan struct{}, func()) {
	return f.slack.blockText(text)
}

// statusRecorder captures the response status for the access log.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

// ---------------------------------------------------------------- Slack ----

type slackMessage struct {
	Channel  string
	ThreadTS string
	Text     string
	TS       string
}

type slackPost struct {
	Channel  string    `json:"channel"`
	ThreadTS string    `json:"threadTs"`
	Text     string    `json:"text"`
	Username string    `json:"username,omitempty"`
	TS       string    `json:"ts"`
	At       time.Time `json:"at"`
}

type slackEdit struct {
	Channel string    `json:"channel"`
	TS      string    `json:"ts"`
	Text    string    `json:"text"`
	At      time.Time `json:"at"`
}

type slackDelete struct {
	Channel string    `json:"channel"`
	TS      string    `json:"ts"`
	At      time.Time `json:"at"`
}

type slackReaction struct {
	Channel string    `json:"channel"`
	TS      string    `json:"ts"`
	Name    string    `json:"name"`
	Added   bool      `json:"added"`
	At      time.Time `json:"at"`
}

type slackTyping struct {
	Channel string    `json:"channel"`
	Thread  string    `json:"thread"`
	Status  string    `json:"status"`
	At      time.Time `json:"at"`
}

type slackUpload struct {
	Path  string `json:"path"`
	Bytes int    `json:"bytes"`
}

type slackFailRule struct {
	substr    string
	remaining int // -1 means forever
}

type slackBlockRule struct {
	substr  string
	once    sync.Once
	started chan struct{}
	release chan struct{}
	closed  sync.Once
}

type slackFake struct {
	mu        sync.Mutex
	messages  map[string]slackMessage
	posts     []slackPost
	edits     []slackEdit
	deletes   []slackDelete
	reactions []slackReaction
	typing    []slackTyping
	uploads   []slackUpload
	failures  []slackFailRule
	blocks    []*slackBlockRule
	nextTS    int64
	nextFile  int64
}

func newSlackFake() *slackFake {
	return &slackFake{
		messages: make(map[string]slackMessage),
		nextTS:   1_700_000_000,
		nextFile: 100,
	}
}

func (s *slackFake) serve(w http.ResponseWriter, r *http.Request) string {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return "method not allowed"
	}
	if err := r.ParseForm(); err != nil {
		writeSlackError(w, "invalid_arguments")
		return "bad form"
	}
	method := strings.TrimPrefix(r.URL.Path, "/api/")
	switch method {
	case "auth.test":
		writeSlackJSON(w, map[string]any{
			"ok": true, "user_id": "UBOT",
			"response_metadata": map[string]any{"scopes": []string{}},
		})
		return method
	case "chat.postMessage":
		text := r.Form.Get("text")
		if text == "" {
			text = r.Form.Get("markdown_text")
		}
		if s.shouldFail(text) {
			w.WriteHeader(http.StatusInternalServerError)
			return "chat.postMessage injected 500: " + text
		}
		if block := s.blockFor(text); block != nil {
			block.once.Do(func() { close(block.started) })
			select {
			case <-block.release:
			case <-r.Context().Done():
				w.WriteHeader(http.StatusInternalServerError)
				return "chat.postMessage aborted: " + text
			}
		}
		channel := r.Form.Get("channel")
		threadTS := r.Form.Get("thread_ts")
		ts := s.nextTimestamp()
		post := slackPost{
			Channel: channel, ThreadTS: threadTS, Text: text,
			Username: r.Form.Get("username"), TS: ts, At: time.Now(),
		}
		s.mu.Lock()
		s.posts = append(s.posts, post)
		s.messages[ts] = slackMessage{Channel: channel, ThreadTS: threadTS, Text: text, TS: ts}
		s.mu.Unlock()
		writeSlackJSON(w, map[string]any{"ok": true, "ts": ts, "channel": channel})
		return "chat.postMessage"
	case "chat.update":
		channel, ts := r.Form.Get("channel"), r.Form.Get("ts")
		text := r.Form.Get("text")
		if text == "" {
			text = r.Form.Get("markdown_text")
		}
		s.mu.Lock()
		s.edits = append(s.edits, slackEdit{Channel: channel, TS: ts, Text: text, At: time.Now()})
		message := s.messages[ts]
		message.Text = text
		s.messages[ts] = message
		s.mu.Unlock()
		writeSlackJSON(w, map[string]any{"ok": true})
		return "chat.update"
	case "chat.delete":
		channel, ts := r.Form.Get("channel"), r.Form.Get("ts")
		s.mu.Lock()
		s.deletes = append(s.deletes, slackDelete{Channel: channel, TS: ts, At: time.Now()})
		delete(s.messages, ts)
		s.mu.Unlock()
		writeSlackJSON(w, map[string]any{"ok": true})
		return "chat.delete"
	case "reactions.add", "reactions.remove":
		s.mu.Lock()
		s.reactions = append(s.reactions, slackReaction{
			Channel: r.Form.Get("channel"), TS: r.Form.Get("timestamp"),
			Name: r.Form.Get("name"), Added: method == "reactions.add", At: time.Now(),
		})
		s.mu.Unlock()
		writeSlackJSON(w, map[string]any{"ok": true})
		return method
	case "conversations.list":
		writeSlackJSON(w, map[string]any{
			"ok": true,
			"channels": []map[string]any{
				{"id": "C_SUPPORT", "name": "support", "is_member": true},
				{"id": "C0BACKFILL", "name": "backfill-e2e", "is_member": true},
				{"id": "C0FUTURE", "name": "future-e2e", "is_member": true},
			},
			"response_metadata": map[string]any{"next_cursor": ""},
		})
		return method
	case "conversations.replies":
		channel, ts := r.Form.Get("channel"), r.Form.Get("ts")
		s.mu.Lock()
		message, found := s.messages[ts]
		s.mu.Unlock()
		messages := []map[string]any{}
		if found {
			messages = append(messages, map[string]any{
				"ts": ts, "text": message.Text, "user": "UBOT", "username": "keel-bot",
				"thread_ts": message.ThreadTS,
			})
		}
		writeSlackJSON(w, map[string]any{"ok": true, "messages": messages, "has_more": false, "channel": channel})
		return method
	case "chat.getPermalink":
		channel, ts := r.Form.Get("channel"), r.Form.Get("message_ts")
		link := "https://acme.slack.com/archives/" + channel + "/p" + strings.ReplaceAll(ts, ".", "")
		writeSlackJSON(w, map[string]any{"ok": true, "permalink": link})
		return method
	case "files.getUploadURLExternal":
		s.mu.Lock()
		s.nextFile++
		fileID := fmt.Sprintf("F%04d", s.nextFile)
		s.mu.Unlock()
		writeSlackJSON(w, map[string]any{
			"ok": true, "upload_url": "https://files.slack.com/upload/v1/" + fileID,
			"file_id": fileID,
		})
		return method
	case "files.completeUploadExternal":
		var refs []struct {
			ID    string `json:"id"`
			Title string `json:"title"`
		}
		_ = json.Unmarshal([]byte(r.Form.Get("files")), &refs)
		ids := make([]map[string]any, 0, len(refs))
		for _, ref := range refs {
			ids = append(ids, map[string]any{"id": ref.ID})
		}
		writeSlackJSON(w, map[string]any{"ok": true, "files": ids})
		return method
	case "files.delete":
		writeSlackJSON(w, map[string]any{"ok": true})
		return method
	case "assistant.threads.setStatus":
		s.mu.Lock()
		s.typing = append(s.typing, slackTyping{
			Channel: r.Form.Get("channel_id"), Thread: r.Form.Get("thread_ts"),
			Status: r.Form.Get("status"), At: time.Now(),
		})
		s.mu.Unlock()
		writeSlackJSON(w, map[string]any{"ok": true})
		return method
	default:
		writeSlackJSON(w, map[string]any{"ok": true})
		return method
	}
}

func (s *slackFake) shouldFail(text string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for index := range s.failures {
		rule := &s.failures[index]
		if strings.Contains(text, rule.substr) && rule.remaining != 0 {
			if rule.remaining > 0 {
				rule.remaining--
			}
			return true
		}
	}
	return false
}

func (s *slackFake) blockFor(text string) *slackBlockRule {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rule := range s.blocks {
		if strings.Contains(text, rule.substr) {
			return rule
		}
	}
	return nil
}

func (s *slackFake) failText(text string, times int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failures = append(s.failures, slackFailRule{substr: text, remaining: times})
}

func (s *slackFake) clearFailures() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failures = nil
}

func (s *slackFake) blockText(text string) (<-chan struct{}, func()) {
	rule := &slackBlockRule{substr: text, started: make(chan struct{}), release: make(chan struct{})}
	s.mu.Lock()
	s.blocks = append(s.blocks, rule)
	s.mu.Unlock()
	release := func() {
		rule.closed.Do(func() { close(rule.release) })
	}
	return rule.started, release
}

func (s *slackFake) recordUpload(path string, bytes int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.uploads = append(s.uploads, slackUpload{Path: path, Bytes: bytes})
}

func (s *slackFake) nextTimestamp() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextTS++
	return fmt.Sprintf("%d.000100", s.nextTS)
}

func (s *slackFake) postsSnapshot() []slackPost {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]slackPost(nil), s.posts...)
}

func (s *slackFake) editsSnapshot() []slackEdit {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]slackEdit(nil), s.edits...)
}

func (s *slackFake) deletesSnapshot() []slackDelete {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]slackDelete(nil), s.deletes...)
}

func (s *slackFake) uploadsSnapshot() []slackUpload {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]slackUpload(nil), s.uploads...)
}

func (s *slackFake) reactionsSnapshot() []slackReaction {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]slackReaction(nil), s.reactions...)
}

func (s *slackFake) typingSnapshot() []slackTyping {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]slackTyping(nil), s.typing...)
}

func writeSlackJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func writeSlackError(w http.ResponseWriter, code string) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": code})
}

// --------------------------------------------------------------- GitHub ----

type githubWrite struct {
	Method string    `json:"method"`
	Path   string    `json:"path"`
	Body   string    `json:"body"`
	Status int       `json:"status"`
	At     time.Time `json:"at"`
}

type githubFake struct {
	mu      sync.Mutex
	nextID  int64
	nextRea int64
	writes  []githubWrite
}

func newGitHubFake() *githubFake { return &githubFake{nextID: 9100, nextRea: 500} }

func (g *githubFake) serve(w http.ResponseWriter, r *http.Request) string {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	path := r.URL.Path
	parts := strings.Split(strings.Trim(path, "/"), "/")
	write := githubWrite{Method: r.Method, Path: path, Body: string(body), At: time.Now()}
	status := http.StatusOK
	respond := func(value any) {
		write.Status = status
		g.mu.Lock()
		g.writes = append(g.writes, write)
		g.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if value != nil {
			_ = json.NewEncoder(w).Encode(value)
		}
	}
	fail := func(code int) {
		status = code
		write.Status = code
		g.mu.Lock()
		g.writes = append(g.writes, write)
		g.mu.Unlock()
		w.WriteHeader(code)
	}

	if r.Method == http.MethodGet && path == "/user" {
		respond(map[string]any{"login": "keel-bot", "id": 7000, "type": "Bot"})
		return "GET /user"
	}
	if len(parts) < 3 || parts[0] != "repos" {
		fail(http.StatusNotFound)
		return "unknown github path"
	}
	repo := parts[1] + "/" + parts[2]
	switch {
	case len(parts) >= 6 && parts[3] == "issues" && parts[5] == "comments" && r.Method == http.MethodPost:
		g.mu.Lock()
		g.nextID++
		id := g.nextID
		g.mu.Unlock()
		status = http.StatusCreated
		respond(map[string]any{"id": id, "html_url": fmt.Sprintf("https://github.com/%s/issues/%s#issuecomment-%d", repo, parts[4], id)})
		return "create issue comment"
	case len(parts) >= 6 && parts[3] == "issues" && parts[5] == "reactions" && r.Method == http.MethodPost:
		g.mu.Lock()
		g.nextRea++
		id := g.nextRea
		g.mu.Unlock()
		status = http.StatusCreated
		respond(map[string]any{"id": id, "content": jsonField(body, "content")})
		return "react issue"
	case len(parts) >= 5 && parts[3] == "issues" && parts[4] == "comments":
		// /repos/{o}/{r}/issues/comments/{id}[/reactions]
		if len(parts) == 6 && r.Method == http.MethodPatch {
			respond(map[string]any{"id": numberAt(parts, 5), "body": jsonField(body, "body")})
			return "update issue comment"
		}
		if len(parts) == 6 && r.Method == http.MethodDelete {
			status = http.StatusNoContent
			respond(nil)
			return "delete issue comment"
		}
		if len(parts) == 7 && parts[6] == "reactions" && r.Method == http.MethodPost {
			g.mu.Lock()
			g.nextRea++
			id := g.nextRea
			g.mu.Unlock()
			status = http.StatusCreated
			respond(map[string]any{"id": id, "content": jsonField(body, "content")})
			return "react issue comment"
		}
	case len(parts) >= 8 && parts[3] == "pulls" && parts[5] == "comments" && parts[7] == "replies" && r.Method == http.MethodPost:
		g.mu.Lock()
		g.nextID++
		id := g.nextID
		g.mu.Unlock()
		status = http.StatusCreated
		respond(map[string]any{"id": id})
		return "create review reply"
	case len(parts) >= 6 && parts[3] == "pulls" && parts[4] == "comments":
		// /repos/{o}/{r}/pulls/comments/{id}[/reactions]
		if len(parts) == 6 && r.Method == http.MethodPatch {
			respond(map[string]any{"id": numberAt(parts, 5)})
			return "update review comment"
		}
		if len(parts) == 6 && r.Method == http.MethodDelete {
			status = http.StatusNoContent
			respond(nil)
			return "delete review comment"
		}
		if len(parts) == 7 && parts[6] == "reactions" && r.Method == http.MethodPost {
			status = http.StatusCreated
			respond(map[string]any{"id": 1})
			return "react review comment"
		}
	}
	if r.Method == http.MethodGet && len(parts) >= 5 {
		if parts[3] == "issues" {
			respond(map[string]any{"number": numberAt(parts, 4), "title": "fake issue", "body": "fake body", "html_url": "https://github.com/" + repo + "/issues/" + parts[4], "user": map[string]any{"login": "ada", "id": 1001}})
			return "get issue"
		}
		if parts[3] == "pulls" {
			respond(map[string]any{"number": numberAt(parts, 4), "title": "fake PR", "body": "fake body", "html_url": "https://github.com/" + repo + "/pull/" + parts[4], "user": map[string]any{"login": "ada", "id": 1001}})
			return "get pull"
		}
	}
	fail(http.StatusNotFound)
	return "unhandled github call"
}

func (g *githubFake) writesSnapshot() []githubWrite {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]githubWrite(nil), g.writes...)
}

func jsonField(body []byte, field string) string {
	var decoded map[string]any
	if json.Unmarshal(body, &decoded) != nil {
		return ""
	}
	value, _ := decoded[field].(string)
	return value
}

func numberAt(parts []string, index int) int64 {
	if index >= len(parts) {
		return 0
	}
	var value int64
	_, _ = fmt.Sscan(parts[index], &value)
	return value
}

// --------------------------------------------------------------- Linear ----

type linearWrite struct {
	Query     string    `json:"query"`
	Variables string    `json:"variables"`
	At        time.Time `json:"at"`
}

type linearFake struct {
	mu     sync.Mutex
	nextID int64
	writes []linearWrite
}

func newLinearFake() *linearFake { return &linearFake{nextID: 9000} }

func (l *linearFake) serve(w http.ResponseWriter, r *http.Request) string {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return "method not allowed"
	}
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	var request struct {
		Query     string         `json:"query"`
		Variables map[string]any `json:"variables"`
	}
	_ = json.Unmarshal(body, &request)
	variables, _ := json.Marshal(request.Variables)
	write := linearWrite{Query: request.Query, Variables: string(variables), At: time.Now()}
	data := map[string]any{}
	operation := "unknown"
	switch {
	case strings.Contains(request.Query, "commentCreate"):
		l.mu.Lock()
		l.nextID++
		id := fmt.Sprintf("comment-%d", l.nextID)
		l.mu.Unlock()
		operation = "commentCreate"
		data["commentCreate"] = map[string]any{"success": true, "comment": map[string]any{"id": id}}
	case strings.Contains(request.Query, "commentUpdate"):
		operation = "commentUpdate"
		data["commentUpdate"] = map[string]any{"success": true}
	case strings.Contains(request.Query, "commentDelete"):
		operation = "commentDelete"
		data["commentDelete"] = map[string]any{"success": true}
	case strings.Contains(request.Query, "reactionCreate"):
		operation = "reactionCreate"
		data["reactionCreate"] = map[string]any{"success": true}
	case strings.Contains(request.Query, "issueCreate"):
		l.mu.Lock()
		l.nextID++
		id := fmt.Sprintf("issue-%d", l.nextID)
		l.mu.Unlock()
		operation = "issueCreate"
		data["issueCreate"] = map[string]any{"success": true, "issue": map[string]any{"id": id, "identifier": "ENG-999", "team": map[string]any{"key": "ENG"}}}
	case strings.Contains(request.Query, "IssueRefByID"):
		operation = "IssueRefByID"
		id, _ := request.Variables["id"].(string)
		data["issue"] = fakeLinearIssue(id)
	case strings.Contains(request.Query, "IssueByID"):
		operation = "IssueByID"
		id, _ := request.Variables["id"].(string)
		data["issue"] = fakeLinearIssue(id)
	case strings.Contains(request.Query, "IssueByKey"):
		operation = "IssueByKey"
		data["issues"] = map[string]any{"nodes": []any{fakeLinearIssue("11111111-1111-1111-1111-111111111111")}}
	case strings.Contains(request.Query, "TeamByKey") || strings.Contains(request.Query, "teams("):
		operation = "TeamByKey"
		data["teams"] = map[string]any{"nodes": []any{map[string]any{"id": "team-1", "key": "ENG", "name": "Engineering"}}}
	case strings.Contains(request.Query, "issue("):
		operation = "IssueByID"
		data["issue"] = fakeLinearIssue("11111111-1111-1111-1111-111111111111")
	}
	l.mu.Lock()
	l.writes = append(l.writes, write)
	l.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
	return operation
}

func fakeLinearIssue(id string) map[string]any {
	if id == "" {
		id = "11111111-1111-1111-1111-111111111111"
	}
	return map[string]any{
		"id": id, "identifier": "ENG-123", "number": 123,
		"title": "Checkout retry", "description": "fake description",
		"url":      "https://linear.app/acme/issue/ENG-123/checkout-retry",
		"team":     map[string]any{"id": "team-1", "key": "ENG", "name": "Engineering"},
		"state":    map[string]any{"id": "state-1", "name": "In Progress", "type": "started"},
		"priority": 2, "priorityLabel": "High",
		"creator": map[string]any{"id": "user-1", "name": "Ada Lovelace", "displayName": "ada"},
		"labels":  map[string]any{"nodes": []any{}},
	}
}

func (l *linearFake) writesSnapshot() []linearWrite {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]linearWrite(nil), l.writes...)
}

// ------------------------------------------------------------- utilities ----

// sortedCalls returns calls sorted for stable report rendering.
func sortedCalls(calls []platformCall) []platformCall {
	out := append([]platformCall(nil), calls...)
	sort.SliceStable(out, func(i, j int) bool { return out[i].At.Before(out[j].At) })
	return out
}
