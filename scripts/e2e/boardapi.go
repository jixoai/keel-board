package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jixoai/keel-board/protocol"
)

// threads lists thread summaries for a channel and text filter.
func (e *e2eEnv) threads(step *Step, channel, text string) (protocol.ThreadsPage, error) {
	status, body, err := e.api(step, "list threads", http.MethodGet, "/threads?channel="+urlQueryEscape(channel)+"&text="+urlQueryEscape(text), nil)
	if err != nil {
		return protocol.ThreadsPage{}, err
	}
	var page protocol.ThreadsPage
	if err := json.Unmarshal(body, &page); err != nil {
		return page, err
	}
	if status != http.StatusOK {
		return page, fmt.Errorf("threads status %d: %s", status, clip(string(body), 500))
	}
	return page, nil
}

// findReply returns the reply whose text contains text, or false.
func findReply(thread threadResponse, text string) (protocol.MessageView, bool) {
	for _, reply := range thread.Replies {
		if strings.Contains(reply.Text, text) {
			return reply, true
		}
	}
	return protocol.MessageView{}, false
}

// countReplies counts replies whose text contains text.
func countReplies(thread threadResponse, text string) int {
	count := 0
	for _, reply := range thread.Replies {
		if strings.Contains(reply.Text, text) {
			count++
		}
	}
	return count
}

// api issues one board request through the harness server and records it.
func (e *e2eEnv) api(step *Step, label, method, path string, body any) (int, []byte, error) {
	status, data, err := e.harness.API(method, path, body)
	request := ""
	if body != nil {
		request = mustSample(body)
	}
	step.Trace(label, method, "/v1/board"+path, status, request, string(data))
	return status, data, err
}

// apiAs is api with an explicit development actor.
func (e *e2eEnv) apiAs(step *Step, label, actor, method, path string, body any) (int, []byte, error) {
	status, data, err := e.harness.APIAs(actor, method, path, body)
	request := ""
	if body != nil {
		request = mustSample(body)
	}
	step.Trace(label, method, "/v1/board"+path+" (actor "+actor+")", status, request, string(data))
	return status, data, err
}

// postMessage creates a board root and returns its view.
func (e *e2eEnv) postMessage(step *Step, channel, text string) (protocol.MessageView, error) {
	status, body, err := e.api(step, "post root", http.MethodPost, "/messages", map[string]any{"channel": channel, "text": text})
	if err != nil {
		return protocol.MessageView{}, err
	}
	var posted struct {
		Message protocol.MessageView `json:"message"`
	}
	if err := json.Unmarshal(body, &posted); err != nil {
		return protocol.MessageView{}, err
	}
	if status != http.StatusCreated {
		return protocol.MessageView{}, fmt.Errorf("post root status %d: %s", status, clip(string(body), 500))
	}
	return posted.Message, nil
}

// postMessageLabels creates a board root with labels.
func (e *e2eEnv) postMessageLabels(step *Step, channel, text string, labels map[string]string) (protocol.MessageView, error) {
	status, body, err := e.api(step, "post labeled root", http.MethodPost, "/messages", map[string]any{
		"channel": channel, "text": text, "labels": labels,
	})
	if err != nil {
		return protocol.MessageView{}, err
	}
	var posted struct {
		Message protocol.MessageView `json:"message"`
	}
	if err := json.Unmarshal(body, &posted); err != nil {
		return protocol.MessageView{}, err
	}
	if status != http.StatusCreated {
		return protocol.MessageView{}, fmt.Errorf("post root status %d: %s", status, clip(string(body), 500))
	}
	return posted.Message, nil
}

// replyMessage appends a board reply.
func (e *e2eEnv) replyMessage(step *Step, re, text string) (protocol.MessageView, error) {
	status, body, err := e.api(step, "post reply", http.MethodPost, "/messages", map[string]any{"re": re, "text": text})
	if err != nil {
		return protocol.MessageView{}, err
	}
	var posted struct {
		Message protocol.MessageView `json:"message"`
	}
	if err := json.Unmarshal(body, &posted); err != nil {
		return protocol.MessageView{}, err
	}
	if status != http.StatusCreated {
		return protocol.MessageView{}, fmt.Errorf("post reply status %d: %s", status, clip(string(body), 500))
	}
	return posted.Message, nil
}

// editMessage edits a board message, optionally as another actor.
func (e *e2eEnv) editMessage(step *Step, id, text, actor string) error {
	path := fmt.Sprintf("/messages/%s/edit", id)
	status, body, err := e.apiAs(step, "edit message", actor, http.MethodPost, path, map[string]any{"text": text})
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("edit status %d: %s", status, clip(string(body), 500))
	}
	return nil
}

// deleteMessage deletes a board message.
func (e *e2eEnv) deleteMessage(step *Step, id, actor string) error {
	path := fmt.Sprintf("/messages/%s/delete", id)
	status, body, err := e.apiAs(step, "delete message", actor, http.MethodPost, path, nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("delete status %d: %s", status, clip(string(body), 500))
	}
	return nil
}

// reactMessage adds or removes a board reaction.
func (e *e2eEnv) reactMessage(step *Step, id, emoji string, remove bool, actor string) error {
	path := fmt.Sprintf("/messages/%s/reactions", id)
	status, body, err := e.apiAs(step, "react message", actor, http.MethodPost, path, map[string]any{"emoji": emoji, "remove": remove})
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("react status %d: %s", status, clip(string(body), 500))
	}
	return nil
}

// getThread reads a thread around any message.
func (e *e2eEnv) getThread(step *Step, id string) (threadResponse, error) {
	status, body, err := e.api(step, "read thread", http.MethodGet, "/messages/"+id, nil)
	if err != nil {
		return threadResponse{}, err
	}
	var thread threadResponse
	if err := json.Unmarshal(body, &thread); err != nil {
		return threadResponse{}, err
	}
	if status != http.StatusOK {
		return thread, fmt.Errorf("thread status %d: %s", status, clip(string(body), 500))
	}
	return thread, nil
}

// findThread finds a root by channel and text.
func (e *e2eEnv) findThread(step *Step, channel, text string) (threadResponse, error) {
	status, body, err := e.api(step, "find thread", http.MethodGet, "/threads?channel="+urlQueryEscape(channel)+"&text="+urlQueryEscape(text), nil)
	if err != nil {
		return threadResponse{}, err
	}
	if status != http.StatusOK {
		return threadResponse{}, fmt.Errorf("threads status %d: %s", status, clip(string(body), 500))
	}
	var page protocol.ThreadsPage
	if err := json.Unmarshal(body, &page); err != nil {
		return threadResponse{}, err
	}
	if len(page.Threads) == 0 {
		return threadResponse{}, fmt.Errorf("no thread in %s matching %q", channel, text)
	}
	root := page.Threads[0].Message
	thread, err := e.getThread(step, root.ID)
	if err != nil {
		return threadResponse{}, err
	}
	step.Note("thread root=%s text=%q replies=%d", root.ID, root.Text, len(thread.Replies))
	return thread, nil
}

// syncStatus reads the management status view.
func (e *e2eEnv) syncStatus(step *Step) (syncStatusResponse, error) {
	status, body, err := e.api(step, "sync status", http.MethodGet, "/sync/status", nil)
	if err != nil {
		return syncStatusResponse{}, err
	}
	var view syncStatusResponse
	if err := json.Unmarshal(body, &view); err != nil {
		return view, err
	}
	if status != http.StatusOK {
		return view, fmt.Errorf("sync status %d: %s", status, clip(string(body), 500))
	}
	return view, nil
}

// syncDeadInbox lists dead inbound events.
func (e *e2eEnv) syncDeadInbox(step *Step, key string) (syncInboxResponse, error) {
	status, body, err := e.api(step, "dead inbox", http.MethodGet, "/sync/inbox?key="+key+"&state=dead", nil)
	if err != nil {
		return syncInboxResponse{}, err
	}
	var view syncInboxResponse
	if err := json.Unmarshal(body, &view); err != nil {
		return view, err
	}
	if status != http.StatusOK {
		return view, fmt.Errorf("dead inbox status %d: %s", status, clip(string(body), 500))
	}
	return view, nil
}

// syncFailures lists dead outbound deliveries.
func (e *e2eEnv) syncFailures(step *Step, key string) (syncFailureResponse, error) {
	status, body, err := e.api(step, "outbox failures", http.MethodGet, "/sync/outbox/failures?key="+key, nil)
	if err != nil {
		return syncFailureResponse{}, err
	}
	var view syncFailureResponse
	if err := json.Unmarshal(body, &view); err != nil {
		return view, err
	}
	if status != http.StatusOK {
		return view, fmt.Errorf("failures status %d: %s", status, clip(string(body), 500))
	}
	return view, nil
}

// attachThread attaches a board thread to a platform target.
func (e *e2eEnv) attachThread(step *Step, key, thread, target string) (syncAttachResponse, error) {
	status, body, err := e.api(step, "attach thread", http.MethodPost, "/sync/threads", map[string]any{
		"key": key, "thread": thread, "target": target,
	})
	if err != nil {
		return syncAttachResponse{}, err
	}
	var view syncAttachResponse
	if err := json.Unmarshal(body, &view); err != nil {
		return view, err
	}
	if status != http.StatusOK {
		return view, fmt.Errorf("attach status %d: %s", status, clip(string(body), 500))
	}
	return view, nil
}

// syncThreads reads one board thread's mappings.
func (e *e2eEnv) syncThreads(step *Step, boardThread string) (syncThreadResponse, error) {
	status, body, err := e.api(step, "sync threads", http.MethodGet, "/sync/threads?thread="+urlQueryEscape(boardThread), nil)
	if err != nil {
		return syncThreadResponse{}, err
	}
	var view syncThreadResponse
	if err := json.Unmarshal(body, &view); err != nil {
		return view, err
	}
	if status != http.StatusOK {
		return view, fmt.Errorf("sync threads status %d: %s", status, clip(string(body), 500))
	}
	return view, nil
}

// webhook posts a signed platform webhook and records it.
func (e *e2eEnv) webhook(step *Step, label, key string, body []byte, headers map[string]string) (int, []byte, error) {
	status, data, err := e.harness.webhook(key, headers, body)
	step.Trace(label, http.MethodPost, "/webhooks/"+key, status, string(body), string(data))
	return status, data, err
}

// slackSignedHeaders signs a Slack body with the harness secret.
func slackSignedHeaders(body []byte) map[string]string {
	secret := "slack-e2e-signing"
	seconds := strconv.FormatInt(time.Now().Unix(), 10)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte("v0:" + seconds + ":"))
	mac.Write(body)
	headers := map[string]string{
		"Content-Type":              "application/json",
		"X-Slack-Request-Timestamp": seconds,
		"X-Slack-Signature":         "v0=" + hex.EncodeToString(mac.Sum(nil)),
	}
	return headers
}

func slackBadSignatureHeaders(body []byte) map[string]string {
	return map[string]string{
		"Content-Type":              "application/json",
		"X-Slack-Request-Timestamp": strconv.FormatInt(time.Now().Unix(), 10),
		"X-Slack-Signature":         "v0=deadbeef",
	}
}

// githubSignedHeaders signs a GitHub body and names its event and delivery.
func githubSignedHeaders(body []byte, event, delivery string) map[string]string {
	secret := "github-e2e-secret"
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return map[string]string{
		"Content-Type":        "application/json",
		"X-GitHub-Event":      event,
		"X-GitHub-Delivery":   delivery,
		"X-Hub-Signature-256": "sha256=" + hex.EncodeToString(mac.Sum(nil)),
	}
}

// linearSignedHeaders signs a Linear body and sets the timestamp header.
func linearSignedHeaders(body []byte) map[string]string {
	secret := "linear-e2e-secret"
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return map[string]string{
		"Content-Type":     "application/json",
		"Linear-Signature": hex.EncodeToString(mac.Sum(nil)),
		"Linear-Delivery":  "delivery-" + strconv.FormatInt(time.Now().UnixNano(), 10),
		"Linear-Timestamp": strconv.FormatInt(time.Now().UnixMilli(), 10),
	}
}

// slackEnvelope wraps one event in Slack's callback envelope.
func slackEnvelope(eventID string, event map[string]any) []byte {
	envelope := map[string]any{
		"type": "event_callback", "team_id": "T_E2E", "api_app_id": "A_E2E",
		"event_id": eventID, "event_time": time.Now().Unix(), "event": event,
	}
	encoded, _ := json.Marshal(envelope)
	return encoded
}

// slackMessageEvent builds one message event.
func slackMessageEvent(eventID, channel, user, text, ts, threadTS, subtype string, files []any) []byte {
	event := map[string]any{
		"type": "message", "channel": channel, "channel_type": "channel",
		"user": user, "text": text, "ts": ts, "event_ts": ts,
	}
	if threadTS != "" {
		event["thread_ts"] = threadTS
	}
	if subtype != "" {
		event["subtype"] = subtype
	}
	if files != nil {
		event["files"] = files
	}
	return slackEnvelope(eventID, event)
}

// helpers used in a few places.

func urlQueryEscape(value string) string {
	return url.QueryEscape(value)
}

func jsonUnmarshal(data []byte, value any) error {
	return json.Unmarshal(data, value)
}

func newHTTPRequest(method, url string, body []byte) (*http.Request, error) {
	return http.NewRequest(method, url, strings.NewReader(string(body)))
}

func httpClientFor() *http.Client {
	return &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 20 * time.Second}
}

func readAllLimited(reader io.Reader, max int64) (string, error) {
	data, err := io.ReadAll(io.LimitReader(reader, max))
	return string(data), err
}

func postRaw(url string, body []byte, headers map[string]string) (int, []byte, error) {
	request, err := http.NewRequest(http.MethodPost, url, strings.NewReader(string(body)))
	if err != nil {
		return 0, nil, err
	}
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	response, err := httpClientFor().Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	return response.StatusCode, data, err
}
