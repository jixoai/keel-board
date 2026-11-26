package slack

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
)

// Config configures a Slack adapter. The key is the registry entry's key; it
// names external IDs and markers.
type Config struct {
	// Key is the sync entry key. When set, the provider refuses targets under
	// another key.
	Key boardsync.Key
	// SigningSecret verifies webhooks. It has no default and is never logged.
	SigningSecret boardsync.Secret
	// BotToken authorizes Web API calls.
	BotToken boardsync.Secret
	// BotUserID and BotID identify the app's own messages, which are never
	// imported back. Either is enough; both are safest.
	BotUserID string
	BotID     string
	// APIBaseURL defaults to https://slack.com/api; tests point it at httptest.
	APIBaseURL string
	HTTPClient *http.Client
	// Now and Tolerance verify webhook timestamps. The defaults are time.Now
	// and five minutes.
	Now       func() time.Time
	Tolerance time.Duration
	// MaxBodyBytes bounds a webhook body. The default is 1 MiB.
	MaxBodyBytes int64
}

// Validate checks the configuration without revealing a secret.
func (c Config) Validate() error {
	if c.Key != "" && !c.Key.Valid() {
		return fmt.Errorf("%w: %q isn't a sync key", boardsync.ErrBadRequest, c.Key)
	}
	if c.SigningSecret.Empty() {
		return fmt.Errorf("%w: slack signing secret is unset", boardsync.ErrUnauthorized)
	}
	if c.BotUserID != "" && !isUserID(c.BotUserID) {
		return fmt.Errorf("%w: slack bot user ID %q isn't a user ID", boardsync.ErrBadRequest, c.BotUserID)
	}
	if c.APIBaseURL != "" {
		if _, err := url.Parse(c.APIBaseURL); err != nil {
			return fmt.Errorf("%w: slack API base URL: %v", boardsync.ErrBadRequest, err)
		}
	}
	return nil
}

func isUserID(id string) bool {
	return isSlackUserID(id)
}

func (c Config) baseURL() string {
	if c.APIBaseURL != "" {
		return strings.TrimRight(c.APIBaseURL, "/")
	}
	return "https://slack.com/api"
}

func (c Config) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

func (c Config) tolerance() time.Duration {
	if c.Tolerance > 0 {
		return c.Tolerance
	}
	return DefaultTolerance
}

func (c Config) maxBody() int64 {
	if c.MaxBodyBytes > 0 {
		return c.MaxBodyBytes
	}
	return 1 << 20
}

func (c Config) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return &http.Client{Timeout: 15 * time.Second}
}

// Client is Slack's Web API client: form-encoded calls with a bearer token.
// It is the only place the token is revealed.
type Client struct {
	baseURL string
	http    *http.Client
	token   boardsync.Secret
}

// NewClient returns a Slack Web API client. baseURL may be empty.
func NewClient(baseURL string, httpClient *http.Client, token boardsync.Secret) *Client {
	if baseURL == "" {
		baseURL = "https://slack.com/api"
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), http: httpClient, token: token}
}

type apiEnvelope struct {
	OK       bool   `json:"ok"`
	Error    string `json:"error,omitempty"`
	Warning  string `json:"warning,omitempty"`
	Needed   string `json:"needed,omitempty"`
	Provided string `json:"provided,omitempty"`
}

// call posts a form to one Slack method and decodes the response. A Slack
// error becomes a classified Go error: auth failures, missing channels, rate
// limits, and permanent refusals stay distinguishable for the retry policy.
func (c *Client) call(ctx context.Context, method string, form url.Values, out any) error {
	if c.token.Empty() {
		return fmt.Errorf("%w: slack bot token is unset", boardsync.ErrUnauthorized)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/"+method, strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Authorization", "Bearer "+c.token.Reveal())
	response, err := c.http.Do(request)
	if err != nil {
		return boardsync.RedactError(err, c.token)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusTooManyRequests {
		return fmt.Errorf("%w: slack %s rate-limited (retry after %ds)", boardsync.ErrRateLimited, method, boardsync.RetryAfter(response.Header))
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("slack %s answered HTTP %d", method, response.StatusCode)
	}
	body, err := readLimited(response.Body, 4<<20)
	if err != nil {
		return err
	}
	var envelope apiEnvelope
	if err := jsonUnmarshal(body, &envelope); err != nil {
		return fmt.Errorf("slack %s answered unreadable JSON: %w", method, err)
	}
	if !envelope.OK {
		return slackAPIError(method, envelope.Error)
	}
	if out != nil {
		if err := jsonUnmarshal(body, out); err != nil {
			return fmt.Errorf("slack %s answered unreadable JSON: %w", method, err)
		}
	}
	return nil
}

func slackAPIError(method, code string) error {
	if code == "" {
		return fmt.Errorf("slack %s failed", method)
	}
	switch code {
	case "invalid_auth", "not_authed", "token_revoked", "account_inactive", "missing_scope":
		return fmt.Errorf("%w: slack %s: %s", boardsync.ErrUnauthorized, method, code)
	case "channel_not_found", "message_not_found", "thread_not_found", "user_not_found":
		return fmt.Errorf("%w: slack %s: %s", boardsync.ErrNotFound, method, code)
	case "ratelimited":
		return fmt.Errorf("%w: slack %s: %s", boardsync.ErrRateLimited, method, code)
	case "not_in_channel", "is_archived", "restricted_action", "cannot_reply_to_message":
		return fmt.Errorf("%w: slack %s: %s", boardsync.Permanent(errors.New(code)), method, code)
	default:
		return fmt.Errorf("slack %s: %s", method, code)
	}
}

// Channel is one Slack channel, as conversations.list describes it.
type Channel struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	IsMember bool   `json:"is_member"`
	Private  bool   `json:"is_private"`
	IM       bool   `json:"is_im"`
	MPIM     bool   `json:"is_mpim"`
}

type listResponse struct {
	apiEnvelope
	Channels []Channel `json:"channels"`
	Metadata struct {
		NextCursor string `json:"next_cursor"`
	} `json:"response_metadata"`
}

// ListChannels lists public and private channels, following Slack's cursor.
func (c *Client) ListChannels(ctx context.Context) ([]Channel, error) {
	var channels []Channel
	cursor := ""
	for {
		form := url.Values{"types": {"public_channel,private_channel"}, "exclude_archived": {"true"}, "limit": {"1000"}}
		if cursor != "" {
			form.Set("cursor", cursor)
		}
		var page listResponse
		if err := c.call(ctx, "conversations.list", form, &page); err != nil {
			return nil, err
		}
		channels = append(channels, page.Channels...)
		cursor = page.Metadata.NextCursor
		if cursor == "" {
			return channels, nil
		}
	}
}

// PostMessageRequest is the subset of chat.postMessage this skeleton sends.
type PostMessageRequest struct {
	Channel      string
	ThreadTS     string
	Text         string
	MarkdownText string
	Username     string
	IconURL      string
}

// PostMessage posts and returns the message's ts.
func (c *Client) PostMessage(ctx context.Context, req PostMessageRequest) (string, error) {
	if req.Channel == "" {
		return "", fmt.Errorf("%w: slack post has no channel", boardsync.ErrBadRequest)
	}
	form := url.Values{
		"channel":      {req.Channel},
		"text":         {req.Text},
		"unfurl_links": {"false"},
		"unfurl_media": {"false"},
	}
	if req.MarkdownText != "" {
		form.Set("markdown_text", req.MarkdownText)
	}
	if req.ThreadTS != "" {
		form.Set("thread_ts", req.ThreadTS)
	}
	if req.Username != "" {
		form.Set("username", req.Username)
	}
	if req.IconURL != "" {
		form.Set("icon_url", req.IconURL)
	}
	var response struct {
		apiEnvelope
		TS string `json:"ts"`
	}
	if err := c.call(ctx, "chat.postMessage", form, &response); err != nil {
		return "", err
	}
	if response.TS == "" {
		return "", errors.New("slack chat.postMessage answered no ts")
	}
	return response.TS, nil
}

// UpdateMessage replaces a message's text.
func (c *Client) UpdateMessage(ctx context.Context, channel, ts, text, markdown string) error {
	if channel == "" || ts == "" {
		return fmt.Errorf("%w: slack update needs a channel and ts", boardsync.ErrBadRequest)
	}
	form := url.Values{"channel": {channel}, "ts": {ts}, "text": {text}}
	if markdown != "" {
		form.Set("markdown_text", markdown)
	}
	return c.call(ctx, "chat.update", form, nil)
}

// DeleteMessage deletes one message.
func (c *Client) DeleteMessage(ctx context.Context, channel, ts string) error {
	if channel == "" || ts == "" {
		return fmt.Errorf("%w: slack delete needs a channel and ts", boardsync.ErrBadRequest)
	}
	return c.call(ctx, "chat.delete", url.Values{"channel": {channel}, "ts": {ts}}, nil)
}

// React adds or removes a reaction.
func (c *Client) React(ctx context.Context, channel, ts, name string, added bool) error {
	if channel == "" || ts == "" || name == "" {
		return fmt.Errorf("%w: slack reaction needs a channel, ts, and name", boardsync.ErrBadRequest)
	}
	method := "reactions.add"
	if !added {
		method = "reactions.remove"
	}
	return c.call(ctx, method, url.Values{"channel": {channel}, "timestamp": {ts}, "name": {name}}, nil)
}

// Permalink asks Slack for a message's own link.
func (c *Client) Permalink(ctx context.Context, channel, ts string) (string, error) {
	if channel == "" || ts == "" {
		return "", fmt.Errorf("%w: slack permalink needs a channel and ts", boardsync.ErrBadRequest)
	}
	var response struct {
		apiEnvelope
		Permalink string `json:"permalink"`
	}
	if err := c.call(ctx, "chat.getPermalink", url.Values{"channel": {channel}, "message_ts": {ts}}, &response); err != nil {
		return "", err
	}
	if response.Permalink == "" {
		return "", errors.New("slack chat.getPermalink answered no permalink")
	}
	return response.Permalink, nil
}

// AuthTest checks which user the token belongs to and which scopes it has.
func (c *Client) AuthTest(ctx context.Context) (userID string, scopes []string, err error) {
	var response struct {
		apiEnvelope
		UserID   string `json:"user_id"`
		Metadata struct {
			Scopes []string `json:"scopes"`
		} `json:"response_metadata"`
	}
	if err := c.call(ctx, "auth.test", url.Values{}, &response); err != nil {
		return "", nil, err
	}
	return response.UserID, response.Metadata.Scopes, nil
}

func readLimited(reader io.Reader, max int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(reader, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > max {
		return nil, errors.New("slack: response is too large")
	}
	return body, nil
}

var channelIDPattern = regexp.MustCompile(`^[A-Z][A-Z0-9]{2,}$`)

var permalinkPattern = regexp.MustCompile(`^https://[a-z0-9-]+(?:\.enterprise)?\.slack\.com/archives/([A-Z0-9]+)/p(\d{10})(\d{6})(?:\?.*)?$`)

// parseTarget reads a Slack target: #name, C0123, C0123:1727.9, or a
// message permalink (its thread). Name resolution uses client.
func parseTarget(ctx context.Context, client *Client, name string) (boardsync.ResolvedTarget, error) {
	target := strings.TrimSpace(name)
	if target == "" {
		return boardsync.ResolvedTarget{}, fmt.Errorf("%w: empty slack target", boardsync.ErrBadRequest)
	}
	if match := permalinkPattern.FindStringSubmatch(target); match != nil {
		ts := match[2] + "." + match[3]
		if parsed, err := url.Parse(target); err == nil {
			if parent := parsed.Query().Get("thread_ts"); parent != "" {
				ts = parent
			}
		}
		return boardsync.ResolvedTarget{Channel: match[1], Thread: match[1] + ":" + ts, Kind: boardsync.TargetThread}, nil
	}
	if strings.HasPrefix(target, "#") {
		channel, err := resolveName(ctx, client, target)
		if err != nil {
			return boardsync.ResolvedTarget{}, err
		}
		return boardsync.ResolvedTarget{Channel: channel, Kind: boardsync.TargetChannel}, nil
	}
	parts := strings.SplitN(target, ":", 2)
	if !channelIDPattern.MatchString(parts[0]) {
		return boardsync.ResolvedTarget{}, fmt.Errorf("%w: not a slack channel, thread, or message link: %s", boardsync.ErrBadRequest, target)
	}
	if len(parts) == 1 {
		return boardsync.ResolvedTarget{Channel: parts[0], Kind: boardsync.TargetChannel}, nil
	}
	if !isTS(parts[1]) {
		return boardsync.ResolvedTarget{}, fmt.Errorf("%w: not a slack thread ts: %s", boardsync.ErrBadRequest, parts[1])
	}
	return boardsync.ResolvedTarget{Channel: parts[0], Thread: parts[0] + ":" + parts[1], Kind: boardsync.TargetThread}, nil
}

func resolveName(ctx context.Context, client *Client, name string) (string, error) {
	if client == nil {
		return "", fmt.Errorf("%w: can't look up slack channel %s without a client", boardsync.ErrUnauthorized, name)
	}
	want := strings.TrimPrefix(name, "#")
	channels, err := client.ListChannels(ctx)
	if err != nil {
		return "", fmt.Errorf("can't list slack channels to find #%s: %w", want, err)
	}
	for _, channel := range channels {
		if channel.Name == want && channel.ID != "" {
			return channel.ID, nil
		}
	}
	return "", fmt.Errorf("%w: no slack channel named #%s", boardsync.ErrNotFound, want)
}

func isTS(ts string) bool {
	parts := strings.SplitN(ts, ".", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return false
	}
	for _, c := range parts[0] {
		if c < '0' || c > '9' {
			return false
		}
	}
	for _, c := range parts[1] {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func splitThread(channel, thread string) (string, error) {
	if thread == "" || thread == channel {
		return "", nil
	}
	prefix := channel + ":"
	if strings.HasPrefix(thread, prefix) {
		ts := strings.TrimPrefix(thread, prefix)
		if !isTS(ts) {
			return "", fmt.Errorf("%w: slack thread ts %q is invalid", boardsync.ErrBadRequest, ts)
		}
		return ts, nil
	}
	if isTS(thread) {
		return thread, nil
	}
	return "", fmt.Errorf("%w: slack thread %q is invalid", boardsync.ErrBadRequest, thread)
}

func tsToTime(ts string) time.Time {
	parts := strings.SplitN(ts, ".", 2)
	if len(parts) != 2 {
		return time.Time{}
	}
	seconds, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return time.Time{}
	}
	micros := parts[1]
	for len(micros) < 6 {
		micros += "0"
	}
	if len(micros) > 6 {
		micros = micros[:6]
	}
	us, _ := strconv.ParseInt(micros, 10, 64)
	return time.Unix(seconds, us*1000).UTC()
}
