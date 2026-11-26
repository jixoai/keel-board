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
	// TeamID pins this entry to one Slack workspace. A webhook carrying
	// another team is refused even if the signing secret verifies.
	TeamID string
	// APIBaseURL defaults to https://slack.com/api; tests point it at httptest.
	APIBaseURL string
	HTTPClient *http.Client
	// Now and Tolerance verify webhook timestamps. The defaults are time.Now
	// and five minutes.
	Now       func() time.Time
	Tolerance time.Duration
	// MaxBodyBytes bounds a webhook body. The default is 1 MiB.
	MaxBodyBytes int64
	// MaxFiles and MaxFileBytes bound attachments in either direction. The
	// defaults are 10 files and 25 MiB each; an inbound file over the byte
	// limit stays in the event but FetchFile refuses it.
	MaxFiles     int
	MaxFileBytes int64
	// FileSharePolls bounds how many times an upload looks for the message
	// Slack made. The default is 10, with Slack's short backoff.
	FileSharePolls int
	// Sleep is the wait between file-share polls. Tests replace it.
	Sleep func(ctx context.Context, d time.Duration) error
	// JoinChannels opts in to conversations.join for source reads. It is off
	// by default: joining announces the app in a channel and Slack starts
	// sending its events.
	JoinChannels bool
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
	if c.BotUserID == "" && c.BotID == "" {
		return fmt.Errorf("%w: slack needs BotUserID or BotID to skip its own posts", boardsync.ErrBadRequest)
	}
	if c.APIBaseURL != "" {
		parsed, err := url.Parse(c.APIBaseURL)
		if err != nil {
			return fmt.Errorf("%w: slack API base URL: %v", boardsync.ErrBadRequest, err)
		}
		if parsed.Scheme != "http" && parsed.Scheme != "https" {
			return fmt.Errorf("%w: slack API base URL must be http or https", boardsync.ErrBadRequest)
		}
	}
	if c.MaxFiles < 0 || c.MaxFileBytes < 0 || c.FileSharePolls < 0 {
		return fmt.Errorf("%w: slack file limits can't be negative", boardsync.ErrBadRequest)
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

func (c Config) maxFiles() int {
	if c.MaxFiles > 0 {
		return c.MaxFiles
	}
	return 10
}

func (c Config) maxFileBytes() int64 {
	if c.MaxFileBytes > 0 {
		return c.MaxFileBytes
	}
	return 25 << 20
}

func (c Config) fileSharePolls() int {
	if c.FileSharePolls > 0 {
		return c.FileSharePolls
	}
	return 10
}

func (c Config) sleep(ctx context.Context, d time.Duration) error {
	if c.Sleep != nil {
		return c.Sleep(ctx, d)
	}
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
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

// ErrFileRefused means Slack will not accept files from this app at all, or
// not this one. It is permanent; the text still goes out with a note.
var ErrFileRefused = errors.New("slack: file upload refused")

// APIError is a Slack Web API answer with ok=false. Code is Slack's error
// string, kept so callers can classify one refusal without matching text.
type APIError struct {
	Method string
	Code   string
	Err    error
}

func (e *APIError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("slack %s: %s (%v)", e.Method, e.Code, e.Err)
	}
	return fmt.Sprintf("slack %s: %s", e.Method, e.Code)
}

func (e *APIError) Unwrap() error { return e.Err }

// RateLimitError is a Slack rate-limit answer with its Retry-After, kept
// structured so a caller can wait before asking again.
type RateLimitError struct {
	Method     string
	RetryAfter time.Duration
}

func (e *RateLimitError) Error() string {
	if e.RetryAfter > 0 {
		return fmt.Sprintf("%s: slack %s rate-limited (retry after %s)", boardsync.ErrRateLimited, e.Method, e.RetryAfter)
	}
	return fmt.Sprintf("%s: slack %s rate-limited", boardsync.ErrRateLimited, e.Method)
}

func (e *RateLimitError) Unwrap() error { return boardsync.ErrRateLimited }

func rateLimitError(method string, header http.Header) *RateLimitError {
	return &RateLimitError{Method: method, RetryAfter: time.Duration(boardsync.RetryAfter(header)) * time.Second}
}

// fileRefusals are Slack's answers that mean "no files from this app", not
// "try again". They differ from a transport failure, where the app may have
// shared the file already.
var fileRefusals = map[string]bool{
	"missing_scope":                       true,
	"not_allowed_token_type":              true,
	"no_permission":                       true,
	"restricted_action":                   true,
	"file_uploads_disabled":               true,
	"file_uploads_except_images_disabled": true,
	"file_upload_size_restricted":         true,
}

// isFileRefusal reports whether err is Slack refusing files.
func isFileRefusal(err error) bool {
	return errors.Is(err, ErrFileRefused)
}

// request posts a form to one Slack method and decodes the response.
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
	response, err := c.apiClient().Do(request)
	if err != nil {
		return boardsync.RedactError(err, c.token)
	}
	defer response.Body.Close()
	if err := httpStatusError(method, response); err != nil {
		return err
	}
	body, err := readLimited(response.Body, 4<<20)
	if err != nil {
		return fmt.Errorf("slack %s answered an unreadable body: %w", method, err)
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

// apiClient refuses a redirect: a Web API call carries the token, and Slack
// does not redirect one. Keeping the token on Slack's host is worth more than
// following a surprise Location.
func (c *Client) apiClient() *http.Client {
	client := *c.http
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errors.New("slack: API redirect refused")
	}
	return &client
}

// httpStatusError classifies a non-2xx answer before any JSON is read. 5xx and
// transport failures are retryable; a refusing 4xx is permanent.
func httpStatusError(method string, response *http.Response) error {
	switch {
	case response.StatusCode == http.StatusTooManyRequests:
		return rateLimitError(method, response.Header)
	case response.StatusCode == http.StatusUnauthorized:
		return fmt.Errorf("%w: slack %s answered HTTP 401", boardsync.ErrUnauthorized, method)
	case response.StatusCode == http.StatusForbidden:
		return fmt.Errorf("%w: slack %s answered HTTP 403", boardsync.ErrUnauthorized, method)
	case response.StatusCode == http.StatusNotFound:
		return fmt.Errorf("%w: slack %s answered HTTP 404", boardsync.ErrNotFound, method)
	case response.StatusCode >= 400 && response.StatusCode < 500:
		return fmt.Errorf("%w: slack %s answered HTTP %d", boardsync.Permanent(errors.New("slack refused the request")), method, response.StatusCode)
	case response.StatusCode >= 500:
		return fmt.Errorf("slack %s answered HTTP %d", method, response.StatusCode)
	default:
		return nil
	}
}

func slackAPIError(method, code string) error {
	if code == "" {
		return &APIError{Method: method, Code: "unknown", Err: boardsync.Permanent(errors.New("slack answered no error code"))}
	}
	apiErr := &APIError{Method: method, Code: code}
	switch {
	case code == "invalid_auth" || code == "not_authed" || code == "token_revoked" || code == "account_inactive" || code == "org_login_required":
		apiErr.Err = boardsync.ErrUnauthorized
	case code == "missing_scope":
		// A missing scope is an authorization failure. On a file call it also
		// means the app may not take files, which the text can survive.
		apiErr.Err = boardsync.ErrUnauthorized
		if strings.HasPrefix(method, "files.") {
			apiErr.Err = fmt.Errorf("%w: %w", boardsync.ErrUnauthorized, ErrFileRefused)
		}
	case code == "channel_not_found" || code == "message_not_found" || code == "thread_not_found" || code == "user_not_found" || code == "file_not_found":
		apiErr.Err = boardsync.ErrNotFound
	case code == "ratelimited":
		apiErr.Err = &RateLimitError{Method: method}
	case fileRefusals[code] && strings.HasPrefix(method, "files."):
		apiErr.Err = fmt.Errorf("%w: %w", boardsync.ErrUnsupported, ErrFileRefused)
	case code == "not_in_channel" || code == "is_archived" || code == "restricted_action" || code == "cannot_reply_to_message" ||
		code == "cant_update_message" || code == "cant_delete_message" || code == "edit_window_closed" || code == "too_many_reactions":
		apiErr.Err = boardsync.Permanent(errors.New(code))
	case code == "not_allowed" || code == "not_supported" || code == "unknown_method" || code == "feature_not_enabled":
		apiErr.Err = boardsync.ErrUnsupported
	default:
		// Slack's remaining codes are request-shaped but not enumerated;
		// treat them as permanent rather than retrying the same refusal.
		apiErr.Err = boardsync.Permanent(errors.New(code))
	}
	return apiErr
}

// Channel is one Slack channel, as conversations.list describes it.
type Channel struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	IsMember    bool   `json:"is_member"`
	Private     bool   `json:"is_private"`
	IM          bool   `json:"is_im"`
	MPIM        bool   `json:"is_mpim"`
	IsExtShared bool   `json:"is_ext_shared"`
	IsArchived  bool   `json:"is_archived"`
}

type listResponse struct {
	apiEnvelope
	Channels []Channel `json:"channels"`
	Metadata struct {
		NextCursor string `json:"next_cursor"`
	} `json:"response_metadata"`
}

// ListChannels lists public and private channels, following Slack's cursor.
// Without groups:read Slack refuses the private listing; names then resolve
// for public channels only, and a private channel must be mapped by ID.
func (c *Client) ListChannels(ctx context.Context) ([]Channel, error) {
	channels, err := c.listChannels(ctx, "public_channel,private_channel")
	if err == nil {
		return channels, nil
	}
	if !isMissingScope(err) {
		return nil, err
	}
	return c.listChannels(ctx, "public_channel")
}

const maxListPages = 64

func (c *Client) listChannels(ctx context.Context, types string) ([]Channel, error) {
	var channels []Channel
	cursor := ""
	for page := 0; ; page++ {
		if page >= maxListPages {
			return nil, fmt.Errorf("slack conversations.list didn't finish after %d pages", maxListPages)
		}
		form := url.Values{"types": {types}, "exclude_archived": {"true"}, "limit": {"1000"}}
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

// isMissingScope reports whether Slack refused for a missing scope.
func isMissingScope(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.Code == "missing_scope"
}

// PostMessageRequest is the subset of chat.postMessage this adapter sends.
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

var errResponseTooLarge = errors.New("slack: response is too large")

func readLimited(reader io.Reader, max int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(reader, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > max {
		return nil, errResponseTooLarge
	}
	return body, nil
}

// channelIDPattern is a channel ID, public or private. A DM or group DM is
// not a sync target.
var channelIDPattern = regexp.MustCompile(`^[CG][A-Z0-9]{2,}$`)

// parseTarget reads a Slack target: #name, C0123, C0123:1727.9, or a
// message permalink (its thread). Name resolution uses client.
func parseTarget(ctx context.Context, client *Client, name string) (boardsync.ResolvedTarget, error) {
	target := strings.TrimSpace(name)
	if target == "" {
		return boardsync.ResolvedTarget{}, fmt.Errorf("%w: empty slack target", boardsync.ErrBadRequest)
	}
	if found, ok := parsePermalink(target); ok {
		return boardsync.ResolvedTarget{Channel: found.Channel, Thread: found.Channel + ":" + found.Thread, Kind: boardsync.TargetThread}, nil
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
