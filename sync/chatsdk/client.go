package chatsdk

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
)

// ClientConfig configures a board-to-sidecar client. The base URL is
// deployment configuration: a caller can never pass a URL per delivery.
type ClientConfig struct {
	// BaseURL is the sidecar's origin plus optional path prefix. It must use
	// HTTPS unless the host is loopback.
	BaseURL string
	// Secret signs requests in both directions. Required.
	Secret boardsync.Secret
	// Key is the entry this client serves; a delivery for another key is
	// refused before it leaves the process.
	Key boardsync.Key
	// Platform is the entry's platform. It lets a minimal resolve answer
	// ({channel,thread,kind}) be completed and validated; RemoteAdapter also
	// supplies it.
	Platform boardsync.Platform
	// SelfIDs are the sidecar's own author IDs, required by RemoteAdapter so
	// the board can drop the bridge's own posts. A bare Client may omit them.
	SelfIDs []string
	// HTTPClient overrides the default client. Redirects are always refused
	// on a copy, so a signed body never leaves the configured origin.
	HTTPClient *http.Client
	// Timeout bounds one attempt. Zero means 15 seconds; it is ignored when
	// HTTPClient supplies its own timeout.
	Timeout time.Duration
	// Retry is the delivery retry policy for a direct Client.Deliver. Zero
	// means sync.DefaultRetry. RemoteAdapter deliveries are single-attempt:
	// the durable outbox owns their retry budget.
	Retry boardsync.RetryPolicy
	// Now and Nonce are test seams. Nonce must return a new value per call.
	Now   func() time.Time
	Nonce func() (string, error)
}

// Client speaks the board-to-sidecar half of bridge v1.
type Client struct {
	baseURL  string
	secret   boardsync.Secret
	key      boardsync.Key
	platform boardsync.Platform
	selfIDs  []string
	http     *http.Client
	retry    boardsync.RetryPolicy
	now      func() time.Time
	nonce    func() (string, error)
}

// NewClient validates cfg and returns a client. It performs no I/O.
func NewClient(cfg ClientConfig) (*Client, error) {
	base, err := normalizeBaseURL(cfg.BaseURL)
	if err != nil {
		return nil, err
	}
	if cfg.Secret.Empty() {
		return nil, fmt.Errorf("%w: bridge client secret is unset", boardsync.ErrUnauthorized)
	}
	if !cfg.Key.Valid() {
		return nil, fmt.Errorf("%w: %q isn't a sync key", boardsync.ErrBadRequest, cfg.Key)
	}
	selfIDs, err := cleanSelfIDs(cfg.SelfIDs)
	if err != nil {
		return nil, err
	}
	retry, err := cfg.Retry.Validate()
	if err != nil {
		return nil, err
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Nonce == nil {
		cfg.Nonce = randomNonce
	}
	client := cfg.HTTPClient
	if client == nil {
		timeout := cfg.Timeout
		if timeout <= 0 {
			timeout = 15 * time.Second
		}
		client = &http.Client{Timeout: timeout}
	} else {
		// Never mutate the caller's client. Even a caller-supplied redirect
		// policy is overridden: a redirect would take a signed body somewhere
		// the operator didn't configure.
		clone := *client
		client = &clone
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &Client{
		baseURL: base, secret: cfg.Secret, key: cfg.Key, platform: cfg.Platform,
		selfIDs: selfIDs, http: client, retry: retry, now: cfg.Now, nonce: cfg.Nonce,
	}, nil
}

// normalizeBaseURL validates a configured base URL and strips its trailing
// slash. Cleartext HTTP is accepted only for loopback, where a test sidecar
// lives.
func normalizeBaseURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("%w: bridge base URL is unset", boardsync.ErrBadRequest)
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("%w: bridge base URL: %v", boardsync.ErrBadRequest, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("%w: bridge base URL must be http or https", boardsync.ErrBadRequest)
	}
	if parsed.Host == "" || parsed.User != nil {
		return "", fmt.Errorf("%w: bridge base URL needs a host and no user info", boardsync.ErrBadRequest)
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("%w: bridge base URL carries no query or fragment", boardsync.ErrBadRequest)
	}
	if parsed.Scheme == "http" && !loopbackHost(parsed.Hostname()) {
		return "", fmt.Errorf("%w: a non-loopback bridge must use https", boardsync.ErrBadRequest)
	}
	return strings.TrimRight(parsed.String(), "/"), nil
}

func loopbackHost(host string) bool {
	host = strings.ToLower(host)
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return true
	}
	address := net.ParseIP(host)
	return address != nil && address.IsLoopback()
}

func randomNonce() (string, error) {
	var raw [24]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("chatsdk: nonce: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}

// Deliver posts, edits, deletes, or reacts through the sidecar. It retries
// transient and rate-limited failures with the configured policy, signing
// every attempt fresh but keeping the delivery ID.
func (c *Client) Deliver(ctx context.Context, req DeliverRequest) (boardsync.Posted, error) {
	return c.deliver(ctx, req, c.retry)
}

// deliverOnce is the outbound-provider path: the durable outbox owns the retry
// budget, so one provider call is one attempt, never five times five.
func (c *Client) deliverOnce(ctx context.Context, req DeliverRequest) (boardsync.Posted, error) {
	return c.deliver(ctx, req, boardsync.RetryPolicy{MaxAttempts: 1})
}

func (c *Client) deliver(ctx context.Context, req DeliverRequest, policy boardsync.RetryPolicy) (boardsync.Posted, error) {
	if req.Key == "" {
		req.Key = c.key
	}
	if req.Key != c.key {
		return boardsync.Posted{}, fmt.Errorf("%w: delivery key %q isn't this client's %q", boardsync.ErrBadRequest, req.Key, c.key)
	}
	if err := req.Validate(); err != nil {
		return boardsync.Posted{}, err
	}
	var posted boardsync.Posted
	if err := c.post(ctx, "/deliver", req, &posted, policy); err != nil {
		return boardsync.Posted{}, err
	}
	if err := posted.Validate(); err != nil {
		return boardsync.Posted{}, boardsync.Permanent(fmt.Errorf("chatsdk: sidecar deliver response: %w", err))
	}
	return posted, nil
}

// Resolve asks the sidecar to turn a channel name or native target into an ID
// or a complete target.
func (c *Client) Resolve(ctx context.Context, req ResolveRequest) (ResolveResponse, error) {
	if err := req.Validate(); err != nil {
		return ResolveResponse{}, err
	}
	var response ResolveResponse
	if err := c.post(ctx, "/resolve", req, &response, c.retry); err != nil {
		return ResolveResponse{}, err
	}
	if req.Name != "" {
		if response.Channel == "" {
			return ResolveResponse{}, boardsync.Permanent(fmt.Errorf("chatsdk: sidecar answered no channel for %q", req.Name))
		}
		return response, nil
	}
	if response.Target == nil {
		return ResolveResponse{}, boardsync.Permanent(fmt.Errorf("chatsdk: sidecar answered no target for %q", req.Target))
	}
	// A sidecar may answer the minimal {channel,thread,kind}; the client owns
	// the entry identity and fills it before validation.
	if response.Target.Key == "" {
		response.Target.Key = c.key
	}
	if response.Target.Platform == "" && c.platform != "" {
		response.Target.Platform = c.platform
	}
	if response.Target.Key != c.key {
		return ResolveResponse{}, boardsync.Permanent(fmt.Errorf("%w: sidecar target belongs to %q, not %q", boardsync.ErrBadRequest, response.Target.Key, c.key))
	}
	if c.platform != "" && response.Target.Platform != c.platform {
		return ResolveResponse{}, boardsync.Permanent(fmt.Errorf("%w: sidecar target is for %q, not %q", boardsync.ErrBadRequest, response.Target.Platform, c.platform))
	}
	if err := response.Target.Validate(); err != nil {
		return ResolveResponse{}, boardsync.Permanent(fmt.Errorf("chatsdk: sidecar target: %w", err))
	}
	return response, nil
}

// post signs and sends one payload, retrying with policy.
func (c *Client) post(ctx context.Context, path string, payload, out any, policy boardsync.RetryPolicy) error {
	_, err := boardsync.DoCount(ctx, policy, func(ctx context.Context) error {
		return c.attempt(ctx, path, payload, out)
	})
	return err
}

func (c *Client) attempt(ctx context.Context, path string, payload, out any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return boardsync.Permanent(fmt.Errorf("chatsdk: encode %s: %w", path, err))
	}
	if len(body) > MaxBodyBytes {
		return boardsync.Permanent(fmt.Errorf("%w: bridge %s body is larger than %d bytes", boardsync.ErrBadRequest, path, MaxBodyBytes))
	}
	timestamp := c.now().Unix()
	nonce, err := c.nonce()
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint(path), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("chatsdk: build %s request: %w", path, err)
	}
	request.Header.Set("Content-Type", "application/json; charset=utf-8")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Cache-Control", "no-store")
	request.Header.Set(HeaderTimestamp, fmt.Sprintf("%d", timestamp))
	request.Header.Set(HeaderNonce, nonce)
	request.Header.Set(HeaderSignature, SignV1(c.secret, timestamp, nonce, body))
	response, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("chatsdk: %s: %w", path, err)
	}
	defer response.Body.Close()
	data, readErr := io.ReadAll(io.LimitReader(response.Body, MaxResponseBytes+1))
	if readErr != nil {
		return fmt.Errorf("chatsdk: read %s response: %w", path, readErr)
	}
	// Status first: a 5xx is retryable even when its body is oversized. Only
	// a successful answer has to fit the response contract.
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return bridgeResponseError(response.StatusCode, response.Header, data)
	}
	if len(data) > MaxResponseBytes {
		return boardsync.Permanent(fmt.Errorf("chatsdk: %s response is larger than %d bytes", path, MaxResponseBytes))
	}
	if out == nil {
		return nil
	}
	if len(data) == 0 {
		return boardsync.Permanent(fmt.Errorf("chatsdk: %s answered an empty body", path))
	}
	if err := json.Unmarshal(data, out); err != nil {
		return boardsync.Permanent(fmt.Errorf("chatsdk: %s answer isn't JSON: %w", path, err))
	}
	return nil
}

func (c *Client) endpoint(path string) string {
	return c.baseURL + path
}

// bridgeResponseError turns a sidecar's non-2xx answer into the matching
// bridge error. A body that isn't a bridge error is classified by status, so
// an arbitrary proxy page can't invent a retryable code.
func bridgeResponseError(status int, header http.Header, body []byte) error {
	code := codeForStatus(status)
	var parsed BridgeError
	if len(body) > 0 {
		if err := json.Unmarshal(body, &parsed); err == nil && parsed.Code.Valid() {
			code = parsed.Code
		}
	}
	failure := &BridgeError{Code: code, Message: parsed.Message, RetryAfter: parsed.RetryAfter}
	if failure.RetryAfter == 0 {
		failure.RetryAfter = boardsync.RetryAfter(header)
	}
	return failure
}

func codeForStatus(status int) ErrorCode {
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return ErrorAuth
	case status == http.StatusTooManyRequests:
		return ErrorRateLimited
	case status >= 500:
		return ErrorTransient
	default:
		return ErrorPermanent
	}
}

// RemoteAdapter turns a sidecar Client into a normal sync.Provider. Inbound
// raw platform webhooks are refused: the sidecar pushes normalized events
// through the bridge route, not through Provider.Receive.
type RemoteAdapter struct {
	Client       *Client
	PlatformName boardsync.Platform
	Caps         boardsync.Capabilities
}

// NewRemoteAdapter validates a sidecar adapter. It requires the sidecar's own
// author IDs: without them the board would import the bridge's own posts.
func NewRemoteAdapter(client *Client, platform boardsync.Platform, caps boardsync.Capabilities) (*RemoteAdapter, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: a remote adapter needs a bridge client", boardsync.ErrBadRequest)
	}
	if strings.TrimSpace(string(platform)) == "" {
		return nil, fmt.Errorf("%w: a remote adapter needs a platform", boardsync.ErrBadRequest)
	}
	if len(client.selfIDs) == 0 {
		return nil, fmt.Errorf("%w: a remote adapter needs client SelfIDs so its own posts are not imported", boardsync.ErrBadRequest)
	}
	if client.platform != "" && client.platform != platform {
		return nil, fmt.Errorf("%w: bridge client serves %q, not %q", boardsync.ErrBadRequest, client.platform, platform)
	}
	client.platform = platform
	return &RemoteAdapter{Client: client, PlatformName: platform, Caps: caps}, nil
}

// Platform implements chatsdk.Adapter.
func (a *RemoteAdapter) Platform() string { return string(a.PlatformName) }

// SelfIDs implements boardsync.SelfIdentifier through the sidecar's identity.
func (a *RemoteAdapter) SelfIDs() []string { return append([]string(nil), a.Client.selfIDs...) }

// Verify refuses a raw platform webhook. A sidecar verifies the platform and
// pushes normalized events to the bridge route.
func (a *RemoteAdapter) Verify(context.Context, *boardsync.WebhookRequest) error {
	return fmt.Errorf("%w: a remote bridge accepts only normalized events", boardsync.ErrUnsupported)
}

// Decode refuses a raw platform webhook for the same reason as Verify.
func (a *RemoteAdapter) Decode(context.Context, boardsync.Key, *boardsync.WebhookRequest) (DecodeResult, error) {
	return DecodeResult{}, fmt.Errorf("%w: a remote bridge accepts only normalized events", boardsync.ErrUnsupported)
}

// Send implements Adapter.
func (a *RemoteAdapter) Send(ctx context.Context, key boardsync.Key, target boardsync.ResolvedTarget, msg boardsync.OutboundMessage) (boardsync.Posted, error) {
	if len(msg.Files) > 0 {
		return boardsync.Posted{}, fmt.Errorf("%w: bridge v1 carries no file bytes", boardsync.ErrUnsupported)
	}
	return a.Client.deliverOnce(ctx, DeliverRequest{
		DeliveryID: a.deliveryID(DeliverMessage, target, "", msg, "", false),
		Key:        key,
		Target:     target,
		Kind:       DeliverMessage,
		Body:       outboundBody(msg),
	})
}

// Edit implements Editor when the adapter's capabilities allow it.
func (a *RemoteAdapter) Edit(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message string, msg boardsync.OutboundMessage) error {
	if !a.Caps.Edits {
		return fmt.Errorf("%w: %s sidecar can't edit", boardsync.ErrUnsupported, a.PlatformName)
	}
	if len(msg.Files) > 0 {
		return fmt.Errorf("%w: bridge v1 carries no file bytes", boardsync.ErrUnsupported)
	}
	target, err := targetForThread(key, thread, a.PlatformName)
	if err != nil {
		return err
	}
	_, err = a.Client.deliverOnce(ctx, DeliverRequest{
		DeliveryID: a.deliveryID(DeliverEdit, target, message, msg, "", false),
		Key:        key,
		Target:     target,
		Kind:       DeliverEdit,
		Body:       outboundBody(msg),
		Message:    message,
	})
	return err
}

// Delete implements Deleter when the adapter's capabilities allow it.
func (a *RemoteAdapter) Delete(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message string) error {
	if !a.Caps.Deletes {
		return fmt.Errorf("%w: %s sidecar can't delete", boardsync.ErrUnsupported, a.PlatformName)
	}
	target, err := targetForThread(key, thread, a.PlatformName)
	if err != nil {
		return err
	}
	_, err = a.Client.deliverOnce(ctx, DeliverRequest{
		DeliveryID: a.deliveryID(DeliverDelete, target, message, boardsync.OutboundMessage{}, "", false),
		Key:        key,
		Target:     target,
		Kind:       DeliverDelete,
		Message:    message,
	})
	return err
}

// React implements Reactor when the adapter's capabilities allow it.
func (a *RemoteAdapter) React(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message, emoji string, added bool) error {
	if !a.Caps.Reactions {
		return fmt.Errorf("%w: %s sidecar can't react", boardsync.ErrUnsupported, a.PlatformName)
	}
	target, err := targetForThread(key, thread, a.PlatformName)
	if err != nil {
		return err
	}
	_, err = a.Client.deliverOnce(ctx, DeliverRequest{
		DeliveryID: a.deliveryID(DeliverReaction, target, message, boardsync.OutboundMessage{}, emoji, added),
		Key:        key,
		Target:     target,
		Kind:       DeliverReaction,
		Message:    message,
		Emoji:      emoji,
		Added:      added,
	})
	return err
}

// ResolveChannel implements ChannelResolver.
func (a *RemoteAdapter) ResolveChannel(ctx context.Context, key boardsync.Key, name string) (string, error) {
	if key != a.Client.key {
		return "", fmt.Errorf("%w: entry %q isn't this sidecar's %q", boardsync.ErrBadRequest, key, a.Client.key)
	}
	response, err := a.Client.Resolve(ctx, ResolveRequest{Name: name})
	if err != nil {
		return "", err
	}
	return response.Channel, nil
}

// ResolveTarget implements TargetResolver.
func (a *RemoteAdapter) ResolveTarget(ctx context.Context, key boardsync.Key, target string) (boardsync.ResolvedTarget, error) {
	if key != a.Client.key {
		return boardsync.ResolvedTarget{}, fmt.Errorf("%w: entry %q isn't this sidecar's %q", boardsync.ErrBadRequest, key, a.Client.key)
	}
	response, err := a.Client.Resolve(ctx, ResolveRequest{Target: target})
	if err != nil {
		return boardsync.ResolvedTarget{}, err
	}
	resolved := *response.Target
	if resolved.Key != "" && resolved.Key != key {
		return boardsync.ResolvedTarget{}, boardsync.Permanent(fmt.Errorf("%w: sidecar target belongs to %q", boardsync.ErrBadRequest, resolved.Key))
	}
	resolved.Key = key
	if resolved.Platform != "" && resolved.Platform != a.PlatformName {
		return boardsync.ResolvedTarget{}, boardsync.Permanent(fmt.Errorf("%w: sidecar target is for %q, not %q", boardsync.ErrBadRequest, resolved.Platform, a.PlatformName))
	}
	resolved.Platform = a.PlatformName
	if err := resolved.Validate(); err != nil {
		return boardsync.ResolvedTarget{}, boardsync.Permanent(err)
	}
	return resolved, nil
}

func targetForThread(key boardsync.Key, thread boardsync.ThreadRef, platform boardsync.Platform) (boardsync.ResolvedTarget, error) {
	if thread.Key != "" && thread.Key != key {
		return boardsync.ResolvedTarget{}, fmt.Errorf("%w: thread belongs to %q, not %q", boardsync.ErrBadRequest, thread.Key, key)
	}
	if thread.Platform != "" && thread.Platform != platform {
		return boardsync.ResolvedTarget{}, fmt.Errorf("%w: thread belongs to %q, not %q", boardsync.ErrBadRequest, thread.Platform, platform)
	}
	target := boardsync.ResolvedTarget{
		Key: key, Platform: platform, Channel: thread.Channel, Thread: thread.Thread,
		Subthread: thread.Subthread, Kind: boardsync.TargetThread,
	}
	if thread.Thread == "" {
		target.Kind = boardsync.TargetChannel
	}
	if err := target.Validate(); err != nil {
		return boardsync.ResolvedTarget{}, err
	}
	return target, nil
}

func outboundBody(msg boardsync.OutboundMessage) OutboundBody {
	return OutboundBody{
		Markdown: msg.Markdown, Sender: msg.Sender, External: msg.External,
		BoardMessage: msg.BoardMessage, BoardThread: msg.BoardThread,
	}
}

// deliveryID is deterministic for one content delivery. A retry reuses it; a
// distinct edit or message gets a different one even when it names the same
// board message.
func (a *RemoteAdapter) deliveryID(kind DeliverKind, target boardsync.ResolvedTarget, message string, msg boardsync.OutboundMessage, emoji string, added bool) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		string(kind), string(a.Client.key), target.External(),
		fmt.Sprintf("%d/%d", msg.BoardMessage, msg.BoardThread),
		message, emoji, fmt.Sprintf("%t/%t", added, msg.External), msg.Markdown,
	}, "\x00")))
	return fmt.Sprintf("delivery:%s:%s:%x", kind, a.Client.key, sum[:12])
}

// Interface checks.
var (
	_ Adapter                  = (*RemoteAdapter)(nil)
	_ boardsync.SelfIdentifier = (*RemoteAdapter)(nil)
	_ Editor                   = (*RemoteAdapter)(nil)
	_ Deleter                  = (*RemoteAdapter)(nil)
	_ Reactor                  = (*RemoteAdapter)(nil)
	_ ChannelResolver          = (*RemoteAdapter)(nil)
	_ TargetResolver           = (*RemoteAdapter)(nil)
)
