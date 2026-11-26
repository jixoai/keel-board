package slack

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync"

	boardsync "github.com/jixoai/keel-board/sync"
)

// Provider implements boardsync.Provider for one Slack app.
type Provider struct {
	cfg    Config
	client *Client

	mu    sync.Mutex
	names map[string]string
}

// New returns a Slack provider. It validates configuration and builds the Web
// API client; it performs no network I/O.
func New(cfg Config) (*Provider, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Provider{
		cfg:    cfg,
		client: NewClient(cfg.baseURL(), cfg.httpClient(), cfg.BotToken),
		names:  make(map[string]string),
	}, nil
}

// Platform implements boardsync.Provider.
func (p *Provider) Platform() boardsync.Platform { return "slack" }

// Capabilities implements boardsync.Provider.
func (p *Provider) Capabilities() boardsync.Capabilities {
	return boardsync.Capabilities{StartThreads: true, Edits: true, Deletes: true, Reactions: true}
}

// Receive verifies Slack's signature before parsing anything, answers a URL
// verification challenge, and normalizes message, edit, delete, and reaction
// events.
func (p *Provider) Receive(ctx context.Context, key boardsync.Key, req *boardsync.WebhookRequest) (boardsync.Intake, error) {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return boardsync.Intake{}, fmt.Errorf("%w: entry %q isn't this slack app's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	if req == nil || len(req.Body) == 0 {
		return boardsync.Intake{}, fmt.Errorf("%w: empty slack webhook", boardsync.ErrBadRequest)
	}
	if int64(len(req.Body)) > p.cfg.maxBody() {
		return boardsync.Intake{}, fmt.Errorf("%w: slack webhook body is too large", boardsync.ErrBadRequest)
	}
	if err := verifyRequest(p.cfg.SigningSecret, req, p.cfg.now(), p.cfg.tolerance()); err != nil {
		return boardsync.Intake{}, err
	}
	var envelope webhookEnvelope
	if err := decodeJSON(req.Body, &envelope); err != nil {
		return boardsync.Intake{}, err
	}
	switch envelope.Type {
	case "url_verification":
		if envelope.Challenge == "" {
			return boardsync.Intake{}, fmt.Errorf("%w: slack challenge is empty", boardsync.ErrBadRequest)
		}
		body, _ := json.Marshal(map[string]string{"challenge": envelope.Challenge})
		return boardsync.Intake{Response: boardsync.WebhookResponse{
			Status: 200, ContentType: "application/json", Body: body,
		}}, nil
	case "event_callback":
		if len(envelope.Event) == 0 {
			return boardsync.Intake{Response: boardsync.OK()}, nil
		}
		var event slackEvent
		if err := json.Unmarshal(envelope.Event, &event); err != nil {
			return boardsync.Intake{}, fmt.Errorf("%w: slack event: %v", boardsync.ErrBadRequest, err)
		}
		record, ok, err := normalizeEvent(key, envelope, event, p.cfg)
		if err != nil {
			return boardsync.Intake{}, err
		}
		if !ok {
			return boardsync.Intake{Response: boardsync.OK()}, nil
		}
		record.Event.Raw = append([]byte(nil), envelope.Event...)
		if record.Event.ID == "" {
			record.Event.ID = record.Dedupe
		}
		return boardsync.Intake{Response: boardsync.OK(), Records: []boardsync.Record{record}}, nil
	default:
		// app_rate_limited and unknown envelope types carry nothing to import.
		return boardsync.Intake{Response: boardsync.OK()}, nil
	}
}

// ResolveChannel turns "#name" into a channel ID. IDs pass through; names use
// one cached conversations.list.
func (p *Provider) ResolveChannel(ctx context.Context, key boardsync.Key, name string) (string, error) {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return "", fmt.Errorf("%w: entry %q isn't this slack app's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	if !strings.HasPrefix(name, "#") {
		if channelIDPattern.MatchString(name) {
			return name, nil
		}
		return "", fmt.Errorf("%w: not a slack channel or name: %s", boardsync.ErrBadRequest, name)
	}
	name = strings.TrimPrefix(name, "#")
	p.mu.Lock()
	if id, ok := p.names[name]; ok {
		p.mu.Unlock()
		return id, nil
	}
	p.mu.Unlock()
	channels, err := p.client.ListChannels(ctx)
	if err != nil {
		return "", fmt.Errorf("can't list slack channels to find #%s: %w", name, err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, channel := range channels {
		if channel.ID != "" {
			p.names[channel.Name] = channel.ID
		}
	}
	if id, ok := p.names[name]; ok {
		return id, nil
	}
	return "", fmt.Errorf("%w: no slack channel named #%s", boardsync.ErrNotFound, name)
}

// ResolveTarget reads #name, C0123, C0123:1727.9, or a message permalink.
func (p *Provider) ResolveTarget(ctx context.Context, key boardsync.Key, target string) (boardsync.ResolvedTarget, error) {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return boardsync.ResolvedTarget{}, fmt.Errorf("%w: entry %q isn't this slack app's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	resolved, err := parseTarget(ctx, p.client, target)
	if err != nil {
		return boardsync.ResolvedTarget{}, err
	}
	resolved.Key = key
	resolved.Platform = "slack"
	if err := resolved.Validate(); err != nil {
		return boardsync.ResolvedTarget{}, err
	}
	return resolved, nil
}

// Post posts a message. A channel target starts a thread; a thread target
// replies in it. Files are not implemented in this skeleton.
func (p *Provider) Post(ctx context.Context, key boardsync.Key, target boardsync.ResolvedTarget, msg boardsync.OutboundMessage) (boardsync.Posted, error) {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return boardsync.Posted{}, fmt.Errorf("%w: entry %q isn't this slack app's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	if err := target.Validate(); err != nil {
		return boardsync.Posted{}, err
	}
	if target.Key != key {
		return boardsync.Posted{}, fmt.Errorf("%w: target belongs to %q", boardsync.ErrBadRequest, target.Key)
	}
	if len(msg.Files) > 0 {
		return boardsync.Posted{}, fmt.Errorf("%w: slack file upload is not implemented in this skeleton", boardsync.ErrUnsupported)
	}
	if strings.TrimSpace(msg.Markdown) == "" {
		return boardsync.Posted{}, fmt.Errorf("%w: slack message has no text", boardsync.ErrBadRequest)
	}
	threadTS, err := splitThread(target.Channel, target.Thread)
	if err != nil {
		return boardsync.Posted{}, err
	}
	request := PostMessageRequest{
		Channel:      target.Channel,
		ThreadTS:     threadTS,
		Text:         msg.Markdown,
		MarkdownText: msg.Markdown,
	}
	if msg.Sender != nil {
		request.Username = clampUsername(msg.Sender.Name, 80)
		request.IconURL = msg.Sender.Avatar
	}
	ts, err := p.client.PostMessage(ctx, request)
	if err != nil {
		return boardsync.Posted{}, err
	}
	thread := target.Channel + ":" + ts
	if threadTS != "" {
		thread = target.Channel + ":" + threadTS
	}
	return boardsync.Posted{Thread: thread, Message: ts}, nil
}

// Edit replaces a delivered message's text.
func (p *Provider) Edit(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message string, msg boardsync.OutboundMessage) error {
	channel, ts, err := p.messageWhere(key, thread, message)
	if err != nil {
		return err
	}
	return p.client.UpdateMessage(ctx, channel, ts, msg.Markdown, msg.Markdown)
}

// Delete deletes a delivered message.
func (p *Provider) Delete(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message string) error {
	channel, ts, err := p.messageWhere(key, thread, message)
	if err != nil {
		return err
	}
	return p.client.DeleteMessage(ctx, channel, ts)
}

// React adds or removes a reaction on a delivered message.
func (p *Provider) React(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message, emoji string, added bool) error {
	channel, ts, err := p.messageWhere(key, thread, message)
	if err != nil {
		return err
	}
	return p.client.React(ctx, channel, ts, emoji, added)
}

// Permalink returns Slack's own URL for an event's message.
func (p *Provider) Permalink(ctx context.Context, key boardsync.Key, event boardsync.Event) (string, error) {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return "", fmt.Errorf("%w: entry %q isn't this slack app's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	if event.Kind == boardsync.KindDeleted || event.Message == "" {
		return "", nil
	}
	return p.client.Permalink(ctx, event.Channel, event.Message)
}

// Locate reads a Slack permalink as its channel, thread, and message.
func (p *Provider) Locate(link string) (string, string, bool) {
	match := permalinkPattern.FindStringSubmatch(strings.TrimSpace(link))
	if match == nil {
		return "", "", false
	}
	ts := match[2] + "." + match[3]
	thread := match[1] + ":" + ts
	if parsed, err := url.Parse(link); err == nil {
		if parent := parsed.Query().Get("thread_ts"); parent != "" {
			thread = match[1] + ":" + parent
		}
	}
	return thread, ts, true
}

// URL has no workspace host to build a link with; Slack's own is fetched by
// Permalink instead.
func (p *Provider) URL(string, string) (string, error) { return "", nil }

func (p *Provider) messageWhere(key boardsync.Key, thread boardsync.ThreadRef, message string) (string, string, error) {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return "", "", fmt.Errorf("%w: entry %q isn't this slack app's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	if thread.Key != key {
		return "", "", fmt.Errorf("%w: thread belongs to %q", boardsync.ErrBadRequest, thread.Key)
	}
	channel := thread.Channel
	ts := message
	if ts == "" {
		tsFromThread, err := splitThread(thread.Channel, thread.Thread)
		if err != nil {
			return "", "", err
		}
		ts = tsFromThread
	}
	if channel == "" || ts == "" {
		return "", "", fmt.Errorf("%w: slack change needs a channel and a message ts", boardsync.ErrBadRequest)
	}
	return channel, ts, nil
}

func clampUsername(name string, max int) string {
	if len(name) <= max {
		return name
	}
	return name[:max]
}

// Interface checks.
var (
	_ boardsync.Provider    = (*Provider)(nil)
	_ boardsync.Editor      = (*Provider)(nil)
	_ boardsync.Deleter     = (*Provider)(nil)
	_ boardsync.Reactor     = (*Provider)(nil)
	_ boardsync.Permalinker = (*Provider)(nil)
	_ boardsync.Linker      = (*Provider)(nil)
)
