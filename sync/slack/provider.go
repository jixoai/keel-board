package slack

import (
	"context"
	"encoding/json"
	"fmt"
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

	scopesMu          sync.Mutex
	scopes            []string
	scopesLoaded      bool
	customizeDisabled bool
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
	return boardsync.Capabilities{
		StartThreads:           true,
		UploadsFiles:           true,
		StartsThreadsWithFiles: false,
		Edits:                  true,
		Deletes:                true,
		Reactions:              true,
		Typing:                 true,
	}
}

// Receive verifies Slack's signature before parsing anything, answers a URL
// verification challenge, and normalizes message, edit, delete, reaction,
// file, and link events. It performs no network I/O, so the registry can
// store what it returns and answer the platform inside Slack's three seconds.
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
		if p.cfg.TeamID != "" && envelope.TeamID != p.cfg.TeamID {
			return boardsync.Intake{}, fmt.Errorf("%w: slack webhook is for team %q, not %q", boardsync.ErrUnauthorized, envelope.TeamID, p.cfg.TeamID)
		}
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
	trimmed := strings.TrimSpace(target)
	if strings.HasPrefix(trimmed, "#") {
		channel, err := p.ResolveChannel(ctx, key, trimmed)
		if err != nil {
			return boardsync.ResolvedTarget{}, err
		}
		resolved := boardsync.ResolvedTarget{Key: key, Platform: "slack", Channel: channel, Kind: boardsync.TargetChannel}
		if err := resolved.Validate(); err != nil {
			return boardsync.ResolvedTarget{}, err
		}
		return resolved, nil
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
// replies in it. A message with files posts its text first, then uploads the
// files into the same thread, so the receipt is always a real text ts. A file
// that fails becomes a line in the text instead of losing the message.
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
	if target.Platform != "" && target.Platform != "slack" {
		return boardsync.Posted{}, fmt.Errorf("%w: %q isn't a slack target", boardsync.ErrBadRequest, target.Platform)
	}
	threadTS, err := splitThread(target.Channel, target.Thread)
	if err != nil {
		return boardsync.Posted{}, err
	}
	rawText := msg.Markdown
	if strings.TrimSpace(outboundText(rawText)) == "" {
		rawText = ""
	}
	plan := planUploads(msg.Files, p.cfg)
	if rawText == "" && len(plan.files) == 0 && len(plan.notes) == 0 {
		return boardsync.Posted{}, fmt.Errorf("%w: slack message has no text and no files", boardsync.ErrBadRequest)
	}
	if len(plan.files) == 0 {
		ts, _, err := p.postText(ctx, target.Channel, threadTS, rawText, plan.notes, msg.Sender, msg.External, key)
		if err != nil {
			return boardsync.Posted{}, err
		}
		return p.posted(target.Channel, threadTS, ts, len(msg.Files) > 0), nil
	}
	// Always post the text first and keep its real ts, then upload the files
	// into the same thread. A file upload has no usable message ID of its own
	// until Slack shares it, and a guessed ID would make a later edit or
	// delete hit the wrong message.
	return p.postTextThenFiles(ctx, target.Channel, threadTS, rawText, plan, msg, key)
}

// postTextThenFiles posts the text first, so a sender name still applies, and
// uploads the files into the same thread as their own message. The text is
// already in Slack, so an upload failure edits in the file notes and is a
// partial success: retrying would duplicate the text.
func (p *Provider) postTextThenFiles(ctx context.Context, channel, threadTS, rawText string, plan uploadPlan, msg boardsync.OutboundMessage, key boardsync.Key) (boardsync.Posted, error) {
	notes := append([]string{}, plan.notes...)
	if rawText == "" {
		notes = append(attachmentNotes(plan.files), notes...)
	}
	ts, named, err := p.postText(ctx, channel, threadTS, rawText, notes, msg.Sender, msg.External, key)
	if err != nil {
		return boardsync.Posted{}, err
	}
	uploadThread := threadTS
	if uploadThread == "" {
		uploadThread = ts
	}
	ids, err := p.uploadFiles(ctx, channel, uploadThread, "", plan.files)
	if err != nil {
		// Do not repeat the attachment list the successful text already
		// carries when the message had no text of its own.
		fallback := append([]string{}, plan.notes...)
		fallback = append(fallback, failureNotes(plan.files, msg.BoardMessage, err)...)
		label := ""
		if named && msg.Sender != nil {
			label = cleanText(msg.Sender.Name)
		}
		body := p.renderBody(rawText, fallback, msg.External, key, label)
		// The text is the receipt; a note edit that fails must not make the
		// delivery retry and post the text again.
		if updateErr := p.updateMessage(ctx, channel, ts, body, msg.External); updateErr != nil {
			p.logf("slack: couldn't add file notes to message %s: %v", ts, updateErr)
		}
		return p.posted(channel, threadTS, ts, true), nil
	}
	posted := p.posted(channel, threadTS, ts, true)
	posted.Files = ids
	return posted, nil
}

func (p *Provider) logf(format string, args ...any) {
	if p.cfg.Logf != nil {
		p.cfg.Logf(format, args...)
	}
}

func (p *Provider) posted(channel, threadTS, ts string, hadFiles bool) boardsync.Posted {
	thread := channel + ":" + ts
	if threadTS != "" {
		thread = channel + ":" + threadTS
	}
	posted := boardsync.Posted{Thread: thread, Message: ts}
	if hadFiles {
		// An empty, non-nil list says files were considered and the platform
		// took none (the text carries the note).
		posted.Files = []string{}
	}
	return posted
}

// postText posts text, under the sender's name when the app may set one and
// falls back to the app with the label when Slack refuses it.
func (p *Provider) postText(ctx context.Context, channel, threadTS, rawText string, notes []string, sender *boardsync.Sender, external bool, key boardsync.Key) (string, bool, error) {
	request := PostMessageRequest{Channel: channel, ThreadTS: threadTS}
	named := false
	label := ""
	if sender != nil {
		if username, icon, ok := p.senderFields(ctx, sender); ok {
			request.Username = username
			request.IconURL = icon
			label = cleanText(sender.Name)
			named = true
		}
	}
	body := p.renderBody(rawText, notes, external, key, label)
	if strings.TrimSpace(body) == "" {
		return "", false, fmt.Errorf("%w: slack message has no text", boardsync.ErrBadRequest)
	}
	if external {
		// External text is escaped plain text only: Slack's markdown field
		// must not get a second chance to revive its control syntax.
		request.Text = body
	} else {
		request.MarkdownText = body
	}
	ts, err := p.client.PostMessage(ctx, request)
	if err == nil {
		return ts, named, nil
	}
	if !named || !isCustomizeRefusal(err) {
		return "", false, err
	}
	// The scope check can be stale or the workspace can refuse a username;
	// the app's own identity still delivers the message.
	p.disableCustomize()
	request.Username = ""
	request.IconURL = ""
	body = p.renderBody(rawText, notes, external, key, "")
	request.Text = ""
	request.MarkdownText = ""
	if external {
		request.Text = body
	} else {
		request.MarkdownText = body
	}
	ts, err = p.client.PostMessage(ctx, request)
	return ts, false, err
}

// updateMessage sends exactly one text representation: escaped plain text
// for external content, markdown_text for board content.
func (p *Provider) updateMessage(ctx context.Context, channel, ts, body string, external bool) error {
	if external {
		return p.client.UpdateMessage(ctx, channel, ts, body, "")
	}
	return p.client.UpdateMessage(ctx, channel, ts, "", body)
}

// Edit replaces a delivered message's text. The label stays unless the
// message actually went out under the sender's name.
func (p *Provider) Edit(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message string, msg boardsync.OutboundMessage) error {
	if len(msg.Files) > 0 {
		return fmt.Errorf("%w: slack can't attach files to an edit", boardsync.ErrUnsupported)
	}
	if strings.TrimSpace(outboundText(msg.Markdown)) == "" {
		return fmt.Errorf("%w: slack edit has no text", boardsync.ErrBadRequest)
	}
	channel, ts, err := p.messageWhere(key, thread, message)
	if err != nil {
		return err
	}
	label := ""
	if msg.Sender != nil && p.namedCopy(ctx, channel, ts, msg.Sender) {
		label = cleanText(msg.Sender.Name)
	}
	body := p.renderBody(msg.Markdown, nil, msg.External, key, label)
	if strings.TrimSpace(body) == "" {
		return fmt.Errorf("%w: slack edit has no text after its label", boardsync.ErrBadRequest)
	}
	return p.updateMessage(ctx, channel, ts, body, msg.External)
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
	name, ok := reactionName(emoji)
	if !ok {
		return fmt.Errorf("%w: %q isn't a slack reaction name", boardsync.ErrBadRequest, emoji)
	}
	return p.client.React(ctx, channel, ts, name, added)
}

// Permalink returns Slack's own URL for an event's message.
func (p *Provider) Permalink(ctx context.Context, key boardsync.Key, event boardsync.Event) (string, error) {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return "", fmt.Errorf("%w: entry %q isn't this slack app's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	if event.Kind == boardsync.KindDeleted || event.Message == "" {
		return "", nil
	}
	link, err := p.client.Permalink(ctx, event.Channel, event.Message)
	if err != nil {
		return "", err
	}
	if !isSlackPermalinkURL(link) {
		return "", fmt.Errorf("%w: slack answered a permalink off its own domain", boardsync.ErrBadRequest)
	}
	return link, nil
}

// FetchRoot recovers a thread root the board hasn't seen, for a reply that
// arrives first. The thread is native: "C0123ABC:1727000000.123456".
func (p *Provider) FetchRoot(ctx context.Context, key boardsync.Key, thread string) (*boardsync.RootDraft, error) {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return nil, fmt.Errorf("%w: entry %q isn't this slack app's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	channel, ts, ok := strings.Cut(strings.TrimSpace(thread), ":")
	if !ok || !channelIDPattern.MatchString(channel) || !isTS(ts) {
		return nil, fmt.Errorf("%w: %q isn't a slack thread", boardsync.ErrBadRequest, thread)
	}
	messages, _, err := p.client.Replies(ctx, channel, ts, 1)
	if err != nil {
		return nil, err
	}
	for _, message := range messages {
		if message.TS != ts {
			continue
		}
		text := mrkdwnToMarkdown(key, message.Text)
		if text == "" && len(message.Files) > 0 {
			names := make([]string, 0, len(message.Files))
			for _, file := range message.Files {
				names = append(names, "📎 "+fileDisplayName(file.Name))
			}
			text = strings.Join(names, "\n")
		}
		root := &boardsync.RootDraft{
			Text:   boardsync.StorableText(text, boardsync.MaxTextBytes),
			Author: authorOf(key, message.User, message.BotID),
		}
		if raw, err := json.Marshal(message); err == nil {
			root.Raw = raw
		}
		return root, nil
	}
	return nil, fmt.Errorf("%w: slack thread %s has no root", boardsync.ErrNotFound, thread)
}

// Locate reads a Slack permalink as its channel, thread, and message.
func (p *Provider) Locate(link string) (string, string, bool) {
	found, ok := parsePermalink(link)
	if !ok {
		return "", "", false
	}
	return found.Channel + ":" + found.Thread, found.TS, true
}

// URL cannot be built from a thread ID: the workspace host is not part of a
// Slack ID. Permalink fetches Slack's own link instead.
func (p *Provider) URL(string, string) (string, error) {
	return "", fmt.Errorf("%w: slack links come from chat.getPermalink", boardsync.ErrUnsupported)
}

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

// Interface checks.
var (
	_ boardsync.Provider     = (*Provider)(nil)
	_ boardsync.Editor       = (*Provider)(nil)
	_ boardsync.Deleter      = (*Provider)(nil)
	_ boardsync.Reactor      = (*Provider)(nil)
	_ boardsync.Permalinker  = (*Provider)(nil)
	_ boardsync.Linker       = (*Provider)(nil)
	_ boardsync.Typist       = (*Provider)(nil)
	_ boardsync.FileUploader = (*Provider)(nil)
	_ boardsync.FileFetcher  = (*Provider)(nil)
	_ boardsync.RootFetcher  = (*Provider)(nil)
)
