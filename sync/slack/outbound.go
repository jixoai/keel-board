package slack

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"unicode/utf8"

	boardsync "github.com/jixoai/keel-board/sync"
)

// maxOutboundText is Slack's message text bound, with room for markup.
const maxOutboundText = 39000

// customizeScope lets an app set a message's username and icon.
const customizeScope = "chat:write.customize"

// outboundText clamps a message to what Slack accepts and strips control
// characters the board may have carried from another platform.
func outboundText(markdown string) string {
	return boardsync.StorableText(cleanText(markdown), maxOutboundText)
}

// withNotes appends the file lines a message carries when a file could not
// come over. A file-only message is just its notes.
func withNotes(text string, notes []string) string {
	if len(notes) == 0 {
		return text
	}
	lines := strings.Join(notes, "\n")
	if text == "" {
		return lines
	}
	return text + "\n\n" + lines
}

// withoutLabel removes the "**name**: " a rendered message opens with, for a
// message Slack shows under its author's name instead. Only the exact label
// is removed; a different name stays in the text.
func withoutLabel(markdown, name string) string {
	if name == "" {
		return markdown
	}
	pattern := regexp.MustCompile(`^\*\*` + regexp.QuoteMeta(name) + `\*\*:(?: |\n\n)`)
	return pattern.ReplaceAllString(markdown, "")
}

// senderFields is what chat.postMessage needs to show the author as the
// sender: a username Slack shows (at most 80 characters) and an icon it can
// load. Null when the app may not customize, the name alone doesn't fit, or
// the picture isn't an https image Slack will show.
func (p *Provider) senderFields(ctx context.Context, sender *boardsync.Sender) (string, string, bool) {
	username, ok := slackUsername(sender)
	if !ok || !p.canCustomize(ctx) {
		return "", "", false
	}
	return username, slackIcon(sender.Avatar), true
}

// slackUsername cuts a sender name to Slack's 80 characters, keeping the
// attribution whole: "a very long name…: via support". ok is false when even
// the attribution wouldn't fit, so the message goes as the app with its
// label.
func slackUsername(sender *boardsync.Sender) (string, bool) {
	name := strings.TrimSpace(sender.Name)
	if name == "" {
		return "", false
	}
	if utf8.RuneCountInString(name) <= 80 {
		return name, true
	}
	attribution := sender.Attribution
	room := 80 - utf8.RuneCountInString(attribution) - 1
	if room < 1 {
		return "", false
	}
	runes := []rune(name)
	cut := strings.TrimRight(string(runes[:room]), " ")
	return cut + "…" + attribution, true
}

// slackIcon keeps an avatar Slack can show: an https URL that isn't an SVG
// (Slack won't render one) and isn't a generated identicon path.
func slackIcon(avatar string) string {
	if avatar == "" {
		return ""
	}
	parsed, err := url.Parse(avatar)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil {
		return ""
	}
	if strings.HasSuffix(strings.ToLower(parsed.Path), ".svg") {
		return ""
	}
	if regexp.MustCompile(`/avatars/[a-z0-9-]+$`).MatchString(parsed.Path) {
		return ""
	}
	return avatar
}

// hasScope asks Slack once which scopes the token has. A failed check isn't
// cached: "not now" is different from "not ever".
func (p *Provider) hasScope(ctx context.Context, want string) (bool, error) {
	p.scopesMu.Lock()
	if p.scopesLoaded {
		has := containsScope(p.scopes, want)
		p.scopesMu.Unlock()
		return has, nil
	}
	p.scopesMu.Unlock()
	_, scopes, err := p.client.AuthTest(ctx)
	if err != nil {
		return false, err
	}
	p.scopesMu.Lock()
	p.scopes = scopes
	p.scopesLoaded = true
	p.scopesMu.Unlock()
	return containsScope(scopes, want), nil
}

// canCustomize reports whether the token may set a message's username and
// icon. A failed check means "not now": the message posts as the app with its
// label, and a later message asks again.
func (p *Provider) canCustomize(ctx context.Context) bool {
	p.scopesMu.Lock()
	disabled := p.customizeDisabled
	p.scopesMu.Unlock()
	if disabled {
		return false
	}
	has, err := p.hasScope(ctx, customizeScope)
	return err == nil && has
}

func (p *Provider) disableCustomize() {
	p.scopesMu.Lock()
	defer p.scopesMu.Unlock()
	p.customizeDisabled = true
}

func containsScope(scopes []string, want string) bool {
	for _, scope := range scopes {
		if scope == want {
			return true
		}
	}
	return false
}

// isCustomizeRefusal reports whether Slack refused the username/icon but
// would take the same message as the app.
func isCustomizeRefusal(err error) bool {
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.Code {
	case "missing_scope", "not_allowed_token_type", "restricted_action", "cannot_use_username":
		return true
	default:
		return false
	}
}

// namedCopy asks Slack whether a message went out under the sender's name.
// An edit keeps the label unless it did.
func (p *Provider) namedCopy(ctx context.Context, channel, ts string, sender *boardsync.Sender) bool {
	username, ok := slackUsername(sender)
	if !ok || !p.canCustomize(ctx) {
		return false
	}
	found, err := p.client.Message(ctx, channel, ts)
	if err != nil {
		return false
	}
	return found.Username == username
}

// MessageInfo is what an edit needs to know about the delivered copy.
type MessageInfo struct {
	TS       string
	User     string
	BotID    string
	Username string
	Text     string
	ThreadTS string
}

// Message reads one message Slack already has. It is used to decide whether
// an edit may leave the label out, never to import content.
func (c *Client) Message(ctx context.Context, channel, ts string) (MessageInfo, error) {
	if channel == "" || ts == "" {
		return MessageInfo{}, fmt.Errorf("%w: slack message read needs a channel and ts", boardsync.ErrBadRequest)
	}
	var response struct {
		apiEnvelope
		Messages []struct {
			TS       string `json:"ts"`
			User     string `json:"user"`
			BotID    string `json:"bot_id"`
			Username string `json:"username"`
			Text     string `json:"text"`
			ThreadTS string `json:"thread_ts"`
		} `json:"messages"`
	}
	form := url.Values{
		"channel":   {channel},
		"ts":        {ts},
		"oldest":    {ts},
		"latest":    {ts},
		"inclusive": {"true"},
		"limit":     {"1"},
	}
	if err := c.call(ctx, "conversations.replies", form, &response); err != nil {
		return MessageInfo{}, err
	}
	for _, message := range response.Messages {
		if message.TS == ts {
			return MessageInfo{
				TS: message.TS, User: message.User, BotID: message.BotID,
				Username: message.Username, Text: message.Text, ThreadTS: message.ThreadTS,
			}, nil
		}
	}
	return MessageInfo{}, fmt.Errorf("%w: slack message %s isn't in %s", boardsync.ErrNotFound, ts, channel)
}

// Typing sets the thread's status. Slack has no typing indicator API; an
// assistant thread shows this status (assistant:write and the Agents & AI
// Apps feature). A channel target has no thread to show it in, so it is a
// no-op, and a workspace without the feature answers unsupported.
func (p *Provider) Typing(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, latest, status string) error {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return fmt.Errorf("%w: entry %q isn't this slack app's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	if thread.Key != key {
		return fmt.Errorf("%w: thread belongs to %q", boardsync.ErrBadRequest, thread.Key)
	}
	threadTS, err := splitThread(thread.Channel, thread.Thread)
	if err != nil {
		return err
	}
	if threadTS == "" {
		return nil
	}
	if status == "" {
		status = "Typing…"
	}
	status = truncateRunes(cleanText(status), 50)
	if err := p.client.SetAssistantStatus(ctx, thread.Channel, threadTS, status); err != nil {
		if isTypingUnavailable(err) {
			return fmt.Errorf("%w: slack typing: %v", boardsync.ErrUnsupported, err)
		}
		return err
	}
	return nil
}

// truncateRunes keeps a status within Slack's 50 characters, counting what a
// person sees.
func truncateRunes(text string, max int) string {
	runes := []rune(text)
	if len(runes) <= max {
		return text
	}
	if max <= 1 {
		return string(runes[:max])
	}
	return string(runes[:max-1]) + "…"
}

func isTypingUnavailable(err error) bool {
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.Code {
	case "missing_scope", "not_allowed", "not_allowed_token_type", "unknown_method", "feature_not_enabled", "invalid_arguments":
		return true
	default:
		return false
	}
}

// SetAssistantStatus shows or clears a status under an assistant thread.
func (c *Client) SetAssistantStatus(ctx context.Context, channel, threadTS, status string) error {
	if channel == "" || threadTS == "" || status == "" {
		return fmt.Errorf("%w: slack assistant status needs a channel, thread, and status", boardsync.ErrBadRequest)
	}
	loading, err := json.Marshal([]string{status})
	if err != nil {
		return err
	}
	form := url.Values{
		"channel_id":       {channel},
		"thread_ts":        {threadTS},
		"status":           {status},
		"loading_messages": {string(loading)},
	}
	return c.call(ctx, "assistant.threads.setStatus", form, nil)
}

// reactionName accepts Slack's reaction names: a word of letters, digits,
// "_", "+", and "-", optionally with a skin-tone suffix. Colons around a
// name are Slack's own decoration, not part of it.
func reactionName(name string) (string, bool) {
	name = strings.Trim(strings.TrimSpace(name), ":")
	if name == "" || len(name) > 100 {
		return "", false
	}
	pattern := regexp.MustCompile(`^[A-Za-z0-9_+\-]+(?:::[A-Za-z0-9_+\-]+)?$`)
	if !pattern.MatchString(name) {
		return "", false
	}
	return name, true
}

// FetchFile implements boardsync.FileFetcher: a Slack message's file comes
// back through the app's token, bounded and only from a Slack domain.
func (p *Provider) FetchFile(ctx context.Context, key boardsync.Key, file boardsync.File) ([]byte, error) {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return nil, fmt.Errorf("%w: entry %q isn't this slack app's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	return p.client.FetchFile(ctx, file, p.cfg.maxFileBytes())
}

// UploadFiles implements boardsync.FileUploader: files follow the message's
// text in the same thread. A file the platform refuses is permanent, so the
// caller does not retry a refusal.
func (p *Provider) UploadFiles(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, files []boardsync.OutboundFile) ([]string, error) {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return nil, fmt.Errorf("%w: entry %q isn't this slack app's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	if thread.Key != key {
		return nil, fmt.Errorf("%w: thread belongs to %q", boardsync.ErrBadRequest, thread.Key)
	}
	if thread.Channel == "" {
		return nil, fmt.Errorf("%w: slack upload needs a channel", boardsync.ErrBadRequest)
	}
	threadTS, err := splitThread(thread.Channel, thread.Thread)
	if err != nil {
		return nil, err
	}
	plan := planUploads(files, p.cfg)
	if len(plan.notes) > 0 {
		return nil, fmt.Errorf("%w: %s", boardsync.ErrBadRequest, strings.Join(plan.notes, "; "))
	}
	if len(plan.files) == 0 {
		return nil, fmt.Errorf("%w: slack upload has no files", boardsync.ErrBadRequest)
	}
	return p.uploadFiles(ctx, thread.Channel, threadTS, "", plan.files)
}

// uploadFiles uploads files and shares them in one message. It does not wait
// for the message Slack makes; callers that need its ts poll after.
func (p *Provider) uploadFiles(ctx context.Context, channel, threadTS, comment string, files []boardsync.OutboundFile) ([]string, error) {
	refs := make([]UploadFileRef, 0, len(files))
	for _, file := range files {
		name := uploadFilename(file.Filename, file.MIMEType)
		uploadURL, fileID, err := p.client.GetUploadURLExternal(ctx, name, int64(len(file.Data)))
		if err != nil {
			return nil, err
		}
		if err := p.client.UploadExternal(ctx, uploadURL, name, file.Data); err != nil {
			return nil, err
		}
		refs = append(refs, UploadFileRef{ID: fileID, Title: name})
	}
	return p.client.CompleteUploadExternal(ctx, refs, channel, threadTS, comment)
}

// uploadAndShare uploads files and finds the message Slack made for them.
// shared says the files reached Slack; a later failure then must not be
// mistaken for a refusal and re-post the text.
func (p *Provider) uploadAndShare(ctx context.Context, channel, threadTS, comment string, files []boardsync.OutboundFile) (ids []string, ts string, shared bool, err error) {
	ids, err = p.uploadFiles(ctx, channel, threadTS, comment, files)
	if err != nil {
		return nil, "", false, err
	}
	ts, err = p.findShare(ctx, channel, ids[0])
	if err != nil {
		return ids, "", true, err
	}
	return ids, ts, true, nil
}

// attachmentList is the plain text a message with no text gets when its files
// go as a second message: the reader still sees what came.
func attachmentList(files []boardsync.OutboundFile) string {
	lines := make([]string, 0, len(files))
	for _, file := range files {
		name := cleanText(file.Filename)
		if name == "" {
			name = "file"
		}
		lines = append(lines, fmt.Sprintf("📎 %s (%s)", name, humanBytes(int64(len(file.Data)))))
	}
	return strings.Join(lines, "\n")
}
