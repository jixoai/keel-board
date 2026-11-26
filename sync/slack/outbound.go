package slack

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
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

// slackControlEscaper neutralizes Slack's control markup: a cross-platform
// message cannot ping a channel, mention someone, or disguise a link.
var slackControlEscaper = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")

// escapeSlackText makes text inert in Slack's mrkdwn. Slack shows the
// entities as the characters they replace without interpreting them.
func escapeSlackText(text string) string {
	return slackControlEscaper.Replace(text)
}

// renderBody prepares what Slack receives. Text from another platform is
// escaped, so raw <!channel>, <@U…>, and <url|label> stay literal; only an
// explicit board mention under this entry's key becomes a Slack mention. File
// notes are always escaped because their names are untrusted too.
func (p *Provider) renderBody(rawText string, notes []string, external bool, key boardsync.Key, label string) string {
	text := outboundText(rawText)
	if label != "" {
		text = withoutLabel(text, label)
	}
	if external {
		text = escapeSlackText(text)
	}
	text = restoreMentions(text, key)
	if len(notes) > 0 {
		// Notes are system-generated around an untrusted file name. They
		// never carry mentions, even if the name looks like one.
		escaped := escapeSlackText(strings.Join(notes, "\n"))
		if text == "" {
			text = escaped
		} else {
			text += "\n\n" + escaped
		}
	}
	return text
}

// restoreMentions turns the board's explicit mention token for this entry
// into Slack's own mention. A user ID from another workspace's key stays
// text: this adapter's token cannot speak for it.
func restoreMentions(text string, key boardsync.Key) string {
	if key == "" || !strings.Contains(text, "@[") {
		return text
	}
	pattern := regexp.MustCompile(`@\[` + regexp.QuoteMeta(string(key)) + `:user:([UW][A-Z0-9_]+)\]`)
	return pattern.ReplaceAllString(text, "<@$1>")
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
	name := cleanText(strings.TrimSpace(sender.Name))
	if name == "" {
		return "", false
	}
	if utf8.RuneCountInString(name) <= 80 {
		return name, true
	}
	attribution := cleanText(sender.Attribution)
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

// canonicalReactions maps the board's canonical reaction names to the
// emoji Slack recognizes. A name already in Slack's form passes through.
var canonicalReactions = map[string]string{
	"thumbs_up":   "thumbsup",
	"thumbs_down": "thumbsdown",
	"check":       "white_check_mark",
}

// reactionName accepts a canonical or Slack reaction name, keeping a
// skin-tone suffix. Colons around a name are Slack's own decoration.
func reactionName(name string) (string, bool) {
	name = strings.Trim(strings.TrimSpace(name), ":")
	if name == "" || len(name) > 100 {
		return "", false
	}
	base, suffix := name, ""
	if before, after, found := strings.Cut(name, "::"); found {
		base, suffix = before, after
	}
	if canonical, ok := canonicalReactions[base]; ok {
		base = canonical
	}
	if suffix != "" {
		name = base + "::" + suffix
	} else {
		name = base
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
	ids, _, err := p.uploadFiles(ctx, thread.Channel, threadTS, "", plan.files)
	return ids, err
}

// fileUploadAttempts bounds the retries of the steps before Slack shares the
// files. A 5xx or 429 gets another try; a refusal does not.
const fileUploadAttempts = 3

// retryFile runs fn while allow accepts the error. The delay is short: an
// upload holds a board delivery, and the outbox is the longer retry.
func (p *Provider) retryFile(ctx context.Context, fn func() error, allow func(error) bool) error {
	var err error
	for attempt := 0; attempt < fileUploadAttempts; attempt++ {
		if err = fn(); err == nil {
			return nil
		}
		if !allow(err) || attempt+1 == fileUploadAttempts {
			return err
		}
		if waitErr := p.cfg.sleep(ctx, time.Duration(100*(1<<attempt))*time.Millisecond); waitErr != nil {
			return waitErr
		}
	}
	return err
}

func retryableFileError(err error) bool {
	return !boardsync.IsPermanent(err)
}

// uploadFiles uploads files and shares them in one message. shares holds the
// message ts Slack reported; callers may still need a files.info read when
// Slack omitted it.
func (p *Provider) uploadFiles(ctx context.Context, channel, threadTS, comment string, files []boardsync.OutboundFile) ([]string, map[string]string, error) {
	refs := make([]UploadFileRef, 0, len(files))
	for _, file := range files {
		name := uploadFilename(file.Filename, file.MIMEType)
		var uploadURL, fileID string
		err := p.retryFile(ctx, func() error {
			var callErr error
			uploadURL, fileID, callErr = p.client.GetUploadURLExternal(ctx, name, int64(len(file.Data)))
			return callErr
		}, retryableFileError)
		if err != nil {
			return nil, nil, err
		}
		if err := p.retryFile(ctx, func() error {
			return p.client.UploadExternal(ctx, uploadURL, name, file.Data)
		}, retryableFileError); err != nil {
			return nil, nil, err
		}
		refs = append(refs, UploadFileRef{ID: fileID, Title: name})
	}
	var result CompleteUploadResult
	// Completing twice could share the files twice; only a rate limit is
	// retried, where Slack refused before doing the work.
	err := p.retryFile(ctx, func() error {
		var callErr error
		result, callErr = p.client.CompleteUploadExternalShares(ctx, refs, channel, threadTS, comment)
		return callErr
	}, func(err error) bool { return errors.Is(err, boardsync.ErrRateLimited) })
	if err != nil {
		return nil, nil, err
	}
	return result.IDs, result.Shares, nil
}

// uploadAndShare uploads files and finds the message Slack made for them.
// shared says the files reached Slack; a later failure then must not be
// mistaken for a refusal and re-post the text.
func (p *Provider) uploadAndShare(ctx context.Context, channel, threadTS, comment string, files []boardsync.OutboundFile) (ids []string, ts string, shared bool, err error) {
	ids, shares, err := p.uploadFiles(ctx, channel, threadTS, comment, files)
	if err != nil {
		return nil, "", false, err
	}
	if found := shares[channel]; found != "" {
		return ids, found, true, nil
	}
	ts, err = p.findShare(ctx, channel, ids[0])
	if err != nil {
		return ids, "", true, err
	}
	return ids, ts, true, nil
}

// attachmentNotes says what a file-only message carried when its files go as
// a second message. The names stay untrusted and are escaped by renderBody.
func attachmentNotes(files []boardsync.OutboundFile) []string {
	notes := make([]string, 0, len(files))
	for _, file := range files {
		name := cleanText(file.Filename)
		if name == "" {
			name = "file"
		}
		notes = append(notes, fmt.Sprintf("📎 %s (%s)", name, humanBytes(int64(len(file.Data)))))
	}
	return notes
}
