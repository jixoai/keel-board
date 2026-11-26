package slack

import (
	"encoding/json"
	"fmt"
	"html"
	"regexp"
	"strings"
	"time"
	"unicode"

	boardsync "github.com/jixoai/keel-board/sync"
)

// webhookEnvelope is the outer Slack callback payload.
type webhookEnvelope struct {
	Type      string          `json:"type"`
	Challenge string          `json:"challenge"`
	TeamID    string          `json:"team_id"`
	APIAppID  string          `json:"api_app_id"`
	EventID   string          `json:"event_id"`
	EventTime int64           `json:"event_time"`
	Event     json.RawMessage `json:"event"`
}

// slackEvent is the subset of a Slack event this adapter reads.
type slackEvent struct {
	Type            string            `json:"type"`
	Subtype         string            `json:"subtype"`
	Channel         string            `json:"channel"`
	ChannelType     string            `json:"channel_type"`
	User            string            `json:"user"`
	BotID           string            `json:"bot_id"`
	Username        string            `json:"username"`
	Text            string            `json:"text"`
	TS              string            `json:"ts"`
	ThreadTS        string            `json:"thread_ts"`
	EventTS         string            `json:"event_ts"`
	DeletedTS       string            `json:"deleted_ts"`
	Reaction        string            `json:"reaction"`
	Message         *slackMessage     `json:"message,omitempty"`
	PreviousMessage *slackMessage     `json:"previous_message,omitempty"`
	Item            slackItem         `json:"item"`
	ItemUser        string            `json:"item_user"`
	Files           []slackFile       `json:"files"`
	Attachments     []slackAttachment `json:"attachments"`
	Edited          *slackEdited      `json:"edited"`
}

type slackItem struct {
	Type    string `json:"type"`
	Channel string `json:"channel"`
	TS      string `json:"ts"`
}

type slackMessage struct {
	Type        string            `json:"type"`
	Subtype     string            `json:"subtype"`
	Channel     string            `json:"channel"`
	User        string            `json:"user"`
	BotID       string            `json:"bot_id"`
	Username    string            `json:"username"`
	Text        string            `json:"text"`
	TS          string            `json:"ts"`
	ThreadTS    string            `json:"thread_ts"`
	ReplyCount  int               `json:"reply_count"`
	Files       []slackFile       `json:"files"`
	Attachments []slackAttachment `json:"attachments"`
	Edited      *slackEdited      `json:"edited"`
	BotProfile  *slackBotProfile  `json:"bot_profile,omitempty"`
}

type slackBotProfile struct {
	Name string `json:"name"`
}

type slackEdited struct {
	User string `json:"user"`
	TS   string `json:"ts"`
}

type slackFile struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Title      string `json:"title"`
	Mimetype   string `json:"mimetype"`
	Filetype   string `json:"filetype"`
	Size       int64  `json:"size"`
	URL        string `json:"url_private_download"`
	URLPrivate string `json:"url_private"`
	Permalink  string `json:"permalink"`
}

// slackAttachment is a Slack unfurl or forward. Slack puts the linked
// message's snapshot here, so a link's preview never needs a network read.
type slackAttachment struct {
	FromURL       string      `json:"from_url"`
	OriginalURL   string      `json:"original_url"`
	AuthorID      string      `json:"author_id"`
	AuthorName    string      `json:"author_name"`
	AuthorSubname string      `json:"author_subname"`
	AuthorLink    string      `json:"author_link"`
	Text          string      `json:"text"`
	Title         string      `json:"title"`
	Fallback      string      `json:"fallback"`
	ServiceName   string      `json:"service_name"`
	TS            string      `json:"ts"`
	IsMsgUnfurl   bool        `json:"is_msg_unfurl"`
	IsShare       bool        `json:"is_share"`
	Files         []slackFile `json:"files"`
}

// normalizeEvent turns one Slack event into a normalized record. It returns
// ok=false for an event this adapter ignores.
func normalizeEvent(key boardsync.Key, envelope webhookEnvelope, event slackEvent, cfg Config) (boardsync.Record, bool, error) {
	occurrence := envelope.EventID
	if occurrence == "" {
		occurrence = event.EventTS
	}
	switch event.Type {
	case "message":
		switch event.Subtype {
		case "", "file_share", "thread_broadcast", "bot_message":
			return normalizeMessage(key, occurrence, event, cfg)
		case "message_changed":
			return normalizeEdited(key, occurrence, event, cfg)
		case "message_deleted":
			return normalizeDeleted(key, occurrence, event, cfg)
		default:
			// Joins, leaves, topic changes, bot adds: not board content.
			return boardsync.Record{}, false, nil
		}
	case "reaction_added", "reaction_removed":
		return normalizeReaction(key, occurrence, event, cfg)
	default:
		// app_mention duplicates message.channels; link_shared asks for an
		// unfurl, which the board never fetches. Other events aren't content.
		return boardsync.Record{}, false, nil
	}
}

func normalizeMessage(key boardsync.Key, occurrence string, event slackEvent, cfg Config) (boardsync.Record, bool, error) {
	if event.Channel == "" || !isTS(event.TS) {
		return boardsync.Record{}, false, fmt.Errorf("%w: slack message has no channel or valid ts", boardsync.ErrBadRequest)
	}
	if event.ThreadTS != "" && !isTS(event.ThreadTS) {
		return boardsync.Record{}, false, fmt.Errorf("%w: slack message has an invalid thread ts", boardsync.ErrBadRequest)
	}
	if isDirectChannel(event.ChannelType, event.Channel) {
		return boardsync.Record{}, false, nil
	}
	if isOwnMessage(event.User, event.BotID, cfg) || isOwnText(event.Text, key) {
		return boardsync.Record{}, false, nil
	}
	if event.User == "" && event.BotID == "" {
		return boardsync.Record{}, false, fmt.Errorf("%w: slack message has no author", boardsync.ErrBadRequest)
	}
	rootTS := event.ThreadTS
	if rootTS == "" {
		rootTS = event.TS
	}
	thread := event.Channel + ":" + rootTS
	text := mrkdwnToMarkdown(event.Text)
	if isOwnText(text, key) {
		return boardsync.Record{}, false, nil
	}
	files := filesOf(event.Files, cfg)
	links := linksOf(key, event.Text, event.Attachments, cfg)
	if text == "" && len(files) == 0 && hasSharedSnapshots(links) {
		// A forward has no text of its own; say what it shares.
		text = forwardText(links)
	}
	if text == "" && len(files) == 0 {
		// Links alone carry no message: a board message needs text or files.
		return boardsync.Record{}, false, nil
	}
	record := boardsync.Record{
		Dedupe: boardsync.MessageDedupe(key, thread, event.TS),
		Event: boardsync.Event{
			Key: key, Platform: "slack",
			Kind: boardsync.KindMessage, Type: boardsync.TypeMessage,
			Channel: event.Channel, Thread: thread,
			Message: event.TS,
			IsRoot:  event.ThreadTS == "" || event.ThreadTS == event.TS,
			Text:    boardsync.StorableText(text, boardsync.MaxTextBytes),
			Author:  authorOf(event.User, event.BotID),
			Files:   files,
			Links:   links,
		},
	}
	return record, true, nil
}

func normalizeEdited(key boardsync.Key, occurrence string, event slackEvent, cfg Config) (boardsync.Record, bool, error) {
	message := event.Message
	if message == nil {
		return boardsync.Record{}, false, fmt.Errorf("%w: slack edit has no message", boardsync.ErrBadRequest)
	}
	if message.Channel == "" {
		message.Channel = event.Channel
	}
	if message.Channel == "" || !isTS(message.TS) {
		return boardsync.Record{}, false, fmt.Errorf("%w: slack edit has no channel or valid ts", boardsync.ErrBadRequest)
	}
	if message.ThreadTS != "" && !isTS(message.ThreadTS) {
		return boardsync.Record{}, false, fmt.Errorf("%w: slack edit has an invalid thread ts", boardsync.ErrBadRequest)
	}
	if isDirectChannel(event.ChannelType, message.Channel) {
		return boardsync.Record{}, false, nil
	}
	if message.User == "" {
		message.User = event.User
	}
	if message.User == "" && message.BotID == "" && event.PreviousMessage != nil {
		message.User = event.PreviousMessage.User
		message.BotID = event.PreviousMessage.BotID
	}
	if isOwnMessage(message.User, message.BotID, cfg) || isOwnText(message.Text, key) {
		return boardsync.Record{}, false, nil
	}
	if message.User == "" && message.BotID == "" {
		return boardsync.Record{}, false, fmt.Errorf("%w: slack edit has no author", boardsync.ErrBadRequest)
	}
	rootTS := message.ThreadTS
	if rootTS == "" {
		rootTS = message.TS
	}
	thread := message.Channel + ":" + rootTS
	text := mrkdwnToMarkdown(message.Text)
	if isOwnText(text, key) {
		return boardsync.Record{}, false, nil
	}
	links := linksOf(key, message.Text, message.Attachments, cfg)
	editedAt := time.Time{}
	if message.Edited != nil {
		editedAt = tsToTime(message.Edited.TS)
	}
	if editedAt.IsZero() && event.EventTS != "" {
		editedAt = tsToTime(event.EventTS)
	}
	dedupe := boardsync.EditedDedupe(key, thread, message.TS, text, editedAt)
	if editedAt.IsZero() {
		// Slack normally stamps an edit. Without one, the envelope's event ID
		// keeps a redelivery stable and two identical edits distinct; with
		// neither, there is no identity to rely on.
		if occurrence == "" {
			return boardsync.Record{}, false, fmt.Errorf("%w: slack edit has no event identity", boardsync.ErrBadRequest)
		}
		dedupe = boardsync.EditedDedupe(key, thread, message.TS, text+"\x00"+occurrence, time.Time{})
	}
	if isUnfurlOnly(event, message) {
		dedupe = unfurlDedupe(key, occurrence, thread, message.TS, links)
	}
	record := boardsync.Record{
		Dedupe: dedupe,
		Event: boardsync.Event{
			Key: key, Platform: "slack",
			Kind: boardsync.KindEdited, Type: boardsync.TypeMessageEdited,
			Channel: message.Channel, Thread: thread,
			Message: message.TS,
			IsRoot:  rootTS == message.TS,
			Text:    boardsync.StorableText(text, boardsync.MaxTextBytes),
			Author:  authorOf(message.User, message.BotID),
			Links:   links,
		},
	}
	return record, true, nil
}

// isUnfurlOnly reports whether Slack is attaching an unfurl to a message
// whose text did not change. It is an edit for the board's links, not for its
// text, and gets its own identity.
func isUnfurlOnly(event slackEvent, message *slackMessage) bool {
	if event.PreviousMessage == nil || event.PreviousMessage.Text != message.Text {
		return false
	}
	for _, attachment := range message.Attachments {
		if attachment.IsMsgUnfurl {
			return true
		}
	}
	return false
}

func normalizeDeleted(key boardsync.Key, occurrence string, event slackEvent, cfg Config) (boardsync.Record, bool, error) {
	previous := event.PreviousMessage
	ts := event.DeletedTS
	channel := event.Channel
	threadTS := ""
	user := ""
	botID := ""
	if previous != nil {
		if ts == "" {
			ts = previous.TS
		}
		if channel == "" {
			channel = previous.Channel
		}
		threadTS = previous.ThreadTS
		user = previous.User
		botID = previous.BotID
	}
	if channel == "" || !isTS(ts) {
		return boardsync.Record{}, false, fmt.Errorf("%w: slack deletion has no channel or valid ts", boardsync.ErrBadRequest)
	}
	if threadTS != "" && !isTS(threadTS) {
		threadTS = ""
	}
	if isDirectChannel(event.ChannelType, channel) {
		return boardsync.Record{}, false, nil
	}
	if isOwnMessage(user, botID, cfg) {
		return boardsync.Record{}, false, nil
	}
	if threadTS == "" {
		threadTS = ts
	}
	thread := channel + ":" + threadTS
	record := boardsync.Record{
		Dedupe: boardsync.DeletedDedupe(key, thread, ts),
		Event: boardsync.Event{
			Key: key, Platform: "slack",
			Kind: boardsync.KindDeleted, Type: boardsync.TypeMessageDeleted,
			Channel: channel, Thread: thread, Message: ts,
			Author: authorOf(user, botID),
		},
	}
	return record, true, nil
}

func normalizeReaction(key boardsync.Key, occurrence string, event slackEvent, cfg Config) (boardsync.Record, bool, error) {
	if event.Item.Type != "" && event.Item.Type != "message" {
		return boardsync.Record{}, false, nil
	}
	channel := event.Item.Channel
	if channel == "" {
		channel = event.Channel
	}
	if channel == "" || !isTS(event.Item.TS) || event.User == "" {
		return boardsync.Record{}, false, fmt.Errorf("%w: slack reaction has no channel, valid ts, or user", boardsync.ErrBadRequest)
	}
	if isDirectChannel(event.ChannelType, channel) {
		return boardsync.Record{}, false, nil
	}
	if isOwnMessage(event.User, "", cfg) {
		return boardsync.Record{}, false, nil
	}
	emoji := strings.Trim(event.Reaction, ":")
	if emoji == "" {
		return boardsync.Record{}, false, nil
	}
	if occurrence == "" {
		// Without Slack's event ID or event_ts, two add/remove/add
		// occurrences would look alike. Refuse rather than merge them.
		return boardsync.Record{}, false, fmt.Errorf("%w: slack reaction has no event identity", boardsync.ErrBadRequest)
	}
	// Slack reaction events don't carry thread_ts. The message ts is the only
	// thread anchor the payload has; the board resolves the copy by channel and
	// platform message ID. See docs/SYNC.md.
	thread := channel + ":" + event.Item.TS
	added := event.Type == "reaction_added"
	typeName := boardsync.TypeReactionRemove
	if added {
		typeName = boardsync.TypeReactionAdded
	}
	record := boardsync.Record{
		Dedupe: boardsync.ReactionDedupe(key, thread, event.Item.TS, emoji, event.User, added, occurrence),
		Event: boardsync.Event{
			Key: key, Platform: "slack",
			Kind: boardsync.KindReaction, Type: typeName,
			Channel: channel, Thread: thread, Message: event.Item.TS,
			Emoji: emoji, Added: added, Author: authorOf(event.User, ""),
		},
	}
	return record, true, nil
}

// isDirectChannel rejects DMs and group DMs. They are not sync targets: a
// board reader may not be in them, and a workspace app should not mirror a
// private conversation it wasn't configured for.
func isDirectChannel(channelType, channel string) bool {
	return channelType == "im" || channelType == "mpim" || strings.HasPrefix(channel, "D")
}

func isOwnMessage(user, botID string, cfg Config) bool {
	if botID != "" && cfg.BotID != "" && botID == cfg.BotID {
		return true
	}
	return user != "" && cfg.BotUserID != "" && user == cfg.BotUserID
}

// isOwnText is the second echo check. Slack doesn't add markers to what the
// adapter posts, but a marker copied into a message still names this entry;
// dropping it is safer than importing a copy of our own post.
func isOwnText(text string, key boardsync.Key) bool {
	return text != "" && boardsync.IsOwnMarker(text, key)
}

func authorOf(user, botID string) boardsync.Author {
	if user == "" {
		user = botID
	}
	if user == "" {
		return boardsync.Author{}
	}
	return boardsync.Author{
		ID: "slack:user:" + user, Handle: user, Name: user,
		Bot: botID != "",
	}
}

func filesOf(files []slackFile, cfg Config) []boardsync.File {
	out := make([]boardsync.File, 0, len(files))
	for _, file := range files {
		if len(out) >= cfg.maxFiles() {
			break
		}
		name := file.Name
		if name == "" {
			name = file.Title
		}
		if name == "" {
			name = "file"
		}
		url := file.URL
		if url == "" {
			url = file.URLPrivate
		}
		if !isSlackFileURL(url) {
			// A payload is untrusted: only a Slack download URL may be
			// fetched later, and only the server holds the token for it.
			url = ""
		}
		out = append(out, boardsync.File{
			Type:     fileKind(file.Mimetype),
			Name:     boardsync.StorableText(cleanText(name), 1024),
			MIMEType: file.Mimetype,
			Size:     file.Size,
			URL:      url,
		})
	}
	return out
}

func fileKind(mimeType string) string {
	switch {
	case strings.HasPrefix(mimeType, "image/"):
		return "image"
	case strings.HasPrefix(mimeType, "video/"):
		return "video"
	case strings.HasPrefix(mimeType, "audio/"):
		return "audio"
	default:
		return "file"
	}
}

func linkFilesOf(files []slackFile) []boardsync.LinkFile {
	out := make([]boardsync.LinkFile, 0, len(files))
	for _, file := range files {
		if len(out) >= 20 {
			break
		}
		name := file.Name
		if name == "" {
			name = file.Title
		}
		if name == "" {
			name = "file"
		}
		url := ""
		if isSlackWebURL(file.Permalink) {
			url = file.Permalink
		}
		out = append(out, boardsync.LinkFile{Name: boardsync.StorableText(name, 512), URL: url})
	}
	return out
}

var (
	userMention    = regexp.MustCompile(`<@([A-Z0-9_]+)(?:\|[^<>]*)?>`)
	channelMention = regexp.MustCompile(`<#([A-Z0-9_]+)(?:\|([^<>]*))?>`)
	slackLink      = regexp.MustCompile(`<((?:https?|mailto):[^|<>\s]+)(?:\|([^<>]*))?>`)
	slackBroadcast = regexp.MustCompile(`<!(channel|here|everyone)(?:\|[^<>]*)?>`)
)

// mrkdwnToMarkdown is the small, deliberate conversion this adapter does:
// Slack's HTML escapes, user mentions, channel mentions, links, and
// broadcasts. Slack's *bold*, _italic_, and ~strike~ are left as written;
// a full converter belongs in the adapter, not in the board.
func mrkdwnToMarkdown(text string) string {
	text = html.UnescapeString(text)
	text = userMention.ReplaceAllString(text, "@[slack:user:$1]")
	text = channelMention.ReplaceAllStringFunc(text, func(match string) string {
		parts := channelMention.FindStringSubmatch(match)
		if len(parts) > 2 && parts[2] != "" {
			return "#" + parts[2]
		}
		return "#" + parts[1]
	})
	text = slackLink.ReplaceAllStringFunc(text, func(match string) string {
		parts := slackLink.FindStringSubmatch(match)
		if len(parts) > 2 && parts[2] != "" {
			return "[" + parts[2] + "](" + parts[1] + ")"
		}
		return parts[1]
	})
	text = slackBroadcast.ReplaceAllString(text, "@$1")
	return cleanText(strings.TrimSpace(text))
}

// cleanText drops the control characters a JSON payload may carry. NUL and
// invalid UTF-8 are left to StorableText; other controls have no place in a
// board message.
func cleanText(text string) string {
	return strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' || r == '\t' {
			return r
		}
		if unicode.IsControl(r) {
			return -1
		}
		if unicode.Is(unicode.Cf, r) {
			// Zero-width joiners hold emoji sequences together; other format
			// characters (bidi overrides, zero-width spaces) have no place in
			// a board message.
			if r == '\u200d' {
				return r
			}
			return -1
		}
		return r
	}, text)
}
