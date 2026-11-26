package slack

import (
	"encoding/json"
	"fmt"
	"html"
	"regexp"
	"strings"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
)

// webhookEnvelope is the outer Slack callback payload.
type webhookEnvelope struct {
	Type      string          `json:"type"`
	Challenge string          `json:"challenge"`
	TeamID    string          `json:"team_id"`
	EventID   string          `json:"event_id"`
	EventTime int64           `json:"event_time"`
	Event     json.RawMessage `json:"event"`
}

// slackEvent is the subset of a Slack event this adapter reads.
type slackEvent struct {
	Type            string        `json:"type"`
	Subtype         string        `json:"subtype"`
	Channel         string        `json:"channel"`
	ChannelType     string        `json:"channel_type"`
	User            string        `json:"user"`
	BotID           string        `json:"bot_id"`
	Text            string        `json:"text"`
	TS              string        `json:"ts"`
	ThreadTS        string        `json:"thread_ts"`
	EventTS         string        `json:"event_ts"`
	DeletedTS       string        `json:"deleted_ts"`
	Reaction        string        `json:"reaction"`
	Message         *slackMessage `json:"message,omitempty"`
	PreviousMessage *slackMessage `json:"previous_message,omitempty"`
	Item            struct {
		Type    string `json:"type"`
		Channel string `json:"channel"`
		TS      string `json:"ts"`
	} `json:"item"`
	ItemUser string       `json:"item_user"`
	Files    []slackFile  `json:"files"`
	Edited   *slackEdited `json:"edited"`
}

type slackMessage struct {
	Type     string       `json:"type"`
	Subtype  string       `json:"subtype"`
	Channel  string       `json:"channel"`
	User     string       `json:"user"`
	BotID    string       `json:"bot_id"`
	Text     string       `json:"text"`
	TS       string       `json:"ts"`
	ThreadTS string       `json:"thread_ts"`
	Files    []slackFile  `json:"files"`
	Edited   *slackEdited `json:"edited"`
}

type slackEdited struct {
	User string `json:"user"`
	TS   string `json:"ts"`
}

type slackFile struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Title     string `json:"title"`
	Mimetype  string `json:"mimetype"`
	Filetype  string `json:"filetype"`
	Size      int64  `json:"size"`
	URL       string `json:"url_private_download"`
	Permalink string `json:"permalink"`
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
		// app_mention duplicates message.channels; other events aren't content.
		return boardsync.Record{}, false, nil
	}
}

func normalizeMessage(key boardsync.Key, occurrence string, event slackEvent, cfg Config) (boardsync.Record, bool, error) {
	if event.Channel == "" || event.TS == "" {
		return boardsync.Record{}, false, fmt.Errorf("%w: slack message has no channel or ts", boardsync.ErrBadRequest)
	}
	if isOwnMessage(event.User, event.BotID, cfg) {
		return boardsync.Record{}, false, nil
	}
	rootTS := event.ThreadTS
	if rootTS == "" {
		rootTS = event.TS
	}
	thread := event.Channel + ":" + rootTS
	text := mrkdwnToMarkdown(event.Text)
	files := filesOf(event.Files)
	if text == "" && len(files) == 0 {
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
	if message.TS == "" || message.Channel == "" {
		return boardsync.Record{}, false, fmt.Errorf("%w: slack edit has no channel or ts", boardsync.ErrBadRequest)
	}
	if isOwnMessage(message.User, message.BotID, cfg) {
		return boardsync.Record{}, false, nil
	}
	rootTS := message.ThreadTS
	if rootTS == "" {
		rootTS = message.TS
	}
	thread := message.Channel + ":" + rootTS
	editedAt := time.Time{}
	if message.Edited != nil {
		editedAt = tsToTime(message.Edited.TS)
	}
	text := mrkdwnToMarkdown(message.Text)
	record := boardsync.Record{
		Dedupe: boardsync.EditedDedupe(key, thread, message.TS, text, editedAt),
		Event: boardsync.Event{
			Key: key, Platform: "slack",
			Kind: boardsync.KindEdited, Type: boardsync.TypeMessageEdited,
			Channel: message.Channel, Thread: thread,
			Message: message.TS,
			Text:    boardsync.StorableText(text, boardsync.MaxTextBytes),
			Author:  authorOf(message.User, message.BotID),
		},
	}
	return record, true, nil
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
	if ts == "" || channel == "" {
		return boardsync.Record{}, false, fmt.Errorf("%w: slack deletion has no channel or ts", boardsync.ErrBadRequest)
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
	if event.Item.Channel == "" || event.Item.TS == "" || event.User == "" {
		return boardsync.Record{}, false, fmt.Errorf("%w: slack reaction has no channel, ts, or user", boardsync.ErrBadRequest)
	}
	if isOwnMessage(event.User, "", cfg) {
		return boardsync.Record{}, false, nil
	}
	emoji := strings.Trim(event.Reaction, ":")
	if emoji == "" {
		return boardsync.Record{}, false, nil
	}
	// Slack reaction events don't carry thread_ts. The message ts is the only
	// thread anchor the payload has; the board resolves the copy by channel and
	// platform message ID. See docs/SYNC.md.
	thread := event.Item.Channel + ":" + event.Item.TS
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
			Channel: event.Item.Channel, Thread: thread, Message: event.Item.TS,
			Emoji: emoji, Added: added, Author: authorOf(event.User, ""),
			Raw: json.RawMessage(occurrence),
		},
	}
	return record, true, nil
}

func isOwnMessage(user, botID string, cfg Config) bool {
	if botID != "" && cfg.BotID != "" && botID == cfg.BotID {
		return true
	}
	return user != "" && cfg.BotUserID != "" && user == cfg.BotUserID
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

func filesOf(files []slackFile) []boardsync.File {
	out := make([]boardsync.File, 0, len(files))
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
		url := file.URL
		if url == "" {
			url = file.Permalink
		}
		kind := "file"
		if strings.HasPrefix(file.Mimetype, "image/") {
			kind = "image"
		} else if strings.HasPrefix(file.Mimetype, "video/") {
			kind = "video"
		} else if strings.HasPrefix(file.Mimetype, "audio/") {
			kind = "audio"
		}
		out = append(out, boardsync.File{
			Type: kind, Name: name, MIMEType: file.Mimetype,
			Size: file.Size, URL: url,
		})
	}
	return out
}

var (
	userMention    = regexp.MustCompile(`<@([A-Z0-9_]+)(?:\|[^<>]*)?>`)
	channelMention = regexp.MustCompile(`<#([A-Z0-9_]+)(?:\|([^<>]*))?>`)
	slackLink      = regexp.MustCompile(`<((?:https?|mailto):[^|<>\s]+)(?:\|([^<>]*))?>`)
	slackBroadcast = regexp.MustCompile(`<!(channel|here|everyone)(?:\|[^<>]*)?>`)
)

// mrkdwnToMarkdown is the small, deliberate conversion this skeleton does:
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
	return strings.TrimSpace(text)
}
