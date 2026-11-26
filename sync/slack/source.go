package slack

import (
	"context"
	"fmt"
	"net/url"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
)

// SourceRead is a channel or thread read on demand. It is not a sync: nothing
// is stored, and reading a channel does not subscribe to it.
type SourceRead struct {
	Channel  string          `json:"channel"`
	Name     string          `json:"name,omitempty"`
	Thread   string          `json:"thread,omitempty"`
	Messages []SourceMessage `json:"messages"`
	More     bool            `json:"more,omitempty"`
}

// SourceMessage is one message a source read returned, oldest first.
type SourceMessage struct {
	ID        string           `json:"id"`
	Thread    string           `json:"thread"`
	Author    string           `json:"author"`
	AuthorID  string           `json:"authorId,omitempty"`
	Text      string           `json:"text"`
	CreatedAt string           `json:"createdAt,omitempty"`
	Replies   int              `json:"replies,omitempty"`
	Files     []boardsync.File `json:"files,omitempty"`
}

// SourceJoin is the answer to an opt-in join.
type SourceJoin struct {
	Channel string `json:"channel"`
	Name    string `json:"name,omitempty"`
	Joined  bool   `json:"joined"`
}

// ReadSource reads a public channel or a thread the app is already in. A
// private channel, a DM, or a Slack Connect channel is refused: the app's
// token reads more than the caller may see, and a snapshot is shown to every
// board reader.
func (p *Provider) ReadSource(ctx context.Context, key boardsync.Key, target string, limit int) (SourceRead, error) {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return SourceRead{}, fmt.Errorf("%w: entry %q isn't this slack app's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	resolved, err := p.ResolveTarget(ctx, key, target)
	if err != nil {
		return SourceRead{}, err
	}
	info, err := p.client.ChannelInfo(ctx, resolved.Channel)
	if err != nil {
		return SourceRead{}, err
	}
	if err := readableChannel(info); err != nil {
		return SourceRead{}, err
	}
	read := SourceRead{Channel: resolved.Channel, Name: info.Name}
	var messages []slackMessage
	var more bool
	if resolved.Kind == boardsync.TargetThread {
		threadTS, err := splitThread(resolved.Channel, resolved.Thread)
		if err != nil {
			return SourceRead{}, err
		}
		found, hasMore, err := p.client.Replies(ctx, resolved.Channel, threadTS, limit)
		if err != nil {
			return SourceRead{}, err
		}
		if len(found) > 0 && found[0].ThreadTS != "" && found[0].ThreadTS != threadTS {
			// A reply's ts reads only that reply; the thread it belongs to is
			// what the caller meant.
			threadTS = found[0].ThreadTS
			found, hasMore, err = p.client.Replies(ctx, resolved.Channel, threadTS, limit)
			if err != nil {
				return SourceRead{}, err
			}
		}
		read.Thread = resolved.Channel + ":" + threadTS
		messages, more = found, hasMore
	} else {
		found, hasMore, err := p.client.History(ctx, resolved.Channel, limit)
		if err != nil {
			return SourceRead{}, err
		}
		// Slack returns newest first; a reader wants oldest first.
		for i, j := 0, len(found)-1; i < j; i, j = i+1, j-1 {
			found[i], found[j] = found[j], found[i]
		}
		messages, more = found, hasMore
	}
	if len(messages) > limit {
		more = true
		messages = messages[:limit]
	}
	read.More = more
	read.Messages = sourceMessages(resolved.Channel, messages, p.cfg)
	return read, nil
}

// JoinSource adds the app to a public channel, only when the entry opted in.
// Slack announces the app there and starts sending the channel's events, but
// sync still imports only the channels it maps.
func (p *Provider) JoinSource(ctx context.Context, key boardsync.Key, target string) (SourceJoin, error) {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return SourceJoin{}, fmt.Errorf("%w: entry %q isn't this slack app's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	if !p.cfg.JoinChannels {
		return SourceJoin{}, fmt.Errorf("%w: join_disabled: this board doesn't join Slack channels", boardsync.ErrUnsupported)
	}
	resolved, err := p.ResolveTarget(ctx, key, target)
	if err != nil {
		return SourceJoin{}, err
	}
	info, err := p.client.ChannelInfo(ctx, resolved.Channel)
	if err != nil {
		return SourceJoin{}, err
	}
	label := info.Name
	if label == "" {
		label = resolved.Channel
	} else {
		label = "#" + label
	}
	switch {
	case info.Private || info.IM || info.MPIM:
		return SourceJoin{}, fmt.Errorf("%w: private_channel: %s is private; it can only be joined by invitation", boardsync.ErrUnsupported, label)
	case info.IsExtShared:
		return SourceJoin{}, fmt.Errorf("%w: shared_channel: %s is shared with another organization", boardsync.ErrUnsupported, label)
	case info.IsMember:
		return SourceJoin{Channel: info.ID, Name: info.Name}, nil
	}
	if err := p.client.JoinChannel(ctx, resolved.Channel); err != nil {
		if isMissingScope(err) {
			return SourceJoin{}, fmt.Errorf("%w: the Slack app needs channels:join to join %s", boardsync.ErrUnauthorized, label)
		}
		return SourceJoin{}, err
	}
	return SourceJoin{Channel: info.ID, Name: info.Name, Joined: true}, nil
}

func readableChannel(info Channel) error {
	label := info.Name
	if label == "" {
		label = info.ID
	} else {
		label = "#" + label
	}
	switch {
	case info.Private || info.IM || info.MPIM:
		return fmt.Errorf("%w: private_channel: %s is private; the board only reads public channels on demand", boardsync.ErrUnsupported, label)
	case info.IsExtShared:
		return fmt.Errorf("%w: shared_channel: %s is shared with another organization", boardsync.ErrUnsupported, label)
	case !info.IsMember:
		return fmt.Errorf("%w: not_in_channel: the app isn't in %s", boardsync.ErrUnsupported, label)
	default:
		return nil
	}
}

func sourceMessages(channel string, messages []slackMessage, cfg Config) []SourceMessage {
	out := make([]SourceMessage, 0, len(messages))
	for _, message := range messages {
		if message.TS == "" {
			continue
		}
		rootTS := message.ThreadTS
		if rootTS == "" {
			rootTS = message.TS
		}
		author := message.Username
		if author == "" && message.BotProfile != nil {
			author = message.BotProfile.Name
		}
		if author == "" {
			author = message.User
		}
		if author == "" {
			author = message.BotID
		}
		authorID := ""
		if message.User != "" {
			authorID = "slack:user:" + message.User
		} else if message.BotID != "" {
			authorID = "slack:user:" + message.BotID
		}
		created := ""
		if at := tsToTime(message.TS); !at.IsZero() {
			created = at.Format(time.RFC3339)
		}
		out = append(out, SourceMessage{
			ID:        message.TS,
			Thread:    channel + ":" + rootTS,
			Author:    boardsync.StorableText(cleanText(author), 256),
			AuthorID:  authorID,
			Text:      boardsync.StorableText(mrkdwnToMarkdown(message.Text), boardsync.MaxTextBytes),
			CreatedAt: created,
			Replies:   message.ReplyCount,
			Files:     filesOf(message.Files, cfg),
		})
	}
	return out
}

// ChannelInfo reads one channel's name and visibility.
func (c *Client) ChannelInfo(ctx context.Context, channel string) (Channel, error) {
	if channel == "" {
		return Channel{}, fmt.Errorf("%w: slack channel info needs a channel", boardsync.ErrBadRequest)
	}
	var response struct {
		apiEnvelope
		Channel Channel `json:"channel"`
	}
	if err := c.call(ctx, "conversations.info", url.Values{"channel": {channel}}, &response); err != nil {
		return Channel{}, err
	}
	if response.Channel.ID == "" {
		response.Channel.ID = channel
	}
	return response.Channel, nil
}

// Replies reads a thread, oldest first, including its root.
func (c *Client) Replies(ctx context.Context, channel, ts string, limit int) ([]slackMessage, bool, error) {
	if channel == "" || ts == "" {
		return nil, false, fmt.Errorf("%w: slack replies need a channel and ts", boardsync.ErrBadRequest)
	}
	var response struct {
		apiEnvelope
		Messages []slackMessage `json:"messages"`
		HasMore  bool           `json:"has_more"`
	}
	form := url.Values{"channel": {channel}, "ts": {ts}, "limit": {fmt.Sprintf("%d", clampLimit(limit))}}
	if err := c.call(ctx, "conversations.replies", form, &response); err != nil {
		return nil, false, err
	}
	return response.Messages, response.HasMore, nil
}

// History reads a channel, newest first.
func (c *Client) History(ctx context.Context, channel string, limit int) ([]slackMessage, bool, error) {
	if channel == "" {
		return nil, false, fmt.Errorf("%w: slack history needs a channel", boardsync.ErrBadRequest)
	}
	var response struct {
		apiEnvelope
		Messages []slackMessage `json:"messages"`
		HasMore  bool           `json:"has_more"`
	}
	form := url.Values{"channel": {channel}, "limit": {fmt.Sprintf("%d", clampLimit(limit))}}
	if err := c.call(ctx, "conversations.history", form, &response); err != nil {
		return nil, false, err
	}
	return response.Messages, response.HasMore, nil
}

// JoinChannel adds the app to a public channel.
func (c *Client) JoinChannel(ctx context.Context, channel string) error {
	if channel == "" {
		return fmt.Errorf("%w: slack join needs a channel", boardsync.ErrBadRequest)
	}
	return c.call(ctx, "conversations.join", url.Values{"channel": {channel}}, nil)
}

func clampLimit(limit int) int {
	if limit <= 0 {
		return 50
	}
	if limit > 200 {
		return 200
	}
	return limit
}
