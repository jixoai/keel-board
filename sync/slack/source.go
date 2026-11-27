package slack

import (
	"context"
	"fmt"
	"net/url"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
)

// The shared source contract. They are aliases so a caller can name either
// package's type; the shape and the JSON are the sync package's.
type (
	SourceRead    = boardsync.SourceRead
	SourceMessage = boardsync.SourceMessage
	SourceJoin    = boardsync.SourceJoin
)

// ReadSource reads a public channel or a thread the app is already in. A
// private channel, a DM, or a Slack Connect channel is refused: the app's
// token reads more than the caller may see, and a snapshot is shown to every
// board reader.
func (p *Provider) ReadSource(ctx context.Context, key boardsync.Key, target string, opts boardsync.ReadOptions) (SourceRead, error) {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return SourceRead{}, fmt.Errorf("%w: entry %q isn't this slack app's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	limit := opts.LimitFor(boardsync.DefaultSourceLimit, boardsync.MaxSourceLimit)
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
	read := SourceRead{Kind: "channel", Channel: resolved.Channel, Name: info.Name}
	var messages []slackMessage
	var more bool
	var next string
	if resolved.Kind == boardsync.TargetThread {
		read.Kind = "thread"
		threadTS, err := splitThread(resolved.Channel, resolved.Thread)
		if err != nil {
			return SourceRead{}, err
		}
		found, hasMore, cursor, err := p.client.Replies(ctx, resolved.Channel, threadTS, limit, opts.After)
		if err != nil {
			return SourceRead{}, err
		}
		if len(found) > 0 && found[0].ThreadTS != "" && found[0].ThreadTS != threadTS {
			// A reply's ts reads only that reply; the thread it belongs to is
			// what the caller meant.
			threadTS = found[0].ThreadTS
			found, hasMore, cursor, err = p.client.Replies(ctx, resolved.Channel, threadTS, limit, opts.After)
			if err != nil {
				return SourceRead{}, err
			}
		}
		read.Thread = resolved.Channel + ":" + threadTS
		messages, more, next = found, hasMore, cursor
	} else {
		found, hasMore, cursor, err := p.client.History(ctx, resolved.Channel, limit, opts.After)
		if err != nil {
			return SourceRead{}, err
		}
		// Slack returns newest first; a reader wants oldest first.
		for i, j := 0, len(found)-1; i < j; i, j = i+1, j-1 {
			found[i], found[j] = found[j], found[i]
		}
		messages, more, next = found, hasMore, cursor
	}
	if len(messages) > limit {
		// Slack's thread read includes the root beside its reply page, so a
		// page can be one over the limit. Trimming keeps the read bounded, but
		// the platform cursor would then resume past the trimmed message, so
		// it isn't offered.
		more = true
		messages = messages[:limit]
		next = ""
	}
	read.More = more
	read.Next = next
	read.Messages = sourceMessages(key, resolved.Channel, messages, p.cfg)
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

func sourceMessages(key boardsync.Key, channel string, messages []slackMessage, cfg Config) []SourceMessage {
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
			authorID = string(key) + ":user:" + message.User
		} else if message.BotID != "" {
			authorID = string(key) + ":user:" + message.BotID
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
			Text:      boardsync.StorableText(mrkdwnToMarkdown(key, message.Text), boardsync.MaxTextBytes),
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

// Replies reads a thread, oldest first, including its root. A non-empty
// cursor continues a previous read; the returned cursor continues this one.
func (c *Client) Replies(ctx context.Context, channel, ts string, limit int, cursor string) ([]slackMessage, bool, string, error) {
	if channel == "" || ts == "" {
		return nil, false, "", fmt.Errorf("%w: slack replies need a channel and ts", boardsync.ErrBadRequest)
	}
	var response struct {
		apiEnvelope
		Messages []slackMessage `json:"messages"`
		HasMore  bool           `json:"has_more"`
		Metadata struct {
			NextCursor string `json:"next_cursor"`
		} `json:"response_metadata"`
	}
	form := url.Values{"channel": {channel}, "ts": {ts}, "limit": {fmt.Sprintf("%d", clampLimit(limit))}}
	if cursor != "" {
		form.Set("cursor", cursor)
	}
	if err := c.call(ctx, "conversations.replies", form, &response); err != nil {
		return nil, false, "", err
	}
	return response.Messages, response.HasMore, response.Metadata.NextCursor, nil
}

// History reads a channel, newest first. A non-empty cursor continues a
// previous read; the returned cursor continues this one.
func (c *Client) History(ctx context.Context, channel string, limit int, cursor string) ([]slackMessage, bool, string, error) {
	if channel == "" {
		return nil, false, "", fmt.Errorf("%w: slack history needs a channel", boardsync.ErrBadRequest)
	}
	var response struct {
		apiEnvelope
		Messages []slackMessage `json:"messages"`
		HasMore  bool           `json:"has_more"`
		Metadata struct {
			NextCursor string `json:"next_cursor"`
		} `json:"response_metadata"`
	}
	form := url.Values{"channel": {channel}, "limit": {fmt.Sprintf("%d", clampLimit(limit))}}
	if cursor != "" {
		form.Set("cursor", cursor)
	}
	if err := c.call(ctx, "conversations.history", form, &response); err != nil {
		return nil, false, "", err
	}
	return response.Messages, response.HasMore, response.Metadata.NextCursor, nil
}

var (
	_ boardsync.SourceReader = (*Provider)(nil)
	_ boardsync.SourceJoiner = (*Provider)(nil)
)

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
