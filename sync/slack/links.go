package slack

import (
	"net/url"
	"regexp"
	"strings"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
)

// maxLinksPerMessage bounds the links a message carries, as upstream does.
const maxLinksPerMessage = 10

// slackPermalinkSource matches a Slack message permalink anywhere in text.
// The query is optional and may carry thread_ts.
const slackPermalinkSource = `https://([a-z0-9][a-z0-9-]*(?:\.enterprise)?\.slack\.com)/archives/([CG][A-Z0-9]+)/p([0-9]{10})([0-9]{6})(?:\?([^\s<>|]*))?`

var (
	slackPermalinkAny  = regexp.MustCompile(slackPermalinkSource)
	slackPermalinkFull = regexp.MustCompile(`^` + slackPermalinkSource + `$`)
)

// permalink is a parsed Slack message link.
type permalink struct {
	URL     string
	Host    string
	Channel string
	TS      string
	Thread  string
}

// parsePermalink reads a Slack message permalink. Anything not on a Slack
// workspace domain is not one.
func parsePermalink(raw string) (permalink, bool) {
	raw = strings.TrimSpace(strings.ReplaceAll(raw, "&amp;", "&"))
	match := slackPermalinkFull.FindStringSubmatch(raw)
	if match == nil {
		return permalink{}, false
	}
	ts := match[3] + "." + match[4]
	thread := ts
	if query := match[5]; query != "" {
		values, err := url.ParseQuery(query)
		if err == nil {
			if parent := values.Get("thread_ts"); parent != "" && isTS(parent) {
				thread = parent
			}
		}
	}
	return permalink{URL: raw, Host: match[1], Channel: match[2], TS: ts, Thread: thread}, true
}

// isSlackPermalinkURL reports whether raw is a Slack message permalink.
func isSlackPermalinkURL(raw string) bool {
	_, ok := parsePermalink(raw)
	return ok
}

// isSlackWebURL reports whether raw is an https URL on a Slack domain. It is
// the weaker check for a link a reader will open; a fetchable file needs
// isSlackFileURL and the app's token.
func isSlackWebURL(raw string) bool {
	if raw == "" {
		return false
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil {
		return false
	}
	if parsed.Port() != "" && parsed.Port() != "443" {
		return false
	}
	host := strings.ToLower(parsed.Hostname())
	return host == "slack.com" || strings.HasSuffix(host, ".slack.com")
}

// isSlackFileURL reports whether raw is an https URL on a Slack domain. File
// bytes are fetched only from one; a payload cannot redirect the board to an
// internal service or a cloud metadata endpoint.
func isSlackFileURL(raw string) bool {
	return isSlackWebURL(raw)
}

// linksOf turns Slack message permalinks and the unfurl attachments Slack
// already supplied into normalized links. It never fetches a URL: a snapshot
// comes from the webhook payload.
func linksOf(key boardsync.Key, text string, attachments []slackAttachment, _ Config) []boardsync.Link {
	links := make([]boardsync.Link, 0, 2)
	index := make(map[string]int)
	add := func(found permalink, attachment *slackAttachment) {
		if len(links) >= maxLinksPerMessage {
			return
		}
		identity := found.Channel + ":" + found.TS
		if at, ok := index[identity]; ok {
			if links[at].Snapshot == nil && attachment != nil {
				links[at].Snapshot = snapshotOf(key, attachment, found)
				if links[at].Snapshot != nil {
					links[at].Available = true
					links[at].Why = ""
				}
			}
			return
		}
		link := boardsync.Link{
			URL:     found.URL,
			Thread:  string(key) + ":" + found.Channel + ":" + found.Thread,
			Message: found.TS,
		}
		if attachment != nil {
			link.Snapshot = snapshotOf(key, attachment, found)
			if link.Snapshot != nil {
				link.Available = true
			}
		}
		if link.Snapshot == nil {
			link.Why = "unfurl_pending"
		}
		index[identity] = len(links)
		links = append(links, link)
	}
	byMessage := make(map[string]*slackAttachment)
	byTS := make(map[string]*slackAttachment)
	for i := range attachments {
		attachment := &attachments[i]
		found, ok := attachmentPermalink(attachment)
		if !ok {
			continue
		}
		if attachment.IsMsgUnfurl || attachment.IsShare {
			if found.Channel == "" {
				byTS[found.TS] = attachment
			} else {
				byMessage[found.Channel+":"+found.TS] = attachment
			}
		}
	}
	text = strings.ReplaceAll(text, "&amp;", "&")
	for _, match := range slackPermalinkAny.FindAllStringSubmatch(text, -1) {
		found, ok := parsePermalink(match[0])
		if !ok {
			continue
		}
		attachment := byMessage[found.Channel+":"+found.TS]
		if attachment == nil {
			attachment = byTS[found.TS]
		}
		add(found, attachment)
	}
	for i := range attachments {
		attachment := &attachments[i]
		if !attachment.IsShare {
			continue
		}
		found, ok := attachmentPermalink(attachment)
		if !ok || found.Channel == "" {
			continue
		}
		add(found, attachment)
	}
	return links
}

// attachmentPermalink reads the message a Slack unfurl or forward names.
func attachmentPermalink(attachment *slackAttachment) (permalink, bool) {
	for _, raw := range []string{attachment.FromURL, attachment.OriginalURL, attachment.AuthorLink} {
		if found, ok := parsePermalink(raw); ok {
			return found, true
		}
	}
	if attachment.TS != "" && isTS(attachment.TS) {
		// An unfurl names its message by ts; the channel is on the link's
		// URL, which callers match by ts.
		return permalink{TS: attachment.TS, Thread: attachment.TS}, true
	}
	return permalink{}, false
}

// snapshotOf is what a linked message said when Slack unfurled it.
func snapshotOf(key boardsync.Key, attachment *slackAttachment, found permalink) *boardsync.LinkSnapshot {
	if attachment == nil {
		return nil
	}
	author := attachment.AuthorName
	if author == "" {
		author = attachment.AuthorSubname
	}
	if author == "" {
		author = "someone"
	}
	snapshot := &boardsync.LinkSnapshot{
		Author: boardsync.StorableText(cleanText(author), 256),
		Text:   boardsync.StorableText(mrkdwnToMarkdown(key, attachment.Text), 16<<10),
		Files:  linkFilesOf(attachment.Files),
	}
	if at := tsToTime(attachment.TS); !at.IsZero() {
		snapshot.CreatedAt = at.Format(time.RFC3339)
	}
	if snapshot.Text == "" && len(snapshot.Files) == 0 && attachment.AuthorName == "" && attachment.AuthorSubname == "" {
		return nil
	}
	return snapshot
}

// hasSharedSnapshots reports whether any link came with a snapshot.
func hasSharedSnapshots(links []boardsync.Link) bool {
	for _, link := range links {
		if link.Snapshot != nil {
			return true
		}
	}
	return false
}

// forwardText is what a forward with no text of its own says: the shared
// message, quoted, with its link.
func forwardText(links []boardsync.Link) string {
	var parts []string
	for _, link := range links {
		if link.Snapshot == nil {
			continue
		}
		head := "> **" + link.Snapshot.Author + "**: " + link.Snapshot.Text
		lines := strings.Split(head, "\n")
		for i := 1; i < len(lines); i++ {
			lines[i] = "> " + strings.TrimPrefix(lines[i], "> ")
		}
		parts = append(parts, strings.TrimRight(strings.Join(lines, "\n"), "\n")+"\n\n"+link.URL)
	}
	return strings.TrimSpace(strings.Join(parts, "\n\n"))
}

// unfurlDedupe is the identity of an unfurl-only message_changed. Slack's
// event ID makes a redelivery stable; without one, the linked messages do.
func unfurlDedupe(key boardsync.Key, occurrence, thread, ts string, links []boardsync.Link) string {
	if occurrence != "" {
		return boardsync.EventDedupe(key, "unfurl:"+occurrence)
	}
	identities := make([]string, 0, len(links))
	for _, link := range links {
		identities = append(identities, link.Thread+"|"+link.Message)
	}
	return boardsync.EventDedupe(key, "unfurl:"+thread+"|"+ts+"|"+boardsync.Hash(strings.Join(identities, ",")))
}
