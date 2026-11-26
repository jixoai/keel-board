package linear

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	boardsync "github.com/jixoai/keel-board/sync"
)

func (p *Provider) normalize(key boardsync.Key, delivery string, body []byte, wh webhook) ([]boardsync.Record, error) {
	if delivery == "" {
		// webhookId names the subscription, not one occurrence, so the
		// timestamp and body always join it.
		delivery = strings.Join([]string{
			wh.WebhookID,
			strconv.FormatInt(wh.WebhookTimestamp, 10),
			boardsync.Hash(string(body)),
		}, ":")
	}
	switch wh.Type {
	case "Issue":
		return p.normalizeIssue(key, delivery, body, wh)
	case "Comment":
		return p.normalizeComment(key, delivery, body, wh)
	case "Reaction":
		// Linear reaction webhooks don't say which issue they're on.
		return nil, nil
	default:
		return nil, nil
	}
}

func (p *Provider) normalizeIssue(key boardsync.Key, delivery string, body []byte, wh webhook) ([]boardsync.Record, error) {
	var item issueData
	if err := json.Unmarshal(wh.Data, &item); err != nil {
		return nil, fmt.Errorf("%w: linear issue data: %v", boardsync.ErrBadRequest, err)
	}
	teamKey := strings.ToUpper(item.Team.Key)
	if teamKey == "" {
		teamKey = teamOfIdentifier(item.Identifier)
	}
	if teamKey == "" || !p.cfg.teamAllowed(teamKey) {
		return nil, nil
	}
	if item.ID == "" || item.Identifier == "" {
		return nil, fmt.Errorf("%w: linear issue has no ID or identifier", boardsync.ErrBadRequest)
	}
	author := actorAuthor(wh.Actor)
	if author.ID == "" {
		author = authorOf(item.Creator)
	}
	properties := propertiesOf(&item)
	lifecycle := boardsync.Lifecycle{
		Type:   "issue.updated",
		Title:  item.Identifier + ": " + item.Title,
		URL:    firstNonEmpty(item.URL, wh.URL),
		Body:   item.Description,
		Labels: properties.Labels,
		Unset:  properties.Unset,
		Owns:   properties.Owns,
	}
	// The app's own events never open a second root: the board already
	// recorded the issue it created. They still carry the issue's properties,
	// so an existing thread's labels stay current.
	own := p.isOwnActor(wh.Actor)
	switch wh.Action {
	case "create":
		lifecycle.Type = "issue.created"
		lifecycle.CreateRoot = !own
	case "update":
		lifecycle.Retitled = updatedField(wh.UpdatedFrom, "title") || updatedField(wh.UpdatedFrom, "description")
		// A person's edit brings an older issue in; Linear's own automations,
		// an issue the app created, and archiving don't.
		lifecycle.CreateRoot = !own && !p.isOwnUser(item.Creator) &&
			wh.Actor != nil && wh.Actor.Type == "user" && item.ArchivedAt == ""
	case "remove":
		return nil, nil
	default:
		return nil, nil
	}
	event := boardsync.Event{
		Key: key, Platform: "linear",
		Kind: boardsync.KindLifecycle, Type: lifecycle.Type,
		Channel: teamKey, Thread: issueThread(teamKey, item.ID),
		Author: author, Lifecycle: &lifecycle,
		Raw: append([]byte(nil), body...),
	}
	event.ID = boardsync.LifecycleDedupe(key, delivery, lifecycle.Type, item.Identifier)
	return []boardsync.Record{{Dedupe: event.ID, Event: event, Raw: body}}, nil
}

func (p *Provider) normalizeComment(key boardsync.Key, delivery string, body []byte, wh webhook) ([]boardsync.Record, error) {
	var item commentData
	if err := json.Unmarshal(wh.Data, &item); err != nil {
		return nil, fmt.Errorf("%w: linear comment data: %v", boardsync.ErrBadRequest, err)
	}
	if item.Issue == nil {
		return nil, fmt.Errorf("%w: linear comment has no issue; fetch the issue or subscribe to issue data", boardsync.ErrBadRequest)
	}
	teamKey := strings.ToUpper(item.Issue.Team.Key)
	if teamKey == "" {
		teamKey = teamOfIdentifier(item.Issue.Identifier)
	}
	if teamKey == "" || !p.cfg.teamAllowed(teamKey) {
		return nil, nil
	}
	issueID := firstNonEmpty(item.Issue.ID, item.IssueID)
	if item.ID == "" || issueID == "" {
		return nil, fmt.Errorf("%w: linear comment has no ID or issue ID", boardsync.ErrBadRequest)
	}
	if p.isOwnUser(item.User) || p.isOwnActor(wh.Actor) {
		return nil, nil
	}
	author := authorOf(item.User)
	if author.ID == "" {
		author = actorAuthor(wh.Actor)
	}
	if author.ID == "" && wh.Action != "remove" {
		return nil, fmt.Errorf("%w: linear comment has no author", boardsync.ErrBadRequest)
	}
	thread := issueThread(teamKey, issueID)
	// A reply groups under its parent comment's sub-thread; a top-level one is
	// its own.
	root := firstNonEmpty(item.ParentID, item.ID)
	subthread := commentThread(teamKey, issueID, root)
	event := boardsync.Event{
		Key: key, Platform: "linear",
		Channel: teamKey, Thread: thread, Subthread: subthread,
		Message: item.ID, Author: author,
		Raw: append([]byte(nil), body...),
	}
	switch wh.Action {
	case "create":
		event.Kind = boardsync.KindMessage
		event.Type = boardsync.TypeComment
		event.ID = boardsync.MessageDedupe(key, subthread, item.ID)
	case "update":
		event.Kind = boardsync.KindEdited
		event.Type = boardsync.TypeMessageEdited
		event.ID = boardsync.EditedDedupe(key, subthread, item.ID, item.Body, parseTime(item.UpdatedAt))
	case "remove":
		event.Kind = boardsync.KindDeleted
		event.Type = boardsync.TypeMessageDeleted
		event.ID = boardsync.DeletedDedupe(key, subthread, item.ID)
	default:
		return nil, nil
	}
	event.Text = boardsync.StorableText(item.Body, boardsync.MaxTextBytes)
	if event.Kind == boardsync.KindMessage && event.Text == "" {
		event.Text = "(no text)"
	}
	return []boardsync.Record{{Dedupe: event.ID, Event: event, Raw: body}}, nil
}

func (p *Provider) isOwnActor(a *actor) bool {
	return a != nil && p.cfg.BotUserID != "" && a.ID == p.cfg.BotUserID
}

func (p *Provider) isOwnUser(u *user) bool {
	return u != nil && p.cfg.BotUserID != "" && u.ID == p.cfg.BotUserID
}

func updatedField(from map[string]any, name string) bool {
	_, ok := from[name]
	return ok
}
