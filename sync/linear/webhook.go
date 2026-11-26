package linear

import (
	"encoding/json"
	"fmt"
	"strings"

	boardsync "github.com/jixoai/keel-board/sync"
)

func (p *Provider) normalize(key boardsync.Key, delivery string, body []byte, wh webhook) ([]boardsync.Record, error) {
	if delivery == "" {
		delivery = firstNonEmpty(wh.WebhookID, boardsync.Hash(string(body)))
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
	teamKey := item.Team.Key
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
	lifecycle := boardsync.Lifecycle{
		Type:   "issue.updated",
		Title:  item.Identifier + ": " + item.Title,
		URL:    item.URL,
		Body:   item.Description,
		Labels: issueLabelsFor(&item),
		Owns:   issueOwns(&item),
	}
	switch wh.Action {
	case "create":
		lifecycle.Type = "issue.created"
		lifecycle.CreateRoot = true
	case "update":
		lifecycle.Retitled = updatedField(wh.UpdatedFrom, "title") || updatedField(wh.UpdatedFrom, "description")
		// A person's edit brings an older issue in; Linear's own automations
		// and archiving don't.
		lifecycle.CreateRoot = wh.Actor != nil && wh.Actor.Type == "user" && item.ArchivedAt == ""
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
	teamKey := item.Issue.Team.Key
	if teamKey == "" {
		teamKey = teamOfIdentifier(item.Issue.Identifier)
	}
	if teamKey == "" || !p.cfg.teamAllowed(teamKey) {
		return nil, nil
	}
	if item.ID == "" || item.Issue.ID == "" {
		return nil, fmt.Errorf("%w: linear comment has no ID or issue ID", boardsync.ErrBadRequest)
	}
	author := authorOf(item.User)
	if author.ID != "" && p.cfg.BotUserID != "" && strings.TrimPrefix(author.ID, "linear:user:") == p.cfg.BotUserID {
		return nil, nil
	}
	if author.ID == "" {
		author = actorAuthor(wh.Actor)
	}
	thread := issueThread(teamKey, item.Issue.ID)
	subthread := commentThread(teamKey, item.Issue.ID, item.ID)
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

func updatedField(from map[string]any, name string) bool {
	_, ok := from[name]
	return ok
}
