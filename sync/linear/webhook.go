package linear

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	boardsync "github.com/jixoai/keel-board/sync"
)

func (p *Provider) normalize(ctx context.Context, key boardsync.Key, delivery string, body []byte, wh webhook) ([]boardsync.Record, error) {
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
		return p.normalizeComment(ctx, key, delivery, body, wh)
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
		Title:  clampText(item.Identifier+": "+item.Title, MaxTextChars),
		URL:    firstNonEmpty(item.URL, wh.URL),
		Body:   clampText(item.Description, MaxTextChars),
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
		Raw: clampRaw(body),
	}
	event.ID = boardsync.LifecycleDedupe(key, delivery, lifecycle.Type, item.Identifier)
	return []boardsync.Record{{Dedupe: event.ID, Event: event, Raw: clampRaw(body)}}, nil
}

func (p *Provider) normalizeComment(ctx context.Context, key boardsync.Key, delivery string, body []byte, wh webhook) ([]boardsync.Record, error) {
	var item commentData
	if err := json.Unmarshal(wh.Data, &item); err != nil {
		return nil, fmt.Errorf("%w: linear comment data: %v", boardsync.ErrBadRequest, err)
	}
	if item.ID == "" {
		return nil, fmt.Errorf("%w: linear comment has no ID", boardsync.ErrBadRequest)
	}
	// Linear's real Comment webhook carries issueId and userId, not a full
	// issue or user object. Identity is available before any lookup, so the
	// app's own comment is dropped without a request.
	if p.isOwnUser(item.User) || p.isOwnUserID(item.UserID) || p.isOwnActor(wh.Actor) {
		return nil, nil
	}
	issue, err := p.resolveCommentIssue(ctx, &item)
	if err != nil {
		return nil, err
	}
	if !p.cfg.teamAllowed(issue.TeamKey) {
		return nil, nil
	}
	author := p.commentAuthor(&item, wh.Actor)
	if author.ID == "" && wh.Action != "remove" {
		return nil, fmt.Errorf("%w: linear comment has no author", boardsync.ErrBadRequest)
	}
	thread := issueThread(issue.TeamKey, issue.ID)
	// A reply groups under its parent comment's sub-thread; a top-level one is
	// its own.
	root := firstNonEmpty(item.ParentID, item.ID)
	subthread := commentThread(issue.TeamKey, issue.ID, root)
	event := boardsync.Event{
		Key: key, Platform: "linear",
		Channel: issue.TeamKey, Thread: thread, Subthread: subthread,
		Message: item.ID, Author: author,
		Raw: clampRaw(body),
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
	event.Text = clampText(item.Body, MaxCommentChars)
	if event.Kind == boardsync.KindMessage && event.Text == "" {
		event.Text = "(no text)"
	}
	return []boardsync.Record{{Dedupe: event.ID, Event: event, Raw: clampRaw(body)}}, nil
}

// resolvedCommentIssue is the identity a comment needs: the issue's UUID, its
// identifier, and its team key. Linear's real Comment webhook names only
// issueId, so the adapter reads the issue before it can place the comment.
type resolvedCommentIssue struct {
	ID         string
	Identifier string
	TeamKey    string
}

func (p *Provider) resolveCommentIssue(ctx context.Context, item *commentData) (resolvedCommentIssue, error) {
	issueID := firstNonEmpty(item.IssueID, issueIDOf(item.Issue))
	if issueID == "" {
		return resolvedCommentIssue{}, fmt.Errorf("%w: linear comment has no issue ID", boardsync.ErrBadRequest)
	}
	resolved := resolvedCommentIssue{ID: issueID}
	if item.Issue != nil {
		resolved.ID = firstNonEmpty(item.Issue.ID, issueID)
		resolved.Identifier = item.Issue.Identifier
		resolved.TeamKey = strings.ToUpper(item.Issue.Team.Key)
	}
	if resolved.TeamKey == "" {
		resolved.TeamKey = teamOfIdentifier(resolved.Identifier)
	}
	if resolved.TeamKey != "" && resolved.Identifier != "" {
		return resolved, nil
	}
	// A payload may name the issue with only an ID, and an issue reference may
	// carry only the ID too. Ask Linear for the team and identifier; a second
	// read fills in a reference that still lacks them.
	ref, err := p.client.IssueRefByID(ctx, issueID)
	if err != nil {
		return resolvedCommentIssue{}, err
	}
	resolved.ID = firstNonEmpty(ref.ID, resolved.ID)
	resolved.Identifier = firstNonEmpty(ref.Identifier, resolved.Identifier)
	resolved.TeamKey = firstNonEmpty(strings.ToUpper(ref.Team.Key), resolved.TeamKey)
	if resolved.TeamKey == "" {
		resolved.TeamKey = teamOfIdentifier(resolved.Identifier)
	}
	if resolved.TeamKey != "" && resolved.Identifier != "" {
		return resolved, nil
	}
	full, err := p.client.IssueByID(ctx, issueID)
	if err != nil {
		return resolvedCommentIssue{}, err
	}
	resolved.ID = firstNonEmpty(full.ID, resolved.ID)
	resolved.Identifier = firstNonEmpty(full.Identifier, resolved.Identifier)
	resolved.TeamKey = firstNonEmpty(strings.ToUpper(full.Team.Key), resolved.TeamKey)
	if resolved.TeamKey == "" {
		resolved.TeamKey = teamOfIdentifier(resolved.Identifier)
	}
	if resolved.TeamKey == "" {
		return resolvedCommentIssue{}, fmt.Errorf("%w: linear issue %s has no team", boardsync.ErrBadRequest, issueID)
	}
	return resolved, nil
}

func issueIDOf(issue *issueData) string {
	if issue == nil {
		return ""
	}
	return issue.ID
}

func (p *Provider) commentAuthor(item *commentData, a *actor) boardsync.Author {
	if item.User != nil {
		if author := authorOf(item.User); author.ID != "" {
			return author
		}
	}
	if item.UserID != "" {
		return boardsync.Author{
			ID: "linear:user:" + item.UserID, Handle: item.UserID, Name: item.UserID,
		}
	}
	return actorAuthor(a)
}

func (p *Provider) isOwnActor(a *actor) bool {
	return a != nil && p.cfg.BotUserID != "" && a.ID == p.cfg.BotUserID
}

func (p *Provider) isOwnUser(u *user) bool {
	return u != nil && p.cfg.BotUserID != "" && u.ID == p.cfg.BotUserID
}

func (p *Provider) isOwnUserID(id string) bool {
	return id != "" && p.cfg.BotUserID != "" && id == p.cfg.BotUserID
}

func updatedField(from map[string]any, name string) bool {
	_, ok := from[name]
	return ok
}
