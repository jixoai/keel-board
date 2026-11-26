package linear

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	boardsync "github.com/jixoai/keel-board/sync"
)

// Provider implements boardsync.Provider for one Linear app.
type Provider struct {
	cfg    Config
	client *Client
}

// New returns a Linear provider. It validates the team allowlist and builds
// the GraphQL client; it performs no network I/O.
func New(cfg Config) (*Provider, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Provider{
		cfg:    cfg,
		client: NewClient(cfg.baseURL(), cfg.httpClient(), cfg.APIToken),
	}, nil
}

// Platform implements boardsync.Provider.
func (p *Provider) Platform() boardsync.Platform { return "linear" }

// Capabilities implements boardsync.Provider.
func (p *Provider) Capabilities() boardsync.Capabilities {
	return boardsync.Capabilities{StartThreads: true, Edits: true, Deletes: true, Reactions: true}
}

// Receive verifies Linear-Signature and the payload's timestamp, checks the
// team allowlist, and normalizes issues and comments.
func (p *Provider) Receive(ctx context.Context, key boardsync.Key, req *boardsync.WebhookRequest) (boardsync.Intake, error) {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return boardsync.Intake{}, fmt.Errorf("%w: entry %q isn't this linear app's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	if req == nil || len(req.Body) == 0 {
		return boardsync.Intake{}, fmt.Errorf("%w: empty linear webhook", boardsync.ErrBadRequest)
	}
	if int64(len(req.Body)) > p.cfg.maxBody() {
		return boardsync.Intake{}, fmt.Errorf("%w: linear webhook body is too large", boardsync.ErrBadRequest)
	}
	signature := signatureFromHeaders(req.Header.Get)
	if err := VerifySignature([]byte(p.cfg.WebhookSecret.Reveal()), signature, req.Body); err != nil {
		return boardsync.Intake{}, err
	}
	var wh webhook
	if err := json.Unmarshal(req.Body, &wh); err != nil {
		return boardsync.Intake{}, fmt.Errorf("%w: linear payload: %v", boardsync.ErrBadRequest, err)
	}
	if err := VerifyTimestamp(wh.WebhookTimestamp, p.cfg.now(), p.cfg.tolerance()); err != nil {
		return boardsync.Intake{}, err
	}
	records, err := p.normalize(key, req.Header.Get(DeliveryHeader), req.Body, wh)
	if err != nil {
		return boardsync.Intake{}, err
	}
	for i := range records {
		records[i].Event.Raw = append([]byte(nil), req.Body...)
	}
	return boardsync.Intake{Response: boardsync.OK(), Records: records}, nil
}

// ResolveChannel returns a team key as the channel ID. Linear channel IDs are
// team keys: "ENG".
func (p *Provider) ResolveChannel(_ context.Context, key boardsync.Key, name string) (string, error) {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return "", fmt.Errorf("%w: entry %q isn't this linear app's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	name = strings.ToUpper(strings.TrimSpace(name))
	if !teamKeyPattern.MatchString(name) {
		return "", fmt.Errorf("%w: not a linear team key: %s", boardsync.ErrBadRequest, name)
	}
	if !p.cfg.teamAllowed(name) {
		return "", fmt.Errorf("%w: team %s is not in the allowlist", boardsync.ErrNotFound, name)
	}
	return name, nil
}

// ResolveTarget reads a team key ("ENG"), an issue identifier ("ENG-123"), an
// issue UUID, or a native "ENG:<uuid>" thread.
func (p *Provider) ResolveTarget(ctx context.Context, key boardsync.Key, target string) (boardsync.ResolvedTarget, error) {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return boardsync.ResolvedTarget{}, fmt.Errorf("%w: entry %q isn't this linear app's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	target = strings.TrimSpace(target)
	if target == "" {
		return boardsync.ResolvedTarget{}, fmt.Errorf("%w: empty linear target", boardsync.ErrBadRequest)
	}
	if teamKeyPattern.MatchString(target) {
		if !p.cfg.teamAllowed(target) {
			return boardsync.ResolvedTarget{}, fmt.Errorf("%w: team %s is not in the allowlist", boardsync.ErrNotFound, target)
		}
		resolved := boardsync.ResolvedTarget{Key: key, Platform: "linear", Channel: target, Kind: boardsync.TargetChannel}
		return resolved, resolved.Validate()
	}
	if match := teamKeyFromIdentifier.FindStringSubmatch(target); match != nil {
		teamKey := match[1]
		if !p.cfg.teamAllowed(teamKey) {
			return boardsync.ResolvedTarget{}, fmt.Errorf("%w: team %s is not in the allowlist", boardsync.ErrNotFound, teamKey)
		}
		number, _ := strconv.Atoi(match[2])
		issue, err := p.client.IssueByKey(ctx, teamKey, number)
		if err != nil {
			return boardsync.ResolvedTarget{}, err
		}
		resolved := boardsync.ResolvedTarget{
			Key: key, Platform: "linear", Channel: teamKey,
			Thread: issueThread(teamKey, issue.ID), Kind: boardsync.TargetThread,
		}
		return resolved, resolved.Validate()
	}
	if strings.Contains(target, ":") {
		teamKey, issueID, err := parseIssueThread(target)
		if err != nil {
			return boardsync.ResolvedTarget{}, err
		}
		if !p.cfg.teamAllowed(teamKey) {
			return boardsync.ResolvedTarget{}, fmt.Errorf("%w: team %s is not in the allowlist", boardsync.ErrNotFound, teamKey)
		}
		resolved := boardsync.ResolvedTarget{
			Key: key, Platform: "linear", Channel: teamKey,
			Thread: issueThread(teamKey, issueID), Kind: boardsync.TargetThread,
		}
		return resolved, resolved.Validate()
	}
	// A bare UUID: ask Linear which team it belongs to.
	issue, err := p.client.IssueByID(ctx, target)
	if err != nil {
		return boardsync.ResolvedTarget{}, err
	}
	teamKey := issue.Team.Key
	if !p.cfg.teamAllowed(teamKey) {
		return boardsync.ResolvedTarget{}, fmt.Errorf("%w: team %s is not in the allowlist", boardsync.ErrNotFound, teamKey)
	}
	resolved := boardsync.ResolvedTarget{
		Key: key, Platform: "linear", Channel: teamKey,
		Thread: issueThread(teamKey, issue.ID), Kind: boardsync.TargetThread,
	}
	return resolved, resolved.Validate()
}

// Post comments on an issue, or opens an issue for a channel target. A root's
// first line is the title and the whole message is the description.
func (p *Provider) Post(ctx context.Context, key boardsync.Key, target boardsync.ResolvedTarget, msg boardsync.OutboundMessage) (boardsync.Posted, error) {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return boardsync.Posted{}, fmt.Errorf("%w: entry %q isn't this linear app's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	if err := target.Validate(); err != nil {
		return boardsync.Posted{}, err
	}
	if target.Key != key {
		return boardsync.Posted{}, fmt.Errorf("%w: target belongs to %q", boardsync.ErrBadRequest, target.Key)
	}
	if len(msg.Files) > 0 {
		return boardsync.Posted{}, fmt.Errorf("%w: linear file upload is not implemented in this skeleton", boardsync.ErrUnsupported)
	}
	if strings.TrimSpace(msg.Markdown) == "" {
		return boardsync.Posted{}, fmt.Errorf("%w: linear message has no text", boardsync.ErrBadRequest)
	}
	if target.Kind == boardsync.TargetChannel {
		if !p.cfg.teamAllowed(target.Channel) {
			return boardsync.Posted{}, fmt.Errorf("%w: team %s is not in the allowlist", boardsync.ErrNotFound, target.Channel)
		}
		teamID, err := p.client.TeamID(ctx, target.Channel)
		if err != nil {
			return boardsync.Posted{}, err
		}
		title, description := splitTitle(msg.Markdown)
		issue, err := p.client.CreateIssue(ctx, teamID, title, description)
		if err != nil {
			return boardsync.Posted{}, err
		}
		return boardsync.Posted{Thread: issueThread(target.Channel, issue.ID), Message: issue.ID}, nil
	}
	teamKey, issueID, err := parseIssueThread(target.Thread)
	if err != nil {
		return boardsync.Posted{}, err
	}
	if !p.cfg.teamAllowed(teamKey) {
		return boardsync.Posted{}, fmt.Errorf("%w: team %s is not in the allowlist", boardsync.ErrNotFound, teamKey)
	}
	commentID, err := p.client.CreateComment(ctx, issueID, msg.Markdown)
	if err != nil {
		return boardsync.Posted{}, err
	}
	return boardsync.Posted{Thread: issueThread(teamKey, issueID), Message: commentID}, nil
}

// Edit replaces a delivered comment's body.
func (p *Provider) Edit(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message string, msg boardsync.OutboundMessage) error {
	if _, _, err := p.threadWhere(key, thread); err != nil {
		return err
	}
	return p.client.UpdateComment(ctx, message, msg.Markdown)
}

// Delete deletes a delivered comment.
func (p *Provider) Delete(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message string) error {
	if _, _, err := p.threadWhere(key, thread); err != nil {
		return err
	}
	return p.client.DeleteComment(ctx, message)
}

// React adds a reaction to a delivered comment. Linear can't take one back.
func (p *Provider) React(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message, emoji string, added bool) error {
	if _, _, err := p.threadWhere(key, thread); err != nil {
		return err
	}
	if !added {
		return fmt.Errorf("%w: linear reactions can't be removed through the API", boardsync.ErrUnsupported)
	}
	return p.client.ReactComment(ctx, message, emoji)
}

// FetchRoot reads an issue the board hasn't seen when a comment on it arrives.
func (p *Provider) FetchRoot(ctx context.Context, key boardsync.Key, thread string) (*boardsync.RootDraft, error) {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return nil, fmt.Errorf("%w: entry %q isn't this linear app's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	teamKey, issueID, err := parseIssueThread(thread)
	if err != nil {
		return nil, err
	}
	if !p.cfg.teamAllowed(teamKey) {
		return nil, fmt.Errorf("%w: team %s is not in the allowlist", boardsync.ErrNotFound, teamKey)
	}
	issue, err := p.client.IssueByID(ctx, issueID)
	if err != nil {
		return nil, err
	}
	return &boardsync.RootDraft{
		Text:   issueRootText(&issue),
		Labels: issueLabelsFor(&issue),
		Author: authorOf(issue.Creator),
	}, nil
}

func (p *Provider) threadWhere(key boardsync.Key, thread boardsync.ThreadRef) (string, string, error) {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return "", "", fmt.Errorf("%w: entry %q isn't this linear app's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	if thread.Key != key {
		return "", "", fmt.Errorf("%w: thread belongs to %q", boardsync.ErrBadRequest, thread.Key)
	}
	teamKey, issueID, err := parseIssueThread(thread.Thread)
	if err != nil {
		return "", "", err
	}
	if !p.cfg.teamAllowed(teamKey) {
		return "", "", fmt.Errorf("%w: team %s is not in the allowlist", boardsync.ErrNotFound, teamKey)
	}
	return teamKey, issueID, nil
}

func parseIssueThread(native string) (teamKey, issueID string, err error) {
	parts := strings.SplitN(native, ":", 2)
	if len(parts) != 2 || !teamKeyPattern.MatchString(parts[0]) || parts[1] == "" {
		return "", "", fmt.Errorf("%w: not a linear issue thread: %s", boardsync.ErrBadRequest, native)
	}
	return parts[0], parts[1], nil
}

func splitTitle(markdown string) (string, string) {
	lines := strings.SplitN(strings.TrimSpace(markdown), "\n", 2)
	title := strings.TrimSpace(lines[0])
	if title == "" {
		title = "New issue"
	}
	if len(title) > 120 {
		title = title[:120]
	}
	description := markdown
	return title, description
}

// Interface checks.
var (
	_ boardsync.Provider    = (*Provider)(nil)
	_ boardsync.Editor      = (*Provider)(nil)
	_ boardsync.Deleter     = (*Provider)(nil)
	_ boardsync.Reactor     = (*Provider)(nil)
	_ boardsync.RootFetcher = (*Provider)(nil)
)
