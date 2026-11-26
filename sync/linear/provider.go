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

// SelfIDs lists the app actor's author IDs.
func (p *Provider) SelfIDs() []string {
	if p.cfg.BotUserID == "" {
		return nil
	}
	return []string{"linear:user:" + p.cfg.BotUserID, p.cfg.BotUserID}
}

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
	if req == nil {
		return boardsync.Intake{}, fmt.Errorf("%w: empty linear webhook", boardsync.ErrBadRequest)
	}
	if int64(len(req.Body)) > p.cfg.maxBody() {
		return boardsync.Intake{}, fmt.Errorf("%w: linear webhook body is too large", boardsync.ErrBadRequest)
	}
	// Verify before parsing, and before an empty body can answer anything but
	// 401: an unsigned request must not learn whether its shape was valid.
	signature := signatureFromHeaders(req.Header.Get)
	if err := VerifySignature([]byte(p.cfg.WebhookSecret.Reveal()), signature, req.Body); err != nil {
		return boardsync.Intake{}, err
	}
	if len(req.Body) == 0 {
		return boardsync.Intake{}, fmt.Errorf("%w: empty linear webhook", boardsync.ErrBadRequest)
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
	if upper := strings.ToUpper(target); teamKeyPattern.MatchString(upper) {
		if !p.cfg.teamAllowed(upper) {
			return boardsync.ResolvedTarget{}, fmt.Errorf("%w: team %s is not in the allowlist", boardsync.ErrNotFound, upper)
		}
		resolved := boardsync.ResolvedTarget{Key: key, Platform: "linear", Channel: upper, Kind: boardsync.TargetChannel}
		return resolved, resolved.Validate()
	}
	if match := teamKeyFromIdentifier.FindStringSubmatch(target); match != nil {
		teamKey := match[1]
		if !p.cfg.teamAllowed(teamKey) {
			return boardsync.ResolvedTarget{}, fmt.Errorf("%w: team %s is not in the allowlist", boardsync.ErrNotFound, teamKey)
		}
		number, err := strconv.Atoi(match[2])
		if err != nil {
			return boardsync.ResolvedTarget{}, fmt.Errorf("%w: linear issue number %q", boardsync.ErrBadRequest, match[2])
		}
		issue, err := p.client.IssueByKey(ctx, teamKey, number)
		if err != nil {
			return boardsync.ResolvedTarget{}, err
		}
		if issue.ID == "" {
			return boardsync.ResolvedTarget{}, fmt.Errorf("%w: linear issue %s has no ID", boardsync.ErrNotFound, target)
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
	teamKey := strings.ToUpper(issue.Team.Key)
	if teamKey == "" {
		return boardsync.ResolvedTarget{}, fmt.Errorf("%w: linear issue %s has no team", boardsync.ErrNotFound, target)
	}
	if !p.cfg.teamAllowed(teamKey) {
		return boardsync.ResolvedTarget{}, fmt.Errorf("%w: team %s is not in the allowlist", boardsync.ErrNotFound, teamKey)
	}
	if issue.ID == "" {
		return boardsync.ResolvedTarget{}, fmt.Errorf("%w: linear issue %s has no ID", boardsync.ErrNotFound, target)
	}
	resolved := boardsync.ResolvedTarget{
		Key: key, Platform: "linear", Channel: teamKey,
		Thread: issueThread(teamKey, issue.ID), Kind: boardsync.TargetThread,
	}
	return resolved, resolved.Validate()
}

// IssueRootMessage is the copy ID a channel target returns: the issue itself,
// not a comment. Edits then change the issue's description and a delete leaves
// the issue in place, because closing it is a person's call.
const IssueRootMessage = "issue"

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
		return boardsync.Posted{}, fmt.Errorf("%w: linear file upload is not implemented", boardsync.ErrUnsupported)
	}
	if strings.TrimSpace(msg.Markdown) == "" {
		return boardsync.Posted{}, fmt.Errorf("%w: linear message has no text", boardsync.ErrBadRequest)
	}
	if target.Kind == boardsync.TargetChannel {
		teamKey := strings.ToUpper(target.Channel)
		if !p.cfg.teamAllowed(teamKey) {
			return boardsync.Posted{}, fmt.Errorf("%w: team %s is not in the allowlist", boardsync.ErrNotFound, teamKey)
		}
		teamID, err := p.client.TeamID(ctx, teamKey)
		if err != nil {
			return boardsync.Posted{}, err
		}
		title, description := splitTitle(msg.Markdown)
		issue, err := p.client.CreateIssue(ctx, teamID, title, description)
		if err != nil {
			return boardsync.Posted{}, err
		}
		return boardsync.Posted{Thread: issueThread(teamKey, issue.ID), Message: IssueRootMessage}, nil
	}
	teamKey, issueID, err := parseIssueThread(target.Thread)
	if err != nil {
		return boardsync.Posted{}, err
	}
	if !p.cfg.teamAllowed(teamKey) {
		return boardsync.Posted{}, fmt.Errorf("%w: team %s is not in the allowlist", boardsync.ErrNotFound, teamKey)
	}
	if err := checkCommentBody(msg.Markdown); err != nil {
		return boardsync.Posted{}, err
	}
	commentID, err := p.client.CreateComment(ctx, issueID, msg.Markdown)
	if err != nil {
		return boardsync.Posted{}, err
	}
	return boardsync.Posted{Thread: issueThread(teamKey, issueID), Message: commentID}, nil
}

// Edit replaces a delivered comment's body, or an issue the board opened.
func (p *Provider) Edit(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message string, msg boardsync.OutboundMessage) error {
	_, issueID, err := p.threadWhere(key, thread)
	if err != nil {
		return err
	}
	if len(msg.Files) > 0 {
		return fmt.Errorf("%w: linear file upload is not implemented", boardsync.ErrUnsupported)
	}
	if message == IssueRootMessage {
		return p.client.UpdateIssue(ctx, issueID, msg.Markdown)
	}
	if err := checkCommentBody(msg.Markdown); err != nil {
		return err
	}
	return p.client.UpdateComment(ctx, message, msg.Markdown)
}

// Delete deletes a delivered comment. An issue the board opened stays: it is
// not the root's copy to remove.
func (p *Provider) Delete(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message string) error {
	if _, _, err := p.threadWhere(key, thread); err != nil {
		return err
	}
	if message == IssueRootMessage {
		return nil
	}
	return p.client.DeleteComment(ctx, message)
}

// React adds a reaction to a delivered comment. Linear can't take one back,
// and an issue isn't a reaction target; both are reported or skipped rather
// than pretending success.
func (p *Provider) React(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message, emoji string, added bool) error {
	if _, _, err := p.threadWhere(key, thread); err != nil {
		return err
	}
	if message == IssueRootMessage {
		return nil
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
	if thread.Subthread != "" {
		if !strings.HasPrefix(thread.Subthread, issueThread(teamKey, issueID)+":c:") {
			return "", "", fmt.Errorf("%w: linear sub-thread %q doesn't extend its issue", boardsync.ErrBadRequest, thread.Subthread)
		}
	}
	if !p.cfg.teamAllowed(teamKey) {
		return "", "", fmt.Errorf("%w: team %s is not in the allowlist", boardsync.ErrNotFound, teamKey)
	}
	return teamKey, issueID, nil
}

// parseIssueThread reads "ENG:<issue UUID>". A comment sub-thread is never a
// target: its issue is.
func parseIssueThread(native string) (teamKey, issueID string, err error) {
	parts := strings.Split(strings.TrimSpace(native), ":")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("%w: not a linear issue thread: %s", boardsync.ErrBadRequest, native)
	}
	teamKey = strings.ToUpper(parts[0])
	issueID = parts[1]
	if !teamKeyPattern.MatchString(teamKey) || strings.TrimSpace(issueID) == "" || strings.ContainsAny(issueID, " \t\r\n") {
		return "", "", fmt.Errorf("%w: not a linear issue thread: %s", boardsync.ErrBadRequest, native)
	}
	return teamKey, issueID, nil
}

func checkCommentBody(markdown string) error {
	if len([]rune(markdown)) > MaxCommentChars {
		return fmt.Errorf("%w: linear comment is longer than %d characters", boardsync.ErrBadRequest, MaxCommentChars)
	}
	return nil
}

// splitTitle makes an issue title from a root's first line, without the
// renderer's "**name**: " label or markdown emphasis.
func splitTitle(markdown string) (string, string) {
	first := markdown
	if index := strings.IndexByte(markdown, '\n'); index >= 0 {
		first = markdown[:index]
	}
	first = strings.TrimSpace(first)
	// The default renderer opens with a bold label and a colon.
	if strings.HasPrefix(first, "**") {
		if end := strings.Index(first[2:], "**:"); end >= 0 {
			first = strings.TrimSpace(first[2+end+3:])
		}
	}
	replacer := strings.NewReplacer("**", "", "__", "", "`", "")
	first = replacer.Replace(first)
	first = strings.TrimLeft(first, "#> \t")
	first = strings.Join(strings.Fields(first), " ")
	if first == "" {
		first = "Untitled"
	}
	runes := []rune(first)
	if len(runes) > MaxTitleChars {
		first = string(runes[:MaxTitleChars])
	}
	return first, markdown
}

// Interface checks.
var (
	_ boardsync.Provider    = (*Provider)(nil)
	_ boardsync.Editor      = (*Provider)(nil)
	_ boardsync.Deleter     = (*Provider)(nil)
	_ boardsync.Reactor     = (*Provider)(nil)
	_ boardsync.RootFetcher = (*Provider)(nil)
)
