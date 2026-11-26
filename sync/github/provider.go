package github

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	boardsync "github.com/jixoai/keel-board/sync"
)

// Provider implements boardsync.Provider for one GitHub App.
type Provider struct {
	cfg    Config
	client *Client
}

// New returns a GitHub provider. It validates the allowlists and builds the
// REST client; it performs no network I/O.
func New(cfg Config) (*Provider, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Provider{
		cfg:    cfg,
		client: NewClient(cfg.baseURL(), cfg.httpClient(), cfg.Token),
	}, nil
}

// Platform implements boardsync.Provider.
func (p *Provider) Platform() boardsync.Platform { return "github" }

// Capabilities implements boardsync.Provider: GitHub can edit, delete, and
// react, but a board root never opens an issue or PR.
func (p *Provider) Capabilities() boardsync.Capabilities {
	return boardsync.Capabilities{StartThreads: false, Edits: true, Deletes: true, Reactions: true}
}

// Receive verifies X-Hub-Signature-256, checks the repo allowlist, and
// normalizes comments, PRs, issues, reviews, and CI runs.
func (p *Provider) Receive(ctx context.Context, key boardsync.Key, req *boardsync.WebhookRequest) (boardsync.Intake, error) {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return boardsync.Intake{}, fmt.Errorf("%w: entry %q isn't this github app's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	if req == nil || len(req.Body) == 0 {
		return boardsync.Intake{}, fmt.Errorf("%w: empty github webhook", boardsync.ErrBadRequest)
	}
	if int64(len(req.Body)) > p.cfg.maxBody() {
		return boardsync.Intake{}, fmt.Errorf("%w: github webhook body is too large", boardsync.ErrBadRequest)
	}
	if err := VerifySignature([]byte(p.cfg.WebhookSecret.Reveal()), req.Header.Get(SignatureHeader), req.Body); err != nil {
		return boardsync.Intake{}, err
	}
	eventName := req.Header.Get(EventHeader)
	if eventName == "" {
		return boardsync.Intake{}, fmt.Errorf("%w: github request has no event header", boardsync.ErrBadRequest)
	}
	var pl payload
	if err := jsonUnmarshal(req.Body, &pl); err != nil {
		return boardsync.Intake{}, fmt.Errorf("%w: github payload: %v", boardsync.ErrBadRequest, err)
	}
	records, err := p.normalize(key, eventName, req.Header.Get(DeliveryHeader), req.Body, pl)
	if err != nil {
		return boardsync.Intake{}, err
	}
	for i := range records {
		records[i].Event.Raw = append([]byte(nil), req.Body...)
	}
	return boardsync.Intake{Response: boardsync.OK(), Records: records}, nil
}

// ResolveChannel accepts "owner/repo", "owner/repo:pulls", or
// "owner/repo:issues". A bare repo resolves to its pulls channel, which is
// only useful for routing; GitHub can't start a thread there.
func (p *Provider) ResolveChannel(_ context.Context, key boardsync.Key, name string) (string, error) {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return "", fmt.Errorf("%w: entry %q isn't this github app's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	if strings.HasSuffix(name, ":pulls") || strings.HasSuffix(name, ":issues") {
		if !validRepo(strings.TrimSuffix(strings.TrimSuffix(name, ":pulls"), ":issues")) {
			return "", fmt.Errorf("%w: not a github repo channel: %s", boardsync.ErrBadRequest, name)
		}
		return name, nil
	}
	if !validRepo(name) {
		return "", fmt.Errorf("%w: not a github repo: %s", boardsync.ErrBadRequest, name)
	}
	return name + ":pulls", nil
}

// ResolveTarget reads "owner/repo" (a channel), "owner/repo:123" (a PR), or
// "owner/repo:issue:45" (an issue). A review thread isn't a target.
func (p *Provider) ResolveTarget(_ context.Context, key boardsync.Key, target string) (boardsync.ResolvedTarget, error) {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return boardsync.ResolvedTarget{}, fmt.Errorf("%w: entry %q isn't this github app's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	target = strings.TrimSpace(target)
	if strings.Contains(target, ":rc:") {
		base := target[:strings.Index(target, ":rc:")]
		return boardsync.ResolvedTarget{}, fmt.Errorf("%w: a review thread can't be a target; sync its PR, %s", boardsync.ErrBadRequest, base)
	}
	if !strings.Contains(target, ":") {
		if !validRepo(target) {
			return boardsync.ResolvedTarget{}, fmt.Errorf("%w: not a github repo: %s", boardsync.ErrBadRequest, target)
		}
		resolved := boardsync.ResolvedTarget{Key: key, Platform: "github", Channel: target + ":pulls", Kind: boardsync.TargetChannel}
		return resolved, resolved.Validate()
	}
	info, err := parseThread(target)
	if err != nil {
		return boardsync.ResolvedTarget{}, err
	}
	resolved := boardsync.ResolvedTarget{
		Key: key, Platform: "github",
		Channel: info.channel(), Thread: info.thread(), Kind: boardsync.TargetThread,
	}
	if err := resolved.Validate(); err != nil {
		return boardsync.ResolvedTarget{}, err
	}
	return resolved, nil
}

// Post comments on a PR or issue, or replies inside an inline review thread.
// A channel target is refused: GitHub threads start on GitHub.
func (p *Provider) Post(ctx context.Context, key boardsync.Key, target boardsync.ResolvedTarget, msg boardsync.OutboundMessage) (boardsync.Posted, error) {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return boardsync.Posted{}, fmt.Errorf("%w: entry %q isn't this github app's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	if err := target.Validate(); err != nil {
		return boardsync.Posted{}, err
	}
	if target.Kind != boardsync.TargetThread {
		return boardsync.Posted{}, fmt.Errorf("%w: github can't start a thread in a channel", boardsync.ErrUnsupported)
	}
	if target.Key != key {
		return boardsync.Posted{}, fmt.Errorf("%w: target belongs to %q", boardsync.ErrBadRequest, target.Key)
	}
	info, err := parseThread(target.Thread)
	if err != nil {
		return boardsync.Posted{}, err
	}
	if target.Subthread != "" {
		sub, err := parseThread(target.Subthread)
		if err != nil {
			return boardsync.Posted{}, err
		}
		info.ReviewID = sub.ReviewID
	}
	if !p.cfg.writeAllowed(info.Full) {
		return boardsync.Posted{}, fmt.Errorf("%w: writes to %s are not allowed; set WriteRepos", boardsync.ErrUnsupported, info.Full)
	}
	body := msg.Markdown
	if !p.cfg.DisableMarker {
		body = boardsync.WithMarker(body, markerFor(key, msg, target))
	}
	var response commentResponse
	if info.ReviewID != 0 {
		response, err = p.client.CreateReviewReply(ctx, info.Full, info.Number, info.ReviewID, body)
	} else {
		response, err = p.client.CreateIssueComment(ctx, info.Full, info.Number, body)
	}
	if err != nil {
		return boardsync.Posted{}, err
	}
	if response.ID == 0 {
		return boardsync.Posted{}, fmt.Errorf("github refused to say which comment it made")
	}
	return boardsync.Posted{Thread: info.thread(), Message: strconv.FormatInt(response.ID, 10)}, nil
}

// Edit replaces a delivered comment's body.
func (p *Provider) Edit(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message string, msg boardsync.OutboundMessage) error {
	info, commentID, err := p.commentWhere(key, thread, message)
	if err != nil {
		return err
	}
	body := msg.Markdown
	if !p.cfg.DisableMarker {
		body = boardsync.WithMarker(body, boardsync.Marker{Key: key, Message: msg.BoardMessage, Thread: thread.External()})
	}
	if info.ReviewID != 0 {
		return p.client.UpdateReviewComment(ctx, info.Full, commentID, body)
	}
	return p.client.UpdateIssueComment(ctx, info.Full, commentID, body)
}

// Delete deletes a delivered comment.
func (p *Provider) Delete(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message string) error {
	info, commentID, err := p.commentWhere(key, thread, message)
	if err != nil {
		return err
	}
	if info.ReviewID != 0 {
		return p.client.DeleteReviewComment(ctx, info.Full, commentID)
	}
	return p.client.DeleteIssueComment(ctx, info.Full, commentID)
}

// React adds a reaction. GitHub has no simple remove without a reaction ID,
// so a removal is refused.
func (p *Provider) React(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message, emoji string, added bool) error {
	if !added {
		return fmt.Errorf("%w: github reaction removal isn't implemented in this skeleton", boardsync.ErrUnsupported)
	}
	info, err := p.threadWhere(key, thread)
	if err != nil {
		return err
	}
	content, ok := reactionContent(emoji)
	if !ok {
		return fmt.Errorf("%w: github has no reaction for %q", boardsync.ErrUnsupported, emoji)
	}
	if info.ReviewID != 0 {
		commentID, err := strconv.ParseInt(message, 10, 64)
		if err != nil {
			return fmt.Errorf("%w: review comment ID %q", boardsync.ErrBadRequest, message)
		}
		return p.client.ReactReviewComment(ctx, info.Full, commentID, content)
	}
	if commentID, err := strconv.ParseInt(message, 10, 64); err == nil {
		return p.client.ReactIssueComment(ctx, info.Full, commentID, content)
	}
	// A root's copy is the PR or issue itself; its ID isn't a comment's.
	return p.client.ReactIssue(ctx, info.Full, info.Number, content)
}

// FetchRoot recovers a PR or issue the board hasn't seen when a comment on it
// arrives.
func (p *Provider) FetchRoot(ctx context.Context, key boardsync.Key, thread string) (*boardsync.RootDraft, error) {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return nil, fmt.Errorf("%w: entry %q isn't this github app's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	info, err := parseThread(thread)
	if err != nil {
		return nil, err
	}
	if !p.cfg.readAllowed(info.Full) {
		return nil, fmt.Errorf("%w: %s is not in the inbound repo allowlist", boardsync.ErrNotFound, info.Full)
	}
	repo := info.Full
	if info.Issue {
		item, err := p.client.fetchIssue(ctx, repo, info.Number)
		if err != nil {
			return nil, err
		}
		return &boardsync.RootDraft{
			Text:   fmt.Sprintf("Issue #%d: %s\n%s\n\n%s", item.Number, item.Title, item.HTMLURL, item.Body),
			Labels: issueLabels(&item, repo),
			Author: authorOf(item.User),
		}, nil
	}
	pr, err := p.client.fetchPull(ctx, repo, info.Number)
	if err != nil {
		return nil, err
	}
	return &boardsync.RootDraft{
		Text:   fmt.Sprintf("PR #%d: %s\n%s\n\n%s", pr.Number, pr.Title, pr.HTMLURL, pr.Body),
		Labels: prLabels(&pr, repo),
		Author: authorOf(pr.User),
	}, nil
}

var githubURLPattern = regexp.MustCompile(`^https://github\.com/([^/\s]+)/([^/\s]+)/(pull|issues)/(\d+)(?:[^#\s]*#(issuecomment-|discussion_r)(\d+))?$`)

// Locate reads a PR, issue, or comment URL as its native thread and message.
func (p *Provider) Locate(link string) (string, string, bool) {
	match := githubURLPattern.FindStringSubmatch(strings.TrimSpace(link))
	if match == nil {
		return "", "", false
	}
	owner, repo, kind, number := match[1], match[2], match[3], match[4]
	thread := fmt.Sprintf("%s/%s:%s", owner, repo, number)
	if kind == "issues" {
		thread = fmt.Sprintf("%s/%s:issue:%s", owner, repo, number)
	}
	if match[5] == "" {
		return thread, "", true
	}
	if match[5] == "discussion_r" && kind != "pull" {
		return "", "", false
	}
	return thread, match[6], true
}

// URL makes a PR, issue, or comment link from native IDs.
func (p *Provider) URL(thread, message string) (string, error) {
	info, err := parseThread(thread)
	if err != nil {
		return "", err
	}
	kind := "pull"
	if info.Issue {
		kind = "issues"
	}
	page := fmt.Sprintf("https://github.com/%s/%s/%s/%d", info.Owner, info.Repo, kind, info.Number)
	if !isDigits(message) {
		return page, nil
	}
	if info.ReviewID != 0 {
		return page + "#discussion_r" + message, nil
	}
	return page + "#issuecomment-" + message, nil
}

func (p *Provider) threadWhere(key boardsync.Key, thread boardsync.ThreadRef) (threadInfo, error) {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return threadInfo{}, fmt.Errorf("%w: entry %q isn't this github app's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	if thread.Key != key {
		return threadInfo{}, fmt.Errorf("%w: thread belongs to %q", boardsync.ErrBadRequest, thread.Key)
	}
	info, err := parseThread(thread.Thread)
	if err != nil {
		return threadInfo{}, err
	}
	if !p.cfg.writeAllowed(info.Full) {
		return threadInfo{}, fmt.Errorf("%w: writes to %s are not allowed", boardsync.ErrUnsupported, info.Full)
	}
	if thread.Subthread != "" {
		sub, err := parseThread(thread.Subthread)
		if err != nil {
			return threadInfo{}, err
		}
		info.ReviewID = sub.ReviewID
	}
	return info, nil
}

func (p *Provider) commentWhere(key boardsync.Key, thread boardsync.ThreadRef, message string) (threadInfo, int64, error) {
	info, err := p.threadWhere(key, thread)
	if err != nil {
		return threadInfo{}, 0, err
	}
	commentID, err := strconv.ParseInt(message, 10, 64)
	if err != nil {
		return threadInfo{}, 0, fmt.Errorf("%w: comment ID %q isn't numeric", boardsync.ErrBadRequest, message)
	}
	return info, commentID, nil
}

func reactionContent(emoji string) (string, bool) {
	switch strings.ToLower(strings.Trim(emoji, ":")) {
	case "+1", "thumbsup", "thumbs_up":
		return "+1", true
	case "-1", "thumbsdown", "thumbs_down":
		return "-1", true
	case "laugh", "laughing", "joy":
		return "laugh", true
	case "confused":
		return "confused", true
	case "heart", "love":
		return "heart", true
	case "hooray", "tada":
		return "hooray", true
	case "rocket":
		return "rocket", true
	case "eyes":
		return "eyes", true
	default:
		return "", false
	}
}

func isDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, c := range value {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// Interface checks.
var (
	_ boardsync.Provider    = (*Provider)(nil)
	_ boardsync.Editor      = (*Provider)(nil)
	_ boardsync.Deleter     = (*Provider)(nil)
	_ boardsync.Reactor     = (*Provider)(nil)
	_ boardsync.RootFetcher = (*Provider)(nil)
	_ boardsync.Linker      = (*Provider)(nil)
)
