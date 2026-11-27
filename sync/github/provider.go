package github

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	boardsync "github.com/jixoai/keel-board/sync"
)

// Provider implements boardsync.Provider for one GitHub App.
type Provider struct {
	cfg    Config
	client *Client

	viewerMu    sync.Mutex
	viewerUser  user
	viewerKnown bool
	viewerErr   error
	viewerErrAt time.Time
}

// viewerRetry bounds how often a failed viewer lookup is retried: an app that
// can't read /user must not call it once per webhook.
const viewerRetry = time.Minute

// New returns a GitHub provider. It validates the allowlists and builds the
// REST client; it performs no network I/O.
func New(cfg Config) (*Provider, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Provider{
		cfg:    cfg,
		client: NewClient(cfg.baseURL(), cfg.doer(), cfg.Token),
	}, nil
}

// Platform implements boardsync.Provider.
func (p *Provider) Platform() boardsync.Platform { return "github" }

// SelfIDs lists the app's own author IDs.
func (p *Provider) SelfIDs() []string {
	var ids []string
	if p.cfg.BotUserID != 0 {
		id := strconv.FormatInt(p.cfg.BotUserID, 10)
		ids = append(ids, "github:user:"+id, id)
	}
	if p.cfg.BotLogin != "" {
		login := strings.ToLower(p.cfg.BotLogin)
		ids = append(ids, "github:user:"+login, login)
	}
	return ids
}

// Capabilities implements boardsync.Provider: GitHub can edit, delete, and
// react, but a board root never opens an issue or PR.
func (p *Provider) Capabilities() boardsync.Capabilities {
	return boardsync.Capabilities{StartThreads: false, Edits: true, Deletes: true, Reactions: true}
}

// Receive verifies X-Hub-Signature-256, checks the delivery ID, the repo
// allowlist, and (when configured) the installation, and normalizes comments,
// PRs, issues, reviews, and CI runs.
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
	delivery := req.Header.Get(DeliveryHeader)
	if !validDelivery(delivery) {
		return boardsync.Intake{}, fmt.Errorf("%w: github request has no usable delivery ID", boardsync.ErrBadRequest)
	}
	var pl payload
	if err := jsonUnmarshal(req.Body, &pl); err != nil {
		return boardsync.Intake{}, fmt.Errorf("%w: github payload: %v", boardsync.ErrBadRequest, err)
	}
	records, err := p.normalize(ctx, key, eventName, delivery, req.Body, pl)
	if err != nil {
		return boardsync.Intake{}, err
	}
	for i := range records {
		records[i].Event.Raw = append([]byte(nil), req.Body...)
	}
	return boardsync.Intake{Response: boardsync.OK(), Records: records}, nil
}

// Viewer reports the account a user token authenticates as, fetching it once.
// GitHub App installation tokens cannot call GET /user; use BotUserID or
// BotLogin for those. A deployment can call Viewer at startup to learn the ID.
func (p *Provider) Viewer(ctx context.Context) (boardsync.Author, error) {
	if isInstallationToken(p.cfg.Token) {
		return boardsync.Author{}, fmt.Errorf("%w: GET /user needs a user token; installation tokens use BotUserID or BotLogin", boardsync.ErrUnsupported)
	}
	p.viewerMu.Lock()
	defer p.viewerMu.Unlock()
	if p.viewerKnown {
		return authorOf(p.viewerUser), nil
	}
	now := p.cfg.now()
	if p.viewerErr != nil && now.Sub(p.viewerErrAt) < viewerRetry {
		return boardsync.Author{}, p.viewerErr
	}
	account, err := p.client.Viewer(ctx)
	if err != nil {
		p.viewerErr = err
		p.viewerErrAt = now
		return boardsync.Author{}, err
	}
	p.viewerErr = nil
	if account.ID == 0 && account.Login == "" {
		err := fmt.Errorf("github answered an empty viewer")
		p.viewerErr = err
		p.viewerErrAt = now
		return boardsync.Author{}, err
	}
	p.viewerUser = account
	p.viewerKnown = true
	return authorOf(account), nil
}

// viewer returns the cached viewer identity without doing I/O.
func (p *Provider) viewer() (id int64, login string, known bool) {
	p.viewerMu.Lock()
	defer p.viewerMu.Unlock()
	return p.viewerUser.ID, p.viewerUser.Login, p.viewerKnown
}

// requireEchoGuard refuses to write when neither the hidden marker nor a bot
// identity can suppress the platform echo. A deployment that disables markers
// must configure BotUserID/BotLogin or DetectViewer; if viewer detection fails,
// sending is refused rather than risking an import loop.
func (p *Provider) requireEchoGuard(ctx context.Context) error {
	if !p.cfg.DisableMarker {
		return nil
	}
	if p.cfg.BotUserID != 0 || p.cfg.BotLogin != "" {
		return nil
	}
	if !p.cfg.DetectViewer {
		return fmt.Errorf("%w: github outbound needs a marker or a bot identity", boardsync.ErrUnsupported)
	}
	if _, err := p.Viewer(ctx); err != nil {
		return fmt.Errorf("github can't establish its own identity and the marker is disabled: %w", err)
	}
	return nil
}

// channelPattern is one repo channel, never a repo with two suffixes.
var channelPattern = regexp.MustCompile(`^([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)(?::(pulls|issues))?$`)

// ResolveChannel accepts "owner/repo", "owner/repo:pulls", or
// "owner/repo:issues". A bare repo resolves to its pulls channel, which is
// only useful for routing; GitHub can't start a thread there.
func (p *Provider) ResolveChannel(_ context.Context, key boardsync.Key, name string) (string, error) {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return "", fmt.Errorf("%w: entry %q isn't this github app's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	match := channelPattern.FindStringSubmatch(name)
	if match == nil || !validRepo(match[1]) {
		return "", fmt.Errorf("%w: not a github repo channel: %s", boardsync.ErrBadRequest, name)
	}
	repo := match[1]
	if !p.cfg.readAllowed(repo) {
		return "", fmt.Errorf("%w: %s is not in the inbound repo allowlist", boardsync.ErrNotFound, repo)
	}
	if match[2] != "" {
		return name, nil
	}
	return repo + ":pulls", nil
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
		if !p.cfg.readAllowed(target) {
			return boardsync.ResolvedTarget{}, fmt.Errorf("%w: %s is not in the inbound repo allowlist", boardsync.ErrNotFound, target)
		}
		resolved := boardsync.ResolvedTarget{Key: key, Platform: "github", Channel: target + ":pulls", Kind: boardsync.TargetChannel}
		return resolved, resolved.Validate()
	}
	info, err := parseThread(target)
	if err != nil {
		return boardsync.ResolvedTarget{}, err
	}
	if !p.cfg.readAllowed(info.Full) {
		return boardsync.ResolvedTarget{}, fmt.Errorf("%w: %s is not in the inbound repo allowlist", boardsync.ErrNotFound, info.Full)
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
	if err := p.requireEchoGuard(ctx); err != nil {
		return boardsync.Posted{}, err
	}
	if len(msg.Files) > 0 {
		return boardsync.Posted{}, fmt.Errorf("%w: github comment files are not implemented", boardsync.ErrUnsupported)
	}
	if strings.TrimSpace(msg.Markdown) == "" {
		return boardsync.Posted{}, fmt.Errorf("%w: github comment has no text", boardsync.ErrBadRequest)
	}
	info, err := parseThread(target.Thread)
	if err != nil {
		return boardsync.Posted{}, err
	}
	if target.Channel != "" && target.Channel != info.channel() {
		return boardsync.Posted{}, fmt.Errorf("%w: channel %s doesn't hold %s", boardsync.ErrBadRequest, target.Channel, target.Thread)
	}
	if target.Subthread != "" {
		sub, err := parseThread(target.Subthread)
		if err != nil {
			return boardsync.Posted{}, err
		}
		if sub.Full != info.Full || sub.Number != info.Number || sub.ReviewID == 0 {
			return boardsync.Posted{}, fmt.Errorf("%w: review sub-thread %s isn't inside %s", boardsync.ErrBadRequest, target.Subthread, target.Thread)
		}
		info.ReviewID = sub.ReviewID
	}
	if !p.cfg.writeAllowed(info.Full) {
		return boardsync.Posted{}, fmt.Errorf("%w: writes to %s are not allowed; set WriteRepos", boardsync.ErrUnsupported, info.Full)
	}
	body := p.commentBody(key, target.Ref(), msg)
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

// commentBody renders and bounds a comment, keeping the hidden marker intact.
// GitHub accepts at most MaxCommentLength characters; a longer board message is
// cut at a rune boundary with an ellipsis rather than failing delivery.
func (p *Provider) commentBody(key boardsync.Key, thread boardsync.ThreadRef, msg boardsync.OutboundMessage) string {
	body := nativeMentions(boardsync.StorableText(msg.Markdown, boardsync.MaxTextBytes), key)
	if p.cfg.DisableMarker {
		return clampComment(body, MaxCommentLength)
	}
	marker := boardsync.Marker{Key: key, Message: msg.BoardMessage, Thread: thread.External()}
	reserve := utf8.RuneCountInString(marker.String()) + 2
	return boardsync.WithMarker(clampComment(body, MaxCommentLength-reserve), marker)
}

// clampComment bounds a body by GitHub's character limit. An already short
// body is returned unchanged.
func clampComment(body string, max int) string {
	if max <= 0 {
		return ""
	}
	if utf8.RuneCountInString(body) <= max {
		return body
	}
	runes := []rune(body)
	if max == 1 {
		return "…"
	}
	return string(runes[:max-1]) + "…"
}

// Edit replaces a delivered comment's body.
func (p *Provider) Edit(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message string, msg boardsync.OutboundMessage) error {
	info, commentID, err := p.commentWhere(key, thread, message)
	if err != nil {
		return err
	}
	if err := p.requireEchoGuard(ctx); err != nil {
		return err
	}
	if len(msg.Files) > 0 {
		return fmt.Errorf("%w: github comments carry no files", boardsync.ErrUnsupported)
	}
	if strings.TrimSpace(msg.Markdown) == "" {
		return fmt.Errorf("%w: github comment has no text", boardsync.ErrBadRequest)
	}
	body := p.commentBody(key, thread, msg)
	if info.ReviewID != 0 {
		return p.client.UpdateReviewComment(ctx, info.Full, commentID, body)
	}
	return p.client.UpdateIssueComment(ctx, info.Full, commentID, body)
}

// Delete deletes a delivered comment. A review comment that is already gone is
// a success: the explicit sub-thread says which endpoint owns it. A bare thread
// is ambiguous when a receipt lost its sub-thread (an issue comment and a
// review comment both look like `repo:number`), so a 404 there is returned
// instead of being recorded as a false success.
func (p *Provider) Delete(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message string) error {
	info, commentID, err := p.commentWhere(key, thread, message)
	if err != nil {
		return err
	}
	if err := p.requireEchoGuard(ctx); err != nil {
		return err
	}
	if info.ReviewID != 0 {
		err = p.client.DeleteReviewComment(ctx, info.Full, commentID)
	} else {
		err = p.client.DeleteIssueComment(ctx, info.Full, commentID)
	}
	if errors.Is(err, boardsync.ErrNotFound) && info.ReviewID != 0 {
		return nil
	}
	return err
}

// React adds a reaction. GitHub has no simple remove without a reaction ID,
// so a removal is refused.
func (p *Provider) React(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message, emoji string, added bool) error {
	if !added {
		return fmt.Errorf("%w: github reaction removal needs a reaction ID lookup", boardsync.ErrUnsupported)
	}
	info, err := p.threadWhere(key, thread)
	if err != nil {
		return err
	}
	if err := p.requireEchoGuard(ctx); err != nil {
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
			Text:   issueSourceText(key, &item),
			Labels: issueLabels(&item, repo),
			Author: authorOf(item.User),
		}, nil
	}
	pr, err := p.client.fetchPull(ctx, repo, info.Number)
	if err != nil {
		return nil, err
	}
	return &boardsync.RootDraft{
		Text:   pullSourceText(key, &pr),
		Labels: prLabels(&pr, repo),
		Author: authorOf(pr.User),
	}, nil
}

var githubURLPattern = regexp.MustCompile(`^https://github\.com/([^/\s]+)/([^/\s]+)/(pull|issues)/(\d+)(?:[^#\s]*#(issuecomment-|discussion_r|r)(\d+))?$`)

// Locate reads a PR, issue, or comment URL as its native thread and message.
// Review comments are accepted by their conversation anchor
// (#discussion_r…) and by the files-view short anchor (#r…).
func (p *Provider) Locate(link string) (string, string, bool) {
	match := githubURLPattern.FindStringSubmatch(strings.TrimSpace(link))
	if match == nil {
		return "", "", false
	}
	owner, repo, kind, number := match[1], match[2], match[3], match[4]
	if !validRepo(owner + "/" + repo) {
		return "", "", false
	}
	thread := fmt.Sprintf("%s/%s:%s", owner, repo, number)
	if kind == "issues" {
		thread = fmt.Sprintf("%s/%s:issue:%s", owner, repo, number)
	}
	if match[5] == "" {
		return thread, "", true
	}
	if (match[5] == "discussion_r" || match[5] == "r") && kind != "pull" {
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
	if thread.Channel != "" && thread.Channel != info.channel() {
		return threadInfo{}, fmt.Errorf("%w: channel %s doesn't hold %s", boardsync.ErrBadRequest, thread.Channel, thread.Thread)
	}
	if !p.cfg.writeAllowed(info.Full) {
		return threadInfo{}, fmt.Errorf("%w: writes to %s are not allowed", boardsync.ErrUnsupported, info.Full)
	}
	if thread.Subthread != "" {
		sub, err := parseThread(thread.Subthread)
		if err != nil {
			return threadInfo{}, err
		}
		if sub.Full != info.Full || sub.Number != info.Number || sub.ReviewID == 0 {
			return threadInfo{}, fmt.Errorf("%w: sub-thread %s isn't a review thread inside %s", boardsync.ErrBadRequest, thread.Subthread, thread.Thread)
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
	if err != nil || commentID <= 0 {
		return threadInfo{}, 0, fmt.Errorf("%w: comment ID %q isn't a positive integer", boardsync.ErrBadRequest, message)
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
