package linear

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	boardsync "github.com/jixoai/keel-board/sync"
)

// The shared source contract. ReadOptions is an alias so existing callers can
// keep naming linear.ReadOptions; the fields and JSON are the sync package's.
type (
	ReadOptions   = boardsync.ReadOptions
	SourceRead    = boardsync.SourceRead
	SourceMessage = boardsync.SourceMessage
)

const (
	// DefaultReadLimit is how many issues or comments one read returns.
	DefaultReadLimit = 50
	// MaxReadLimit is Linear's practical page cap for these reads.
	MaxReadLimit = 100
	// listedDescriptionBytes bounds an issue description shown in a list.
	listedDescriptionBytes = 1500
)

var (
	_ boardsync.SourceReader = (*Provider)(nil)

	issueURLPattern   = regexp.MustCompile(`(?i)^https://linear\.app/[^/\s]+/issue/([A-Z][A-Z0-9_]*-\d+)(?:[/?#].*)?$`)
	projectURLPattern = regexp.MustCompile(`(?i)^https://linear\.app/[^/\s]+/project/(?:[^/?#\s]*?-)?([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|[0-9a-f]{8,})(?:[/?#].*)?$`)
	issueIDPattern    = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
)

// ReadSource reads a team ("ENG"), a project (its link or "project:<id>"), or
// an issue ("ENG-123", its UUID, or its link). Only allowlisted teams are
// readable: a team outside the allowlist, or a project with any team outside
// it, answers ErrNotFound rather than leaking the platform's data.
func (p *Provider) ReadSource(ctx context.Context, key boardsync.Key, target string, opts ReadOptions) (SourceRead, error) {
	if err := p.checkKey(key); err != nil {
		return SourceRead{}, err
	}
	target = strings.TrimSpace(target)
	if target == "" {
		return SourceRead{}, fmt.Errorf("%w: empty linear source", boardsync.ErrBadRequest)
	}
	limit := opts.LimitFor(DefaultReadLimit, MaxReadLimit)
	switch {
	case strings.HasPrefix(target, "project:"):
		ref := strings.TrimSpace(strings.TrimPrefix(target, "project:"))
		if ref == "" {
			return SourceRead{}, fmt.Errorf("%w: empty linear project", boardsync.ErrBadRequest)
		}
		bySlug := !issueIDPattern.MatchString(ref) && !strings.Contains(ref, "-")
		return p.readProject(ctx, key, ref, bySlug, opts, limit)
	case projectURLPattern.MatchString(target):
		ref := projectURLPattern.FindStringSubmatch(target)[1]
		return p.readProject(ctx, key, ref, !issueIDPattern.MatchString(ref), opts, limit)
	case issueURLPattern.MatchString(target):
		return p.readIssueIdentifier(ctx, key, issueURLPattern.FindStringSubmatch(target)[1], opts, limit)
	case teamKeyFromIdentifier.MatchString(strings.ToUpper(target)):
		return p.readIssueIdentifier(ctx, key, strings.ToUpper(target), opts, limit)
	case issueIDPattern.MatchString(target):
		return p.readIssue(ctx, key, target, opts, limit)
	case teamKeyPattern.MatchString(strings.ToUpper(target)):
		return p.readTeam(ctx, key, strings.ToUpper(target), opts, limit)
	default:
		return SourceRead{}, fmt.Errorf("%w: can't read %q from linear; name a team (ENG), a project link, or an issue (ENG-123)", boardsync.ErrBadRequest, target)
	}
}

func (p *Provider) readTeam(ctx context.Context, key boardsync.Key, teamKey string, opts ReadOptions, limit int) (SourceRead, error) {
	if !p.cfg.teamAllowed(teamKey) {
		return SourceRead{}, fmt.Errorf("%w: team %s is not in the allowlist", boardsync.ErrNotFound, teamKey)
	}
	teamInfo, err := p.client.TeamByKey(ctx, teamKey)
	if err != nil {
		return SourceRead{}, err
	}
	read, err := p.readIssues(ctx, key, map[string]any{"team": map[string]any{"key": map[string]any{"eq": teamKey}}}, opts, limit)
	if err != nil {
		return SourceRead{}, err
	}
	read.Kind = "team"
	read.Channel = teamKey
	read.Name = teamInfo.Name
	return read, nil
}

func (p *Provider) readProject(ctx context.Context, key boardsync.Key, ref string, bySlug bool, opts ReadOptions, limit int) (SourceRead, error) {
	ref = strings.TrimSpace(ref)
	var (
		project projectInfo
		err     error
	)
	if bySlug {
		project, err = p.client.ProjectBySlug(ctx, ref)
	} else {
		project, err = p.client.ProjectByID(ctx, ref)
	}
	if err != nil {
		return SourceRead{}, err
	}
	if !p.projectAllowed(project) {
		return SourceRead{}, fmt.Errorf("%w: linear project %s isn't in the allowlist", boardsync.ErrNotFound, project.ID)
	}
	read, err := p.readIssues(ctx, key, map[string]any{"project": map[string]any{"id": map[string]any{"eq": project.ID}}}, opts, limit)
	if err != nil {
		return SourceRead{}, err
	}
	read.Kind = "project"
	read.Channel = project.ID
	read.Name = project.Name
	return read, nil
}

func (p *Provider) readIssues(ctx context.Context, key boardsync.Key, filter map[string]any, opts ReadOptions, limit int) (SourceRead, error) {
	page, err := p.client.Issues(ctx, filter, limit, opts.After)
	if err != nil {
		return SourceRead{}, err
	}
	messages := make([]SourceMessage, 0, len(page.Nodes))
	for i := range page.Nodes {
		issue := &page.Nodes[i]
		teamKey := p.issueTeam(&issue.issueData)
		if !p.cfg.teamAllowed(teamKey) {
			continue
		}
		message := sourceIssueMessage(key, &issue.issueData, issueThread(teamKey, issue.ID), listedDescriptionBytes)
		message.Replies = len(issue.Comments.Nodes)
		messages = append(messages, message)
	}
	return SourceRead{Messages: messages, More: page.PageInfo.HasNextPage, Next: page.PageInfo.EndCursor}, nil
}

func (p *Provider) readIssueIdentifier(ctx context.Context, key boardsync.Key, identifier string, opts ReadOptions, limit int) (SourceRead, error) {
	identifier = strings.ToUpper(identifier)
	teamKey := teamOfIdentifier(identifier)
	if teamKey == "" {
		return SourceRead{}, fmt.Errorf("%w: not a linear issue identifier: %s", boardsync.ErrBadRequest, identifier)
	}
	if !p.cfg.teamAllowed(teamKey) {
		return SourceRead{}, fmt.Errorf("%w: team %s is not in the allowlist", boardsync.ErrNotFound, teamKey)
	}
	number, err := strconv.Atoi(identifier[strings.LastIndexByte(identifier, '-')+1:])
	if err != nil {
		return SourceRead{}, fmt.Errorf("%w: linear issue identifier %q", boardsync.ErrBadRequest, identifier)
	}
	issue, err := p.client.IssueByKey(ctx, teamKey, number)
	if err != nil {
		return SourceRead{}, err
	}
	if issue.ID == "" {
		return SourceRead{}, fmt.Errorf("%w: linear issue %s has no ID", boardsync.ErrNotFound, identifier)
	}
	if got := strings.ToUpper(issue.Team.Key); got != "" && got != teamKey {
		return SourceRead{}, fmt.Errorf("%w: linear issue %s is in %s, not %s", boardsync.ErrNotFound, identifier, got, teamKey)
	}
	return p.readIssue(ctx, key, issue.ID, opts, limit)
}

func (p *Provider) readIssue(ctx context.Context, key boardsync.Key, issueID string, opts ReadOptions, limit int) (SourceRead, error) {
	issue, err := p.client.IssueWithComments(ctx, issueID, limit, opts.After)
	if err != nil {
		return SourceRead{}, err
	}
	teamKey := p.issueTeam(&issue.issueData)
	if !p.cfg.teamAllowed(teamKey) {
		return SourceRead{}, fmt.Errorf("%w: team %s is not in the allowlist", boardsync.ErrNotFound, teamKey)
	}
	thread := issueThread(teamKey, issue.ID)
	read := SourceRead{Kind: "issue", Channel: teamKey, Thread: thread}
	if opts.After == "" {
		root := sourceIssueMessage(key, &issue.issueData, thread, 0)
		root.Replies = len(issue.Comments.Nodes)
		read.Messages = append(read.Messages, root)
	}
	for _, comment := range issue.Comments.Nodes {
		read.Messages = append(read.Messages, SourceMessage{
			ID: comment.ID, Thread: thread, Author: handleOf(comment.User),
			AuthorID: sourceAuthorID(key, comment.User),
			Text:     clampText(comment.Body, MaxCommentChars), CreatedAt: comment.CreatedAt,
		})
	}
	read.More = issue.Comments.PageInfo.HasNextPage
	read.Next = issue.Comments.PageInfo.EndCursor
	return read, nil
}

// Permalink returns Linear's own link to an event's comment or issue.
func (p *Provider) Permalink(ctx context.Context, key boardsync.Key, event boardsync.Event) (string, error) {
	if err := p.checkKey(key); err != nil {
		return "", err
	}
	teamKey, issueID, err := parseIssueThread(event.Thread)
	if err != nil {
		return "", err
	}
	if !p.cfg.teamAllowed(teamKey) {
		return "", fmt.Errorf("%w: team %s is not in the allowlist", boardsync.ErrNotFound, teamKey)
	}
	if event.Message != "" && event.Kind != boardsync.KindLifecycle {
		link, err := p.client.CommentURL(ctx, event.Message)
		if err == nil && link != "" {
			return link, nil
		}
		// A deleted comment has no link; fall back to the issue's.
		if err != nil && (isAuthError(err) || isRateLimit(err)) {
			return "", err
		}
	}
	issue, err := p.client.IssueByID(ctx, issueID)
	if err != nil {
		return "", err
	}
	if issueTeam := p.issueTeam(&issue); issueTeam == "" || issueTeam != teamKey {
		return "", fmt.Errorf("%w: linear issue %s is in %q, not %s", boardsync.ErrNotFound, issueID, issueTeam, teamKey)
	}
	if issue.URL == "" {
		return "", fmt.Errorf("%w: linear issue %s has no link", boardsync.ErrNotFound, issueID)
	}
	return issue.URL, nil
}

func (p *Provider) checkKey(key boardsync.Key) error {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return fmt.Errorf("%w: entry %q isn't this linear app's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	return nil
}

func (p *Provider) issueTeam(issue *issueData) string {
	if key := strings.ToUpper(issue.Team.Key); key != "" {
		return key
	}
	return teamOfIdentifier(issue.Identifier)
}

func (p *Provider) projectAllowed(project projectInfo) bool {
	if p.cfg.AllowAllTeams {
		return true
	}
	if len(project.Teams.Nodes) == 0 {
		return false
	}
	for _, team := range project.Teams.Nodes {
		if !p.cfg.teamAllowed(strings.ToUpper(team.Key)) {
			return false
		}
	}
	return true
}

// sourceAuthorID is the connection-prefixed identity the board would store
// for a Linear user, matching sync.KeyedID's "key:user:<id>" shape.
func sourceAuthorID(key boardsync.Key, u *user) string {
	if u == nil || u.ID == "" {
		return ""
	}
	return boardsync.KeyedID(key, "linear", "linear:user:"+u.ID)
}

func sourceIssueMessage(key boardsync.Key, issue *issueData, thread string, descriptionMax int) SourceMessage {
	properties := make([]string, 0, 5)
	if issue.State.Name != "" {
		properties = append(properties, issue.State.Name)
	} else if issue.State.Type != "" {
		properties = append(properties, issue.State.Type)
	}
	if issue.Priority != 0 && issue.PriorityLabel != "" {
		properties = append(properties, issue.PriorityLabel)
	}
	if issue.Assignee != nil {
		if handle := handleOf(issue.Assignee); handle != "" {
			properties = append(properties, "assigned to "+handle)
		}
	} else {
		properties = append(properties, "unassigned")
	}
	if issue.Project != nil && issue.Project.Name != "" {
		properties = append(properties, issue.Project.Name)
	}
	for _, label := range issue.Labels.Nodes {
		if label.Name != "" {
			properties = append(properties, label.Name)
		}
	}
	head := issue.Identifier + ": " + issue.Title
	if len(properties) > 0 {
		head += "\n" + strings.Join(properties, " · ")
	}
	if issue.URL != "" {
		head += "\n" + issue.URL
	}
	description := strings.TrimSpace(issue.Description)
	if description != "" {
		description = clampText(description, MaxTextChars)
		if descriptionMax > 0 {
			description = truncateRunes(description, descriptionMax)
		}
		head += "\n\n" + description
	}
	return SourceMessage{
		ID: issue.Identifier, Thread: thread, Author: handleOf(issue.Creator),
		AuthorID: sourceAuthorID(key, issue.Creator),
		Text:     head, CreatedAt: issue.CreatedAt,
	}
}

func truncateRunes(text string, max int) string {
	if max <= 0 || utf8.RuneCountInString(text) <= max {
		return text
	}
	runes := []rune(text)
	return string(runes[:max]) + "…"
}

func isAuthError(err error) bool {
	return errors.Is(err, boardsync.ErrUnauthorized)
}

func isRateLimit(err error) bool {
	return errors.Is(err, boardsync.ErrRateLimited)
}
