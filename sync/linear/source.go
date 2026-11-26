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

// ReadOptions page a source read. Limit defaults to 50 and is capped at 100;
// After is an opaque Linear cursor from a previous read's Next.
type ReadOptions struct {
	Limit int
	After string
}

const (
	// DefaultReadLimit is how many issues or comments one read returns.
	DefaultReadLimit = 50
	// MaxReadLimit is Linear's practical page cap for these reads.
	MaxReadLimit = 100
	// listedDescriptionBytes bounds an issue description shown in a list.
	listedDescriptionBytes = 1500
)

// SourceFile is a file a source message carries. Linear comments don't carry
// files through the webhook or this read; the field keeps the shape other
// providers share.
type SourceFile struct {
	Name string `json:"name"`
	URL  string `json:"url,omitempty"`
}

// SourceMessage is one issue or comment read on demand. Nothing about it is
// stored on the board.
type SourceMessage struct {
	ID        string       `json:"id"`
	Thread    string       `json:"thread"`
	Author    string       `json:"author"`
	Text      string       `json:"text"`
	CreatedAt string       `json:"createdAt,omitempty"`
	Replies   int          `json:"replies,omitempty"`
	Files     []SourceFile `json:"files,omitempty"`
}

// SourceRead is a team, a project, or one issue with its comments, read for
// whoever asked. Kind is "team", "project", or "issue".
type SourceRead struct {
	Kind     string          `json:"kind"`
	Channel  string          `json:"channel"`
	Name     string          `json:"name,omitempty"`
	Thread   string          `json:"thread,omitempty"`
	Messages []SourceMessage `json:"messages"`
	More     bool            `json:"more"`
	Next     string          `json:"next,omitempty"`
}

var (
	issueURLPattern   = regexp.MustCompile(`(?i)^https://linear\.app/[^/\s]+/issue/([A-Z][A-Z0-9_]*-\d+)(?:[/?#].*)?$`)
	projectURLPattern = regexp.MustCompile(`(?i)^https://linear\.app/[^/\s]+/project/(?:[^/?#\s]*-)?([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|[0-9a-f]{8,})(?:[/?#].*)?$`)
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
	limit := readLimit(opts.Limit)
	switch {
	case strings.HasPrefix(target, "project:"):
		ref := strings.TrimSpace(strings.TrimPrefix(target, "project:"))
		if ref == "" {
			return SourceRead{}, fmt.Errorf("%w: empty linear project", boardsync.ErrBadRequest)
		}
		bySlug := !issueIDPattern.MatchString(ref) && !strings.Contains(ref, "-")
		return p.readProject(ctx, ref, bySlug, opts, limit)
	case projectURLPattern.MatchString(target):
		return p.readProject(ctx, projectURLPattern.FindStringSubmatch(target)[1], true, opts, limit)
	case issueURLPattern.MatchString(target):
		return p.readIssueIdentifier(ctx, issueURLPattern.FindStringSubmatch(target)[1], opts, limit)
	case teamKeyFromIdentifier.MatchString(strings.ToUpper(target)):
		return p.readIssueIdentifier(ctx, strings.ToUpper(target), opts, limit)
	case issueIDPattern.MatchString(target):
		return p.readIssue(ctx, target, opts, limit)
	case teamKeyPattern.MatchString(strings.ToUpper(target)):
		return p.readTeam(ctx, strings.ToUpper(target), opts, limit)
	default:
		return SourceRead{}, fmt.Errorf("%w: can't read %q from linear; name a team (ENG), a project link, or an issue (ENG-123)", boardsync.ErrBadRequest, target)
	}
}

func (p *Provider) readTeam(ctx context.Context, teamKey string, opts ReadOptions, limit int) (SourceRead, error) {
	if !p.cfg.teamAllowed(teamKey) {
		return SourceRead{}, fmt.Errorf("%w: team %s is not in the allowlist", boardsync.ErrNotFound, teamKey)
	}
	teamInfo, err := p.client.TeamByKey(ctx, teamKey)
	if err != nil {
		return SourceRead{}, err
	}
	read, err := p.readIssues(ctx, map[string]any{"team": map[string]any{"key": map[string]any{"eq": teamKey}}}, opts, limit)
	if err != nil {
		return SourceRead{}, err
	}
	read.Kind = "team"
	read.Channel = teamKey
	read.Name = teamInfo.Name
	return read, nil
}

func (p *Provider) readProject(ctx context.Context, ref string, bySlug bool, opts ReadOptions, limit int) (SourceRead, error) {
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
	read, err := p.readIssues(ctx, map[string]any{"project": map[string]any{"id": map[string]any{"eq": project.ID}}}, opts, limit)
	if err != nil {
		return SourceRead{}, err
	}
	read.Kind = "project"
	read.Channel = project.ID
	read.Name = project.Name
	return read, nil
}

func (p *Provider) readIssues(ctx context.Context, filter map[string]any, opts ReadOptions, limit int) (SourceRead, error) {
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
		message := sourceIssueMessage(&issue.issueData, issueThread(teamKey, issue.ID), listedDescriptionBytes)
		message.Replies = len(issue.Comments.Nodes)
		messages = append(messages, message)
	}
	return SourceRead{Messages: messages, More: page.PageInfo.HasNextPage, Next: page.PageInfo.EndCursor}, nil
}

func (p *Provider) readIssueIdentifier(ctx context.Context, identifier string, opts ReadOptions, limit int) (SourceRead, error) {
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
	return p.readIssue(ctx, issue.ID, opts, limit)
}

func (p *Provider) readIssue(ctx context.Context, issueID string, opts ReadOptions, limit int) (SourceRead, error) {
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
		root := sourceIssueMessage(&issue.issueData, thread, 0)
		root.Replies = len(issue.Comments.Nodes)
		read.Messages = append(read.Messages, root)
	}
	for _, comment := range issue.Comments.Nodes {
		read.Messages = append(read.Messages, SourceMessage{
			ID: comment.ID, Thread: thread, Author: handleOf(comment.User),
			Text: comment.Body, CreatedAt: comment.CreatedAt,
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

func readLimit(limit int) int {
	if limit <= 0 {
		return DefaultReadLimit
	}
	if limit > MaxReadLimit {
		return MaxReadLimit
	}
	return limit
}

func sourceIssueMessage(issue *issueData, thread string, descriptionMax int) SourceMessage {
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
		if descriptionMax > 0 {
			description = truncateRunes(description, descriptionMax)
		}
		head += "\n\n" + description
	}
	return SourceMessage{
		ID: issue.Identifier, Thread: thread, Author: handleOf(issue.Creator),
		Text: head, CreatedAt: issue.CreatedAt,
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
