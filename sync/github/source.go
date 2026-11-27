package github

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	boardsync "github.com/jixoai/keel-board/sync"
)

const (
	// maxSourceCommentsPage is GitHub's per_page cap for a comments read.
	maxSourceCommentsPage = 100
	// maxSourcePage bounds the page cursor so a silly value can't overflow.
	maxSourcePage = 1_000_000
)

var _ boardsync.SourceReader = (*Provider)(nil)

// SourceRead is the shared normalized read result.
type SourceRead = boardsync.SourceRead

// sourceRefPattern is the issue/PR shorthand: owner/repo#123. GitHub's issue
// object says whether the number is a PR, so the kind is resolved by one read
// rather than guessed from the number.
var sourceRefPattern = regexp.MustCompile(`^([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)#([0-9]+)$`)

// sourceRef is a parsed read target. unknownKind is set for owner/repo#123,
// where the number could be either an issue or a PR.
type sourceRef struct {
	info        threadInfo
	unknownKind bool
}

// ReadSource reads a PR or issue and a page of its conversation comments,
// normalized and not stored. A target can be the native thread
// ("acme/www:123", "acme/www:issue:45"), a PR/issue URL, or
// "acme/www#123"; only repos in the inbound allowlist are read. The app's
// installation scopes what GitHub actually returns.
func (p *Provider) ReadSource(ctx context.Context, key boardsync.Key, target string, opts boardsync.ReadOptions) (SourceRead, error) {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return SourceRead{}, fmt.Errorf("%w: entry %q isn't this github app's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	page, err := sourcePage(opts.After)
	if err != nil {
		return SourceRead{}, err
	}
	ref, err := p.sourceRef(target)
	if err != nil {
		return SourceRead{}, err
	}
	if !p.cfg.readAllowed(ref.info.Full) {
		// A repo outside the allowlist is indistinguishable from a missing one.
		return SourceRead{}, fmt.Errorf("%w: %s is not in the inbound repo allowlist", boardsync.ErrNotFound, ref.info.Full)
	}
	limit := opts.LimitFor(boardsync.DefaultSourceLimit, maxSourceCommentsPage)
	return p.readSource(ctx, key, ref, limit, page)
}

// sourcePage parses the opaque cursor. GitHub's is a 1-based comment page.
func sourcePage(after string) (int, error) {
	after = strings.TrimSpace(after)
	if after == "" {
		return 1, nil
	}
	page, err := strconv.Atoi(after)
	if err != nil || page < 1 || page > maxSourcePage {
		return 0, fmt.Errorf("%w: a github source cursor is a positive page number", boardsync.ErrBadRequest)
	}
	return page, nil
}

// sourceRef parses one target into a thread and whether its kind is unknown.
func (p *Provider) sourceRef(target string) (sourceRef, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		return sourceRef{}, fmt.Errorf("%w: empty github source", boardsync.ErrBadRequest)
	}
	if strings.HasPrefix(target, "https://github.com/") {
		thread, _, ok := p.Locate(target)
		if !ok {
			return sourceRef{}, fmt.Errorf("%w: not a github PR or issue link: %s", boardsync.ErrBadRequest, target)
		}
		info, err := parseThread(thread)
		if err != nil {
			return sourceRef{}, err
		}
		return sourceRef{info: info}, nil
	}
	if match := sourceRefPattern.FindStringSubmatch(target); match != nil {
		repo := match[1]
		if !validRepo(repo) {
			return sourceRef{}, fmt.Errorf("%w: not a github repo: %s", boardsync.ErrBadRequest, repo)
		}
		number, err := strconv.Atoi(match[2])
		if err != nil || number <= 0 {
			return sourceRef{}, fmt.Errorf("%w: github source number: %s", boardsync.ErrBadRequest, match[2])
		}
		owner, name, _ := strings.Cut(repo, "/")
		return sourceRef{
			info:        threadInfo{Owner: owner, Repo: name, Full: repo, Number: number},
			unknownKind: true,
		}, nil
	}
	info, err := parseThread(target)
	if err != nil {
		return sourceRef{}, err
	}
	return sourceRef{info: info}, nil
}

// readSource fetches the root and one comments page. Page 1 carries the root;
// a later page carries only comments, as Linear's issue read does.
func (p *Provider) readSource(ctx context.Context, key boardsync.Key, ref sourceRef, limit, page int) (SourceRead, error) {
	info := ref.info
	var (
		root  boardsync.SourceMessage
		title string
		kind  string
	)
	switch {
	case ref.unknownKind:
		item, err := p.client.fetchIssue(ctx, info.Full, info.Number)
		if err != nil {
			return SourceRead{}, err
		}
		if item.PullRequest != nil {
			pr, err := p.client.fetchPull(ctx, info.Full, info.Number)
			if err != nil {
				return SourceRead{}, err
			}
			info.Issue = false
			root, title = pullSourceRoot(key, info, &pr)
			kind = "pull_request"
		} else {
			info.Issue = true
			root, title = issueSourceRoot(key, info, &item)
			kind = "issue"
		}
	case info.Issue:
		item, err := p.client.fetchIssue(ctx, info.Full, info.Number)
		if err != nil {
			return SourceRead{}, err
		}
		root, title = issueSourceRoot(key, info, &item)
		kind = "issue"
	default:
		pr, err := p.client.fetchPull(ctx, info.Full, info.Number)
		if err != nil {
			return SourceRead{}, err
		}
		root, title = pullSourceRoot(key, info, &pr)
		kind = "pull_request"
	}
	comments, more, err := p.client.IssueComments(ctx, info.Full, info.Number, limit, page)
	if err != nil {
		return SourceRead{}, err
	}
	thread := info.thread()
	messages := make([]boardsync.SourceMessage, 0, len(comments)+1)
	if page == 1 {
		messages = append(messages, root)
	}
	for _, comment := range comments {
		messages = append(messages, commentSourceMessage(key, thread, comment))
	}
	read := SourceRead{
		Kind: kind, Channel: info.channel(), Name: title, Thread: thread,
		Messages: messages, More: more,
	}
	if more {
		read.Next = strconv.Itoa(page + 1)
	}
	return read, nil
}

// issueSourceRoot renders an issue as the normalized root a board would
// import.
func issueSourceRoot(key boardsync.Key, info threadInfo, item *issue) (boardsync.SourceMessage, string) {
	return boardsync.SourceMessage{
		ID:        sourceRootID(info),
		Thread:    info.thread(),
		Author:    sourceAuthor(item.User),
		AuthorID:  sourceAuthorID(key, item.User),
		Text:      issueSourceText(key, item),
		CreatedAt: item.CreatedAt,
		Replies:   item.Comments,
	}, item.Title
}

// pullSourceRoot renders a PR as the normalized root a board would import.
func pullSourceRoot(key boardsync.Key, info threadInfo, pr *pullRequest) (boardsync.SourceMessage, string) {
	return boardsync.SourceMessage{
		ID:        sourceRootID(info),
		Thread:    info.thread(),
		Author:    sourceAuthor(pr.User),
		AuthorID:  sourceAuthorID(key, pr.User),
		Text:      pullSourceText(key, pr),
		CreatedAt: pr.CreatedAt,
		Replies:   pr.Comments,
	}, pr.Title
}

// issueSourceText is the text an issue root carries, shared with FetchRoot.
func issueSourceText(key boardsync.Key, item *issue) string {
	return boardsync.StorableText(fmt.Sprintf("Issue #%d: %s\n%s\n\n%s",
		item.Number, item.Title, item.HTMLURL, boardMentions(item.Body, key)), boardsync.MaxTextBytes)
}

// pullSourceText is the text a PR root carries, shared with FetchRoot.
func pullSourceText(key boardsync.Key, pr *pullRequest) string {
	return boardsync.StorableText(fmt.Sprintf("PR #%d: %s\n%s\n\n%s",
		pr.Number, pr.Title, pr.HTMLURL, boardMentions(pr.Body, key)), boardsync.MaxTextBytes)
}

// sourceRootID names the PR or issue itself; comments use their numeric IDs.
func sourceRootID(info threadInfo) string {
	return fmt.Sprintf("%s#%d", info.Full, info.Number)
}

// commentSourceMessage normalizes one conversation comment.
func commentSourceMessage(key boardsync.Key, thread string, item comment) boardsync.SourceMessage {
	return boardsync.SourceMessage{
		ID:        strconv.FormatInt(item.ID, 10),
		Thread:    thread,
		Author:    sourceAuthor(item.User),
		AuthorID:  sourceAuthorID(key, item.User),
		Text:      boardsync.StorableText(boardMentions(item.Body, key), boardsync.MaxTextBytes),
		CreatedAt: item.CreatedAt,
	}
}

// sourceAuthor is the display name a read shows: the login when GitHub has
// one, else the fallback authorOf uses for a deleted account.
func sourceAuthor(u user) string {
	if u.ID == 0 && u.Login == "" {
		return ""
	}
	author := authorOf(u)
	if author.Handle != "" {
		return author.Handle
	}
	return author.Name
}

// sourceAuthorID is the connection-prefixed identity the board would store:
// key:user:<id> (or key:user:<login> when the numeric ID is absent).
func sourceAuthorID(key boardsync.Key, u user) string {
	if u.ID == 0 && u.Login == "" {
		return ""
	}
	author := authorOf(u)
	if author.ID == "" || author.ID == "github:user:" {
		return ""
	}
	return boardsync.KeyedID(key, "github", author.ID)
}
