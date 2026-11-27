package github

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
)

// normalize turns one GitHub webhook payload into normalized records. It does
// not check the signature or the delivery ID; Receive does both first. A repo
// outside the allowlist, an installation that isn't this entry's, or an event
// type this adapter doesn't speak produces no records: fail closed, never a
// half-imported event.
func (p *Provider) normalize(ctx context.Context, key boardsync.Key, eventName, delivery string, body []byte, pl payload) ([]boardsync.Record, error) {
	repo := pl.Repository.FullName
	if repo == "" || !validRepo(repo) {
		return nil, nil
	}
	if !p.cfg.readAllowed(repo) {
		return nil, nil
	}
	if p.cfg.InstallationID != 0 {
		if pl.Installation == nil || pl.Installation.ID != p.cfg.InstallationID {
			return nil, nil
		}
	}
	if p.cfg.DetectViewer {
		// Resolving the viewer is an optimisation, never a reason to lose an
		// event: the marker and a configured ID remain the echo guards.
		_, _ = p.Viewer(ctx)
	}
	switch eventName {
	case "ping":
		return nil, nil
	case "issue_comment", "pull_request_review_comment":
		return p.normalizeComment(key, delivery, eventName, body, pl)
	case "pull_request":
		return p.normalizePullRequest(key, delivery, body, pl)
	case "issues":
		return p.normalizeIssue(key, delivery, body, pl)
	case "pull_request_review":
		return p.normalizeReview(key, delivery, body, pl)
	case "workflow_run":
		return p.normalizeWorkflowRun(key, delivery, body, pl)
	default:
		return nil, nil
	}
}

func (p *Provider) normalizeComment(key boardsync.Key, delivery, eventName string, body []byte, pl payload) ([]boardsync.Record, error) {
	if pl.Comment == nil {
		return nil, fmt.Errorf("%w: github comment event has no comment", boardsync.ErrBadRequest)
	}
	comment := pl.Comment
	info := threadInfo{}
	switch eventName {
	case "pull_request_review_comment":
		if pl.PullRequest == nil {
			return nil, fmt.Errorf("%w: review comment event has no pull request", boardsync.ErrBadRequest)
		}
		parsed, err := parseThread(fmt.Sprintf("%s:%d", pl.Repository.FullName, pl.PullRequest.Number))
		if err != nil {
			return nil, err
		}
		info = parsed
		reviewID := comment.InReplyTo
		if reviewID == 0 {
			reviewID = comment.ID
		}
		info.ReviewID = reviewID
	case "issue_comment":
		if pl.Issue == nil {
			return nil, fmt.Errorf("%w: issue comment event has no issue", boardsync.ErrBadRequest)
		}
		isPull := pl.Issue.PullRequest != nil
		parsed, err := parseThread(fmt.Sprintf("%s:%d", pl.Repository.FullName, pl.Issue.Number))
		if err != nil {
			return nil, err
		}
		parsed.Issue = !isPull
		info = parsed
	default:
		return nil, nil
	}
	event := boardsync.Event{
		Key: key, Platform: "github",
		Channel: info.channel(), Thread: info.thread(), Subthread: info.subthread(),
		Message: strconv.FormatInt(comment.ID, 10),
		Author:  authorFor(comment.User, pl.Sender),
		Raw:     append([]byte(nil), body...),
	}
	if comment.Path != "" {
		line := comment.Line
		if line == 0 {
			line = comment.Original
		}
		if line > 0 {
			event.Context = sanitizeContext(fmt.Sprintf("%s:%d", comment.Path, line))
		} else {
			event.Context = sanitizeContext(comment.Path)
		}
	}
	text := boardsync.StripMarker(comment.Body)
	if text == "" {
		text = "(no text)"
	}
	event.Text = boardsync.StorableText(text, boardsync.MaxTextBytes)
	if p.isOwnComment(key, comment) {
		return nil, nil
	}
	lane := event.Thread
	if event.Subthread != "" {
		lane = event.Subthread
	}
	switch pl.Action {
	case "created":
		event.Kind = boardsync.KindMessage
		event.Type = boardsync.TypeComment
		event.ID = boardsync.MessageDedupe(key, lane, event.Message)
	case "edited":
		event.Kind = boardsync.KindEdited
		event.Type = boardsync.TypeMessageEdited
		at := parseTime(comment.UpdatedAt)
		event.ID = boardsync.EditedDedupe(key, lane, event.Message, event.Text, at)
	case "deleted":
		event.Kind = boardsync.KindDeleted
		event.Type = boardsync.TypeMessageDeleted
		event.Author = boardsync.Author{}
		event.ID = boardsync.DeletedDedupe(key, lane, event.Message)
	default:
		return nil, nil
	}
	if err := event.Validate(); err != nil {
		return nil, err
	}
	return []boardsync.Record{{Dedupe: event.ID, Event: event, Raw: body}}, nil
}

func (p *Provider) normalizePullRequest(key boardsync.Key, delivery string, body []byte, pl payload) ([]boardsync.Record, error) {
	pr := pl.PullRequest
	if pr == nil {
		return nil, fmt.Errorf("%w: pull_request event has no pull request", boardsync.ErrBadRequest)
	}
	if pr.Number <= 0 {
		return nil, fmt.Errorf("%w: pull_request event has no number", boardsync.ErrBadRequest)
	}
	repo := pl.Repository.FullName
	labels := prLabels(pr, repo)
	lifecycle := boardsync.Lifecycle{
		Type:   "pr." + pl.Action,
		Title:  storableTitle("PR #%d: %s", pr.Number, pr.Title),
		URL:    pr.HTMLURL,
		Body:   boardsync.StorableText(pr.Body, boardsync.MaxTextBytes),
		Labels: labels,
		Owns:   ownedBy(pr.Labels, pr.Assignees),
	}
	login := pl.Sender.Login
	switch pl.Action {
	case "opened":
		lifecycle.CreateRoot = true
	case "edited":
		lifecycle.Retitled = true
	case "ready_for_review":
		lifecycle.Type = "pr.ready"
		lifecycle.CreateRoot = true
	case "converted_to_draft":
		lifecycle.Type = "pr.draft"
		lifecycle.CreateRoot = true
	case "reopened":
		lifecycle.CreateRoot = true
		lifecycle.Reply = "reopened by " + loginOrSomeone(login)
	case "synchronize":
		lifecycle.Type = "pr.synchronized"
		if prefix := ciPrefix(pl.Repository.ID, pr.Number); prefix != "" {
			lifecycle.Owns = append(lifecycle.Owns, prefix)
		}
		delete(lifecycle.Labels, "head")
		if validSHA(pr.Head.SHA) {
			lifecycle.Labels["head"] = pr.Head.SHA
		} else {
			// Without an explicit unset, a host would keep the old head label.
			lifecycle.Unset = []string{"head"}
		}
	case "closed":
		if pr.Merged {
			lifecycle.Reply = "merged by " + loginOrSomeone(login)
		} else {
			lifecycle.Reply = "closed by " + loginOrSomeone(login)
		}
	case "labeled", "unlabeled", "assigned", "unassigned":
	default:
		return nil, nil
	}
	return []boardsync.Record{lifecycleRecord(key, delivery, body, lifecycle, authorOf(pl.Sender), repo, pr.Number)}, nil
}

func (p *Provider) normalizeIssue(key boardsync.Key, delivery string, body []byte, pl payload) ([]boardsync.Record, error) {
	item := pl.Issue
	if item == nil {
		return nil, fmt.Errorf("%w: issues event has no issue", boardsync.ErrBadRequest)
	}
	if item.Number <= 0 {
		return nil, fmt.Errorf("%w: issues event has no number", boardsync.ErrBadRequest)
	}
	repo := pl.Repository.FullName
	// An `issues` event can carry a pull request's issue view. Lifecycle
	// actions (open/close/reopen) also arrive through `pull_request`, which
	// alone knows `merged`; importing both would race two states and duplicate
	// replies. Keep the issue view for what it uniquely carries here: GitHub
	// label and assignee reconciliation on the PR thread.
	if item.PullRequest != nil {
		switch pl.Action {
		case "labeled", "unlabeled", "assigned", "unassigned":
		default:
			return nil, nil
		}
		labels := tagLabels(item.Labels, item.Assignees)
		labels["pr"] = fmt.Sprintf("%s#%d", repo, item.Number)
		lifecycle := boardsync.Lifecycle{
			Type:   "pr." + pl.Action,
			Title:  storableTitle("PR #%d: %s", item.Number, item.Title),
			URL:    item.HTMLURL,
			Labels: labels,
			Owns:   ownedBy(item.Labels, item.Assignees),
		}
		return []boardsync.Record{lifecycleRecord(key, delivery, body, lifecycle, authorOf(pl.Sender), repo, item.Number)}, nil
	}
	labels := issueLabels(item, repo)
	lifecycle := boardsync.Lifecycle{
		Type:   "issue." + pl.Action,
		Title:  storableTitle("Issue #%d: %s", item.Number, item.Title),
		URL:    item.HTMLURL,
		Body:   boardsync.StorableText(item.Body, boardsync.MaxTextBytes),
		Labels: labels,
		Owns:   ownedBy(item.Labels, item.Assignees),
	}
	switch pl.Action {
	case "opened":
		lifecycle.Labels["state"] = "open"
		lifecycle.CreateRoot = true
	case "edited":
		lifecycle.Retitled = true
	case "reopened":
		lifecycle.Labels["state"] = "open"
		lifecycle.CreateRoot = true
		lifecycle.Reply = "reopened by " + loginOrSomeone(pl.Sender.Login)
	case "closed":
		lifecycle.Labels["state"] = "closed"
		lifecycle.Reply = "closed by " + loginOrSomeone(pl.Sender.Login)
	case "labeled", "unlabeled", "assigned", "unassigned":
	default:
		return nil, nil
	}
	return []boardsync.Record{lifecycleRecord(key, delivery, body, lifecycle, authorOf(pl.Sender), repo, item.Number)}, nil
}

func (p *Provider) normalizeReview(key boardsync.Key, delivery string, body []byte, pl payload) ([]boardsync.Record, error) {
	if pl.PullRequest == nil || pl.Review == nil {
		return nil, fmt.Errorf("%w: review event has no pull request or review", boardsync.ErrBadRequest)
	}
	repo := pl.Repository.FullName
	pr := pl.PullRequest
	if pr.Number <= 0 {
		return nil, fmt.Errorf("%w: review event has no pull request number", boardsync.ErrBadRequest)
	}
	lifecycle := boardsync.Lifecycle{
		Type:   "review.submitted",
		Title:  storableTitle("PR #%d: %s", pr.Number, pr.Title),
		URL:    pr.HTMLURL,
		Labels: map[string]string{},
	}
	switch pl.Action {
	case "dismissed":
		lifecycle.Type = "review.dismissed"
		lifecycle.Unset = []string{"review"}
	case "submitted":
		switch pl.Review.State {
		case "approved":
			lifecycle.Labels["review"] = "approved"
			lifecycle.Reply = "approved by " + loginOrSomeone(pl.Review.User.Login)
		case "changes_requested":
			lifecycle.Labels["review"] = "changes_requested"
			lifecycle.Reply = "changes requested by " + loginOrSomeone(pl.Review.User.Login)
		default:
			// A plain comment review doesn't change the review label.
			return nil, nil
		}
		if pl.Review.Body != "" {
			lifecycle.Reply += "\n\n" + pl.Review.Body
		}
		lifecycle.Reply = boardsync.StorableText(lifecycle.Reply, boardsync.MaxTextBytes)
	default:
		return nil, nil
	}
	return []boardsync.Record{lifecycleRecord(key, delivery, body, lifecycle, authorFor(pl.Review.User, pl.Sender), repo, pr.Number)}, nil
}

func (p *Provider) normalizeWorkflowRun(key boardsync.Key, delivery string, body []byte, pl payload) ([]boardsync.Record, error) {
	run := pl.WorkflowRun
	if run == nil || pl.Action != "completed" || run.Conclusion == "" {
		return nil, nil
	}
	repo := pl.Repository.FullName
	if pl.Repository.ID <= 0 {
		// CI labels are owned per repository number; without one, two repos
		// could share an "owned" prefix and unset each other's labels.
		return nil, nil
	}
	conclusion := strings.ToLower(run.Conclusion)
	failed := conclusion == "failure" || conclusion == "timed_out" || conclusion == "startup_failure"
	if !failed && conclusion != "success" {
		return nil, nil
	}
	name := run.Path
	if name == "" {
		name = run.Name
	}
	if index := strings.LastIndex(name, "/"); index >= 0 {
		name = name[index+1:]
	}
	name = strings.TrimSuffix(strings.TrimSuffix(name, ".yml"), ".yaml")
	fileSlug := slug(name)
	if fileSlug == "" {
		return nil, nil
	}
	var records []boardsync.Record
	for _, pr := range run.PullRequests {
		if pr.Number <= 0 {
			continue
		}
		if pr.Head.SHA != "" && run.HeadSHA != "" && pr.Head.SHA != run.HeadSHA {
			continue
		}
		prefix := ciPrefix(pl.Repository.ID, pr.Number)
		if prefix == "" {
			continue
		}
		keyName := boundedKey(prefix, fileSlug)
		labels := map[string]string{}
		lifecycle := boardsync.Lifecycle{
			Type:  "ci.passed",
			Title: storableTitle("PR #%d", pr.Number),
			URL:   run.HTMLURL,
			// Own no prefix: one workflow's completion must never unset
			// another workflow's label. pr.synchronize owns the prefix and
			// clears the whole CI set when the head changes.
		}
		if failed {
			lifecycle.Type = "ci.failed"
			labels[keyName] = conclusion
			lifecycle.Reply = boardsync.StorableText(fmt.Sprintf("%s %s on %s#%d at %s%s",
				run.Name, conclusion, repoDisplay(repo, pl.Repository.Owner.Login), pr.Number,
				shortSHA(run.HeadSHA), runLink(run.HTMLURL)), boardsync.MaxTextBytes)
		} else {
			lifecycle.Unset = []string{keyName}
		}
		lifecycle.Labels = labels
		author := boardsync.Author{ID: "github:user:41898282", Handle: "github-actions[bot]", Name: "github-actions[bot]", Bot: true}
		records = append(records, lifecycleRecord(key, delivery, body, lifecycle, author, repo, pr.Number))
	}
	return records, nil
}

func lifecycleRecord(key boardsync.Key, delivery string, body []byte, lifecycle boardsync.Lifecycle, author boardsync.Author, repo string, number int) boardsync.Record {
	thread := fmt.Sprintf("%s:%d", repo, number)
	channel := repo + ":pulls"
	if strings.HasPrefix(lifecycle.Type, "issue.") {
		thread = fmt.Sprintf("%s:issue:%d", repo, number)
		channel = repo + ":issues"
	}
	event := boardsync.Event{
		Key: key, Platform: "github",
		Kind: boardsync.KindLifecycle, Type: lifecycle.Type,
		Channel: channel, Thread: thread,
		Author: author, Lifecycle: &lifecycle,
		Raw: append([]byte(nil), body...),
	}
	event.ID = boardsync.LifecycleDedupe(key, delivery, lifecycle.Type, fmt.Sprintf("%s#%d", repo, number))
	return boardsync.Record{Dedupe: event.ID, Event: event, Raw: body}
}

func (p *Provider) isOwnComment(key boardsync.Key, comment *comment) bool {
	if p.cfg.BotUserID != 0 && comment.User.ID == p.cfg.BotUserID {
		return true
	}
	if p.cfg.BotLogin != "" && strings.EqualFold(comment.User.Login, p.cfg.BotLogin) {
		return true
	}
	if id, login, known := p.viewer(); known {
		if id != 0 && comment.User.ID == id {
			return true
		}
		if login != "" && strings.EqualFold(comment.User.Login, login) {
			return true
		}
	}
	if !p.cfg.DisableMarker {
		// A marker for this entry is fail-closed: a copied marker loses one
		// genuine comment, an echoed one would loop forever. Scan every
		// marker: a foreign marker before ours must not hide ours.
		if boardsync.HasOwnMarker(comment.Body, key) {
			return true
		}
	}
	return false
}

func loginOrSomeone(login string) string {
	if login == "" {
		return "someone"
	}
	return login
}

// storableTitle bounds a lifecycle title. GitHub titles are short; the bound is
// defensive, not an expected truncation.
func storableTitle(format string, args ...any) string {
	return boardsync.StorableText(fmt.Sprintf(format, args...), 1024)
}

// sanitizeContext names a review comment's code location without letting a
// payload smuggle newlines or control characters into a board field.
func sanitizeContext(value string) string {
	value = boardsync.StorableText(value, 300)
	value = strings.Map(func(r rune) rune {
		switch {
		case r == '\n' || r == '\r' || r == '\t':
			return ' '
		case r < 0x20 || r == 0x7f:
			return -1
		default:
			return r
		}
	}, value)
	return strings.TrimSpace(value)
}

// repoDisplay is "owner/repo" without the owner, for a short reply line.
func repoDisplay(repo, owner string) string {
	if owner != "" {
		if trimmed := strings.TrimPrefix(repo, owner+"/"); trimmed != repo {
			return trimmed
		}
	}
	return repo
}

func parseTime(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}
	}
	return parsed
}

func shortSHA(sha string) string {
	if len(sha) > 7 {
		return sha[:7]
	}
	return sha
}

func runLink(url string) string {
	if url == "" {
		return ""
	}
	return ": " + url
}
