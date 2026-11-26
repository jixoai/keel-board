package github

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
)

// normalize turns one GitHub webhook payload into normalized records. It does
// not check the signature or the allowlist; Receive does both first.
func (p *Provider) normalize(key boardsync.Key, eventName, delivery string, body []byte, pl payload) ([]boardsync.Record, error) {
	repo := pl.Repository.FullName
	if repo == "" {
		return nil, nil
	}
	if !p.cfg.readAllowed(repo) {
		return nil, nil
	}
	if delivery == "" {
		delivery = boardsync.Hash(string(body))
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
		Author:  authorOf(comment.User),
		Raw:     append([]byte(nil), body...),
	}
	if comment.Path != "" {
		line := comment.Line
		if line == 0 {
			line = comment.Original
		}
		if line > 0 {
			event.Context = fmt.Sprintf("%s:%d", comment.Path, line)
		} else {
			event.Context = comment.Path
		}
	}
	text := boardsync.StripMarker(comment.Body)
	if text == "" {
		text = "(no text)"
	}
	event.Text = boardsync.StorableText(text, boardsync.MaxTextBytes)
	if p.isOwnComment(comment) {
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
	repo := pl.Repository.FullName
	labels := prLabels(pr, repo)
	lifecycle := boardsync.Lifecycle{
		Type:   "pr." + pl.Action,
		Title:  fmt.Sprintf("PR #%d: %s", pr.Number, pr.Title),
		URL:    pr.HTMLURL,
		Body:   pr.Body,
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
		lifecycle.Owns = append(lifecycle.Owns, ciPrefix(pl.Repository.ID, pr.Number))
		delete(lifecycle.Labels, "head")
		if pr.Head.SHA != "" {
			lifecycle.Labels["head"] = pr.Head.SHA
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
	repo := pl.Repository.FullName
	// An `issues` event can carry a pull request's issue view. Route it to the
	// PR thread so labels and state land with the conversation.
	if item.PullRequest != nil {
		pr := &pullRequest{
			Number: item.Number, Title: item.Title, Body: item.Body,
			HTMLURL: item.HTMLURL, State: item.State,
			User: item.User, Labels: item.Labels, Assignees: item.Assignees,
		}
		labels := prLabels(pr, repo)
		lifecycle := boardsync.Lifecycle{
			Type: "pr." + pl.Action, Title: fmt.Sprintf("PR #%d: %s", item.Number, item.Title),
			URL: item.HTMLURL, Body: item.Body, Labels: labels,
			Owns: ownedBy(item.Labels, item.Assignees),
		}
		switch pl.Action {
		case "opened":
			lifecycle.CreateRoot = true
		case "edited":
			lifecycle.Retitled = true
		case "reopened":
			lifecycle.CreateRoot = true
			lifecycle.Reply = "reopened by " + loginOrSomeone(pl.Sender.Login)
		case "closed":
			lifecycle.Reply = "closed by " + loginOrSomeone(pl.Sender.Login)
		case "labeled", "unlabeled", "assigned", "unassigned":
		default:
			return nil, nil
		}
		return []boardsync.Record{lifecycleRecord(key, delivery, body, lifecycle, authorOf(pl.Sender), repo, item.Number)}, nil
	}
	labels := issueLabels(item, repo)
	lifecycle := boardsync.Lifecycle{
		Type: "issue." + pl.Action, Title: fmt.Sprintf("Issue #%d: %s", item.Number, item.Title),
		URL: item.HTMLURL, Body: item.Body, Labels: labels,
		Owns: ownedBy(item.Labels, item.Assignees),
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
	lifecycle := boardsync.Lifecycle{
		Type:   "review.submitted",
		Title:  fmt.Sprintf("PR #%d: %s", pr.Number, pr.Title),
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
	default:
		return nil, nil
	}
	return []boardsync.Record{lifecycleRecord(key, delivery, body, lifecycle, authorOf(pl.Review.User), repo, pr.Number)}, nil
}

func (p *Provider) normalizeWorkflowRun(key boardsync.Key, delivery string, body []byte, pl payload) ([]boardsync.Record, error) {
	run := pl.WorkflowRun
	if run == nil || pl.Action != "completed" || run.Conclusion == "" {
		return nil, nil
	}
	repo := pl.Repository.FullName
	conclusion := strings.ToLower(run.Conclusion)
	failed := conclusion == "failure" || conclusion == "timed_out" || conclusion == "startup_failure"
	if !failed && conclusion != "success" {
		return nil, nil
	}
	file := run.Path
	if index := strings.LastIndex(file, "/"); index >= 0 {
		file = file[index+1:]
	}
	file = strings.TrimSuffix(strings.TrimSuffix(file, ".yml"), ".yaml")
	fileSlug := slug(file)
	if fileSlug == "" {
		return nil, nil
	}
	var records []boardsync.Record
	for _, pr := range run.PullRequests {
		if pr.Head.SHA != "" && run.HeadSHA != "" && pr.Head.SHA != run.HeadSHA {
			continue
		}
		labels := map[string]string{}
		unset := []string{}
		keyName := (ciPrefix(pl.Repository.ID, pr.Number) + fileSlug)
		if len(keyName) > 64 {
			keyName = keyName[:64]
		}
		lifecycle := boardsync.Lifecycle{
			Type:  "ci.passed",
			Title: fmt.Sprintf("PR #%d", pr.Number),
			URL:   run.HTMLURL,
			Owns:  []string{ciPrefix(pl.Repository.ID, pr.Number)},
			Unset: unset,
		}
		if failed {
			lifecycle.Type = "ci.failed"
			labels[keyName] = conclusion
			lifecycle.Reply = fmt.Sprintf("%s %s on %s#%d at %s%s",
				run.Name, conclusion, strings.TrimPrefix(repo, pl.Repository.Owner.Login+"/"), pr.Number,
				shortSHA(run.HeadSHA), runLink(run.HTMLURL))
		} else {
			unset = append(unset, keyName)
			lifecycle.Unset = unset
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

func (p *Provider) isOwnComment(comment *comment) bool {
	if p.cfg.BotUserID != 0 && comment.User.ID == p.cfg.BotUserID {
		return true
	}
	if p.cfg.BotLogin != "" && strings.EqualFold(comment.User.Login, p.cfg.BotLogin) {
		return true
	}
	if !p.cfg.DisableMarker {
		if marker, ok := boardsync.ParseMarker(comment.Body); ok && string(marker.Key) == string(p.cfg.Key) && p.cfg.Key != "" {
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

// markerFor is the hidden comment marker for an outbound comment.
func markerFor(key boardsync.Key, msg boardsync.OutboundMessage, target boardsync.ResolvedTarget) boardsync.Marker {
	return boardsync.Marker{Key: key, Message: msg.BoardMessage, Thread: target.External()}
}
