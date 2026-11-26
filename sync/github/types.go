package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
)

// Config configures a GitHub App adapter.
type Config struct {
	Key           boardsync.Key
	WebhookSecret boardsync.Secret
	// Token authorizes REST calls (an installation token in practice).
	Token boardsync.Secret
	// BotUserID is the app's own user ID; comments by it are never imported.
	BotUserID int64
	// BotLogin is the app's login, used when no ID is known.
	BotLogin string
	// Repos is the inbound allowlist: only these "owner/repo" names are read.
	// It is required unless AllowAllRepos is set.
	Repos         []string
	AllowAllRepos bool
	// WriteRepos is the outbound allowlist. Empty means GitHub receives
	// nothing: a board never writes to a public PR unless explicitly allowed.
	WriteRepos     []string
	AllowAllWrites bool
	// DisableMarker turns off the hidden comment marker. Markers are the
	// second line of defence against echo; keep them on unless a platform's
	// rendering breaks.
	DisableMarker bool
	APIBaseURL    string
	HTTPClient    *http.Client
	Now           func() time.Time
	MaxBodyBytes  int64
}

// Validate checks configuration without revealing a secret.
func (c Config) Validate() error {
	if c.Key != "" && !c.Key.Valid() {
		return fmt.Errorf("%w: %q isn't a sync key", boardsync.ErrBadRequest, c.Key)
	}
	if c.WebhookSecret.Empty() {
		return fmt.Errorf("%w: github webhook secret is unset", boardsync.ErrUnauthorized)
	}
	if !c.AllowAllRepos && len(c.Repos) == 0 {
		return fmt.Errorf("%w: github needs an inbound repo allowlist (or AllowAllRepos)", boardsync.ErrBadRequest)
	}
	for _, repo := range append(append([]string(nil), c.Repos...), c.WriteRepos...) {
		if !validRepo(repo) {
			return fmt.Errorf("%w: %q isn't an owner/repo", boardsync.ErrBadRequest, repo)
		}
	}
	return nil
}

func validRepo(full string) bool {
	parts := strings.Split(full, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return false
	}
	for _, part := range parts {
		for _, c := range part {
			if !(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') && !(c >= '0' && c <= '9') && c != '-' && c != '_' && c != '.' {
				return false
			}
		}
	}
	return true
}

func (c Config) baseURL() string {
	if c.APIBaseURL != "" {
		return strings.TrimRight(c.APIBaseURL, "/")
	}
	return "https://api.github.com"
}

func (c Config) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

func (c Config) maxBody() int64 {
	if c.MaxBodyBytes > 0 {
		return c.MaxBodyBytes
	}
	return 2 << 20
}

func (c Config) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return &http.Client{Timeout: 15 * time.Second}
}

func (c Config) readAllowed(repo string) bool {
	if c.AllowAllRepos {
		return true
	}
	for _, allowed := range c.Repos {
		if strings.EqualFold(allowed, repo) {
			return true
		}
	}
	return false
}

func (c Config) writeAllowed(repo string) bool {
	if c.AllowAllWrites {
		return true
	}
	for _, allowed := range c.WriteRepos {
		if strings.EqualFold(allowed, repo) {
			return true
		}
	}
	return false
}

// user is a GitHub account.
type user struct {
	Login     string `json:"login"`
	ID        int64  `json:"id"`
	Type      string `json:"type"`
	AvatarURL string `json:"avatar_url"`
}

type label struct {
	Name string `json:"name"`
}

type repository struct {
	ID         int64  `json:"id"`
	FullName   string `json:"full_name"`
	Name       string `json:"name"`
	Private    bool   `json:"private"`
	Visibility string `json:"visibility"`
	Owner      user   `json:"owner"`
}

type issue struct {
	Number      int       `json:"number"`
	Title       string    `json:"title"`
	Body        string    `json:"body"`
	HTMLURL     string    `json:"html_url"`
	State       string    `json:"state"`
	User        user      `json:"user"`
	Labels      []label   `json:"labels"`
	Assignees   []user    `json:"assignees"`
	PullRequest *struct{} `json:"pull_request,omitempty"`
	CreatedAt   string    `json:"created_at"`
	UpdatedAt   string    `json:"updated_at"`
}

type pullRequest struct {
	Number    int     `json:"number"`
	Title     string  `json:"title"`
	Body      string  `json:"body"`
	HTMLURL   string  `json:"html_url"`
	State     string  `json:"state"`
	Draft     bool    `json:"draft"`
	Merged    bool    `json:"merged"`
	User      user    `json:"user"`
	Labels    []label `json:"labels"`
	Assignees []user  `json:"assignees"`
	Head      struct {
		SHA string `json:"sha"`
		Ref string `json:"ref"`
	} `json:"head"`
	Base struct {
		Ref string `json:"ref"`
	} `json:"base"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

type comment struct {
	ID        int64  `json:"id"`
	Body      string `json:"body"`
	User      user   `json:"user"`
	Path      string `json:"path"`
	Line      int    `json:"line"`
	Original  int    `json:"original_line"`
	InReplyTo int64  `json:"in_reply_to_id"`
	HTMLURL   string `json:"html_url"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

type review struct {
	ID          int64  `json:"id"`
	State       string `json:"state"`
	Body        string `json:"body"`
	User        user   `json:"user"`
	HTMLURL     string `json:"html_url"`
	SubmittedAt string `json:"submitted_at"`
}

type workflowRun struct {
	ID           int64  `json:"id"`
	Name         string `json:"name"`
	Path         string `json:"path"`
	Conclusion   string `json:"conclusion"`
	HeadSHA      string `json:"head_sha"`
	HTMLURL      string `json:"html_url"`
	PullRequests []struct {
		Number int `json:"number"`
		Head   struct {
			SHA string `json:"sha"`
		} `json:"head"`
	} `json:"pull_requests"`
}

type payload struct {
	Action      string       `json:"action"`
	Number      int          `json:"number"`
	Repository  repository   `json:"repository"`
	Sender      user         `json:"sender"`
	Issue       *issue       `json:"issue"`
	PullRequest *pullRequest `json:"pull_request"`
	Comment     *comment     `json:"comment"`
	Review      *review      `json:"review"`
	WorkflowRun *workflowRun `json:"workflow_run"`
}

var threadPattern = regexp.MustCompile(`^([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+):(issue:)?(\d+)(?::rc:(\d+))?$`)

type threadInfo struct {
	Owner    string
	Repo     string
	Full     string
	Number   int
	Issue    bool
	ReviewID int64
}

func parseThread(native string) (threadInfo, error) {
	match := threadPattern.FindStringSubmatch(native)
	if match == nil {
		return threadInfo{}, fmt.Errorf("%w: not a github thread: %s", boardsync.ErrBadRequest, native)
	}
	number, err := strconv.Atoi(match[4])
	if err != nil {
		return threadInfo{}, fmt.Errorf("%w: github thread number: %v", boardsync.ErrBadRequest, err)
	}
	info := threadInfo{
		Owner: match[1], Repo: match[2],
		Full:   match[1] + "/" + match[2],
		Number: number, Issue: match[3] != "",
	}
	if match[5] != "" {
		info.ReviewID, _ = strconv.ParseInt(match[5], 10, 64)
	}
	return info, nil
}

func (t threadInfo) thread() string {
	if t.Issue {
		return fmt.Sprintf("%s/%s:issue:%d", t.Owner, t.Repo, t.Number)
	}
	return fmt.Sprintf("%s/%s:%d", t.Owner, t.Repo, t.Number)
}

func (t threadInfo) channel() string {
	if t.Issue {
		return t.Full + ":issues"
	}
	return t.Full + ":pulls"
}

func (t threadInfo) subthread() string {
	if t.ReviewID == 0 {
		return ""
	}
	return fmt.Sprintf("%s/%s:%d:rc:%d", t.Owner, t.Repo, t.Number, t.ReviewID)
}

func authorOf(u user) boardsync.Author {
	id := fmt.Sprintf("github:user:%d", u.ID)
	if u.ID == 0 {
		id = "github:user:" + strings.ToLower(u.Login)
	}
	name := u.Login
	if name == "" {
		name = "github"
	}
	return boardsync.Author{
		ID: id, Handle: u.Login, Name: name,
		Avatar: u.AvatarURL, Bot: strings.EqualFold(u.Type, "Bot"),
	}
}

// slug is a GitHub label or login as a board label key part: accents dropped,
// lowercased, runs of other characters turned into "-", at most 64.
func slug(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	var b strings.Builder
	lastDash := false
	for _, r := range name {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '.':
			b.WriteRune(r)
			lastDash = false
		case r == '-':
			if !lastDash && b.Len() > 0 {
				b.WriteByte('-')
			}
			lastDash = true
		default:
			if !lastDash && b.Len() > 0 {
				b.WriteByte('-')
			}
			lastDash = true
		}
	}
	out := strings.Trim(b.String(), "-.")
	if len(out) > 64 {
		out = out[:64]
	}
	return out
}

func tagLabels(labels []label, assignees []user) map[string]string {
	out := make(map[string]string)
	for _, l := range labels {
		key := slug(l.Name)
		if key != "" {
			out["tag."+key] = l.Name
		}
	}
	for _, a := range assignees {
		key := slug(a.Login)
		if key != "" {
			out["assignee."+key] = a.Login
		}
	}
	return out
}

func ownedBy(labels []label, assignees []user) []string {
	var owns []string
	if labels != nil {
		owns = append(owns, "tag.")
	}
	if assignees != nil {
		owns = append(owns, "assignee.")
	}
	return owns
}

func stateOfPull(pr *pullRequest) string {
	switch {
	case pr.Merged:
		return "merged"
	case pr.State == "closed":
		return "closed"
	case pr.Draft:
		return "draft"
	default:
		return "open"
	}
}

func prLabels(pr *pullRequest, repo string) map[string]string {
	labels := tagLabels(pr.Labels, pr.Assignees)
	labels["pr"] = fmt.Sprintf("%s#%d", repo, pr.Number)
	labels["state"] = stateOfPull(pr)
	if pr.Head.SHA != "" {
		labels["head"] = pr.Head.SHA
	}
	return labels
}

func issueLabels(item *issue, repo string) map[string]string {
	labels := tagLabels(item.Labels, item.Assignees)
	labels["issue"] = fmt.Sprintf("%s#%d", repo, item.Number)
	state := item.State
	if state == "" {
		state = "open"
	}
	labels["state"] = state
	return labels
}

func ciPrefix(repoID int64, number int) string {
	return fmt.Sprintf("ci.%d-%d.", repoID, number)
}

// Client is GitHub's REST client. It is the only place the token is revealed.
type Client struct {
	baseURL string
	http    *http.Client
	token   boardsync.Secret
}

// NewClient returns a REST client. baseURL may be empty.
func NewClient(baseURL string, httpClient *http.Client, token boardsync.Secret) *Client {
	if baseURL == "" {
		baseURL = "https://api.github.com"
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), http: httpClient, token: token}
}

type commentResponse struct {
	ID      int64  `json:"id"`
	HTMLURL string `json:"html_url"`
}

func (c *Client) call(ctx context.Context, method, path string, input, output any) error {
	if c.token.Empty() {
		return fmt.Errorf("%w: github token is unset", boardsync.ErrUnauthorized)
	}
	var bodyReader *strings.Reader
	if input != nil {
		data, err := jsonMarshal(input)
		if err != nil {
			return err
		}
		bodyReader = strings.NewReader(string(data))
	} else {
		bodyReader = strings.NewReader("")
	}
	request, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bodyReader)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	request.Header.Set("Authorization", "Bearer "+c.token.Reveal())
	if input != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.http.Do(request)
	if err != nil {
		return boardsync.RedactError(err, c.token)
	}
	defer response.Body.Close()
	body, err := readLimited(response.Body, 4<<20)
	if err != nil {
		return err
	}
	if response.StatusCode == http.StatusTooManyRequests ||
		(response.StatusCode == http.StatusForbidden && response.Header.Get("X-RateLimit-Remaining") == "0") {
		return fmt.Errorf("%w: github rate-limited", boardsync.ErrRateLimited)
	}
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		return fmt.Errorf("%w: github HTTP %d", boardsync.ErrUnauthorized, response.StatusCode)
	}
	if response.StatusCode == http.StatusNotFound {
		return fmt.Errorf("%w: github HTTP 404", boardsync.ErrNotFound)
	}
	if response.StatusCode == http.StatusUnprocessableEntity {
		return fmt.Errorf("%w: github refused the request", boardsync.Permanent(errors.New(string(body))))
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("github HTTP %d", response.StatusCode)
	}
	if output != nil && len(body) > 0 {
		if err := jsonUnmarshal(body, output); err != nil {
			return fmt.Errorf("github answered unreadable JSON: %w", err)
		}
	}
	return nil
}

// CreateIssueComment comments on a PR or issue conversation.
func (c *Client) CreateIssueComment(ctx context.Context, repo string, number int, body string) (commentResponse, error) {
	var out commentResponse
	err := c.call(ctx, http.MethodPost, fmt.Sprintf("/repos/%s/issues/%d/comments", repo, number), map[string]string{"body": body}, &out)
	return out, err
}

// CreateReviewReply replies inside an inline review comment's thread.
func (c *Client) CreateReviewReply(ctx context.Context, repo string, number int, reviewCommentID int64, body string) (commentResponse, error) {
	var out commentResponse
	err := c.call(ctx, http.MethodPost, fmt.Sprintf("/repos/%s/pulls/%d/comments/%d/replies", repo, number, reviewCommentID), map[string]string{"body": body}, &out)
	return out, err
}

// UpdateIssueComment edits an issue or PR conversation comment.
func (c *Client) UpdateIssueComment(ctx context.Context, repo string, commentID int64, body string) error {
	return c.call(ctx, http.MethodPatch, fmt.Sprintf("/repos/%s/issues/comments/%d", repo, commentID), map[string]string{"body": body}, nil)
}

// UpdateReviewComment edits an inline review comment.
func (c *Client) UpdateReviewComment(ctx context.Context, repo string, commentID int64, body string) error {
	return c.call(ctx, http.MethodPatch, fmt.Sprintf("/repos/%s/pulls/comments/%d", repo, commentID), map[string]string{"body": body}, nil)
}

// DeleteIssueComment deletes an issue or PR conversation comment.
func (c *Client) DeleteIssueComment(ctx context.Context, repo string, commentID int64) error {
	return c.call(ctx, http.MethodDelete, fmt.Sprintf("/repos/%s/issues/comments/%d", repo, commentID), nil, nil)
}

// DeleteReviewComment deletes an inline review comment.
func (c *Client) DeleteReviewComment(ctx context.Context, repo string, commentID int64) error {
	return c.call(ctx, http.MethodDelete, fmt.Sprintf("/repos/%s/pulls/comments/%d", repo, commentID), nil, nil)
}

// ReactIssue adds a reaction to a PR or issue itself.
func (c *Client) ReactIssue(ctx context.Context, repo string, number int, content string) error {
	return c.call(ctx, http.MethodPost, fmt.Sprintf("/repos/%s/issues/%d/reactions", repo, number), map[string]string{"content": content}, nil)
}

// ReactIssueComment adds a reaction to a conversation comment.
func (c *Client) ReactIssueComment(ctx context.Context, repo string, commentID int64, content string) error {
	return c.call(ctx, http.MethodPost, fmt.Sprintf("/repos/%s/issues/comments/%d/reactions", repo, commentID), map[string]string{"content": content}, nil)
}

// ReactReviewComment adds a reaction to an inline review comment.
func (c *Client) ReactReviewComment(ctx context.Context, repo string, commentID int64, content string) error {
	return c.call(ctx, http.MethodPost, fmt.Sprintf("/repos/%s/pulls/comments/%d/reactions", repo, commentID), map[string]string{"content": content}, nil)
}

func (c *Client) fetchIssue(ctx context.Context, repo string, number int) (issue, error) {
	var out issue
	err := c.call(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/issues/%d", repo, number), nil, &out)
	return out, err
}

func (c *Client) fetchPull(ctx context.Context, repo string, number int) (pullRequest, error) {
	var out pullRequest
	err := c.call(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/pulls/%d", repo, number), nil, &out)
	return out, err
}

func readLimited(reader io.Reader, max int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(reader, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > max {
		return nil, errors.New("github: response is too large")
	}
	return body, nil
}

func jsonMarshal(value any) ([]byte, error) { return json.Marshal(value) }

func jsonUnmarshal(data []byte, value any) error { return json.Unmarshal(data, value) }
