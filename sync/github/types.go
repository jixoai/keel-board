package github

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	boardsync "github.com/jixoai/keel-board/sync"
)

// MaxCommentLength is GitHub's comment body limit, in characters. A longer
// outbound body is clamped; the hidden marker is kept.
const MaxCommentLength = 65536

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
	// InstallationID, when set, binds this entry to one GitHub App
	// installation. A webhook whose installation is missing or different is
	// dropped before normalization: an entry must never import another
	// installation's private events.
	InstallationID int64
	// DetectViewer resolves the token's own user once (GET /user) and treats
	// its comments as the app's own. BotUserID avoids the extra request; a
	// failed lookup leaves the marker and BotLogin as the echo guards.
	DetectViewer bool
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
	// APIBaseURL defaults to https://api.github.com. Plain http is accepted
	// only for loopback hosts, so a token never crosses a cleartext network.
	APIBaseURL string
	// Doer performs REST calls. nil uses an http.Client with a timeout.
	Doer Doer
	// Now is a test seam for time-dependent behavior. GitHub webhooks carry no
	// timestamp; replay identity is X-GitHub-Delivery.
	Now          func() time.Time
	MaxBodyBytes int64
}

// Doer sends one HTTP request. *http.Client implements it; a test may supply a
// recording or failing implementation without a network.
type Doer interface {
	Do(*http.Request) (*http.Response, error)
}

// DoerFunc adapts a function to Doer.
type DoerFunc func(*http.Request) (*http.Response, error)

// Do implements Doer.
func (f DoerFunc) Do(r *http.Request) (*http.Response, error) { return f(r) }

// Validate checks configuration without revealing a secret.
func (c Config) Validate() error {
	if c.Key != "" && !c.Key.Valid() {
		return fmt.Errorf("%w: %q isn't a sync key", boardsync.ErrBadRequest, c.Key)
	}
	if c.WebhookSecret.Empty() {
		return fmt.Errorf("%w: github webhook secret is unset", boardsync.ErrUnauthorized)
	}
	if c.InstallationID < 0 {
		return fmt.Errorf("%w: github installation ID can't be negative", boardsync.ErrBadRequest)
	}
	if c.BotUserID < 0 {
		return fmt.Errorf("%w: github bot user ID can't be negative", boardsync.ErrBadRequest)
	}
	if c.DetectViewer {
		if c.Token.Empty() {
			return fmt.Errorf("%w: DetectViewer needs a user token; GitHub App installation tokens can't read GET /user", boardsync.ErrBadRequest)
		}
		if isInstallationToken(c.Token) {
			return fmt.Errorf("%w: DetectViewer only supports user tokens; an installation token can't read GET /user, set BotUserID or BotLogin instead", boardsync.ErrBadRequest)
		}
	}
	if c.APIBaseURL != "" {
		if err := validateAPIBaseURL(c.APIBaseURL); err != nil {
			return err
		}
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

// validateAPIBaseURL refuses a cleartext base URL for a non-loopback host: the
// token travels in a header, and a typo must not send it over plain HTTP.
func validateAPIBaseURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("%w: github API base URL: %v", boardsync.ErrBadRequest, err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("%w: github API base URL needs a scheme and host", boardsync.ErrBadRequest)
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("%w: github API base URL can't carry credentials, a query, or a fragment", boardsync.ErrBadRequest)
	}
	if parsed.Scheme == "https" {
		return nil
	}
	if parsed.Scheme == "http" {
		switch strings.ToLower(parsed.Hostname()) {
		case "127.0.0.1", "::1", "localhost":
			return nil
		}
	}
	return fmt.Errorf("%w: github API base URL must be https (http only for loopback)", boardsync.ErrBadRequest)
}

func validRepo(full string) bool {
	parts := strings.Split(full, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return false
	}
	if len(parts[0]) > 100 || len(parts[1]) > 100 {
		return false
	}
	for _, part := range parts {
		// "." and ".." satisfy the character set but would traverse a REST
		// path; a repo name is a name, never a relative segment.
		if part == "." || part == ".." {
			return false
		}
		for _, c := range part {
			if !(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') && !(c >= '0' && c <= '9') && c != '-' && c != '_' && c != '.' {
				return false
			}
		}
	}
	return true
}

var deliveryPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)

// isInstallationToken reports the GitHub App installation token shape. Such a
// token can call the REST API as the installation but not GET /user, which
// names a human account.
func isInstallationToken(token boardsync.Secret) bool {
	return strings.HasPrefix(token.Reveal(), "ghs_")
}

// validDelivery reports whether X-GitHub-Delivery is a bounded, printable ID.
// It is the replay identity a webhook redelivery keeps.
func validDelivery(delivery string) bool { return deliveryPattern.MatchString(delivery) }

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

func (c Config) doer() Doer {
	if c.Doer != nil {
		return c.Doer
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

type installation struct {
	ID int64 `json:"id"`
}

type payload struct {
	Action       string        `json:"action"`
	Number       int           `json:"number"`
	Repository   repository    `json:"repository"`
	Installation *installation `json:"installation"`
	Sender       user          `json:"sender"`
	Issue        *issue        `json:"issue"`
	PullRequest  *pullRequest  `json:"pull_request"`
	Comment      *comment      `json:"comment"`
	Review       *review       `json:"review"`
	WorkflowRun  *workflowRun  `json:"workflow_run"`
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
	if match == nil || !validRepo(match[1]+"/"+match[2]) {
		return threadInfo{}, fmt.Errorf("%w: not a github thread: %s", boardsync.ErrBadRequest, native)
	}
	number, err := strconv.Atoi(match[4])
	if err != nil || number <= 0 {
		return threadInfo{}, fmt.Errorf("%w: github thread number: %s", boardsync.ErrBadRequest, match[4])
	}
	info := threadInfo{
		Owner: match[1], Repo: match[2],
		Full:   match[1] + "/" + match[2],
		Number: number, Issue: match[3] != "",
	}
	if match[5] != "" {
		info.ReviewID, _ = strconv.ParseInt(match[5], 10, 64)
		if info.ReviewID <= 0 {
			return threadInfo{}, fmt.Errorf("%w: github review comment ID: %s", boardsync.ErrBadRequest, match[5])
		}
		// Issues have no review threads; only a pull request does.
		if info.Issue {
			return threadInfo{}, fmt.Errorf("%w: an issue has no review sub-thread: %s", boardsync.ErrBadRequest, native)
		}
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

// authorFor falls back to the webhook sender when the subject omits its own
// user (a deleted account, or a review event without a review object).
func authorFor(primary, sender user) boardsync.Author {
	if primary.ID == 0 && primary.Login == "" {
		primary = sender
	}
	return authorOf(primary)
}

// slug is a GitHub label, login, or workflow name as a board label key part:
// lowercased, runs of other characters turned into "-". boundedKey applies the
// board's length limit and keeps long names distinct.
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
	return strings.Trim(b.String(), "-.")
}

func tagLabels(labels []label, assignees []user) map[string]string {
	out := make(map[string]string)
	for _, l := range labels {
		if key := boundedKey("tag.", slug(l.Name)); key != "" {
			out[key] = labelValue(l.Name)
		}
	}
	for _, a := range assignees {
		if key := boundedKey("assignee.", slug(a.Login)); key != "" {
			out[key] = labelValue(a.Login)
		}
	}
	return out
}

// labelValue keeps a platform label or login inside the board's label rules:
// valid UTF-8, no NUL, no control characters, at most 256 characters.
func labelValue(name string) string {
	name = boardsync.StorableText(name, 1024)
	name = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, name)
	if runes := []rune(name); len(runes) > 256 {
		name = string(runes[:256])
	}
	return strings.TrimSpace(name)
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
	case strings.EqualFold(pr.State, "closed"):
		return "closed"
	case pr.Draft:
		return "draft"
	default:
		return "open"
	}
}

// stateOfIssue keeps GitHub's two states and refuses anything else it might
// send: a label value is data, not a place to smuggle an arbitrary string.
func stateOfIssue(item *issue) string {
	if strings.EqualFold(item.State, "closed") {
		return "closed"
	}
	return "open"
}

func prLabels(pr *pullRequest, repo string) map[string]string {
	labels := tagLabels(pr.Labels, pr.Assignees)
	labels["pr"] = fmt.Sprintf("%s#%d", repo, pr.Number)
	labels["state"] = stateOfPull(pr)
	if validSHA(pr.Head.SHA) {
		labels["head"] = pr.Head.SHA
	}
	return labels
}

func issueLabels(item *issue, repo string) map[string]string {
	labels := tagLabels(item.Labels, item.Assignees)
	labels["issue"] = fmt.Sprintf("%s#%d", repo, item.Number)
	labels["state"] = stateOfIssue(item)
	return labels
}

var shaPattern = regexp.MustCompile(`^[0-9a-fA-F]{7,64}$`)

// validSHA accepts the commit hashes GitHub puts in a head label.
func validSHA(value string) bool { return shaPattern.MatchString(value) }

func ciPrefix(repoID int64, number int) string {
	if repoID <= 0 || number <= 0 {
		return ""
	}
	return fmt.Sprintf("ci.%d-%d.", repoID, number)
}

// boundedKey joins a prefix and a slug within the board's 64-character key
// limit without letting two long names collide: the tail becomes a hash of the
// full name while the prefix stays readable and owns.prefix matching still
// works.
func boundedKey(prefix, slug string) string {
	if slug == "" {
		return ""
	}
	if len(prefix)+len(slug) <= 64 {
		return prefix + slug
	}
	sum := sha256.Sum256([]byte(prefix + slug))
	suffix := "-" + hex.EncodeToString(sum[:4])
	budget := 64 - len(prefix) - len(suffix)
	if budget < 1 {
		return prefix + suffix
	}
	return prefix + slug[:budget] + suffix
}

// Client is GitHub's REST client. It is the only place the token is revealed.
type Client struct {
	baseURL  string
	doer     Doer
	token    boardsync.Secret
	setupErr error
}

// NewClient returns a REST client. baseURL may be empty; a nil doer uses a
// default http.Client. A direct caller gets a base-URL validation error on its
// first call, matching Provider.New's configuration checks.
func NewClient(baseURL string, doer Doer, token boardsync.Secret) *Client {
	if baseURL == "" {
		baseURL = "https://api.github.com"
	}
	if doer == nil {
		doer = &http.Client{Timeout: 15 * time.Second}
	}
	client := &Client{baseURL: strings.TrimRight(baseURL, "/"), doer: doer, token: token}
	if err := validateAPIBaseURL(baseURL); err != nil {
		client.setupErr = err
	}
	return client
}

type commentResponse struct {
	ID      int64  `json:"id"`
	HTMLURL string `json:"html_url"`
}

// RateLimitError is GitHub asking the caller to slow down. It unwraps to
// boardsync.ErrRateLimited, so the retry policy retries it, and carries the
// platform's own delay when one was given.
type RateLimitError struct {
	Status     int
	RetryAfter time.Duration
}

// Error says what happened without the response body.
func (e *RateLimitError) Error() string {
	if e.RetryAfter > 0 {
		return fmt.Sprintf("github rate-limited (HTTP %d, retry after %s)", e.Status, e.RetryAfter)
	}
	return fmt.Sprintf("github rate-limited (HTTP %d)", e.Status)
}

// Unwrap makes errors.Is(err, boardsync.ErrRateLimited) true.
func (e *RateLimitError) Unwrap() error { return boardsync.ErrRateLimited }

func (c *Client) call(ctx context.Context, method, path string, input, output any) error {
	if c.setupErr != nil {
		return c.setupErr
	}
	if c.token.Empty() {
		return fmt.Errorf("%w: github token is unset", boardsync.ErrUnauthorized)
	}
	var bodyReader io.Reader = http.NoBody
	if input != nil {
		data, err := jsonMarshal(input)
		if err != nil {
			return err
		}
		bodyReader = strings.NewReader(string(data))
	}
	request, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bodyReader)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	request.Header.Set("User-Agent", "keel-board-sync")
	request.Header.Set("Authorization", "Bearer "+c.token.Reveal())
	if input != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.doer.Do(request)
	if err != nil {
		return boardsync.RedactError(err, c.token)
	}
	defer response.Body.Close()
	body, err := readLimited(response.Body, 4<<20)
	if err != nil {
		return err
	}
	switch response.StatusCode {
	case http.StatusTooManyRequests:
		return &RateLimitError{Status: response.StatusCode, RetryAfter: retryAfter(response.Header)}
	case http.StatusForbidden:
		if rateLimited(response, body) {
			return &RateLimitError{Status: response.StatusCode, RetryAfter: retryAfter(response.Header)}
		}
		return fmt.Errorf("%w: github refused the request (HTTP 403)", boardsync.ErrUnauthorized)
	case http.StatusUnauthorized:
		return fmt.Errorf("%w: github HTTP 401", boardsync.ErrUnauthorized)
	case http.StatusNotFound, http.StatusGone:
		return fmt.Errorf("%w: github HTTP %d", boardsync.ErrNotFound, response.StatusCode)
	case http.StatusUnprocessableEntity:
		return fmt.Errorf("%w: github refused the request (HTTP 422)", boardsync.ErrBadRequest)
	}
	if response.StatusCode >= 500 {
		// A 5xx is transport-shaped: the retry policy tries again.
		return fmt.Errorf("github HTTP %d", response.StatusCode)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("%w: github HTTP %d", boardsync.ErrBadRequest, response.StatusCode)
	}
	if output != nil && len(body) > 0 {
		if err := jsonUnmarshal(body, output); err != nil {
			return fmt.Errorf("github answered unreadable JSON: %w", err)
		}
	}
	return nil
}

// rateLimited recognizes GitHub's secondary rate limit, which arrives as a 403
// with a Retry-After header or a body that says "rate limit" (never logged).
func rateLimited(response *http.Response, body []byte) bool {
	if response.Header.Get("Retry-After") != "" {
		return true
	}
	if response.Header.Get("X-RateLimit-Remaining") == "0" {
		return true
	}
	return bytesContainsFold(body, "rate limit")
}

func retryAfter(header http.Header) time.Duration {
	value := strings.TrimSpace(header.Get("Retry-After"))
	if value == "" {
		if reset := header.Get("X-RateLimit-Reset"); reset != "" {
			if seconds, err := strconv.ParseInt(reset, 10, 64); err == nil {
				if wait := time.Until(time.Unix(seconds, 0)); wait > 0 {
					return wait
				}
			}
		}
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil {
		if wait := time.Until(when); wait > 0 {
			return wait
		}
	}
	return 0
}

func bytesContainsFold(data []byte, needle string) bool {
	return strings.Contains(strings.ToLower(string(data)), needle)
}

// Viewer reads the token's own user. A GitHub App installation token answers
// with the app's bot account; a deployment stores that ID as BotUserID.
func (c *Client) Viewer(ctx context.Context) (user, error) {
	var out user
	err := c.call(ctx, http.MethodGet, "/user", nil, &out)
	return out, err
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
