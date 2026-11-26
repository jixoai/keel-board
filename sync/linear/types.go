package linear

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

// Config configures a Linear app adapter.
type Config struct {
	Key           boardsync.Key
	WebhookSecret boardsync.Secret
	// APIToken authorizes GraphQL calls.
	APIToken boardsync.Secret
	// TeamKeys is the inbound allowlist of team keys ("ENG"). It is required
	// unless AllowAllTeams is set.
	TeamKeys      []string
	AllowAllTeams bool
	// BotUserID is the app's own user ID; its comments are never imported.
	BotUserID string
	// APIBaseURL defaults to https://api.linear.app/graphql.
	APIBaseURL   string
	HTTPClient   *http.Client
	Now          func() time.Time
	Tolerance    time.Duration
	MaxBodyBytes int64
}

// Validate checks configuration without revealing a secret.
func (c Config) Validate() error {
	if c.Key != "" && !c.Key.Valid() {
		return fmt.Errorf("%w: %q isn't a sync key", boardsync.ErrBadRequest, c.Key)
	}
	if c.WebhookSecret.Empty() {
		return fmt.Errorf("%w: linear signing secret is unset", boardsync.ErrUnauthorized)
	}
	if !c.AllowAllTeams && len(c.TeamKeys) == 0 {
		return fmt.Errorf("%w: linear needs an inbound team allowlist (or AllowAllTeams)", boardsync.ErrBadRequest)
	}
	for _, key := range c.TeamKeys {
		if !teamKeyPattern.MatchString(key) {
			return fmt.Errorf("%w: %q isn't a linear team key", boardsync.ErrBadRequest, key)
		}
	}
	return nil
}

var teamKeyPattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,31}$`)

func (c Config) baseURL() string {
	if c.APIBaseURL != "" {
		return c.APIBaseURL
	}
	return "https://api.linear.app/graphql"
}

func (c Config) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

func (c Config) tolerance() time.Duration {
	if c.Tolerance > 0 {
		return c.Tolerance
	}
	return DefaultTolerance
}

func (c Config) maxBody() int64 {
	if c.MaxBodyBytes > 0 {
		return c.MaxBodyBytes
	}
	return 1 << 20
}

func (c Config) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return &http.Client{Timeout: 15 * time.Second}
}

func (c Config) teamAllowed(key string) bool {
	if c.AllowAllTeams {
		return true
	}
	for _, allowed := range c.TeamKeys {
		if strings.EqualFold(allowed, key) {
			return true
		}
	}
	return false
}

// webhook is Linear's webhook envelope.
type webhook struct {
	Action           string          `json:"action"`
	Type             string          `json:"type"`
	Data             json.RawMessage `json:"data"`
	URL              string          `json:"url"`
	CreatedAt        string          `json:"createdAt"`
	UpdatedAt        string          `json:"updatedAt"`
	Actor            *actor          `json:"actor"`
	UpdatedFrom      map[string]any  `json:"updatedFrom"`
	OrganizationID   string          `json:"organizationId"`
	WebhookID        string          `json:"webhookId"`
	WebhookTimestamp int64           `json:"webhookTimestamp"`
}

type actor struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Type string `json:"type"`
	URL  string `json:"url"`
}

type user struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	DisplayName string `json:"displayName"`
	Email       string `json:"email"`
	URL         string `json:"url"`
	AvatarURL   string `json:"avatarUrl"`
}

type team struct {
	ID   string `json:"id"`
	Key  string `json:"key"`
	Name string `json:"name"`
}

type workflowState struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Type string `json:"type"`
}

type project struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	URL  string `json:"url"`
}

type issueLabel struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Color string `json:"color"`
}

// labelList reads Linear's two shapes: an array in some webhook payloads, and
// a connection's `{ nodes: [...] }` in GraphQL answers.
type labelList []issueLabel

// UnmarshalJSON reads an array or a nodes connection.
func (l *labelList) UnmarshalJSON(data []byte) error {
	trimmed := strings.TrimSpace(string(data))
	if strings.HasPrefix(trimmed, "[") {
		var items []issueLabel
		if err := json.Unmarshal(data, &items); err != nil {
			return err
		}
		*l = items
		return nil
	}
	var connection struct {
		Nodes []issueLabel `json:"nodes"`
	}
	if err := json.Unmarshal(data, &connection); err != nil {
		return err
	}
	*l = connection.Nodes
	return nil
}

type issueData struct {
	ID            string        `json:"id"`
	Identifier    string        `json:"identifier"`
	Number        int           `json:"number"`
	Title         string        `json:"title"`
	Description   string        `json:"description"`
	URL           string        `json:"url"`
	Team          team          `json:"team"`
	State         workflowState `json:"state"`
	Priority      int           `json:"priority"`
	PriorityLabel string        `json:"priorityLabel"`
	Project       *project      `json:"project"`
	Labels        labelList     `json:"labels"`
	Assignee      *user         `json:"assignee"`
	Creator       *user         `json:"creator"`
	ArchivedAt    string        `json:"archivedAt"`
	CreatedAt     string        `json:"createdAt"`
	UpdatedAt     string        `json:"updatedAt"`
}

type commentData struct {
	ID        string     `json:"id"`
	Body      string     `json:"body"`
	IssueID   string     `json:"issueId"`
	Issue     *issueData `json:"issue"`
	User      *user      `json:"user"`
	ParentID  string     `json:"parentId"`
	URL       string     `json:"url"`
	CreatedAt string     `json:"createdAt"`
	UpdatedAt string     `json:"updatedAt"`
}

func authorOf(u *user) boardsync.Author {
	if u == nil {
		return boardsync.Author{}
	}
	handle := handleOf(u)
	return boardsync.Author{
		ID: "linear:user:" + u.ID, Handle: handle,
		Name:   firstNonEmpty(u.DisplayName, u.Name, handle),
		Avatar: u.AvatarURL,
	}
}

func actorAuthor(a *actor) boardsync.Author {
	if a == nil {
		return boardsync.Author{}
	}
	handle := strings.ToLower(a.Name)
	if handle == "" {
		handle = a.ID
	}
	return boardsync.Author{
		ID: "linear:user:" + a.ID, Handle: handle, Name: firstNonEmpty(a.Name, a.ID),
		Bot: a.Type == "application" || a.Type == "integration",
	}
}

func handleOf(u *user) string {
	if u == nil {
		return ""
	}
	const marker = "/profiles/"
	if index := strings.Index(u.URL, marker); index >= 0 {
		handle := u.URL[index+len(marker):]
		if slash := strings.IndexByte(handle, '/'); slash >= 0 {
			handle = handle[:slash]
		}
		if handle != "" {
			return handle
		}
	}
	return firstNonEmpty(u.DisplayName, u.Name, u.ID)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

var teamKeyFromIdentifier = regexp.MustCompile(`^([A-Z][A-Z0-9_]*)-(\d+)$`)

func teamOfIdentifier(identifier string) string {
	match := teamKeyFromIdentifier.FindStringSubmatch(identifier)
	if len(match) < 3 {
		return ""
	}
	return match[1]
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

func issueThread(teamKey, issueID string) string {
	return teamKey + ":" + issueID
}

func commentThread(teamKey, issueID, commentID string) string {
	return teamKey + ":" + issueID + ":c:" + commentID
}

func stateType(state workflowState) string {
	if state.Type != "" {
		return strings.ToLower(state.Type)
	}
	return strings.ToLower(state.Name)
}

func issueLabelsFor(item *issueData) map[string]string {
	labels := map[string]string{
		"issue": item.Identifier,
		"state": stateType(item.State),
	}
	if item.PriorityLabel != "" {
		labels["priority"] = strings.ToLower(item.PriorityLabel)
	} else if item.Priority != 0 {
		labels["priority"] = strconv.Itoa(item.Priority)
	}
	if item.Project != nil && item.Project.Name != "" {
		labels["project"] = item.Project.Name
	}
	for _, label := range item.Labels {
		key := slug(label.Name)
		if key != "" {
			labels["tag."+key] = label.Name
		}
	}
	if item.Assignee != nil {
		handle := slug(handleOf(item.Assignee))
		if handle != "" {
			labels["assignee."+handle] = handleOf(item.Assignee)
		}
	}
	return labels
}

func issueOwns(item *issueData) []string {
	owns := []string{"state", "priority", "project"}
	if item.Labels != nil {
		owns = append(owns, "tag.")
	}
	if item.Assignee != nil {
		owns = append(owns, "assignee.")
	}
	return owns
}

func issueRootText(item *issueData) string {
	head := item.Title
	if item.Identifier != "" {
		head = item.Identifier + ": " + item.Title
	}
	if item.URL != "" {
		head += "\n" + item.URL
	}
	if strings.TrimSpace(item.Description) != "" {
		head += "\n\n" + item.Description
	}
	return head
}

// slug is a Linear label name as a board label key part.
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

// Client is Linear's GraphQL client. It is the only place the token is
// revealed.
type Client struct {
	endpoint string
	http     *http.Client
	token    boardsync.Secret
}

// NewClient returns a GraphQL client. endpoint may be empty.
func NewClient(endpoint string, httpClient *http.Client, token boardsync.Secret) *Client {
	if endpoint == "" {
		endpoint = "https://api.linear.app/graphql"
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	return &Client{endpoint: endpoint, http: httpClient, token: token}
}

type graphqlRequest struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables,omitempty"`
}

type graphqlError struct {
	Message string `json:"message"`
}

type graphqlResponse struct {
	Data   json.RawMessage `json:"data"`
	Errors []graphqlError  `json:"errors"`
}

func (c *Client) do(ctx context.Context, query string, variables map[string]any, output any) error {
	if c.token.Empty() {
		return fmt.Errorf("%w: linear API token is unset", boardsync.ErrUnauthorized)
	}
	body, err := json.Marshal(graphqlRequest{Query: query, Variables: variables})
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+c.token.Reveal())
	response, err := c.http.Do(request)
	if err != nil {
		return boardsync.RedactError(err, c.token)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if err != nil {
		return err
	}
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		return fmt.Errorf("%w: linear HTTP %d", boardsync.ErrUnauthorized, response.StatusCode)
	}
	if response.StatusCode == http.StatusTooManyRequests {
		return fmt.Errorf("%w: linear rate-limited", boardsync.ErrRateLimited)
	}
	if response.StatusCode == http.StatusBadRequest {
		return fmt.Errorf("%w: linear refused the query", boardsync.ErrBadRequest)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("linear HTTP %d", response.StatusCode)
	}
	var decoded graphqlResponse
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return fmt.Errorf("linear answered unreadable JSON: %w", err)
	}
	if len(decoded.Errors) > 0 {
		return fmt.Errorf("linear: %s", decoded.Errors[0].Message)
	}
	if output != nil && len(decoded.Data) > 0 {
		if err := json.Unmarshal(decoded.Data, output); err != nil {
			return fmt.Errorf("linear answered unreadable data: %w", err)
		}
	}
	return nil
}

const teamQuery = `query Team($key: String!) { teams(filter: { key: { eq: $key } }) { nodes { id key } } }`

const issueByKeyQuery = `query IssueByKey($team: String!, $number: Float!) {
  issues(filter: { team: { key: { eq: $team } }, number: { eq: $number } }) { nodes { id identifier team { key } } }
}`

const issueByIDQuery = `query IssueByID($id: String!) {
  issue(id: $id) {
    id identifier number title description url team { id key name }
    state { id name type } priority priorityLabel project { id name url }
    labels { nodes { id name color } }
    assignee { id name displayName url avatarUrl }
    creator { id name displayName url avatarUrl }
    archivedAt createdAt updatedAt
  }
}`

const commentCreateMutation = `mutation CommentCreate($issueId: String!, $body: String!) {
  commentCreate(input: { issueId: $issueId, body: $body }) { success comment { id } }
}`

const issueCreateMutation = `mutation IssueCreate($teamId: String!, $title: String!, $description: String!) {
  issueCreate(input: { teamId: $teamId, title: $title, description: $description }) { success issue { id identifier team { key } } }
}`

const commentUpdateMutation = `mutation CommentUpdate($id: String!, $body: String!) {
  commentUpdate(id: $id, input: { body: $body }) { success }
}`

const commentDeleteMutation = `mutation CommentDelete($id: String!) { commentDelete(id: $id) { success } }`

const reactionCreateMutation = `mutation ReactionCreate($commentId: String!, $emoji: String!) {
  reactionCreate(input: { commentId: $commentId, emoji: $emoji }) { success }
}`

type issueRef struct {
	ID         string `json:"id"`
	Identifier string `json:"identifier"`
	Team       team   `json:"team"`
}

// IssueByKey finds an issue by team key and number.
func (c *Client) IssueByKey(ctx context.Context, teamKey string, number int) (issueRef, error) {
	var out struct {
		Issues struct {
			Nodes []issueRef `json:"nodes"`
		} `json:"issues"`
	}
	if err := c.do(ctx, issueByKeyQuery, map[string]any{"team": teamKey, "number": number}, &out); err != nil {
		return issueRef{}, err
	}
	if len(out.Issues.Nodes) == 0 {
		return issueRef{}, fmt.Errorf("%w: no linear issue %s-%d", boardsync.ErrNotFound, teamKey, number)
	}
	return out.Issues.Nodes[0], nil
}

// IssueByID reads one issue.
func (c *Client) IssueByID(ctx context.Context, id string) (issueData, error) {
	var out struct {
		Issue issueData `json:"issue"`
	}
	if err := c.do(ctx, issueByIDQuery, map[string]any{"id": id}, &out); err != nil {
		return issueData{}, err
	}
	return out.Issue, nil
}

// CreateComment comments on an issue.
func (c *Client) CreateComment(ctx context.Context, issueID, body string) (string, error) {
	var out struct {
		CommentCreate struct {
			Success bool `json:"success"`
			Comment struct {
				ID string `json:"id"`
			} `json:"comment"`
		} `json:"commentCreate"`
	}
	if err := c.do(ctx, commentCreateMutation, map[string]any{"issueId": issueID, "body": body}, &out); err != nil {
		return "", err
	}
	if !out.CommentCreate.Success || out.CommentCreate.Comment.ID == "" {
		return "", errors.New("linear commentCreate failed")
	}
	return out.CommentCreate.Comment.ID, nil
}

// CreateIssue opens an issue in a team.
func (c *Client) CreateIssue(ctx context.Context, teamID, title, description string) (issueRef, error) {
	var out struct {
		IssueCreate struct {
			Success bool     `json:"success"`
			Issue   issueRef `json:"issue"`
		} `json:"issueCreate"`
	}
	if err := c.do(ctx, issueCreateMutation, map[string]any{"teamId": teamID, "title": title, "description": description}, &out); err != nil {
		return issueRef{}, err
	}
	if !out.IssueCreate.Success || out.IssueCreate.Issue.ID == "" {
		return issueRef{}, errors.New("linear issueCreate failed")
	}
	return out.IssueCreate.Issue, nil
}

// UpdateComment replaces a comment's body.
func (c *Client) UpdateComment(ctx context.Context, id, body string) error {
	var out struct {
		CommentUpdate struct {
			Success bool `json:"success"`
		} `json:"commentUpdate"`
	}
	if err := c.do(ctx, commentUpdateMutation, map[string]any{"id": id, "body": body}, &out); err != nil {
		return err
	}
	if !out.CommentUpdate.Success {
		return errors.New("linear commentUpdate failed")
	}
	return nil
}

// DeleteComment removes a comment.
func (c *Client) DeleteComment(ctx context.Context, id string) error {
	var out struct {
		CommentDelete struct {
			Success bool `json:"success"`
		} `json:"commentDelete"`
	}
	if err := c.do(ctx, commentDeleteMutation, map[string]any{"id": id}, &out); err != nil {
		return err
	}
	if !out.CommentDelete.Success {
		return errors.New("linear commentDelete failed")
	}
	return nil
}

// ReactComment adds a reaction to a comment.
func (c *Client) ReactComment(ctx context.Context, commentID, emoji string) error {
	var out struct {
		ReactionCreate struct {
			Success bool `json:"success"`
		} `json:"reactionCreate"`
	}
	if err := c.do(ctx, reactionCreateMutation, map[string]any{"commentId": commentID, "emoji": emoji}, &out); err != nil {
		return err
	}
	if !out.ReactionCreate.Success {
		return errors.New("linear reactionCreate failed")
	}
	return nil
}

// TeamID resolves a team key to its ID for issue creation.
func (c *Client) TeamID(ctx context.Context, key string) (string, error) {
	var out struct {
		Teams struct {
			Nodes []team `json:"nodes"`
		} `json:"teams"`
	}
	if err := c.do(ctx, teamQuery, map[string]any{"key": key}, &out); err != nil {
		return "", err
	}
	if len(out.Teams.Nodes) == 0 || out.Teams.Nodes[0].ID == "" {
		return "", fmt.Errorf("%w: no linear team %s", boardsync.ErrNotFound, key)
	}
	return out.Teams.Nodes[0].ID, nil
}
