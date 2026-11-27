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
	"unicode"
	"unicode/utf8"

	boardsync "github.com/jixoai/keel-board/sync"
)

// MaxCommentChars bounds a comment body. Linear documents no limit; ~10k
// characters is what its API reliably takes, and a longer body is refused
// before the call rather than failing opaquely.
const MaxCommentChars = 10000

// MaxTitleChars bounds an issue title. Linear's title field accepts 255.
const MaxTitleChars = 255

// MaxTextChars bounds normalized text and an issue description. It is a
// code-point limit, not a byte limit, because Linear counts characters.
const MaxTextChars = 10000

// MaxRawBytes bounds the raw payload kept on a Record. A larger webhook is
// represented by a small note rather than a truncated JSON document.
const MaxRawBytes = 256 << 10

// MaxLabelValueChars bounds a board label value; keys are bounded by slug.
const MaxLabelValueChars = 256

// Config configures a Linear app adapter.
type Config struct {
	Key           boardsync.Key
	WebhookSecret boardsync.Secret
	// APIToken authorizes GraphQL calls.
	APIToken boardsync.Secret
	// TokenScheme is "api_key" for a personal API key (Authorization: <key>),
	// "bearer" for an OAuth/app token, or "auto" (the default) to decide from
	// the token's prefix. Linear personal keys start with "lin_api_".
	TokenScheme string
	// TeamKeys is the inbound allowlist of team keys ("ENG"). It is required
	// unless AllowAllTeams is set.
	TeamKeys      []string
	AllowAllTeams bool
	// BotUserID is the app's own user ID; its comments and issues never open a
	// second thread or import as someone else's. It is required: without it
	// the adapter can't fail closed on its own events.
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
	if c.BotUserID == "" {
		return fmt.Errorf("%w: linear needs the app's own user ID (BotUserID) to fail closed", boardsync.ErrBadRequest)
	}
	switch strings.ToLower(strings.TrimSpace(c.TokenScheme)) {
	case "", "auto", "api_key", "bearer":
	default:
		return fmt.Errorf("%w: token scheme %q isn't api_key, bearer, or auto", boardsync.ErrBadRequest, c.TokenScheme)
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

func (c Config) tokenScheme() string {
	if scheme := strings.ToLower(strings.TrimSpace(c.TokenScheme)); scheme != "" {
		return scheme
	}
	return "auto"
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
// a connection's `{ nodes: [...] }` in GraphQL answers. Present distinguishes
// an empty list (the issue has no labels, so its old tags go) from a payload
// that didn't include the field (leave the board's tags alone).
type labelList struct {
	Nodes   []issueLabel
	Present bool
}

// UnmarshalJSON reads an array or a nodes connection.
func (l *labelList) UnmarshalJSON(data []byte) error {
	l.Present = true
	trimmed := strings.TrimSpace(string(data))
	if strings.HasPrefix(trimmed, "[") {
		var items []issueLabel
		if err := json.Unmarshal(data, &items); err != nil {
			return err
		}
		l.Nodes = items
		return nil
	}
	var connection struct {
		Nodes []issueLabel `json:"nodes"`
	}
	if err := json.Unmarshal(data, &connection); err != nil {
		return err
	}
	l.Nodes = connection.Nodes
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
	ProjectID     string        `json:"projectId"`
	Labels        labelList     `json:"labels"`
	Assignee      *user         `json:"assignee"`
	AssigneeID    string        `json:"assigneeId"`
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
	UserID    string     `json:"userId"`
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
	if value := strings.TrimSpace(state.Type); value != "" {
		return strings.ToLower(value)
	}
	return strings.ToLower(strings.TrimSpace(state.Name))
}

// issueProperties is an issue's state and properties as board labels. Unset
// names single keys the payload says are empty; Owns lists the prefixes whose
// keys the platform owns, so a tag or assignee it dropped is unset too.
type issueProperties struct {
	Labels map[string]string
	Unset  []string
	Owns   []string
}

func propertiesOf(item *issueData) issueProperties {
	out := issueProperties{Labels: map[string]string{}}
	if item.Identifier != "" {
		out.Labels["issue"] = clampLabelValue(item.Identifier)
	}
	if state := stateType(item.State); state != "" {
		out.Labels["state"] = clampLabelValue(state)
	} else {
		out.Unset = append(out.Unset, "state")
	}
	switch {
	case item.Priority == 0 || strings.EqualFold(item.PriorityLabel, "No priority"):
		out.Unset = append(out.Unset, "priority")
	case item.PriorityLabel != "":
		out.Labels["priority"] = slug(item.PriorityLabel)
	default:
		out.Labels["priority"] = strconv.Itoa(item.Priority)
	}
	if item.Project != nil && item.Project.Name != "" {
		out.Labels["project"] = clampLabelValue(item.Project.Name)
	} else if item.ProjectID == "" {
		out.Unset = append(out.Unset, "project")
	}
	if item.Labels.Present {
		out.Owns = append(out.Owns, "tag.")
		for _, label := range item.Labels.Nodes {
			key := labelKey("tag.", label.Name)
			if key != "" {
				out.Labels[key] = clampLabelValue(label.Name)
			}
		}
	}
	if item.Assignee != nil {
		out.Owns = append(out.Owns, "assignee.")
		handle := handleOf(item.Assignee)
		if key := labelKey("assignee.", handle); key != "" {
			out.Labels[key] = clampLabelValue(handle)
		}
	} else if item.AssigneeID == "" {
		// Linear leaves an empty assignee out of the payload, sometimes
		// without a null; no details and no ID means nobody is assigned.
		out.Owns = append(out.Owns, "assignee.")
	}
	return out
}

func issueLabelsFor(item *issueData) map[string]string {
	return propertiesOf(item).Labels
}

func issueRootText(item *issueData) string {
	head := clampText(item.Title, MaxTextChars)
	if item.Identifier != "" {
		head = item.Identifier + ": " + head
	}
	if item.URL != "" {
		head += "\n" + item.URL
	}
	if description := strings.TrimSpace(item.Description); description != "" {
		head += "\n\n" + clampText(description, MaxTextChars)
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

func labelKey(prefix, name string) string {
	key := prefix + slug(name)
	if len(key) > 64 {
		key = key[:64]
	}
	return key
}

func clampLabelValue(value string) string {
	value = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, value)
	return clampRunes(value, MaxLabelValueChars)
}

func clampText(text string, max int) string {
	return clampRunes(boardsync.StorableText(text, boardsync.MaxTextBytes), max)
}

func clampRunes(text string, max int) string {
	if max <= 0 || utf8.RuneCountInString(text) <= max {
		return text
	}
	runes := []rune(text)
	return string(runes[:max])
}

func clampRaw(body []byte) json.RawMessage {
	if len(body) <= MaxRawBytes {
		return json.RawMessage(append([]byte(nil), body...))
	}
	note, _ := json.Marshal(map[string]any{"truncated": true, "bytes": len(body)})
	return json.RawMessage(note)
}

// Client is Linear's GraphQL client. It is the only place the token is
// revealed.
type Client struct {
	endpoint string
	http     *http.Client
	token    boardsync.Secret
	scheme   string
}

// NewClient returns a GraphQL client. endpoint may be empty.
func NewClient(endpoint string, httpClient *http.Client, token boardsync.Secret) *Client {
	if endpoint == "" {
		endpoint = "https://api.linear.app/graphql"
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	return &Client{endpoint: endpoint, http: httpClient, token: token, scheme: "auto"}
}

// authorization is the header value for the configured token scheme. Linear
// personal API keys go bare; OAuth and app tokens use Bearer.
func (c *Client) authorization() string {
	token := c.token.Reveal()
	switch strings.ToLower(strings.TrimSpace(c.scheme)) {
	case "api_key":
		return token
	case "bearer":
		return "Bearer " + token
	default:
		if strings.HasPrefix(token, "lin_api_") {
			return token
		}
		return "Bearer " + token
	}
}

type graphqlRequest struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables,omitempty"`
}

type graphqlError struct {
	Message    string `json:"message"`
	Extensions struct {
		Code string `json:"code"`
	} `json:"extensions"`
}

type graphqlResponse struct {
	Data   json.RawMessage `json:"data"`
	Errors []graphqlError  `json:"errors"`
}

// RateLimitedError is a 429 with its Retry-After, when Linear sent one.
type RateLimitedError struct {
	After int
}

func (e *RateLimitedError) Error() string {
	if e.After > 0 {
		return fmt.Sprintf("sync: rate limited: linear rate-limited (retry after %ds)", e.After)
	}
	return "sync: rate limited: linear rate-limited"
}

func (e *RateLimitedError) Unwrap() error { return boardsync.ErrRateLimited }

// RetryAfterSeconds reads the Retry-After an error carries, or zero.
func RetryAfterSeconds(err error) int {
	var limited *RateLimitedError
	if errors.As(err, &limited) {
		return limited.After
	}
	return 0
}

// classifyStatus maps a non-2xx GraphQL HTTP answer to the sync error the
// outbox and webhook path understand. 408, 409, 425, 429, and 5xx are
// transient; other 4xx answers are permanent.
func classifyStatus(status int, header http.Header) error {
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return fmt.Errorf("%w: linear HTTP %d", boardsync.ErrUnauthorized, status)
	case status == http.StatusNotFound:
		return fmt.Errorf("%w: linear HTTP %d", boardsync.ErrNotFound, status)
	case status == http.StatusTooManyRequests:
		return &RateLimitedError{After: boardsync.RetryAfter(header)}
	case status == http.StatusRequestTimeout || status == http.StatusConflict || status == http.StatusTooEarly:
		return fmt.Errorf("linear transient HTTP %d", status)
	case status == http.StatusBadRequest || status == http.StatusUnprocessableEntity:
		return fmt.Errorf("%w: linear refused the request (HTTP %d)", boardsync.ErrBadRequest, status)
	case status >= 500 && status <= 599:
		return fmt.Errorf("linear server error HTTP %d", status)
	case status >= 400 && status <= 499:
		return boardsync.Permanent(fmt.Errorf("linear HTTP %d", status))
	default:
		return fmt.Errorf("linear HTTP %d", status)
	}
}

// classifyGraphQL maps a GraphQL error to a sync error. GraphQL answers with
// HTTP 200 even for a bad query or a refused mutation, so the extension code
// is the only classification available; unknown codes are permanent because
// the same query will fail the same way. Linear's message isn't carried: it
// may quote the input, and an operator reads the code to know what failed.
func classifyGraphQL(err graphqlError) error {
	code := strings.ToUpper(strings.TrimSpace(err.Extensions.Code))
	switch code {
	case "AUTHENTICATION_ERROR", "AUTHENTICATION_FAILED", "UNAUTHORIZED":
		return fmt.Errorf("%w: linear authentication failed", boardsync.ErrUnauthorized)
	case "FORBIDDEN", "ACCESS_DENIED":
		return fmt.Errorf("%w: linear refused the token's access", boardsync.ErrUnauthorized)
	case "RATELIMITED", "RATE_LIMITED":
		return fmt.Errorf("%w: linear rate-limited", boardsync.ErrRateLimited)
	case "NOT_FOUND", "ENTITY_NOT_FOUND":
		return fmt.Errorf("%w: linear resource not found", boardsync.ErrNotFound)
	case "INVALID_INPUT", "BAD_USER_INPUT", "GRAPHQL_VALIDATION_FAILED":
		return fmt.Errorf("%w: linear rejected the query (%s)", boardsync.ErrBadRequest, code)
	case "INTERNAL_SERVER_ERROR", "INTERNAL", "SERVICE_UNAVAILABLE", "UNAVAILABLE",
		"TIMEOUT", "DEADLINE_EXCEEDED", "TRANSIENT_ERROR", "TRY_AGAIN":
		return fmt.Errorf("linear transient graphql error (%s)", code)
	default:
		if code == "" {
			return boardsync.Permanent(errors.New("linear graphql error"))
		}
		return boardsync.Permanent(fmt.Errorf("linear graphql error %s", code))
	}
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
	request.Header.Set("Authorization", c.authorization())
	response, err := c.http.Do(request)
	if err != nil {
		return boardsync.RedactError(err, c.token)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if err != nil {
		return err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return classifyStatus(response.StatusCode, response.Header)
	}
	var decoded graphqlResponse
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return fmt.Errorf("linear answered unreadable JSON: %w", err)
	}
	if len(decoded.Errors) > 0 {
		return classifyGraphQL(decoded.Errors[0])
	}
	if output != nil && len(decoded.Data) > 0 {
		if err := json.Unmarshal(decoded.Data, output); err != nil {
			return fmt.Errorf("linear answered unreadable data: %w", err)
		}
	}
	return nil
}

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

const commentCreateMutation = `mutation CommentCreate($issueId: String!, $body: String!, $parentId: String) {
  commentCreate(input: { issueId: $issueId, body: $body, parentId: $parentId }) { success comment { id } }
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

const issueRefByIDQuery = `query IssueRefByID($id: String!) {
  issue(id: $id) { id identifier team { id key } }
}`

// IssueRefByID reads the identity a comment needs without the issue's body.
func (c *Client) IssueRefByID(ctx context.Context, id string) (issueRef, error) {
	var out struct {
		Issue issueRef `json:"issue"`
	}
	if err := c.do(ctx, issueRefByIDQuery, map[string]any{"id": id}, &out); err != nil {
		return issueRef{}, err
	}
	if out.Issue.ID == "" {
		return issueRef{}, fmt.Errorf("%w: no linear issue %s", boardsync.ErrNotFound, id)
	}
	return out.Issue, nil
}

// IssueByID reads one issue.
func (c *Client) IssueByID(ctx context.Context, id string) (issueData, error) {
	var out struct {
		Issue issueData `json:"issue"`
	}
	if err := c.do(ctx, issueByIDQuery, map[string]any{"id": id}, &out); err != nil {
		return issueData{}, err
	}
	if out.Issue.ID == "" {
		return issueData{}, fmt.Errorf("%w: no linear issue %s", boardsync.ErrNotFound, id)
	}
	return out.Issue, nil
}

// CreateComment comments on an issue, or replies to a parent comment.
func (c *Client) CreateComment(ctx context.Context, issueID, body, parentID string) (string, error) {
	var out struct {
		CommentCreate struct {
			Success bool `json:"success"`
			Comment struct {
				ID string `json:"id"`
			} `json:"comment"`
		} `json:"commentCreate"`
	}
	variables := map[string]any{"issueId": issueID, "body": body}
	if parentID != "" {
		variables["parentId"] = parentID
	}
	if err := c.do(ctx, commentCreateMutation, variables, &out); err != nil {
		return "", err
	}
	if !out.CommentCreate.Success || out.CommentCreate.Comment.ID == "" {
		return "", boardsync.Permanent(errors.New("linear commentCreate failed"))
	}
	return out.CommentCreate.Comment.ID, nil
}

// CreateIssue opens an issue in a team with its board properties.
func (c *Client) CreateIssue(ctx context.Context, teamID, title, description string, update issueUpdate) (issueRef, error) {
	input := newGraphqlInput(map[string]any{})
	input.add("teamId", "String!", teamID)
	input.add("title", "String!", title)
	input.add("description", "String!", description)
	update.SetDescription = false
	input.addIssueProperties(update)
	query := fmt.Sprintf("mutation IssueCreate(%s) { issueCreate(input: { %s }) { success issue { id identifier team { key } } } }",
		strings.Join(input.defs, ", "), strings.Join(input.fields, ", "))
	var out struct {
		IssueCreate struct {
			Success bool     `json:"success"`
			Issue   issueRef `json:"issue"`
		} `json:"issueCreate"`
	}
	if err := c.do(ctx, query, input.variables, &out); err != nil {
		return issueRef{}, err
	}
	if !out.IssueCreate.Success || out.IssueCreate.Issue.ID == "" {
		return issueRef{}, boardsync.Permanent(errors.New("linear issueCreate failed"))
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
		return boardsync.Permanent(errors.New("linear commentUpdate failed"))
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
		return boardsync.Permanent(errors.New("linear commentDelete failed"))
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
		return boardsync.Permanent(errors.New("linear reactionCreate failed"))
	}
	return nil
}

// TeamID resolves a team key to its ID for issue creation.
func (c *Client) TeamID(ctx context.Context, key string) (string, error) {
	teamInfo, err := c.TeamByKey(ctx, key)
	if err != nil {
		return "", err
	}
	return teamInfo.ID, nil
}
