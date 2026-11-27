package linear

import (
	"context"
	"fmt"
	"sort"
	"strings"

	boardsync "github.com/jixoai/keel-board/sync"
)

// issueUpdate is one issueUpdate call's fields, already resolved to Linear
// UUIDs. The Set flags distinguish "leave unchanged" from "clear".
type issueUpdate struct {
	SetDescription bool
	Description    string

	SetState bool
	StateID  string

	SetPriority bool
	Priority    int

	SetProject bool
	ProjectID  *string

	SetAssignee bool
	AssigneeID  *string

	SetLabels bool
	LabelIDs  []string
}

func (u issueUpdate) empty() bool {
	return !u.SetDescription && !u.SetState && !u.SetPriority &&
		!u.SetProject && !u.SetAssignee && !u.SetLabels
}

// Labels implements boardsync.Labeler: a labels.changed event updates the
// issue's state, priority, project, tags, and assignee without editing text.
func (p *Provider) Labels(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, labels map[string]string) error {
	if thread.Platform != "linear" {
		return fmt.Errorf("%w: thread platform %q isn't linear", boardsync.ErrBadRequest, thread.Platform)
	}
	if thread.Thread == "" {
		// A team channel isn't one issue; a label change there has no target.
		return nil
	}
	teamKey, issueID, err := p.threadWhere(key, thread)
	if err != nil {
		return err
	}
	update, err := p.resolveIssueUpdate(ctx, teamKey, labels)
	if err != nil {
		return err
	}
	if update.empty() {
		return nil
	}
	return p.client.UpdateIssue(ctx, issueID, update)
}

// resolveIssueUpdate translates board labels into Linear property IDs. A nil
// map leaves everything unchanged; a non-nil map is the complete state, so an
// absent tag/assignee/project clears the Linear value.
func (p *Provider) resolveIssueUpdate(ctx context.Context, teamKey string, labels map[string]string) (issueUpdate, error) {
	if labels == nil {
		return issueUpdate{}, nil
	}
	var update issueUpdate
	if value, ok := labels["state"]; ok {
		if strings.TrimSpace(value) == "" {
			return issueUpdate{}, fmt.Errorf("%w: empty linear state label", boardsync.ErrBadRequest)
		}
		stateID, err := p.client.WorkflowStateID(ctx, teamKey, value)
		if err != nil {
			return issueUpdate{}, err
		}
		update.SetState, update.StateID = true, stateID
	}
	if value, ok := labels["priority"]; ok {
		priority, err := parsePriority(value)
		if err != nil {
			return issueUpdate{}, err
		}
		update.SetPriority, update.Priority = true, priority
	}
	update.SetProject = true
	if value, ok := labels["project"]; ok && strings.TrimSpace(value) != "" {
		projectID, err := p.client.ProjectIDByName(ctx, teamKey, value)
		if err != nil {
			return issueUpdate{}, err
		}
		update.ProjectID = &projectID
	}
	update.SetAssignee = true
	handles := labelValues(labels, "assignee.")
	sort.Strings(handles)
	if len(handles) > 0 {
		userID, err := p.client.UserIDByHandle(ctx, handles[0])
		if err != nil {
			return issueUpdate{}, err
		}
		update.AssigneeID = &userID
	}
	update.SetLabels = true
	names := labelValues(labels, "tag.")
	sort.Strings(names)
	seen := make(map[string]bool, len(names))
	var teamID string
	for _, name := range names {
		if teamID == "" {
			var err error
			teamID, err = p.client.TeamID(ctx, teamKey)
			if err != nil {
				return issueUpdate{}, err
			}
		}
		labelID, err := p.client.EnsureIssueLabel(ctx, teamKey, teamID, name)
		if err != nil {
			return issueUpdate{}, err
		}
		if !seen[labelID] {
			seen[labelID] = true
			update.LabelIDs = append(update.LabelIDs, labelID)
		}
	}
	return update, nil
}

func labelValues(labels map[string]string, prefix string) []string {
	values := make([]string, 0, len(labels))
	for key, value := range labels {
		if strings.HasPrefix(key, prefix) && strings.TrimSpace(value) != "" {
			values = append(values, value)
		}
	}
	return values
}

func parsePriority(value string) (int, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "0", "none", "no priority":
		return 0, nil
	case "1", "urgent":
		return 1, nil
	case "2", "high":
		return 2, nil
	case "3", "medium":
		return 3, nil
	case "4", "low":
		return 4, nil
	default:
		return 0, fmt.Errorf("%w: %q isn't a linear priority", boardsync.ErrBadRequest, value)
	}
}

const workflowStatesQuery = `query WorkflowStates($team: String!) {
  workflowStates(filter: { team: { key: { eq: $team } } }, first: 250) {
    nodes { id name type }
  }
}`

// WorkflowStateID resolves a state type or name to its UUID in a team.
func (c *Client) WorkflowStateID(ctx context.Context, teamKey, value string) (string, error) {
	var out struct {
		WorkflowStates struct {
			Nodes []workflowState `json:"nodes"`
		} `json:"workflowStates"`
	}
	if err := c.do(ctx, workflowStatesQuery, map[string]any{"team": teamKey}, &out); err != nil {
		return "", err
	}
	wanted := strings.ToLower(strings.TrimSpace(value))
	for _, state := range out.WorkflowStates.Nodes {
		if strings.EqualFold(state.ID, value) ||
			strings.EqualFold(state.Type, wanted) || strings.EqualFold(state.Name, wanted) {
			return state.ID, nil
		}
	}
	return "", fmt.Errorf("%w: linear team %s has no state %q", boardsync.ErrBadRequest, teamKey, value)
}

const projectByNameQuery = `query ProjectByName($name: String!, $team: String!) {
  projects(filter: { name: { eq: $name }, accessibleTeams: { some: { key: { eq: $team } } } }, first: 1) {
    nodes { id name }
  }
}`

// ProjectIDByName resolves a project's display name in a team.
func (c *Client) ProjectIDByName(ctx context.Context, teamKey, name string) (string, error) {
	var out struct {
		Projects struct {
			Nodes []projectInfo `json:"nodes"`
		} `json:"projects"`
	}
	if err := c.do(ctx, projectByNameQuery, map[string]any{"name": name, "team": teamKey}, &out); err != nil {
		return "", err
	}
	if len(out.Projects.Nodes) == 0 || out.Projects.Nodes[0].ID == "" {
		return "", fmt.Errorf("%w: linear team %s has no project %q", boardsync.ErrNotFound, teamKey, name)
	}
	return out.Projects.Nodes[0].ID, nil
}

const issueLabelByNameQuery = `query IssueLabelByName($name: String!) {
  issueLabels(filter: { name: { eq: $name } }, first: 10) {
    nodes { id name team { id key } }
  }
}`

const issueLabelCreateMutation = `mutation IssueLabelCreate($name: String!, $teamId: String!) {
  issueLabelCreate(input: { name: $name, teamId: $teamId }) {
    success
    issueLabel { id }
  }
}`

// EnsureIssueLabel finds a workspace or team label by name, creating a team
// label when Linear has none.
func (c *Client) EnsureIssueLabel(ctx context.Context, teamKey, teamID, name string) (string, error) {
	name = clampText(name, 80)
	if strings.TrimSpace(name) == "" {
		return "", fmt.Errorf("%w: empty linear label name", boardsync.ErrBadRequest)
	}
	var found struct {
		IssueLabels struct {
			Nodes []struct {
				ID   string `json:"id"`
				Name string `json:"name"`
				Team *team  `json:"team"`
			} `json:"nodes"`
		} `json:"issueLabels"`
	}
	if err := c.do(ctx, issueLabelByNameQuery, map[string]any{"name": name}, &found); err != nil {
		return "", err
	}
	for _, label := range found.IssueLabels.Nodes {
		if label.Team == nil || label.Team.Key == "" || strings.EqualFold(label.Team.Key, teamKey) {
			return label.ID, nil
		}
	}
	var created struct {
		IssueLabelCreate struct {
			Success    bool `json:"success"`
			IssueLabel struct {
				ID string `json:"id"`
			} `json:"issueLabel"`
		} `json:"issueLabelCreate"`
	}
	if err := c.do(ctx, issueLabelCreateMutation, map[string]any{"name": name, "teamId": teamID}, &created); err != nil {
		return "", err
	}
	if !created.IssueLabelCreate.Success || created.IssueLabelCreate.IssueLabel.ID == "" {
		return "", boardsync.Permanent(fmt.Errorf("linear issueLabelCreate failed"))
	}
	return created.IssueLabelCreate.IssueLabel.ID, nil
}

const usersPageQuery = `query UsersPage($first: Int!, $after: String) {
  users(first: $first, after: $after) {
    nodes { id name displayName url }
    pageInfo { hasNextPage endCursor }
  }
}`

// UserIDByHandle finds a workspace user by profile handle, display name, or
// full name. Linear's users query has no server-side filter, so pages are read
// and matched here.
func (c *Client) UserIDByHandle(ctx context.Context, handle string) (string, error) {
	handle = strings.TrimSpace(handle)
	if handle == "" {
		return "", fmt.Errorf("%w: empty linear assignee", boardsync.ErrBadRequest)
	}
	cursor := ""
	for page := 0; page < 10; page++ {
		var out struct {
			Users struct {
				Nodes    []user   `json:"nodes"`
				PageInfo pageInfo `json:"pageInfo"`
			} `json:"users"`
		}
		variables := map[string]any{"first": 250}
		if cursor != "" {
			variables["after"] = cursor
		}
		if err := c.do(ctx, usersPageQuery, variables, &out); err != nil {
			return "", err
		}
		for i := range out.Users.Nodes {
			user := &out.Users.Nodes[i]
			if strings.EqualFold(handleOf(user), handle) ||
				strings.EqualFold(user.DisplayName, handle) || strings.EqualFold(user.Name, handle) {
				return user.ID, nil
			}
		}
		if !out.Users.PageInfo.HasNextPage || out.Users.PageInfo.EndCursor == "" {
			break
		}
		cursor = out.Users.PageInfo.EndCursor
	}
	return "", fmt.Errorf("%w: no linear user %q", boardsync.ErrNotFound, handle)
}

// graphqlInput builds a mutation's variable definitions and input fields.
type graphqlInput struct {
	defs      []string
	fields    []string
	variables map[string]any
}

func newGraphqlInput(variables map[string]any) *graphqlInput {
	return &graphqlInput{variables: variables}
}

func (b *graphqlInput) add(name, typ string, value any) {
	b.defs = append(b.defs, "$"+name+": "+typ)
	b.fields = append(b.fields, name+": $"+name)
	b.variables[name] = value
}

func (b *graphqlInput) addIssueProperties(update issueUpdate) {
	if update.SetDescription {
		b.add("description", "String", update.Description)
	}
	if update.SetState {
		b.add("stateId", "String", update.StateID)
	}
	if update.SetPriority {
		b.add("priority", "Int", update.Priority)
	}
	if update.SetProject {
		var value any
		if update.ProjectID != nil {
			value = *update.ProjectID
		}
		b.add("projectId", "String", value)
	}
	if update.SetAssignee {
		var value any
		if update.AssigneeID != nil {
			value = *update.AssigneeID
		}
		b.add("assigneeId", "String", value)
	}
	if update.SetLabels {
		labelIDs := update.LabelIDs
		if labelIDs == nil {
			labelIDs = []string{}
		}
		b.add("labelIds", "[String!]", labelIDs)
	}
}

// UpdateIssue sends one issueUpdate with only the fields the update sets.
func (c *Client) UpdateIssue(ctx context.Context, id string, update issueUpdate) error {
	if update.empty() {
		return nil
	}
	input := newGraphqlInput(map[string]any{"id": id})
	input.add("id", "String!", id)
	input.addIssueProperties(update)
	query := fmt.Sprintf("mutation IssueUpdate(%s) { issueUpdate(id: $id, input: { %s }) { success } }",
		strings.Join(input.defs, ", "), strings.Join(input.fields, ", "))
	var out struct {
		IssueUpdate struct {
			Success bool `json:"success"`
		} `json:"issueUpdate"`
	}
	if err := c.do(ctx, query, input.variables, &out); err != nil {
		return err
	}
	if !out.IssueUpdate.Success {
		return boardsync.Permanent(fmt.Errorf("linear issueUpdate failed"))
	}
	return nil
}
