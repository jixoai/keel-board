package linear

import (
	"context"
	"fmt"

	boardsync "github.com/jixoai/keel-board/sync"
)

type pageInfo struct {
	HasNextPage bool   `json:"hasNextPage"`
	EndCursor   string `json:"endCursor"`
}

type issueComment struct {
	ID        string `json:"id"`
	Body      string `json:"body"`
	URL       string `json:"url"`
	CreatedAt string `json:"createdAt"`
	UpdatedAt string `json:"updatedAt"`
	User      *user  `json:"user"`
}

type commentConnection struct {
	Nodes    []issueComment `json:"nodes"`
	PageInfo pageInfo       `json:"pageInfo"`
}

type issueRead struct {
	issueData
	Comments commentConnection `json:"comments"`
}

type issuePage struct {
	Nodes    []issueRead `json:"nodes"`
	PageInfo pageInfo    `json:"pageInfo"`
}

type projectInfo struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	URL    string `json:"url"`
	SlugID string `json:"slugId"`
	Teams  struct {
		Nodes []team `json:"nodes"`
	} `json:"teams"`
}

const readTeamQuery = `query ReadTeam($key: String!) {
  teams(filter: { key: { eq: $key } }) { nodes { id key name } }
}`

const readIssuesQuery = `query ReadIssues($filter: IssueFilter, $first: Int!, $after: String) {
  issues(filter: $filter, first: $first, after: $after) {
    nodes {
      id identifier title description url createdAt updatedAt
      state { id name type } priority priorityLabel
      project { id name url }
      labels { nodes { id name } }
      assignee { id name displayName url avatarUrl }
      creator { id name displayName url avatarUrl }
      comments(first: 20) { nodes { id } }
    }
    pageInfo { hasNextPage endCursor }
  }
}`

const readIssueQuery = `query ReadIssue($id: String!, $first: Int!, $after: String) {
  issue(id: $id) {
    id identifier title description url createdAt updatedAt
    state { id name type } priority priorityLabel
    project { id name url }
    labels { nodes { id name } }
    assignee { id name displayName url avatarUrl }
    creator { id name displayName url avatarUrl }
    comments(first: $first, after: $after) {
      nodes { id body url createdAt updatedAt user { id name displayName url avatarUrl } }
      pageInfo { hasNextPage endCursor }
    }
  }
}`

const readProjectQuery = `query ReadProject($id: String!) {
  project(id: $id) { id name url slugId teams(first: 50) { nodes { id key name } } }
}`

const readProjectSlugQuery = `query ReadProjectSlug($slugId: String!) {
  projects(filter: { slugId: { eq: $slugId } }, first: 1) {
    nodes { id name url slugId teams(first: 50) { nodes { id key name } } }
  }
}`

const commentURLQuery = `query CommentURL($id: String!) { comment(id: $id) { id url } }`

// TeamByKey reads one team.
func (c *Client) TeamByKey(ctx context.Context, key string) (team, error) {
	var out struct {
		Teams struct {
			Nodes []team `json:"nodes"`
		} `json:"teams"`
	}
	if err := c.do(ctx, readTeamQuery, map[string]any{"key": key}, &out); err != nil {
		return team{}, err
	}
	if len(out.Teams.Nodes) == 0 || out.Teams.Nodes[0].ID == "" {
		return team{}, fmt.Errorf("%w: no linear team %s", boardsync.ErrNotFound, key)
	}
	return out.Teams.Nodes[0], nil
}

// Issues reads one page of issues matching filter.
func (c *Client) Issues(ctx context.Context, filter map[string]any, first int, after string) (issuePage, error) {
	var out struct {
		Issues issuePage `json:"issues"`
	}
	variables := map[string]any{"filter": filter, "first": first}
	if after != "" {
		variables["after"] = after
	}
	if err := c.do(ctx, readIssuesQuery, variables, &out); err != nil {
		return issuePage{}, err
	}
	return out.Issues, nil
}

// IssueWithComments reads one issue and a page of its comments.
func (c *Client) IssueWithComments(ctx context.Context, id string, first int, after string) (issueRead, error) {
	var out struct {
		Issue issueRead `json:"issue"`
	}
	variables := map[string]any{"id": id, "first": first}
	if after != "" {
		variables["after"] = after
	}
	if err := c.do(ctx, readIssueQuery, variables, &out); err != nil {
		return issueRead{}, err
	}
	if out.Issue.ID == "" {
		return issueRead{}, fmt.Errorf("%w: no linear issue %s", boardsync.ErrNotFound, id)
	}
	return out.Issue, nil
}

// ProjectByID reads one project by its Linear ID.
func (c *Client) ProjectByID(ctx context.Context, id string) (projectInfo, error) {
	var out struct {
		Project projectInfo `json:"project"`
	}
	if err := c.do(ctx, readProjectQuery, map[string]any{"id": id}, &out); err != nil {
		return projectInfo{}, err
	}
	if out.Project.ID == "" {
		return projectInfo{}, fmt.Errorf("%w: no linear project %s", boardsync.ErrNotFound, id)
	}
	return out.Project, nil
}

// ProjectBySlug reads one project by the slug in its link.
func (c *Client) ProjectBySlug(ctx context.Context, slugID string) (projectInfo, error) {
	var out struct {
		Projects struct {
			Nodes []projectInfo `json:"nodes"`
		} `json:"projects"`
	}
	if err := c.do(ctx, readProjectSlugQuery, map[string]any{"slugId": slugID}, &out); err != nil {
		return projectInfo{}, err
	}
	if len(out.Projects.Nodes) == 0 || out.Projects.Nodes[0].ID == "" {
		return projectInfo{}, fmt.Errorf("%w: no linear project %s", boardsync.ErrNotFound, slugID)
	}
	return out.Projects.Nodes[0], nil
}

// CommentURL reads a comment's Linear link.
func (c *Client) CommentURL(ctx context.Context, id string) (string, error) {
	var out struct {
		Comment struct {
			ID  string `json:"id"`
			URL string `json:"url"`
		} `json:"comment"`
	}
	if err := c.do(ctx, commentURLQuery, map[string]any{"id": id}, &out); err != nil {
		return "", err
	}
	if out.Comment.ID == "" {
		return "", fmt.Errorf("%w: no linear comment %s", boardsync.ErrNotFound, id)
	}
	return out.Comment.URL, nil
}
