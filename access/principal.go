package access

import (
	"encoding/json"
	"sort"
	"strings"

	"github.com/jixoai/keel-board/protocol"
)

// PrincipalKind is what a principal is. Grant principals name people, a
// project's deployments, a repository's runs, an issuer's tokens, an app, a
// group of any of those, or everyone. A caller's kind is one of the first
// five; the group kinds only appear in grants.
type PrincipalKind string

const (
	// PrincipalUser is a person, by ID (or a login that stands for it).
	PrincipalUser PrincipalKind = "user"
	// PrincipalProject is a project's deployments in its environments.
	PrincipalProject PrincipalKind = "project"
	// PrincipalGitHub is a GitHub Actions run in a repository.
	PrincipalGitHub PrincipalKind = "github"
	// PrincipalOIDC is an issuer's token carrying the grant's claims.
	PrincipalOIDC PrincipalKind = "oidc"
	// PrincipalApp is an app, by its client ID.
	PrincipalApp PrincipalKind = "app"
	// PrincipalAllUsers names every person on the board's team.
	PrincipalAllUsers PrincipalKind = "all-users"
	// PrincipalAllProjects names every project on the board's team.
	PrincipalAllProjects PrincipalKind = "all-projects"
	// PrincipalAllApps names every app installed on the board's team.
	PrincipalAllApps PrincipalKind = "all-apps"
	// PrincipalEveryone names everyone something else lets in. It never lets
	// anyone in on its own.
	PrincipalEveryone PrincipalKind = "everyone"
)

// Principal is who a grant names, or who a caller is. One struct serves both:
// a grant principal fills the fields its kind uses (and may name several
// environments), a caller fills the one environment its token names and the
// verified refs and claims it carries. Fields a kind doesn't use are ignored.
type Principal struct {
	Kind PrincipalKind `json:"kind"`

	// A user: its ID, and optionally the email it stands for.
	UserID string `json:"userId,omitempty"`
	Email  string `json:"email,omitempty"`

	// A project: its ID, the environments its deployments get in from, and the
	// team it belongs to.
	ProjectID    string   `json:"projectId,omitempty"`
	Environments []string `json:"environments,omitempty"`
	TeamID       string   `json:"teamId,omitempty"`
	// Environment is a caller project's one environment.
	Environment string `json:"environment,omitempty"`

	// A GitHub run: the repository, and the refs a grant may narrow it by.
	Repository   string `json:"repository,omitempty"`
	RepositoryID string `json:"repositoryId,omitempty"`
	Workflow     string `json:"workflow,omitempty"`
	Branch       string `json:"branch,omitempty"`
	Event        string `json:"event,omitempty"`
	// WorkflowRef and Ref are a caller's own token refs.
	WorkflowRef string `json:"workflowRef,omitempty"`
	Ref         string `json:"ref,omitempty"`

	// An OIDC token: its issuer, and every claim a grant requires.
	Issuer string              `json:"issuer,omitempty"`
	Claims map[string][]string `json:"claims,omitempty"`

	// An app: its client ID.
	ClientID string `json:"clientId,omitempty"`
}

// UserPrincipal returns a user principal.
func UserPrincipal(userID string) Principal {
	return Principal{Kind: PrincipalUser, UserID: userID}
}

// ProjectPrincipal returns a caller project in environment.
func ProjectPrincipal(projectID, environment string) Principal {
	return Principal{Kind: PrincipalProject, ProjectID: projectID, Environment: environment}
}

// GitHubPrincipal returns a caller GitHub run.
func GitHubPrincipal(repository string) Principal {
	return Principal{Kind: PrincipalGitHub, Repository: repository}
}

// OIDCPrincipal returns an OIDC caller token.
func OIDCPrincipal(issuer string, claims map[string][]string) Principal {
	return Principal{Kind: PrincipalOIDC, Issuer: issuer, Claims: claims}
}

// AppPrincipal returns an app caller.
func AppPrincipal(clientID string) Principal {
	return Principal{Kind: PrincipalApp, ClientID: clientID}
}

// EveryonePrincipal returns the principal a grant names to shape everyone
// something else lets in.
func EveryonePrincipal() Principal { return Principal{Kind: PrincipalEveryone} }

// AllUsersPrincipal returns the group of every person on the team.
func AllUsersPrincipal() Principal { return Principal{Kind: PrincipalAllUsers} }

// AllProjectsPrincipal returns the group of every project on the team, in
// environments.
func AllProjectsPrincipal(environments ...string) Principal {
	return Principal{Kind: PrincipalAllProjects, Environments: environments}
}

// AllAppsPrincipal returns the group of every app installed on the team.
func AllAppsPrincipal() Principal { return Principal{Kind: PrincipalAllApps} }

// IsGroup reports whether a principal kind is a group rather than one
// principal.
func (k PrincipalKind) IsGroup() bool {
	switch k {
	case PrincipalAllUsers, PrincipalAllProjects, PrincipalAllApps, PrincipalEveryone:
		return true
	default:
		return false
	}
}

// ValidPrincipalKind reports whether k is a kind a grant may name.
func ValidPrincipalKind(k PrincipalKind) bool {
	switch k {
	case PrincipalUser, PrincipalProject, PrincipalGitHub, PrincipalOIDC, PrincipalApp,
		PrincipalAllUsers, PrincipalAllProjects, PrincipalAllApps, PrincipalEveryone:
		return true
	default:
		return false
	}
}

// NormalizePrincipal checks a grant principal and fills what its kind implies:
// a user's email becomes its ID, a project's environments default to
// production, a workflow becomes its path, and a branch loses refs/heads/.
// It is for the principals a grant names, not for callers.
func NormalizePrincipal(principal Principal) (Principal, error) {
	out := principal
	out.UserID = strings.TrimSpace(principal.UserID)
	out.Email = strings.TrimSpace(principal.Email)
	out.ProjectID = strings.TrimSpace(principal.ProjectID)
	out.Repository = strings.TrimSpace(principal.Repository)
	out.RepositoryID = strings.TrimSpace(principal.RepositoryID)
	out.Workflow = strings.TrimSpace(principal.Workflow)
	out.Branch = strings.TrimSpace(principal.Branch)
	out.Event = strings.TrimSpace(principal.Event)
	out.Issuer = strings.TrimSpace(principal.Issuer)
	out.ClientID = strings.TrimSpace(principal.ClientID)
	out.TeamID = strings.TrimSpace(principal.TeamID)

	if !ValidPrincipalKind(principal.Kind) {
		return Principal{}, invalidGrantf("unknown principal kind %q", principal.Kind)
	}

	switch principal.Kind {
	case PrincipalUser:
		if out.UserID != "" && out.Email != "" && !strings.EqualFold(out.UserID, out.Email) {
			return Principal{}, invalidGrantf("a user is its email or its userId, not both")
		}
		if out.UserID == "" {
			if out.Email == "" {
				return Principal{}, invalidGrantf("a user principal names a userId (or an email)")
			}
			out.UserID = strings.ToLower(out.Email)
		}
		if len(out.UserID) > 200 || !protocol.Storable(out.UserID) || strings.ContainsAny(out.UserID, " \t\r\n") {
			return Principal{}, invalidGrantf("a user principal's ID is 1 to 200 characters without spaces, got %q", out.UserID)
		}
		if out.Email != "" {
			out.Email = strings.ToLower(out.Email)
			if len(out.Email) < 3 || len(out.Email) > 320 || !strings.Contains(out.Email, "@") {
				return Principal{}, invalidGrantf("%q isn't an email a user is known by", out.Email)
			}
		}
	case PrincipalProject:
		if out.ProjectID == "" {
			return Principal{}, invalidGrantf("a project principal names a projectId")
		}
		if len(out.ProjectID) > 200 || !protocol.Storable(out.ProjectID) {
			return Principal{}, invalidGrantf("a project principal's ID is at most 200 characters")
		}
		out.Environments = normalizeEnvironments(principal.Environments)
	case PrincipalGitHub:
		if out.Repository == "" && out.RepositoryID == "" {
			return Principal{}, invalidGrantf("a github principal names a repository (or its ID)")
		}
		if len(out.Repository) > 200 {
			return Principal{}, invalidGrantf("a github principal's repository is at most 200 characters")
		}
		if out.Repository != "" && !strings.Contains(out.Repository, "/") {
			return Principal{}, invalidGrantf("a github principal's repository is owner/name, got %q", out.Repository)
		}
		if out.Workflow != "" {
			out.Workflow = workflowPath(out.Workflow)
		}
		out.Branch = strings.TrimPrefix(out.Branch, "refs/heads/")
	case PrincipalOIDC:
		if out.Issuer == "" {
			return Principal{}, invalidGrantf("an oidc principal names an issuer")
		}
		if len(out.Issuer) > 1000 {
			return Principal{}, invalidGrantf("an oidc issuer is at most 1000 characters")
		}
		claims, err := normalizeClaims(principal.Claims)
		if err != nil {
			return Principal{}, err
		}
		out.Claims = claims
	case PrincipalApp:
		if out.ClientID == "" {
			return Principal{}, invalidGrantf("an app principal names a clientId")
		}
		if len(out.ClientID) > 200 || !protocol.Storable(out.ClientID) || strings.ContainsAny(out.ClientID, " \t\r\n") {
			return Principal{}, invalidGrantf("an app principal's client ID is 1 to 200 characters without spaces")
		}
	case PrincipalAllProjects:
		out.Environments = normalizeEnvironments(principal.Environments)
	case PrincipalAllUsers, PrincipalAllApps, PrincipalEveryone:
		// Nothing to fill.
	}
	return out, nil
}

// principalKey is a principal's identity as one string. Two principals with
// the same key are the same principal.
func principalKey(principal Principal) string {
	encoded, err := json.Marshal(principal)
	if err != nil {
		return ""
	}
	return string(encoded)
}

// normalizeEnvironments dedupes environments and puts production, preview,
// and development first, then custom ones by name.
func normalizeEnvironments(environments []string) []string {
	order := map[string]int{"production": 0, "preview": 1, "development": 2}
	seen := make(map[string]struct{}, len(environments))
	out := make([]string, 0, len(environments))
	for _, environment := range environments {
		value := strings.TrimSpace(environment)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool {
		left, leftKnown := order[out[i]]
		right, rightKnown := order[out[j]]
		if leftKnown != rightKnown {
			return leftKnown
		}
		if leftKnown {
			return left < right
		}
		return out[i] < out[j]
	})
	if len(out) == 0 {
		return []string{"production"}
	}
	return out
}

// environmentAllowed reports whether a caller's environment is one a grant
// names. A grant that names none is its production deployments.
func environmentAllowed(environments []string, environment string) bool {
	if len(environments) == 0 {
		environments = []string{"production"}
	}
	for _, allowed := range environments {
		if allowed == environment {
			return true
		}
	}
	return false
}

// workflowPath names a workflow the way a GitHub token does: `ci.yml` is
// `.github/workflows/ci.yml`.
func workflowPath(workflow string) string {
	if strings.Contains(workflow, "/") {
		return workflow
	}
	return ".github/workflows/" + workflow
}

// SubPrefix returns the prefix a `sub` pattern matches, the part before a
// trailing `*` that follows `:` or `/`. It returns false for any other value:
// a `*` only ends a `sub` pattern, so no pattern lets `acme-evil` in for
// `acme`.
func SubPrefix(value string) (string, bool) {
	if !strings.HasSuffix(value, "*") {
		return "", false
	}
	prefix := strings.TrimSuffix(value, "*")
	if len(prefix) < 2 || strings.Contains(prefix, "*") {
		return "", false
	}
	last := prefix[len(prefix)-1]
	if last != ':' && last != '/' {
		return "", false
	}
	return prefix, true
}

// normalizeClaims checks an OIDC principal's claims: at least a sub, no empty
// value, and a sub's `*` only where SubPrefix allows it.
func normalizeClaims(claims map[string][]string) (map[string][]string, error) {
	if len(claims) == 0 {
		return nil, invalidGrantf("an oidc principal names a sub, like project_path:acme/api:*")
	}
	if len(claims) > 32 {
		return nil, invalidGrantf("an oidc principal names at most 32 claims")
	}
	out := make(map[string][]string, len(claims))
	hasSub := false
	for name, values := range claims {
		if name == "" || len(name) > 200 {
			return nil, invalidGrantf("a claim's name is 1 to 200 characters")
		}
		if len(values) == 0 {
			return nil, invalidGrantf("claim %s has no value to match", name)
		}
		if len(values) > 32 {
			return nil, invalidGrantf("claim %s lists at most 32 values", name)
		}
		seen := make(map[string]struct{}, len(values))
		normalized := make([]string, 0, len(values))
		for _, value := range values {
			if value == "" {
				return nil, invalidGrantf("claim %s has an empty value", name)
			}
			if len(value) > 1000 {
				return nil, invalidGrantf("claim %s has a value of more than 1000 characters", name)
			}
			if name == "sub" {
				hasSub = true
				if value == "*" {
					return nil, invalidGrantf("sub * is everyone the issuer serves: name a sub, or how one starts")
				}
				if strings.Contains(value, "*") {
					if _, ok := SubPrefix(value); !ok {
						return nil, invalidGrantf("a sub's * goes last, right after a : or a /, like project_path:acme/api:*, got %q", value)
					}
				}
			}
			if _, ok := seen[value]; ok {
				continue
			}
			seen[value] = struct{}{}
			normalized = append(normalized, value)
		}
		sort.Strings(normalized)
		out[name] = normalized
	}
	if !hasSub {
		return nil, invalidGrantf("an oidc principal names a sub, like project_path:acme/api:*")
	}
	return out, nil
}

// claimsMatch reports whether caller holds every claim a grant requires. A
// `sub` value ending in `*` matches how a sub starts; every other value is
// exact.
func claimsMatch(required, held map[string][]string) bool {
	for name, allowed := range required {
		values, ok := held[name]
		if !ok || len(values) == 0 {
			return false
		}
		matched := false
		for _, expected := range allowed {
			prefix, wildcard := "", false
			if name == "sub" {
				prefix, wildcard = SubPrefix(expected)
			}
			for _, value := range values {
				if wildcard {
					if strings.HasPrefix(value, prefix) {
						matched = true
					}
				} else if value == expected {
					matched = true
				}
				if matched {
					break
				}
			}
			if matched {
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

// claimsEqual reports whether two claims maps hold the same values.
func claimsEqual(left, right map[string][]string) bool {
	if len(left) != len(right) {
		return false
	}
	for name, leftValues := range left {
		rightValues, ok := right[name]
		if !ok || len(leftValues) != len(rightValues) {
			return false
		}
		remaining := make(map[string]int, len(rightValues))
		for _, value := range rightValues {
			remaining[value]++
		}
		for _, value := range leftValues {
			if remaining[value] == 0 {
				return false
			}
			remaining[value]--
		}
	}
	return true
}
