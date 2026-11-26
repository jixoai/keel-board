package access

import (
	"fmt"
	"strings"
	"time"

	"github.com/jixoai/keel-board/protocol"
)

// MaxGrantPrincipals is how many principals one grant may name.
const MaxGrantPrincipals = 32

// Grant names principals and says what they may do, in one can for all of
// them. The same principals again are another grant.
type Grant struct {
	ID         string      `json:"id,omitempty"`
	Tenant     string      `json:"tenant,omitempty"`
	Principals []Principal `json:"principals"`
	Can        Can         `json:"can"`
	// Capped records whether a person's team role caps what this grant gives.
	// It is display metadata: the cap itself comes from the caller's facts.
	Capped    bool      `json:"capped,omitempty"`
	CreatedAt time.Time `json:"createdAt,omitempty"`
	UpdatedAt time.Time `json:"updatedAt,omitempty"`
}

// Clone returns a deep copy of the grant.
func (g Grant) Clone() Grant {
	out := g
	out.Principals = make([]Principal, len(g.Principals))
	for index, principal := range g.Principals {
		out.Principals[index] = clonePrincipal(principal)
	}
	out.Can = g.Can.Clone()
	return out
}

// NormalizeGrant checks a grant and fills what its principals and can imply:
// a user's email becomes its ID, a project's environments default to
// production, a workflow becomes its path, and can patterns and permissions
// are checked and de-duplicated. It accepts a grant with no principals, for a
// store validating a full replacement; a create refuses one, and an update
// that would remove the last principal is grant_empty.
func NormalizeGrant(grant Grant) (Grant, error) {
	out := grant.Clone()
	out.ID = strings.TrimSpace(grant.ID)
	if len(out.ID) > 64 || strings.ContainsAny(out.ID, " \t\r\n") {
		return Grant{}, invalidGrantf("a grant's ID is at most 64 characters without spaces")
	}
	if len(out.Principals) > MaxGrantPrincipals {
		return Grant{}, invalidGrantf("a grant names at most %d principals", MaxGrantPrincipals)
	}
	principals := make([]Principal, 0, len(out.Principals))
	seen := make(map[string]struct{}, len(out.Principals))
	for _, principal := range out.Principals {
		normalized, err := NormalizePrincipal(principal)
		if err != nil {
			return Grant{}, err
		}
		key := principalKey(normalized)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		principals = append(principals, normalized)
	}
	out.Principals = principals

	if len(out.Can) == 0 {
		return Grant{}, invalidCanf("a grant's can names at least one channel pattern, like {'*': ['read']}")
	}
	if _, err := out.Can.Entries(); err != nil {
		return Grant{}, err
	}
	can := make(Can, len(out.Can))
	for raw, value := range out.Can {
		pattern, err := ParsePattern(raw)
		if err != nil {
			return Grant{}, err
		}
		if _, err := value.Expand(); err != nil {
			return Grant{}, err
		}
		if value.All {
			can[pattern.String()] = All()
			continue
		}
		can[pattern.String()] = Only(value.Permissions...)
	}
	out.Can = can
	return out, nil
}

// GrantUpdate changes a grant: principals to add, principals to remove, and a
// can that replaces the grant's. A nil Can leaves it as it is.
type GrantUpdate struct {
	Add    []Principal
	Remove []Principal
	Can    *Can
}

// ApplyUpdate returns the grant as update leaves it.
func ApplyUpdate(grant Grant, update GrantUpdate) (Grant, error) {
	next := grant.Clone()
	if len(update.Remove) > 0 {
		removals := make([]Principal, 0, len(update.Remove))
		for _, principal := range update.Remove {
			normalized, err := NormalizePrincipal(principal)
			if err != nil {
				return Grant{}, err
			}
			removals = append(removals, normalized)
		}
		kept := make([]Principal, 0, len(next.Principals))
		for _, principal := range next.Principals {
			removed := false
			for _, removal := range removals {
				if Removes(removal, principal) {
					removed = true
					break
				}
			}
			if !removed {
				kept = append(kept, principal)
			}
		}
		for _, removal := range removals {
			matched := false
			for _, principal := range next.Principals {
				if Removes(removal, principal) {
					matched = true
					break
				}
			}
			if !matched {
				return Grant{}, &protocol.Error{
					Status:  400,
					Code:    CodeNotInGrant,
					Message: fmt.Sprintf("the grant names no %s principal to remove", principalLabel(removal)),
				}
			}
		}
		next.Principals = kept
	}
	for _, principal := range update.Add {
		added, err := NormalizePrincipal(principal)
		if err != nil {
			return Grant{}, err
		}
		joined := false
		for index, existing := range next.Principals {
			if merged, ok := Join(added, existing); ok {
				next.Principals[index] = merged
				joined = true
				break
			}
		}
		if !joined {
			next.Principals = append(next.Principals, added)
		}
	}
	if len(next.Principals) == 0 {
		return Grant{}, &protocol.Error{
			Status:  400,
			Code:    CodeGrantEmpty,
			Message: "the update would leave the grant naming no principal",
		}
	}
	if update.Can != nil {
		next.Can = update.Can.Clone()
	}
	return NormalizeGrant(next)
}

// Removes reports whether removal takes principal out of a grant. A user,
// project, app, or group is the same whatever its environments; a project is
// its ID whatever its environments; a GitHub principal matches the workflow,
// branch, and event it names, any of them where it names none; an OIDC
// principal is its issuer and claims. Both are expected normalized.
func Removes(removal, principal Principal) bool {
	if removal.Kind != principal.Kind {
		return false
	}
	switch removal.Kind {
	case PrincipalUser:
		return removal.UserID != "" && normalizeUserID(removal.UserID) == normalizeUserID(principal.UserID)
	case PrincipalProject:
		return removal.ProjectID != "" && removal.ProjectID == principal.ProjectID
	case PrincipalGitHub:
		if removal.RepositoryID != "" {
			// An ID is the repository's identity: a caller or grant that only
			// knows its name is a different principal, not a fallback.
			if principal.RepositoryID == "" || removal.RepositoryID != principal.RepositoryID {
				return false
			}
		} else if removal.Repository != "" {
			if removal.Repository != principal.Repository {
				return false
			}
		} else {
			return false
		}
		if removal.Workflow != "" && workflowPath(removal.Workflow) != workflowPath(principal.Workflow) {
			return false
		}
		if removal.Branch != "" && strings.TrimPrefix(removal.Branch, "refs/heads/") != strings.TrimPrefix(principal.Branch, "refs/heads/") {
			return false
		}
		if removal.Event != "" && removal.Event != principal.Event {
			return false
		}
		return true
	case PrincipalOIDC:
		return removal.Issuer == principal.Issuer && claimsEqual(removal.Claims, principal.Claims)
	case PrincipalApp:
		return removal.ClientID != "" && removal.ClientID == principal.ClientID
	case PrincipalAllUsers, PrincipalAllProjects, PrincipalAllApps, PrincipalEveryone:
		return true
	default:
		return false
	}
}

// Join reports whether added is the same principal as existing, and if so
// the principal they make: a project's environments add up, and any other
// principal is itself.
func Join(added, existing Principal) (Principal, bool) {
	if added.Kind != existing.Kind {
		return Principal{}, false
	}
	switch added.Kind {
	case PrincipalAllProjects:
		merged := existing
		merged.Environments = normalizeEnvironments(append(append([]string(nil), existing.Environments...), added.Environments...))
		return merged, true
	case PrincipalProject:
		if added.ProjectID != existing.ProjectID {
			return Principal{}, false
		}
		merged := existing
		merged.Environments = normalizeEnvironments(append(append([]string(nil), existing.Environments...), added.Environments...))
		return merged, true
	}
	if principalKey(added) == principalKey(existing) {
		return existing, true
	}
	return Principal{}, false
}

// grantPatterns returns the patterns a grant's can names, most specific
// first. A malformed can names none.
func grantPatterns(grant *Grant) []ChannelPattern {
	if grant == nil {
		return nil
	}
	entries, err := grant.Can.Entries()
	if err != nil {
		return nil
	}
	out := make([]ChannelPattern, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry.Pattern)
	}
	sortPatterns(out)
	return out
}

// cloneGrants returns deep copies of grants.
func cloneGrants(grants []Grant) []Grant {
	out := make([]Grant, len(grants))
	for index, grant := range grants {
		out[index] = grant.Clone()
	}
	return out
}

// clonePrincipal returns a deep copy of a principal.
func clonePrincipal(principal Principal) Principal {
	out := principal
	out.Environments = append([]string(nil), principal.Environments...)
	if principal.Claims != nil {
		out.Claims = make(map[string][]string, len(principal.Claims))
		for name, values := range principal.Claims {
			out.Claims[name] = append([]string(nil), values...)
		}
	}
	return out
}

// hasEveryone reports whether the grant names everyone.
func (g Grant) hasEveryone() bool {
	for _, principal := range g.Principals {
		if principal.Kind == PrincipalEveryone {
			return true
		}
	}
	return false
}

// principalLabel names a principal the way a 400 reads it.
func principalLabel(principal Principal) string {
	switch principal.Kind {
	case PrincipalUser:
		if principal.UserID != "" {
			return "user " + principal.UserID
		}
	case PrincipalProject:
		if principal.ProjectID != "" {
			return "project " + principal.ProjectID
		}
	case PrincipalGitHub:
		if principal.RepositoryID != "" {
			return "github repository " + principal.RepositoryID
		}
		if principal.Repository != "" {
			return "github repository " + principal.Repository
		}
	case PrincipalOIDC:
		if principal.Issuer != "" {
			return "oidc issuer " + principal.Issuer
		}
	case PrincipalApp:
		if principal.ClientID != "" {
			return "app " + principal.ClientID
		}
	case PrincipalAllUsers, PrincipalAllProjects, PrincipalAllApps, PrincipalEveryone:
		return string(principal.Kind)
	}
	return string(principal.Kind)
}
