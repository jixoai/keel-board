package access

import (
	"sort"
	"strings"
)

// Level is how close a grant's principal is to a caller. Where grants name
// the same pattern, the closest principal wins; grants tied there add up.
type Level string

const (
	// LevelPrincipal is the caller itself: its user, project, repository's
	// runs, issuer's claims, or app.
	LevelPrincipal Level = "principal"
	// LevelGroup is a group that names it: all-users, all-projects, all-apps.
	LevelGroup Level = "group"
	// LevelEveryone is everyone something else lets in.
	LevelEveryone Level = "everyone"
)

func (l Level) rank() int {
	switch l {
	case LevelPrincipal:
		return 0
	case LevelGroup:
		return 1
	case LevelEveryone:
		return 2
	default:
		return 3
	}
}

// Explanation is what decides a caller's access in one channel: the pattern
// that won there, the patterns it won over (most specific first), the level
// that won, the grants at that level, which add up, and what they give,
// after the caller's cap and token scope. Pattern and Level are empty when no
// grant decides; Admitted is false when nothing but everyone names the caller.
type Explanation struct {
	Channel     string
	Admitted    bool
	Pattern     string
	Over        []string
	Level       Level
	Grants      []Grant
	Capped      bool
	Permissions []Permission
}

// Allows reports whether the explanation gives permission.
func (e Explanation) Allows(permission Permission) bool {
	for _, held := range e.Permissions {
		if held == permission {
			return true
		}
	}
	return false
}

// Caller is who asks: what verified them, whether they are on the board's
// team, and the most they may do anywhere (their role or an app's scopes).
// Owner skips the grant-change checks, not the access policy: an owner still
// reads only what a grant lets it read.
type Caller struct {
	Principal   Principal
	Member      bool
	TeamProject bool
	TeamApp     bool
	Cap         []Permission
	Scope       *Can
	Owner       bool
}

// facts returns the caller and a grant set as one decision sees them.
func (c Caller) facts(tenant string, grants []Grant) Facts {
	return Facts{
		Tenant:      tenant,
		Caller:      c.Principal,
		Member:      c.Member,
		TeamProject: c.TeamProject,
		TeamApp:     c.TeamApp,
		Cap:         c.Cap,
		Scope:       c.Scope,
		Grants:      grants,
	}
}

// Facts is a caller and the grants that decide for it, as plain data. It is
// what Evaluate and Explain read.
type Facts struct {
	Tenant      string
	Caller      Principal
	Member      bool
	TeamProject bool
	TeamApp     bool
	Cap         []Permission
	Scope       *Can
	Grants      []Grant
}

// Policy is a board's grants and what the board knows about callers, from
// which it answers for one caller and channel. It does no I/O.
type Policy struct {
	Tenant      string
	Grants      []Grant
	Member      bool
	TeamProject bool
	TeamApp     bool
	Cap         []Permission
	Scope       *Can
}

// Explain returns what decides caller's access in channel.
func (p Policy) Explain(caller Principal, channel string) Explanation {
	return Evaluate(p.caller(caller).facts(p.Tenant, p.Grants), channel)
}

// Can reports whether caller may do permission in channel.
func (p Policy) Can(caller Principal, channel string, permission Permission) bool {
	return p.Explain(caller, channel).Allows(permission)
}

// Permissions returns what caller may do in channel.
func (p Policy) Permissions(caller Principal, channel string) []Permission {
	return p.Explain(caller, channel).Permissions
}

func (p Policy) caller(principal Principal) Caller {
	return Caller{
		Principal:   principal,
		Member:      p.Member,
		TeamProject: p.TeamProject,
		TeamApp:     p.TeamApp,
		Cap:         p.Cap,
		Scope:       p.Scope,
	}
}

// Evaluate resolves caller's access in channel: the most specific pattern a
// matching grant names (an exact channel, then the longest subtree, then `*`),
// the closest principal naming it, and the union of grants tied there, capped
// by the caller's facts and narrowed by its scope. `everyone` never admits a
// caller by itself.
func Evaluate(facts Facts, channel string) Explanation {
	matched := matchGrants(facts, false)
	admitted := admits(facts, matched)
	view := newAccessView(facts, matched)
	explanation := view.explain(channel)
	explanation.Admitted = admitted
	if !admitted {
		return Explanation{Channel: channel, Admitted: false, Permissions: []Permission{}}
	}
	return explanation
}

// matchedGrant is a grant that names the caller, at the closest level any of
// its principals does.
type matchedGrant struct {
	grant Grant
	level Level
}

// patternGroup is what matched grants name at one pattern after resolution:
// the closest level that names it, and the union of grants at that level.
type patternGroup struct {
	pattern     ChannelPattern
	level       Level
	permissions PermissionSet
	grants      []Grant
}

// matchGrants returns the grants that name the caller, at the closest level
// any of their principals does. With own, everyone principals don't count:
// that's what the caller may do without everyone's grants.
func matchGrants(facts Facts, own bool) []matchedGrant {
	matched := make([]matchedGrant, 0, len(facts.Grants))
	for _, grant := range facts.Grants {
		if !tenantMatches(grant.Tenant, facts.Tenant) {
			continue
		}
		best := Level("")
		for _, principal := range grant.Principals {
			if own && principal.Kind == PrincipalEveryone {
				continue
			}
			level, ok := matchPrincipal(principal, facts)
			if !ok {
				continue
			}
			if best == "" || level.rank() < best.rank() {
				best = level
			}
		}
		if best != "" {
			matched = append(matched, matchedGrant{grant: grant, level: best})
		}
	}
	return matched
}

// tenantMatches reports whether a grant belongs to the tenant being decided.
// Both sides must name the same tenant: an unlabeled grant is not a wildcard,
// so a store that forgets to label one cannot leak it into another board.
func tenantMatches(grantTenant, tenant string) bool {
	return grantTenant == tenant
}

// admits reports whether anything but everyone lets the caller in: a grant,
// or, for a person, the team. A caller only everyone names is refused.
func admits(facts Facts, matched []matchedGrant) bool {
	if facts.Caller.Kind == PrincipalUser && facts.Member {
		return true
	}
	for _, found := range matched {
		if found.level != LevelEveryone {
			return true
		}
	}
	return false
}

// matchPrincipal reports how close principal is to the caller, or false when
// it doesn't name it.
func matchPrincipal(principal Principal, facts Facts) (Level, bool) {
	caller := facts.Caller
	switch principal.Kind {
	case PrincipalEveryone:
		return LevelEveryone, true
	case PrincipalAllUsers:
		if caller.Kind == PrincipalUser && facts.Member {
			return LevelGroup, true
		}
	case PrincipalUser:
		callerID := caller.UserID
		if callerID == "" {
			callerID = caller.Email
		}
		grantID := principal.UserID
		if grantID == "" {
			grantID = principal.Email
		}
		if caller.Kind == PrincipalUser && callerID != "" && grantID != "" &&
			normalizeUserID(callerID) == normalizeUserID(grantID) {
			return LevelPrincipal, true
		}
	case PrincipalAllProjects:
		if caller.Kind == PrincipalProject && facts.TeamProject && environmentAllowed(principal.Environments, caller.Environment) {
			return LevelGroup, true
		}
	case PrincipalProject:
		if caller.Kind == PrincipalProject &&
			principal.ProjectID != "" && principal.ProjectID == caller.ProjectID &&
			teamsMatch(principal.TeamID, caller.TeamID) &&
			environmentAllowed(principal.Environments, caller.Environment) {
			return LevelPrincipal, true
		}
	case PrincipalGitHub:
		if caller.Kind != PrincipalGitHub {
			return "", false
		}
		repositoryMatches := false
		if principal.RepositoryID != "" {
			// A grant that keeps the repository's ID matches that identity only:
			// another repository with the same name is not it.
			repositoryMatches = caller.RepositoryID != "" && principal.RepositoryID == caller.RepositoryID
		} else {
			repositoryMatches = principal.Repository != "" && principal.Repository == caller.Repository
		}
		if !repositoryMatches {
			return "", false
		}
		if principal.Workflow != "" {
			workflow := workflowPath(principal.Workflow)
			if caller.WorkflowRef != caller.Repository+"/"+workflow+"@"+caller.Ref {
				return "", false
			}
		}
		if principal.Branch != "" && caller.Ref != "refs/heads/"+strings.TrimPrefix(principal.Branch, "refs/heads/") {
			return "", false
		}
		if principal.Event != "" && principal.Event != caller.Event {
			return "", false
		}
		return LevelPrincipal, true
	case PrincipalOIDC:
		if caller.Kind == PrincipalOIDC && principal.Issuer != "" && principal.Issuer == caller.Issuer && claimsMatch(principal.Claims, caller.Claims) {
			return LevelPrincipal, true
		}
	case PrincipalApp:
		if caller.Kind == PrincipalApp && principal.ClientID != "" && principal.ClientID == caller.ClientID {
			return LevelPrincipal, true
		}
	case PrincipalAllApps:
		if caller.Kind == PrincipalApp && facts.TeamApp {
			return LevelGroup, true
		}
	}
	return "", false
}

func teamsMatch(grantTeam, callerTeam string) bool {
	// A grant scoped to a team names only that team's projects; a caller whose
	// token names no team is not it.
	if grantTeam == "" {
		return true
	}
	return callerTeam != "" && grantTeam == callerTeam
}

// groupMatches resolves matched grants into one group per pattern: the
// closest level naming it, and the union of grants at that level.
func groupMatches(matched []matchedGrant) map[string]*patternGroup {
	groups := make(map[string]*patternGroup, len(matched))
	for _, found := range matched {
		entries, err := found.grant.Can.Entries()
		if err != nil {
			// A malformed grant names nothing: fail closed.
			continue
		}
		for _, entry := range entries {
			key := entry.Pattern.String()
			group := groups[key]
			if group == nil {
				groups[key] = &patternGroup{
					pattern:     entry.Pattern,
					level:       found.level,
					permissions: entry.Permissions.Clone(),
					grants:      []Grant{found.grant},
				}
				continue
			}
			switch {
			case found.level.rank() < group.level.rank():
				group.level = found.level
				group.permissions = entry.Permissions.Clone()
				group.grants = []Grant{found.grant}
			case found.level.rank() == group.level.rank():
				for permission := range entry.Permissions {
					group.permissions[permission] = struct{}{}
				}
				if !containsGrant(group.grants, found.grant) {
					group.grants = append(group.grants, found.grant)
				}
			}
		}
	}
	return groups
}

func containsGrant(grants []Grant, grant Grant) bool {
	if grant.ID == "" {
		return false
	}
	for _, existing := range grants {
		if existing.ID == grant.ID {
			return true
		}
	}
	return false
}

// resolveGroups picks the pattern that decides channel: an exact channel,
// then the longest subtree, then `*`. With subtreeOnly it ignores an exact
// rule for the channel, as `at` does for a subtree pattern's root. It returns
// the winning group and the other covering patterns, most specific first.
func resolveGroups(groups map[string]*patternGroup, channel string, subtreeOnly bool) (*patternGroup, []string) {
	candidates := make([]*patternGroup, 0, len(groups))
	for _, group := range groups {
		switch group.pattern.Kind {
		case PatternExact:
			if subtreeOnly || group.pattern.Path != channel {
				continue
			}
		default:
			if !group.pattern.Covers(channel) {
				continue
			}
		}
		candidates = append(candidates, group)
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		left, right := candidates[i].pattern, candidates[j].pattern
		if left.specificity() != right.specificity() {
			return left.specificity() > right.specificity()
		}
		return left.String() < right.String()
	})
	over := make([]string, 0, len(candidates)-1)
	for _, candidate := range candidates[1:] {
		over = append(over, candidate.pattern.String())
	}
	return candidates[0], over
}

// accessView is a caller's access compiled: pattern groups, the cap, and the
// token scope.
type accessView struct {
	facts       Facts
	groups      map[string]*patternGroup
	cap         *PermissionSet
	scopeGroups map[string]*patternGroup
}

func newAccessView(facts Facts, matched []matchedGrant) *accessView {
	view := &accessView{facts: facts, groups: groupMatches(matched)}
	if facts.Cap != nil {
		set, err := ExpandPermissions(facts.Cap)
		if err != nil {
			set = PermissionSet{}
		}
		view.cap = &set
	}
	if facts.Scope != nil {
		view.scopeGroups = groupsFromCan(*facts.Scope)
	}
	return view
}

// atChannel returns what the caller may do in channel, after its cap and its
// token scope, and whether the cap took anything away.
func (v *accessView) atChannel(channel string) (PermissionSet, bool) {
	group, _ := resolveGroups(v.groups, channel, false)
	set := PermissionSet{}
	if group != nil {
		set = group.permissions.Clone()
	}
	var scope PermissionSet
	if v.scopeGroups != nil {
		scopeGroup, _ := resolveGroups(v.scopeGroups, channel, false)
		if scopeGroup != nil {
			scope = scopeGroup.permissions
		}
	}
	return v.limit(set, scope)
}

// atPattern returns what the caller may do where pattern itself decides: at
// `*`, what `*` gives; at `security/`, what covers security as a subtree,
// ignoring an exact rule for security.
func (v *accessView) atPattern(pattern ChannelPattern) PermissionSet {
	set := PermissionSet{}
	switch pattern.Kind {
	case PatternAll:
		if group := v.groups["*"]; group != nil {
			set = group.permissions.Clone()
		}
	case PatternExact:
		if group, _ := resolveGroups(v.groups, pattern.Path, false); group != nil {
			set = group.permissions.Clone()
		}
	case PatternSubtree:
		if group, _ := resolveGroups(v.groups, pattern.Path, true); group != nil {
			set = group.permissions.Clone()
		}
	}
	var scope PermissionSet
	if v.scopeGroups != nil {
		switch pattern.Kind {
		case PatternAll:
			if group := v.scopeGroups["*"]; group != nil {
				scope = group.permissions
			}
		case PatternExact:
			if group, _ := resolveGroups(v.scopeGroups, pattern.Path, false); group != nil {
				scope = group.permissions
			}
		case PatternSubtree:
			if group, _ := resolveGroups(v.scopeGroups, pattern.Path, true); group != nil {
				scope = group.permissions
			}
		}
	}
	set, _ = v.limit(set, scope)
	return set
}

func (v *accessView) limit(set, scope PermissionSet) (PermissionSet, bool) {
	capped := false
	if v.cap != nil {
		for permission := range set {
			if !v.cap.Has(permission) {
				delete(set, permission)
				capped = true
			}
		}
	}
	if v.scopeGroups != nil {
		for permission := range set {
			if !scope.Has(permission) {
				delete(set, permission)
			}
		}
	}
	return set, capped
}

func (v *accessView) explain(channel string) Explanation {
	group, over := resolveGroups(v.groups, channel, false)
	explanation := Explanation{Channel: channel, Over: over, Permissions: []Permission{}}
	if group == nil {
		return explanation
	}
	set, capped := v.atChannel(channel)
	explanation.Pattern = group.pattern.String()
	explanation.Level = group.level
	explanation.Grants = cloneGrants(group.grants)
	explanation.Capped = capped
	explanation.Permissions = set.List()
	return explanation
}
