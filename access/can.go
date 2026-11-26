package access

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// MaxCanPatterns is how many channel patterns one can may name, as a grant's
// can is written.
const MaxCanPatterns = 64

// MaxCanText is the longest x-board-can header a board reads.
const MaxCanText = 4000

// CanAt is what a can gives at one channel pattern: `all`, a list of
// permissions, or none. `None` (an empty list) is meaningful: it closes a
// subtree though it gives nothing.
type CanAt struct {
	All         bool
	Permissions []Permission
}

// All returns a can value granting every permission.
func All() CanAt { return CanAt{All: true} }

// None returns a can value granting nothing. On its own it's write-only's
// opposite: it decides that a subtree is closed.
func None() CanAt { return CanAt{} }

// Only returns a can value granting exactly permissions, before implications.
func Only(permissions ...Permission) CanAt {
	if len(permissions) == 0 {
		return None()
	}
	return CanAt{Permissions: sortPermissions(permissions)}
}

// Expand returns what the value grants, implications included.
func (c CanAt) Expand() (PermissionSet, error) {
	if c.All && len(c.Permissions) > 0 {
		return nil, invalidCanf("a pattern takes all or a list of permissions, not both")
	}
	if c.All {
		return expandAll(), nil
	}
	return ExpandPermissions(c.Permissions)
}

// MarshalJSON writes `"all"` or a list of permissions, `[]` for none.
func (c CanAt) MarshalJSON() ([]byte, error) {
	if c.All {
		return []byte(`"all"`), nil
	}
	if len(c.Permissions) == 0 {
		return []byte(`[]`), nil
	}
	return json.Marshal(sortPermissions(c.Permissions))
}

// UnmarshalJSON reads `"all"`, `"none"`, or a list of permissions.
func (c *CanAt) UnmarshalJSON(data []byte) error {
	var raw any
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	switch value := raw.(type) {
	case string:
		switch value {
		case "all":
			*c = All()
			return nil
		case "none":
			*c = None()
			return nil
		default:
			return invalidCanf("a can value is all, none, or a list of permissions, got %q", value)
		}
	case []any:
		permissions := make([]Permission, 0, len(value))
		for _, item := range value {
			name, ok := item.(string)
			if !ok {
				return invalidCanf("a permission is its name, got %v", item)
			}
			permission := Permission(name)
			if !ValidPermission(permission) {
				return invalidCanf("unknown permission %q: use %s", name, permissionList())
			}
			permissions = append(permissions, permission)
		}
		*c = Only(permissions...)
		return nil
	default:
		return invalidCanf("a can value is all, none, or a list of permissions")
	}
}

// String returns the value as a can header writes it.
func (c CanAt) String() string {
	if c.All {
		return "all"
	}
	if len(c.Permissions) == 0 {
		return ""
	}
	names := make([]string, 0, len(c.Permissions))
	for _, permission := range sortPermissions(c.Permissions) {
		names = append(names, string(permission))
	}
	return strings.Join(names, ",")
}

// Can is what a grant gives, written as a map from channel pattern to what it
// gives there. The patterns are checked when the grant is normalized.
type Can map[string]CanAt

// CanEntry is one compiled pattern and what it gives.
type CanEntry struct {
	Pattern     ChannelPattern
	Permissions PermissionSet
}

// Entries compiles the can's patterns and permissions, `*` first and the
// rest sorted by name. A can that names no pattern, or one that doesn't
// parse, is an InvalidCan.
func (c Can) Entries() ([]CanEntry, error) {
	return c.entries(MaxCanPatterns)
}

// entries compiles the can. A limit above zero bounds how many patterns it may
// name; zero leaves it unbounded, as a programmatic scope is.
func (c Can) entries(limit int) ([]CanEntry, error) {
	if len(c) == 0 {
		return nil, invalidCanf("a grant's can names at least one channel pattern, like {'*': ['read']}")
	}
	if limit > 0 && len(c) > limit {
		return nil, invalidCanf("a can names at most %d channel patterns", limit)
	}
	entries := make([]CanEntry, 0, len(c))
	for raw, value := range c {
		pattern, err := ParsePattern(raw)
		if err != nil {
			return nil, err
		}
		permissions, err := value.Expand()
		if err != nil {
			return nil, err
		}
		entries = append(entries, CanEntry{Pattern: pattern, Permissions: permissions})
	}
	sort.SliceStable(entries, func(i, j int) bool {
		left, right := entries[i].Pattern.String(), entries[j].Pattern.String()
		if left == "*" {
			return right != "*"
		}
		if right == "*" {
			return false
		}
		return left < right
	})
	return entries, nil
}

// Validate checks that every pattern and permission parses.
func (c Can) Validate() error {
	_, err := c.Entries()
	return err
}

// Clone returns a copy of the can, its permission lists included.
func (c Can) Clone() Can {
	if c == nil {
		return nil
	}
	out := make(Can, len(c))
	for pattern, value := range c {
		out[pattern] = CanAt{All: value.All, Permissions: append([]Permission(nil), value.Permissions...)}
	}
	return out
}

// Patterns returns the can's patterns, `*` first and the rest sorted.
func (c Can) Patterns() []string {
	out := make([]string, 0, len(c))
	for pattern := range c {
		out = append(out, pattern)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i] == "*" {
			return out[j] != "*"
		}
		if out[j] == "*" {
			return false
		}
		return out[i] < out[j]
	})
	return out
}

// groupsFromCan compiles a can into pattern groups for resolution. A bad can
// is empty: a scope or token that doesn't parse can only take access away.
// It doesn't bound the pattern count: a programmatic scope still narrows
// channel by channel, unlike a header, which ParseCan bounds.
func groupsFromCan(can Can) map[string]*patternGroup {
	groups := make(map[string]*patternGroup, len(can))
	entries, err := can.entries(0)
	if err != nil {
		return groups
	}
	for _, entry := range entries {
		groups[entry.Pattern.String()] = &patternGroup{
			pattern:     entry.Pattern,
			level:       LevelEveryone,
			permissions: entry.Permissions.Clone(),
		}
	}
	return groups
}

// intersectCan returns the can that allows only what both cans allow, channel
// by channel. It is how an x-board-can header and a host's own scope compose:
// neither can widen the other.
func intersectCan(left, right Can) Can {
	leftGroups := groupsFromCan(left)
	rightGroups := groupsFromCan(right)
	leftView := &accessView{groups: leftGroups}
	rightView := &accessView{groups: rightGroups}
	patterns := map[string]ChannelPattern{"*": {Kind: PatternAll}}
	for _, group := range leftGroups {
		patterns[group.pattern.String()] = group.pattern
	}
	for _, group := range rightGroups {
		patterns[group.pattern.String()] = group.pattern
	}
	out := make(Can, len(patterns))
	for key, pattern := range patterns {
		set := leftView.atPattern(pattern).Intersect(rightView.atPattern(pattern))
		out[key] = Only(set.List()...)
	}
	return out
}

// parseCanText parses a can as a header writes it, `dev/=read,post;ci/=read`,
// `*=all`, or `*=` for none. It is the shape a proxy's x-board-can narrows a
// caller with.
func parseCanText(value string) (Can, error) {
	if len(value) > MaxCanText {
		return nil, invalidCanf("a can is longer than %d characters", MaxCanText)
	}
	if strings.TrimSpace(value) == "" {
		return nil, invalidCanf("a can is empty: write pattern=permissions, like dev/=read,post")
	}
	out := make(Can)
	for _, rawEntry := range strings.Split(value, ";") {
		entry := strings.TrimSpace(rawEntry)
		if entry == "" {
			continue
		}
		split := strings.IndexByte(entry, '=')
		if split < 0 {
			return nil, invalidCanf("can entry %q needs =: write pattern=permissions, like dev/=read,post", entry)
		}
		pattern := strings.TrimSpace(entry[:split])
		list := strings.TrimSpace(entry[split+1:])
		if _, err := ParsePattern(pattern); err != nil {
			return nil, err
		}
		if _, exists := out[pattern]; exists {
			return nil, invalidCanf("can lists %s twice", pattern)
		}
		if list == "all" {
			out[pattern] = All()
			continue
		}
		if list == "" {
			out[pattern] = None()
			continue
		}
		permissions := make([]Permission, 0)
		for _, rawPermission := range strings.Split(list, ",") {
			name := strings.TrimSpace(rawPermission)
			permission := Permission(name)
			if !ValidPermission(permission) {
				return nil, invalidCanf("unknown permission %q: use %s", name, permissionList())
			}
			permissions = append(permissions, permission)
		}
		out[pattern] = Only(permissions...)
	}
	if len(out) == 0 {
		return nil, invalidCanf("a can is empty: write pattern=permissions, like dev/=read,post")
	}
	if len(out) > MaxCanPatterns {
		return nil, invalidCanf("a can names at most %d channel patterns", MaxCanPatterns)
	}
	return out, nil
}

// ParseCan parses an x-board-can header: `dev/=read,post;ci/=read`, `*=all`,
// or `*=` for none. Invalid input is an InvalidCan.
func ParseCan(header string) (Can, error) {
	return parseCanText(header)
}

// CanText writes a can as an x-board-can header, `dev/=read,post;ci/=read`,
// or `*=` for none.
func CanText(can Can) string {
	patterns := can.Patterns()
	entries := make([]string, 0, len(patterns))
	for _, pattern := range patterns {
		entries = append(entries, fmt.Sprintf("%s=%s", pattern, can[pattern].String()))
	}
	if len(entries) == 0 {
		return "*="
	}
	return strings.Join(entries, ";")
}
