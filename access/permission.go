package access

import (
	"sort"

	"github.com/jixoai/keel-board/protocol"
)

// Permission is one thing a caller may do in a channel. It is the board's
// wire permission, so a policy and the handlers that ask it name the same
// values.
type Permission = protocol.Permission

const (
	PermissionRead   Permission = protocol.PermissionRead
	PermissionPost   Permission = protocol.PermissionPost
	PermissionReact  Permission = protocol.PermissionReact
	PermissionLabel  Permission = protocol.PermissionLabel
	PermissionSync   Permission = protocol.PermissionSync
	PermissionManage Permission = protocol.PermissionManage
)

// Permissions is every permission, in the order a board lists them.
var Permissions = []Permission{
	PermissionRead, PermissionPost, PermissionReact,
	PermissionLabel, PermissionSync, PermissionManage,
}

// implies is one hop of implication. Every verb that addresses a message by
// ID implies read, so none is a blind existence oracle; manage implies every
// other permission. post implies nothing: write-only is ['post'].
var implies = map[Permission][]Permission{
	PermissionRead:   nil,
	PermissionPost:   nil,
	PermissionReact:  {PermissionRead},
	PermissionLabel:  {PermissionRead},
	PermissionSync:   {PermissionRead},
	PermissionManage: {PermissionRead, PermissionPost, PermissionReact, PermissionLabel, PermissionSync},
}

// ValidPermission reports whether permission is one the board knows.
func ValidPermission(permission Permission) bool {
	switch permission {
	case PermissionRead, PermissionPost, PermissionReact,
		PermissionLabel, PermissionSync, PermissionManage:
		return true
	default:
		return false
	}
}

// PermissionSet is a set of permissions. Its List method returns them in the
// board's order.
type PermissionSet map[Permission]struct{}

// NewPermissionSet returns a set of the permissions given.
func NewPermissionSet(permissions ...Permission) PermissionSet {
	set := make(PermissionSet, len(permissions))
	for _, permission := range permissions {
		set[permission] = struct{}{}
	}
	return set
}

// Has reports whether the set holds permission.
func (s PermissionSet) Has(permission Permission) bool {
	_, ok := s[permission]
	return ok
}

// IsEmpty reports whether the set holds nothing.
func (s PermissionSet) IsEmpty() bool { return len(s) == 0 }

// Clone returns a copy of the set.
func (s PermissionSet) Clone() PermissionSet {
	out := make(PermissionSet, len(s))
	for permission := range s {
		out[permission] = struct{}{}
	}
	return out
}

// Union returns a set holding what either set holds.
func (s PermissionSet) Union(other PermissionSet) PermissionSet {
	out := s.Clone()
	for permission := range other {
		out[permission] = struct{}{}
	}
	return out
}

// Intersect returns a set holding what both sets hold.
func (s PermissionSet) Intersect(other PermissionSet) PermissionSet {
	out := make(PermissionSet, len(s))
	for permission := range s {
		if other.Has(permission) {
			out[permission] = struct{}{}
		}
	}
	return out
}

// List returns the set's permissions in the board's order, without the
// implications a set may not spell out.
func (s PermissionSet) List() []Permission {
	out := make([]Permission, 0, len(s))
	for _, permission := range Permissions {
		if s.Has(permission) {
			out = append(out, permission)
		}
	}
	return out
}

// Equal reports whether both sets hold the same permissions.
func (s PermissionSet) Equal(other PermissionSet) bool {
	if len(s) != len(other) {
		return false
	}
	for permission := range s {
		if !other.Has(permission) {
			return false
		}
	}
	return true
}

// ExpandPermissions closes permissions under their implications: ['label'] is
// read and label, and ['manage'] is everything. An unknown permission is an
// InvalidCan.
func ExpandPermissions(permissions []Permission) (PermissionSet, error) {
	out := make(PermissionSet, len(permissions))
	var add func(Permission) error
	add = func(permission Permission) error {
		if !ValidPermission(permission) {
			return invalidCanf("unknown permission %q: use %s", permission, permissionList())
		}
		if out.Has(permission) {
			return nil
		}
		out[permission] = struct{}{}
		for _, implied := range implies[permission] {
			if err := add(implied); err != nil {
				return err
			}
		}
		return nil
	}
	for _, permission := range permissions {
		if err := add(permission); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// expandAll returns every permission, implications included.
func expandAll() PermissionSet {
	out := make(PermissionSet, len(Permissions))
	for _, permission := range Permissions {
		out[permission] = struct{}{}
	}
	return out
}

// sortPermissions sorts permissions in the board's order and drops repeats.
func sortPermissions(permissions []Permission) []Permission {
	seen := make(map[Permission]struct{}, len(permissions))
	out := make([]Permission, 0, len(permissions))
	for _, permission := range permissions {
		if _, ok := seen[permission]; ok {
			continue
		}
		seen[permission] = struct{}{}
		out = append(out, permission)
	}
	sort.SliceStable(out, func(i, j int) bool { return permissionRank(out[i]) < permissionRank(out[j]) })
	return out
}

func permissionRank(permission Permission) int {
	for index, known := range Permissions {
		if known == permission {
			return index
		}
	}
	return len(Permissions)
}

func permissionList() string {
	out := ""
	for index, permission := range Permissions {
		if index > 0 {
			out += ", "
		}
		out += string(permission)
	}
	return out
}
