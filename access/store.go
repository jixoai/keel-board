package access

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jixoai/keel-board/protocol"
)

// Errors a grant store reports. A caller that reached the service sees them
// through AsProtocolError.
var (
	// ErrGrantNotFound is a grant the tenant doesn't have.
	ErrGrantNotFound = errors.New("access: grant not found")
	// ErrGrantExists is a grant whose ID is already used.
	ErrGrantExists = errors.New("access: a grant with that ID exists")
	// ErrTenantMismatch is a grant written for one tenant through another.
	ErrTenantMismatch = errors.New("access: a grant belongs to one tenant")
)

// GrantStore keeps a board's grants, one set per tenant. It stores and does
// no policy; GrantService checks a change before it writes. A store labels
// every grant with its tenant and returns copies, so a caller can't mutate
// what it keeps.
//
// A PostgreSQL store (not shipped in this phase) implements the same four
// methods. It should make Update conditional on the grant's version, or run
// the service's read-check-write in one transaction, since the service's
// lockout check reads the grants it is about to change.
type GrantStore interface {
	// List returns the tenant's grants.
	List(ctx context.Context, tenant string) ([]Grant, error)
	// Create stores a grant, assigning an ID when it has none.
	Create(ctx context.Context, tenant string, grant Grant) (Grant, error)
	// Update applies update to the grant id and returns the result.
	Update(ctx context.Context, tenant, id string, update GrantUpdate) (Grant, error)
	// Revoke removes the grant id.
	Revoke(ctx context.Context, tenant, id string) error
}

// MemoryStore is an in-memory GrantStore. Safe for concurrent use.
type MemoryStore struct {
	mu      sync.RWMutex
	tenants map[string]map[string]Grant
	next    int
	now     func() time.Time
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		tenants: make(map[string]map[string]Grant),
		now:     time.Now,
	}
}

// List implements GrantStore.
func (s *MemoryStore) List(ctx context.Context, tenant string) ([]Grant, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	tenant = tenantName(tenant)
	s.mu.RLock()
	defer s.mu.RUnlock()
	kept := s.tenants[tenant]
	grants := make([]Grant, 0, len(kept))
	for _, grant := range kept {
		grants = append(grants, grant.Clone())
	}
	sort.Slice(grants, func(i, j int) bool {
		if !grants[i].CreatedAt.Equal(grants[j].CreatedAt) {
			return grants[i].CreatedAt.Before(grants[j].CreatedAt)
		}
		return grants[i].ID < grants[j].ID
	})
	return grants, nil
}

// Create implements GrantStore.
func (s *MemoryStore) Create(ctx context.Context, tenant string, grant Grant) (Grant, error) {
	if err := contextError(ctx); err != nil {
		return Grant{}, err
	}
	tenant = tenantName(tenant)
	if grant.Tenant != "" && grant.Tenant != tenant {
		return Grant{}, ErrTenantMismatch
	}
	grant.Tenant = tenant
	if len(grant.Principals) == 0 {
		return Grant{}, invalidGrantf("a grant names at least one principal")
	}
	normalized, err := NormalizeGrant(grant)
	if err != nil {
		return Grant{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tenants == nil {
		s.tenants = make(map[string]map[string]Grant)
	}
	if s.tenants[tenant] == nil {
		s.tenants[tenant] = make(map[string]Grant)
	}
	if normalized.ID == "" {
		for {
			s.next++
			normalized.ID = fmt.Sprintf("g%d", s.next)
			if _, exists := s.tenants[tenant][normalized.ID]; !exists {
				break
			}
		}
	} else if _, exists := s.tenants[tenant][normalized.ID]; exists {
		return Grant{}, ErrGrantExists
	}
	now := s.clock()
	normalized.CreatedAt = now
	normalized.UpdatedAt = now
	s.tenants[tenant][normalized.ID] = normalized.Clone()
	return normalized, nil
}

// Update implements GrantStore.
func (s *MemoryStore) Update(ctx context.Context, tenant, id string, update GrantUpdate) (Grant, error) {
	if err := contextError(ctx); err != nil {
		return Grant{}, err
	}
	tenant = tenantName(tenant)
	s.mu.Lock()
	defer s.mu.Unlock()
	kept, ok := s.tenants[tenant][id]
	if !ok {
		return Grant{}, ErrGrantNotFound
	}
	next, err := ApplyUpdate(kept, update)
	if err != nil {
		return Grant{}, err
	}
	next.ID = kept.ID
	next.Tenant = tenant
	next.CreatedAt = kept.CreatedAt
	next.UpdatedAt = s.clock()
	s.tenants[tenant][id] = next.Clone()
	return next, nil
}

// Revoke implements GrantStore.
func (s *MemoryStore) Revoke(ctx context.Context, tenant, id string) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	tenant = tenantName(tenant)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tenants[tenant][id]; !ok {
		return ErrGrantNotFound
	}
	delete(s.tenants[tenant], id)
	return nil
}

// GrantService writes grants with the board's rules: a caller may make,
// change, or revoke a grant only where it may manage, it can't let anyone in
// where everyone gets more than it has, and a change that would take its own
// manage is a grant_lockout unless it asks to go ahead.
//
// Seed a new board's first grants through the store directly: no caller can
// have manage before a grant gives it, so the service refuses the first write
// unless its caller is an owner.
type GrantService struct {
	store GrantStore
	mu    sync.Mutex
}

// NewGrantService returns a service over store. It panics without one.
func NewGrantService(store GrantStore) *GrantService {
	if store == nil {
		panic("access: a GrantStore is required")
	}
	return &GrantService{store: store}
}

// List returns the tenant's grants.
func (s *GrantService) List(ctx context.Context, tenant string) ([]Grant, error) {
	return s.store.List(ctx, tenantName(tenant))
}

// Create makes a grant. caller must be able to manage at every pattern the
// grant names. lockMeOut allows a change that takes caller's own manage at a
// pattern it names, which is otherwise a grant_lockout.
func (s *GrantService) Create(ctx context.Context, tenant string, caller Caller, grant Grant, lockMeOut bool) (Grant, error) {
	tenant = tenantName(tenant)
	if grant.Tenant != "" && grant.Tenant != tenant {
		return Grant{}, ErrTenantMismatch
	}
	grant.Tenant = tenant
	if len(grant.Principals) == 0 {
		return Grant{}, invalidGrantf("a grant names at least one principal")
	}
	normalized, err := NormalizeGrant(grant)
	if err != nil {
		return Grant{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	before, err := s.store.List(ctx, tenant)
	if err != nil {
		return Grant{}, err
	}
	after := append(cloneGrants(before), normalized)
	refusal := CheckGrantChange(GrantChange{
		Tenant:    tenant,
		Caller:    caller,
		Before:    before,
		After:     after,
		Changed:   &normalized,
		LockMeOut: lockMeOut,
	})
	if refusal != nil {
		return Grant{}, refusal
	}
	return s.store.Create(ctx, tenant, normalized)
}

// Update changes a grant: adding and removing principals, or replacing its
// can. It is judged like Create, on the grant's patterns before and after.
func (s *GrantService) Update(ctx context.Context, tenant, id string, caller Caller, update GrantUpdate, lockMeOut bool) (Grant, error) {
	tenant = tenantName(tenant)
	s.mu.Lock()
	defer s.mu.Unlock()
	before, err := s.store.List(ctx, tenant)
	if err != nil {
		return Grant{}, err
	}
	old, ok := findGrant(before, id)
	if !ok {
		return Grant{}, ErrGrantNotFound
	}
	next, err := ApplyUpdate(old, update)
	if err != nil {
		return Grant{}, err
	}
	next.ID = old.ID
	next.Tenant = tenant
	after := replaceGrant(before, id, next)
	// A removal lets no one in, so it is judged by reach alone; adding or
	// widening is judged by admission too.
	var changed, revoked *Grant
	if update.Can != nil || len(update.Add) > 0 {
		changed = &next
	}
	if update.Can != nil || len(update.Remove) > 0 {
		revoked = &old
	}
	refusal := CheckGrantChange(GrantChange{
		Tenant:    tenant,
		Caller:    caller,
		Before:    before,
		After:     after,
		Changed:   changed,
		Revoked:   revoked,
		LockMeOut: lockMeOut,
	})
	if refusal != nil {
		return Grant{}, refusal
	}
	return s.store.Update(ctx, tenant, id, update)
}

// Revoke removes a grant. Lifting what it held back widens whoever it named,
// so it takes manage at its patterns, and a grant that keeps caller in is a
// lockout.
func (s *GrantService) Revoke(ctx context.Context, tenant, id string, caller Caller, lockMeOut bool) error {
	tenant = tenantName(tenant)
	s.mu.Lock()
	defer s.mu.Unlock()
	before, err := s.store.List(ctx, tenant)
	if err != nil {
		return err
	}
	old, ok := findGrant(before, id)
	if !ok {
		return ErrGrantNotFound
	}
	after := make([]Grant, 0, len(before)-1)
	for _, grant := range before {
		if grant.ID != id {
			after = append(after, grant)
		}
	}
	refusal := CheckGrantChange(GrantChange{
		Tenant:    tenant,
		Caller:    caller,
		Before:    before,
		After:     after,
		Revoked:   &old,
		LockMeOut: lockMeOut,
	})
	if refusal != nil {
		return refusal
	}
	return s.store.Revoke(ctx, tenant, id)
}

// Explain returns what decides caller's access in channel, from the tenant's
// current grants.
func (s *GrantService) Explain(ctx context.Context, tenant string, caller Caller, channel string) (Explanation, error) {
	tenant = tenantName(tenant)
	grants, err := s.store.List(ctx, tenant)
	if err != nil {
		return Explanation{}, err
	}
	return Evaluate(caller.facts(tenant, grants), channel), nil
}

// GrantChange is a grant write as CheckGrantChange judges it: the grants as
// they are, the grants as they would be, and the grant made or widened
// (Changed) or revoked/removed from (Revoked).
type GrantChange struct {
	Tenant    string
	Caller    Caller
	Before    []Grant
	After     []Grant
	Changed   *Grant
	Revoked   *Grant
	LockMeOut bool
}

// RefusalReason says why a grant change is refused.
type RefusalReason string

const (
	// RefusalReach is a change at a spot the caller can't manage: you can't
	// grant what you don't have.
	RefusalReach RefusalReason = "reach"
	// RefusalAdmission is a change that would let someone in where everyone
	// gets something the caller doesn't have.
	RefusalAdmission RefusalReason = "admission"
	// RefusalLockout is a change that would take the caller's own manage and
	// leave it unable to undo the change.
	RefusalLockout RefusalReason = "lockout"
)

// Refusal is why a grant change is refused, and where.
type Refusal struct {
	Reason     RefusalReason
	Pattern    string
	Permission Permission
}

// Error implements error.
func (r *Refusal) Error() string {
	switch r.Reason {
	case RefusalReach:
		return fmt.Sprintf("you can't manage %s: you can't grant what you don't have", r.Pattern)
	case RefusalAdmission:
		return fmt.Sprintf("everyone gets %s at %s, and you don't", r.Permission, r.Pattern)
	case RefusalLockout:
		return fmt.Sprintf("this change takes your own manage at %s: keep yourself in with a grant there, or pass lockMeOut", r.Pattern)
	default:
		return "this grant change is refused"
	}
}

// ProtocolError returns the failure as the board writes it. A lockout is 409
// grant_lockout; the others are 403.
func (r *Refusal) ProtocolError() *protocol.Error {
	switch r.Reason {
	case RefusalLockout:
		return &protocol.Error{
			Status:  http.StatusConflict,
			Code:    CodeGrantLockout,
			Message: r.Error(),
			Hint:    "keep yourself in with a grant at each pattern the change names, or pass lockMeOut",
		}
	default:
		return protocol.Forbidden(r.Error())
	}
}

// CheckGrantChange judges a grant change against the board's rules. It
// returns nil when the change may land, or a Refusal naming the reason and
// the pattern it failed at.
func CheckGrantChange(change GrantChange) *Refusal {
	if change.Caller.Owner {
		return nil
	}
	tenant := tenantName(change.Tenant)
	beforeFacts := change.Caller.facts(tenant, change.Before)
	afterFacts := change.Caller.facts(tenant, change.After)
	beforeCaller := newAccessView(beforeFacts, matchGrants(beforeFacts, false))
	beforeOwn := newAccessView(beforeFacts, matchGrants(beforeFacts, true))
	afterCaller := newAccessView(afterFacts, matchGrants(afterFacts, false))
	afterOwn := newAccessView(afterFacts, matchGrants(afterFacts, true))

	made := grantPatterns(change.Changed)
	lifted := grantPatterns(change.Revoked)
	if change.Changed != nil && len(made) == 0 {
		return &Refusal{Reason: RefusalReach, Pattern: "*", Permission: PermissionManage}
	}
	if change.Revoked != nil && len(lifted) == 0 {
		return &Refusal{Reason: RefusalReach, Pattern: "*", Permission: PermissionManage}
	}
	touched := uniquePatterns(made, lifted)
	// Spots are where access can change or where the caller's own limits
	// narrow it. A grant at a broad pattern must also be judgeable at every
	// more specific spot under it.
	spots := uniquePatterns(
		touched,
		viewPatterns(beforeCaller),
		viewPatterns(beforeOwn),
		everyonePatterns(tenant, change.After),
	)

	// Reach: you can't grant what you don't have, and your token's and your
	// own grants' limits count under each pattern too.
	for _, pattern := range touched {
		if !beforeCaller.atPattern(pattern).Has(PermissionManage) || !beforeOwn.atPattern(pattern).Has(PermissionManage) {
			return &Refusal{Reason: RefusalReach, Pattern: pattern.String(), Permission: PermissionManage}
		}
		for _, spot := range spots {
			if !isUnder(spot, pattern) {
				continue
			}
			if !beforeOwn.atPattern(spot).Has(PermissionManage) {
				return &Refusal{Reason: RefusalReach, Pattern: spot.String(), Permission: PermissionManage}
			}
		}
	}

	// Admission: anyone a grant lets in gets what everyone gets, so the
	// caller must have that too.
	if len(made) > 0 {
		gives := everyoneView(tenant, change.After)
		for _, spot := range spots {
			for _, permission := range Permissions {
				if gives.atPattern(spot).Has(permission) && !afterCaller.atPattern(spot).Has(permission) {
					return &Refusal{Reason: RefusalAdmission, Pattern: spot.String(), Permission: permission}
				}
			}
		}
	}

	// Lockout: a change you couldn't take back takes saying so.
	if !change.LockMeOut {
		for _, pattern := range touched {
			if !afterCaller.atPattern(pattern).Has(PermissionManage) || !afterOwn.atPattern(pattern).Has(PermissionManage) {
				return &Refusal{Reason: RefusalLockout, Pattern: pattern.String(), Permission: PermissionManage}
			}
		}
	}
	return nil
}

// everyoneView compiles the after-grants that name everyone, at the everyone
// level, to what they give.
func everyoneView(tenant string, grants []Grant) *accessView {
	matched := make([]matchedGrant, 0, len(grants))
	for _, grant := range grants {
		if tenantMatches(grant.Tenant, tenant) && grant.hasEveryone() {
			matched = append(matched, matchedGrant{grant: grant, level: LevelEveryone})
		}
	}
	return newAccessView(Facts{Tenant: tenant, Grants: grants}, matched)
}

// everyonePatterns returns every pattern a grant that names everyone has.
func everyonePatterns(tenant string, grants []Grant) []ChannelPattern {
	var patterns []ChannelPattern
	for _, grant := range grants {
		if tenantMatches(grant.Tenant, tenant) && grant.hasEveryone() {
			patterns = append(patterns, grantPatterns(&grant)...)
		}
	}
	return patterns
}

// viewPatterns returns the patterns a caller's access names.
func viewPatterns(view *accessView) []ChannelPattern {
	patterns := make([]ChannelPattern, 0, len(view.groups))
	for _, group := range view.groups {
		patterns = append(patterns, group.pattern)
	}
	return patterns
}

// isUnder reports whether spot is a pattern more specific than pattern and
// under it.
func isUnder(spot, pattern ChannelPattern) bool {
	if spot.String() == pattern.String() {
		return false
	}
	switch pattern.Kind {
	case PatternAll:
		return true
	case PatternSubtree:
		switch spot.Kind {
		case PatternExact:
			return spot.Path == pattern.Path || strings.HasPrefix(spot.Path, pattern.Path+"/")
		case PatternSubtree:
			return strings.HasPrefix(spot.String(), pattern.String())
		default:
			return false
		}
	default:
		return false
	}
}

// uniquePatterns merges pattern lists without repeats, most specific first.
func uniquePatterns(lists ...[]ChannelPattern) []ChannelPattern {
	seen := make(map[string]struct{})
	out := make([]ChannelPattern, 0)
	for _, patterns := range lists {
		for _, pattern := range patterns {
			key := pattern.String()
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, pattern)
		}
	}
	sortPatterns(out)
	return out
}

func findGrant(grants []Grant, id string) (Grant, bool) {
	for _, grant := range grants {
		if grant.ID == id {
			return grant, true
		}
	}
	return Grant{}, false
}

func replaceGrant(grants []Grant, id string, replacement Grant) []Grant {
	out := make([]Grant, 0, len(grants))
	for _, grant := range grants {
		if grant.ID == id {
			out = append(out, replacement)
			continue
		}
		out = append(out, grant)
	}
	return out
}

func tenantName(tenant string) string {
	if tenant == "" {
		return "default"
	}
	return tenant
}

func (s *MemoryStore) clock() time.Time {
	if s.now == nil {
		return time.Now().UTC()
	}
	return s.now().UTC()
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}
