package sync

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"unicode/utf8"
)

// Entry is one configured sync account: a tenant, a key, a platform provider,
// and the function that decides what every event does. A key is unique within
// a tenant, so two tenants may both run "slack" with different credentials and
// identities without sharing a provider.
type Entry struct {
	Tenant   string
	Key      Key
	Platform Platform
	Provider Provider
	Sync     SyncFunc
	// Routes says which platform channels this entry maps. It is optional; a
	// function of the caller's own routes everything and leaves it nil.
	Routes RouteFunc
}

// Validate checks the entry is usable and internally consistent.
func (e Entry) Validate() error {
	if !ValidTenant(e.Tenant) {
		return fmt.Errorf("%w: sync entry %q has no usable tenant", ErrBadRequest, e.Key)
	}
	if !e.Key.Valid() {
		return fmt.Errorf("%w: %q isn't a sync key", ErrBadRequest, e.Key)
	}
	if e.Platform == "" {
		return fmt.Errorf("%w: sync entry %q has no platform", ErrBadRequest, e.Key)
	}
	if e.Provider == nil {
		return fmt.Errorf("%w: sync entry %q has no provider", ErrBadRequest, e.Key)
	}
	if providerPlatform := e.Provider.Platform(); providerPlatform != e.Platform {
		return fmt.Errorf("%w: sync entry %q names platform %q but its provider is %q",
			ErrBadRequest, e.Key, e.Platform, providerPlatform)
	}
	if e.Sync == nil {
		return fmt.Errorf("%w: sync entry %q has no sync function", ErrBadRequest, e.Key)
	}
	return nil
}

// ValidTenant reports whether a tenant name is usable in a registry entry.
// The host owns tenant resolution; sync only refuses the empty name and
// control characters, which would make a row or a log ambiguous.
func ValidTenant(tenant string) bool {
	if tenant == "" || len(tenant) > 200 || !utf8.ValidString(tenant) {
		return false
	}
	for _, r := range tenant {
		if r == 0 || r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

// Registry is the set of sync entries one process runs, keyed by tenant and
// then by connection key. Two tenants may use the same key with different
// providers; adding the same (tenant, key) twice fails, because two syncs on
// one platform channel would each import and each deliver.
type Registry struct {
	mu      sync.RWMutex
	entries map[string]map[Key]Entry
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{entries: make(map[string]map[Key]Entry)}
}

// Add registers an entry. Its Tenant and Platform are part of its identity.
func (r *Registry) Add(entry Entry) error {
	if err := entry.Validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.entries == nil {
		r.entries = make(map[string]map[Key]Entry)
	}
	tenant := r.entries[entry.Tenant]
	if tenant == nil {
		tenant = make(map[Key]Entry)
		r.entries[entry.Tenant] = tenant
	}
	if _, exists := tenant[entry.Key]; exists {
		return fmt.Errorf("%w: sync key %q is already registered for tenant %q", ErrBadRequest, entry.Key, entry.Tenant)
	}
	tenant[entry.Key] = entry
	return nil
}

// Get returns one tenant's entry.
func (r *Registry) Get(tenant string, key Key) (Entry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, ok := r.entries[tenant][key]
	return entry, ok
}

// Keys returns one tenant's registered keys, sorted.
func (r *Registry) Keys(tenant string) []Key {
	r.mu.RLock()
	defer r.mu.RUnlock()
	keys := make([]Key, 0, len(r.entries[tenant]))
	for key := range r.entries[tenant] {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

// Len is how many entries are registered across all tenants.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	total := 0
	for _, tenant := range r.entries {
		total += len(tenant)
	}
	return total
}

// HandleWebhook verifies and normalizes a platform webhook, keeps the records
// the gate accepts, and stores them in inbox before returning. The provider
// answers a platform challenge even when it carries no records.
//
// A record whose channel the entry's Routes names is kept; when the entry has
// no Routes, the gate's tenant/key-scoped Routes answers. Otherwise it is kept
// only when the board already syncs its thread. Everything else is dropped. A
// gate that can't answer fails the webhook, so the platform retries rather
// than the event being lost.
//
// A nil gate is fail-closed for thread matching: only a record whose channel
// the entry's own Routes explicitly names is kept. With neither an entry
// Routes nor a gate, nothing is stored.
func (r *Registry) HandleWebhook(ctx context.Context, tenant string, key Key, req *WebhookRequest, gate Gate, inbox Inbox) (WebhookResponse, error) {
	entry, ok := r.Get(tenant, key)
	if !ok {
		return WebhookResponse{Status: 404}, fmt.Errorf("%w: no sync entry named %q for tenant %q", ErrNotFound, key, tenant)
	}
	if req == nil {
		return WebhookResponse{Status: 400}, fmt.Errorf("%w: no webhook request", ErrBadRequest)
	}
	if err := req.Validate(); err != nil {
		return WebhookResponse{Status: 400}, fmt.Errorf("%w: %v", ErrBadRequest, err)
	}
	intake, err := entry.Provider.Receive(ctx, key, req)
	if err != nil {
		return WebhookResponse{Status: StatusCode(err)}, err
	}
	response := intake.Response
	if response.Status == 0 {
		response.Status = 200
	}
	if len(intake.Records) == 0 {
		return response, nil
	}
	kept := make([]Record, 0, len(intake.Records))
	for _, record := range intake.Records {
		if err := record.Validate(); err != nil {
			return WebhookResponse{Status: StatusCode(err)}, err
		}
		mapped := false
		if entry.Routes != nil {
			mapped, err = entry.Routes(ctx, record.Event.Channel)
		} else if gate != nil {
			mapped, err = gate.Routes(ctx, tenant, key, record.Event.Channel)
		}
		if err != nil {
			return WebhookResponse{Status: 500}, err
		}
		if !mapped {
			if gate == nil {
				continue
			}
			synced, gateErr := gate.ThreadSynced(ctx, tenant, key, record.Event.ThreadRef().External())
			if gateErr != nil {
				return WebhookResponse{Status: 500}, gateErr
			}
			if !synced {
				continue
			}
		}
		kept = append(kept, record)
	}
	if len(kept) == 0 {
		return response, nil
	}
	if inbox != nil {
		if _, err := inbox.Push(ctx, key, kept); err != nil {
			// Storing failed, so the platform must send the event again.
			return WebhookResponse{Status: 500}, err
		}
	}
	return response, nil
}

// ResolveChannel delegates to a tenant's entry provider.
func (r *Registry) ResolveChannel(ctx context.Context, tenant string, key Key, name string) (string, error) {
	entry, ok := r.Get(tenant, key)
	if !ok {
		return "", fmt.Errorf("%w: no sync entry named %q for tenant %q", ErrNotFound, key, tenant)
	}
	return entry.Provider.ResolveChannel(ctx, key, name)
}

// ResolveTarget delegates to a tenant's entry provider.
func (r *Registry) ResolveTarget(ctx context.Context, tenant string, key Key, target string) (ResolvedTarget, error) {
	entry, ok := r.Get(tenant, key)
	if !ok {
		return ResolvedTarget{}, fmt.Errorf("%w: no sync entry named %q for tenant %q", ErrNotFound, key, tenant)
	}
	return entry.Provider.ResolveTarget(ctx, key, target)
}

// Post delegates to a tenant's entry provider.
func (r *Registry) Post(ctx context.Context, tenant string, key Key, target ResolvedTarget, msg OutboundMessage) (Posted, error) {
	entry, ok := r.Get(tenant, key)
	if !ok {
		return Posted{}, fmt.Errorf("%w: no sync entry named %q for tenant %q", ErrNotFound, key, tenant)
	}
	return entry.Provider.Post(ctx, key, target, msg)
}
