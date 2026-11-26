package sync

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// Entry is one configured sync account: a key, a provider, and the function
// that decides what every event does.
type Entry struct {
	Key      Key
	Provider Provider
	Sync     SyncFunc
	// Routes says which platform channels this entry maps. It is optional; a
	// function of the caller's own routes everything and leaves it nil.
	Routes RouteFunc
}

// Validate checks the entry is usable.
func (e Entry) Validate() error {
	if !e.Key.Valid() {
		return fmt.Errorf("%w: %q isn't a sync key", ErrBadRequest, e.Key)
	}
	if e.Provider == nil {
		return fmt.Errorf("%w: sync entry %q has no provider", ErrBadRequest, e.Key)
	}
	if e.Sync == nil {
		return fmt.Errorf("%w: sync entry %q has no sync function", ErrBadRequest, e.Key)
	}
	return nil
}

// Registry is the set of sync entries the server runs, keyed by name. Adding
// the same key twice fails: two syncs on one platform channel would each
// import and each deliver.
type Registry struct {
	mu      sync.RWMutex
	entries map[Key]Entry
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{entries: make(map[Key]Entry)}
}

// Add registers an entry.
func (r *Registry) Add(entry Entry) error {
	if err := entry.Validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.entries == nil {
		r.entries = make(map[Key]Entry)
	}
	if _, exists := r.entries[entry.Key]; exists {
		return fmt.Errorf("%w: sync key %q is already registered", ErrBadRequest, entry.Key)
	}
	r.entries[entry.Key] = entry
	return nil
}

// Get returns one entry.
func (r *Registry) Get(key Key) (Entry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, ok := r.entries[key]
	return entry, ok
}

// Keys returns the registered keys, sorted.
func (r *Registry) Keys() []Key {
	r.mu.RLock()
	defer r.mu.RUnlock()
	keys := make([]Key, 0, len(r.entries))
	for key := range r.entries {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

// Len is how many entries are registered.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.entries)
}

// HandleWebhook verifies and normalizes a platform webhook, keeps the records
// the gate accepts, and stores them in inbox before returning. The provider
// answers a platform challenge even when it carries no records.
//
// A record whose channel the entry routes is kept; otherwise it is kept only
// when the board already syncs its thread. Everything else is dropped. A gate
// that can't answer fails the webhook, so the platform retries rather than the
// event being lost.
func (r *Registry) HandleWebhook(ctx context.Context, key Key, req *WebhookRequest, gate Gate, inbox Inbox) (WebhookResponse, error) {
	entry, ok := r.Get(key)
	if !ok {
		return WebhookResponse{Status: 404}, fmt.Errorf("%w: no sync entry named %q", ErrNotFound, key)
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
		if gate == nil {
			kept = append(kept, record)
			continue
		}
		mapped, gateErr := gate.Routes(ctx, record.Event.Channel)
		if gateErr != nil {
			return WebhookResponse{Status: 500}, gateErr
		}
		if !mapped {
			synced, gateErr := gate.ThreadSynced(ctx, record.Event.ThreadRef().External())
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

// ResolveChannel delegates to an entry's provider.
func (r *Registry) ResolveChannel(ctx context.Context, key Key, name string) (string, error) {
	entry, ok := r.Get(key)
	if !ok {
		return "", fmt.Errorf("%w: no sync entry named %q", ErrNotFound, key)
	}
	return entry.Provider.ResolveChannel(ctx, key, name)
}

// ResolveTarget delegates to an entry's provider.
func (r *Registry) ResolveTarget(ctx context.Context, key Key, target string) (ResolvedTarget, error) {
	entry, ok := r.Get(key)
	if !ok {
		return ResolvedTarget{}, fmt.Errorf("%w: no sync entry named %q", ErrNotFound, key)
	}
	return entry.Provider.ResolveTarget(ctx, key, target)
}

// Post delegates to an entry's provider.
func (r *Registry) Post(ctx context.Context, key Key, target ResolvedTarget, msg OutboundMessage) (Posted, error) {
	entry, ok := r.Get(key)
	if !ok {
		return Posted{}, fmt.Errorf("%w: no sync entry named %q", ErrNotFound, key)
	}
	return entry.Provider.Post(ctx, key, target, msg)
}
