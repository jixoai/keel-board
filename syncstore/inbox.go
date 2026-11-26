package syncstore

import (
	"context"

	boardsync "github.com/jixoai/keel-board/sync"
)

// TenantInbox adapts a Store to the sync.Inbox seam for one tenant. The
// registry's webhook path only pushes; a worker uses Lease and Ack too.
// Ownership of a lease is the owner string, so two workers of the same key
// never ack each other's items.
type TenantInbox struct {
	store  Store
	tenant string
	owner  string
}

// NewTenantInbox returns a sync.Inbox scoped to one tenant.
func NewTenantInbox(store Store, tenant, owner string) *TenantInbox {
	return &TenantInbox{store: store, tenant: tenant, owner: leaseOwner(owner)}
}

// Push implements sync.Inbox.
func (i *TenantInbox) Push(ctx context.Context, key boardsync.Key, records []boardsync.Record) (int, error) {
	pushed, err := i.store.PushInbox(ctx, i.tenant, key, records)
	return pushed.Added, err
}

// Lease implements sync.Inbox.
func (i *TenantInbox) Lease(ctx context.Context, key boardsync.Key, limit int) ([]boardsync.InboxItem, error) {
	return i.store.LeaseInbox(ctx, i.tenant, key, limit, i.owner)
}

// Ack implements sync.Inbox.
func (i *TenantInbox) Ack(ctx context.Context, key boardsync.Key, id, lease string, jobErr error) error {
	_, err := i.store.AckInbox(ctx, i.tenant, key, id, lease, jobErr)
	return err
}

var _ boardsync.Inbox = (*TenantInbox)(nil)
