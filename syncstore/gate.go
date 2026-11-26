package syncstore

import (
	"context"

	boardsync "github.com/jixoai/keel-board/sync"
)

// StoreGate answers the thread half of sync.Gate from board_sync_threads. It
// deliberately answers Routes false: an entry's own Routes owns channel
// mapping, so a handler builds one StoreGate per (tenant, key) and lets the
// registry consult the entry first.
//
// A deployment wires it through WebhookConfig.GateFactory:
//
//	GateFactory: func(tenant string, key boardsync.Key) (boardsync.Gate, error) {
//	    return syncstore.StoreGate{Store: store}, nil
//	}
type StoreGate struct {
	Store Store
}

// Routes implements sync.Gate.
func (g StoreGate) Routes(context.Context, string, boardsync.Key, string) (bool, error) {
	return false, nil
}

// ThreadSynced implements sync.Gate from the durable thread mappings.
func (g StoreGate) ThreadSynced(ctx context.Context, tenant string, key boardsync.Key, thread string) (bool, error) {
	return g.Store.ThreadSynced(ctx, tenant, key, thread)
}

var _ boardsync.Gate = StoreGate{}
