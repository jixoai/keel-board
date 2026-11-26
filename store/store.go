// Package store keeps a board's raw log. It knows nothing about messages or
// tasks: an event's type and payload are opaque bytes here, and the log package
// above folds them.
//
// The interface is the seam a PostgreSQL implementation plugs into. Any store
// must make an append all-or-nothing and must reject one whose expected head
// has moved, so the log's conditional writes stay conditional.
package store

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrHeadMoved is returned by an append whose expected head isn't the store's
// current head anymore: another append landed first.
var ErrHeadMoved = errors.New("store: the log moved on")

// ErrNotImplemented is returned by stores that are documented seams only.
var ErrNotImplemented = errors.New("store: not implemented")

// Record is one persisted event.
type Record struct {
	// Seq is the event's 1-based position in its tenant's log.
	Seq uint64
	// Type is the event's type, such as "message.posted".
	Type string
	// Payload is the event's JSON payload, as the log wrote it.
	Payload []byte
	// At is when the board recorded it.
	At time.Time
	// RequestKey, Digest, and RequestLast describe the idempotent batch this
	// record starts: its key, the digest of what it asked for, and the seq of
	// the batch's last record. They are set on the batch's first record only,
	// so (tenant, request_key) can be unique.
	RequestKey  string
	Digest      string
	RequestLast uint64
}

// EventStore persists a board's events.
type EventStore interface {
	// Load returns every record of a tenant's log, ordered by Seq. A tenant
	// with none returns an empty slice, not an error.
	Load(ctx context.Context, tenant string) ([]Record, error)
	// Append writes records atomically. It succeeds only while expectedHead
	// is the tenant's current head: seqs start at 1, so a tenant with no
	// records has head 0. It returns ErrHeadMoved otherwise.
	Append(ctx context.Context, tenant string, records []Record, expectedHead uint64) error
}

// Memory is an in-process EventStore for tests and development. It keeps
// every tenant's log in memory and is safe for concurrent use.
type Memory struct {
	mu       sync.Mutex
	byTenant map[string][]Record
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory {
	return &Memory{byTenant: make(map[string][]Record)}
}

// Load implements EventStore.
func (m *Memory) Load(_ context.Context, tenant string) ([]Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	records := m.byTenant[tenant]
	out := make([]Record, len(records))
	for i, record := range records {
		out[i] = record
		out[i].Payload = append([]byte(nil), record.Payload...)
	}
	return out, nil
}

// Append implements EventStore.
func (m *Memory) Append(_ context.Context, tenant string, records []Record, expectedHead uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.byTenant[tenant]
	if uint64(len(current)) != expectedHead {
		return ErrHeadMoved
	}
	for _, record := range records {
		if record.Seq != uint64(len(current))+1 {
			return ErrHeadMoved
		}
		record.Payload = append([]byte(nil), record.Payload...)
		current = append(current, record)
	}
	m.byTenant[tenant] = current
	return nil
}
