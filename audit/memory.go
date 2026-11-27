package audit

import (
	"context"
	"sort"
	"sync"
)

// Memory is an in-process Store for tests and development. It is safe for
// concurrent use and isolates tenants by construction: Recent answers one
// tenant's rows only.
type Memory struct {
	mu     sync.Mutex
	rows   map[string][]Event
	nextID uint64
}

// NewMemory returns an empty in-memory audit store.
func NewMemory() *Memory {
	return &Memory{rows: make(map[string][]Event)}
}

// Append implements Store.
func (m *Memory) Append(_ context.Context, event Event) (Event, error) {
	prepared, err := prepare(event)
	if err != nil {
		return Event{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextID++
	prepared.ID = m.nextID
	m.rows[prepared.Tenant] = append(m.rows[prepared.Tenant], prepared)
	return prepared, nil
}

// Recent implements Store. limit <= 0 means 100; a larger request is clamped
// to 500. Rows are newest first (time, then ID).
func (m *Memory) Recent(_ context.Context, tenant string, limit int) ([]Event, error) {
	if err := validTenant(tenant); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	m.mu.Lock()
	rows := append([]Event(nil), m.rows[tenant]...)
	m.mu.Unlock()
	sort.Slice(rows, func(i, j int) bool {
		if !rows[i].At.Equal(rows[j].At) {
			return rows[i].At.After(rows[j].At)
		}
		return rows[i].ID > rows[j].ID
	})
	if len(rows) > limit {
		rows = rows[:limit]
	}
	return cloneEvents(rows), nil
}

var _ Store = (*Memory)(nil)
