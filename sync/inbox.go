package sync

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrLeaseLost means an inbox ack named a lease that isn't the item's live
// one: another pull took it after this one's lease expired.
var ErrLeaseLost = errors.New("sync: inbox lease is not live")

// InboxItem is one platform event waiting to be imported.
type InboxItem struct {
	ID       string `json:"id"`
	Key      Key    `json:"key"`
	Dedupe   string `json:"dedupe"`
	Event    Event  `json:"event"`
	Raw      []byte `json:"raw,omitempty"`
	Thread   string `json:"thread"`
	Attempts int    `json:"attempts"`
	Lease    string `json:"lease,omitempty"`
}

// Inbox is the durable queue of platform events. The board keeps it beside the
// log, not in it: raw platform events never appear in a tail.
//
// Push dedupes by the record's Dedupe. Lease hands out the oldest available
// event of each external thread, one at a time per thread, so a thread's
// imports stay in order while other threads proceed in parallel. Ack removes
// a finished event or schedules a failed one again.
type Inbox interface {
	Push(ctx context.Context, key Key, records []Record) (int, error)
	Lease(ctx context.Context, key Key, limit int) ([]InboxItem, error)
	Ack(ctx context.Context, key Key, id, lease string, jobErr error) error
}

type inboxItem struct {
	InboxItem
	availableAt time.Time
	leaseUntil  time.Time
}

type inboxLane struct {
	order []string
	items map[string][]*inboxItem
	seen  map[string]struct{}
	dead  []*inboxItem
}

func newInboxLane() *inboxLane {
	return &inboxLane{items: make(map[string][]*inboxItem), seen: make(map[string]struct{})}
}

// MemoryInbox is an in-process Inbox. It is the executable form of the inbox
// semantics; a deployment replaces it with a store shared by its workers.
type MemoryInbox struct {
	mu       sync.Mutex
	lanes    map[Key]*inboxLane
	policy   RetryPolicy
	leaseTTL time.Duration
	now      func() time.Time
	leaseSeq uint64
}

// NewMemoryInbox returns an inbox. A zero policy uses InboundRetry; ttl <= 0
// means one minute.
func NewMemoryInbox(policy RetryPolicy, leaseTTL time.Duration) *MemoryInbox {
	checked, err := policy.Validate()
	if err != nil {
		checked = InboundRetry
	}
	if leaseTTL <= 0 {
		leaseTTL = time.Minute
	}
	return &MemoryInbox{lanes: make(map[Key]*inboxLane), policy: checked, leaseTTL: leaseTTL, now: time.Now}
}

// WithClock makes lease and retry times deterministic in tests.
func (m *MemoryInbox) WithClock(now func() time.Time) *MemoryInbox {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = now
	return m
}

// Push validates records and appends the ones whose Dedupe is new. It stores
// every record or none: a malformed record refuses the whole push, which makes
// the webhook answer 500 and the platform retry.
func (m *MemoryInbox) Push(_ context.Context, key Key, records []Record) (int, error) {
	if !key.Valid() {
		return 0, fmt.Errorf("%w: %q isn't a sync key", ErrBadRequest, key)
	}
	prepared := make([]Record, len(records))
	for i := range records {
		record := records[i]
		if record.Event.Key == "" {
			record.Event.Key = key
		}
		if record.Event.Key != key {
			return 0, fmt.Errorf("%w: record key %q isn't the entry key %q", ErrBadRequest, record.Event.Key, key)
		}
		if err := record.Validate(); err != nil {
			return 0, err
		}
		prepared[i] = record
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	lane := m.lane(key)
	added := 0
	for _, record := range prepared {
		if _, seen := lane.seen[record.Dedupe]; seen {
			continue
		}
		thread := record.Event.Thread
		if record.Event.Subthread != "" {
			thread = record.Event.Subthread
		}
		item := &inboxItem{InboxItem: InboxItem{
			ID:     record.Dedupe,
			Key:    key,
			Dedupe: record.Dedupe,
			Event:  record.Event,
			Raw:    append([]byte(nil), record.Raw...),
			Thread: thread,
		}}
		if len(item.Raw) == 0 && len(record.Event.Raw) > 0 {
			item.Raw = append([]byte(nil), record.Event.Raw...)
		}
		if _, ok := lane.items[thread]; !ok {
			lane.order = append(lane.order, thread)
		}
		lane.items[thread] = append(lane.items[thread], item)
		lane.seen[record.Dedupe] = struct{}{}
		added++
	}
	return added, nil
}

// Lease returns at most limit items: the oldest available event of each
// external thread, leased until ttl passes. An item behind a leased or backed
// off one waits, so its thread's order holds.
func (m *MemoryInbox) Lease(_ context.Context, key Key, limit int) ([]InboxItem, error) {
	if !key.Valid() {
		return nil, fmt.Errorf("%w: %q isn't a sync key", ErrBadRequest, key)
	}
	if limit <= 0 {
		limit = 10
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	lane := m.lanes[key]
	if lane == nil {
		return nil, nil
	}
	now := m.now()
	out := make([]InboxItem, 0, limit)
	for _, thread := range append([]string(nil), lane.order...) {
		if len(out) >= limit {
			break
		}
		queue := lane.items[thread]
		if len(queue) == 0 {
			continue
		}
		item := queue[0]
		if item.Lease != "" && item.leaseUntil.After(now) {
			continue
		}
		if item.availableAt.After(now) {
			continue
		}
		m.leaseSeq++
		item.Lease = fmt.Sprintf("lease-%d-%s", m.leaseSeq, Hash(item.ID+now.String()))
		item.leaseUntil = now.Add(m.leaseTTL)
		out = append(out, item.InboxItem)
	}
	return out, nil
}

// Ack finishes an item, or schedules it again. A nil jobErr removes it; an
// error raises its attempt count, and after the policy's limit the item is
// dead-lettered and its lane moves on.
func (m *MemoryInbox) Ack(_ context.Context, key Key, id, lease string, jobErr error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	lane := m.lanes[key]
	if lane == nil {
		return ErrLeaseLost
	}
	for _, thread := range lane.order {
		queue := lane.items[thread]
		for i, item := range queue {
			if item.ID != id {
				continue
			}
			if item.Lease == "" || item.Lease != lease || !item.leaseUntil.After(m.now()) {
				return ErrLeaseLost
			}
			if jobErr == nil {
				lane.items[thread] = append(queue[:i], queue[i+1:]...)
				if len(lane.items[thread]) == 0 {
					delete(lane.items, thread)
					lane.order = removeString(lane.order, thread)
				}
				return nil
			}
			item.Attempts++
			item.Lease = ""
			item.leaseUntil = time.Time{}
			if item.Attempts >= m.policy.MaxAttempts {
				lane.items[thread] = append(queue[:i], queue[i+1:]...)
				if len(lane.items[thread]) == 0 {
					delete(lane.items, thread)
					lane.order = removeString(lane.order, thread)
				}
				item.Event.Raw = append([]byte(nil), item.Raw...)
				lane.dead = append(lane.dead, item)
				return nil
			}
			item.availableAt = m.now().Add(m.policy.Delay(item.Attempts))
			return nil
		}
	}
	return ErrLeaseLost
}

// Dead returns what the inbox gave up on, oldest first.
func (m *MemoryInbox) Dead(key Key) []InboxItem {
	m.mu.Lock()
	defer m.mu.Unlock()
	lane := m.lanes[key]
	if lane == nil {
		return nil
	}
	out := make([]InboxItem, 0, len(lane.dead))
	for _, item := range lane.dead {
		out = append(out, item.InboxItem)
	}
	return out
}

// Pending is how many live items the inbox holds for key.
func (m *MemoryInbox) Pending(key Key) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	lane := m.lanes[key]
	if lane == nil {
		return 0
	}
	total := 0
	for _, queue := range lane.items {
		total += len(queue)
	}
	return total
}

func (m *MemoryInbox) lane(key Key) *inboxLane {
	lane := m.lanes[key]
	if lane == nil {
		lane = newInboxLane()
		m.lanes[key] = lane
	}
	return lane
}

func removeString(list []string, want string) []string {
	for i, item := range list {
		if item == want {
			return append(list[:i], list[i+1:]...)
		}
	}
	return list
}
