package syncstore

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
)

// Memory is an in-process Store. It is the executable form of the durable
// contract, used by development and by tests; PostgreSQL is the deployment
// implementation. It is safe for concurrent use.
type Memory struct {
	mu      sync.Mutex
	cfg     Config
	now     func() time.Time
	seq     uint64
	tenants map[string]*memoryTenant
}

type memoryTenant struct {
	entries    map[boardsync.Key]Entry
	inboxes    map[boardsync.Key]*memoryInbox
	outbox     []*memoryOutboxRow
	byDelivery map[string]*memoryOutboxRow
	threads    map[string]Thread
	copies     map[string]Copy
	cursors    map[string]string
	nonces     map[string]time.Time
}

type memoryInbox struct {
	rows []*memoryInboxRow
}

type memoryInboxRow struct {
	seq         uint64
	item        boardsync.InboxItem
	lane        string
	state       string
	availableAt time.Time
	leaseOwner  string
	leaseUntil  time.Time
	lastError   string
	createdAt   time.Time
	completedAt time.Time
}

type memoryOutboxRow struct {
	seq         uint64
	delivery    Delivery
	lane        string
	targetKey   string
	state       string
	availableAt time.Time
	leaseOwner  string
	leaseUntil  time.Time
	lastError   string
	clearedAt   time.Time
	createdAt   time.Time
	completedAt time.Time
}

// NewMemory returns an empty in-memory store.
func NewMemory(cfg Config) (*Memory, error) {
	checked, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	return &Memory{
		cfg:     checked,
		now:     time.Now,
		tenants: make(map[string]*memoryTenant),
	}, nil
}

// WithClock makes leases and retry times deterministic in tests.
func (m *Memory) WithClock(now func() time.Time) *Memory {
	m.mu.Lock()
	defer m.mu.Unlock()
	if now != nil {
		m.now = now
	}
	return m
}

func (m *Memory) tenantLocked(tenant string) *memoryTenant {
	data := m.tenants[tenant]
	if data == nil {
		data = &memoryTenant{
			entries:    make(map[boardsync.Key]Entry),
			inboxes:    make(map[boardsync.Key]*memoryInbox),
			byDelivery: make(map[string]*memoryOutboxRow),
			threads:    make(map[string]Thread),
			copies:     make(map[string]Copy),
			cursors:    make(map[string]string),
			nonces:     make(map[string]time.Time),
		}
		m.tenants[tenant] = data
	}
	return data
}

func (m *Memory) inboxLocked(tenant string, key boardsync.Key) *memoryInbox {
	data := m.tenantLocked(tenant)
	inbox := data.inboxes[key]
	if inbox == nil {
		inbox = &memoryInbox{}
		data.inboxes[key] = inbox
	}
	return inbox
}

func (m *Memory) nextSeqLocked() uint64 {
	m.seq++
	return m.seq
}

// UpsertEntry implements Store.
func (m *Memory) UpsertEntry(_ context.Context, entry Entry) (Entry, error) {
	if err := entry.Validate(); err != nil {
		return Entry{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	data := m.tenantLocked(entry.Tenant)
	stored, exists := data.entries[entry.Key]
	if exists {
		stored.Platform = entry.Platform
		stored.UpdatedAt = now
		data.entries[entry.Key] = stored
		return stored, nil
	}
	entry.CreatedAt = now
	entry.UpdatedAt = now
	data.entries[entry.Key] = entry
	return entry, nil
}

// Entry implements Store.
func (m *Memory) Entry(_ context.Context, tenant string, key boardsync.Key) (Entry, bool, error) {
	if err := validTenant(tenant); err != nil {
		return Entry{}, false, err
	}
	if err := expectKey(key); err != nil {
		return Entry{}, false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.tenantLocked(tenant).entries[key]
	return entry, ok, nil
}

// Entries implements Store.
func (m *Memory) Entries(_ context.Context, tenant string) ([]Entry, error) {
	if err := validTenant(tenant); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	data := m.tenantLocked(tenant)
	entries := make([]Entry, 0, len(data.entries))
	for _, entry := range data.entries {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	return entries, nil
}

// SetEntryEnabled implements Store.
func (m *Memory) SetEntryEnabled(_ context.Context, tenant string, key boardsync.Key, enabled bool) (bool, error) {
	if err := validTenant(tenant); err != nil {
		return false, err
	}
	if err := expectKey(key); err != nil {
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	data := m.tenantLocked(tenant)
	entry, ok := data.entries[key]
	if !ok {
		return false, nil
	}
	entry.Enabled = enabled
	entry.UpdatedAt = m.now()
	data.entries[key] = entry
	return true, nil
}

// PushInbox implements Store.
func (m *Memory) PushInbox(_ context.Context, tenant string, key boardsync.Key, records []boardsync.Record) (InboxPush, error) {
	if err := validTenant(tenant); err != nil {
		return InboxPush{}, err
	}
	prepared, err := prepareRecords(key, records)
	if err != nil {
		return InboxPush{}, err
	}
	cloned := make([]boardsync.Record, len(prepared))
	for i := range prepared {
		event, err := cloneJSON(prepared[i].Event)
		if err != nil {
			return InboxPush{}, fmt.Errorf("%w: event %q can't be stored", boardsync.ErrBadRequest, prepared[i].Event.ID)
		}
		cloned[i] = prepared[i]
		cloned[i].Event = event
		cloned[i].Raw = append([]byte(nil), prepared[i].Raw...)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	inbox := m.inboxLocked(tenant, key)
	existingDedupe := make(map[string]struct{}, len(inbox.rows))
	existingID := make(map[string]string, len(inbox.rows))
	for _, row := range inbox.rows {
		existingDedupe[row.item.Dedupe] = struct{}{}
		existingID[row.item.ID] = row.item.Dedupe
	}
	pendingDedupe := make(map[string]struct{}, len(cloned))
	pendingID := make(map[string]string, len(cloned))
	for _, record := range cloned {
		if previous, ok := existingID[record.Event.ID]; ok && previous != record.Dedupe {
			return InboxPush{}, fmt.Errorf("%w: event ID %q already names another occurrence", boardsync.ErrBadRequest, record.Event.ID)
		}
		if previous, ok := pendingID[record.Event.ID]; ok && previous != record.Dedupe {
			return InboxPush{}, fmt.Errorf("%w: event ID %q is used twice in one batch", boardsync.ErrBadRequest, record.Event.ID)
		}
		pendingID[record.Event.ID] = record.Dedupe
		pendingDedupe[record.Dedupe] = struct{}{}
	}
	now := m.now()
	added := 0
	for _, record := range cloned {
		if _, seen := existingDedupe[record.Dedupe]; seen {
			continue
		}
		if _, seen := pendingDedupe[record.Dedupe]; seen {
			delete(pendingDedupe, record.Dedupe)
		}
		lane := record.Event.Thread
		if record.Event.Subthread != "" {
			lane = record.Event.Subthread
		}
		raw := append([]byte(nil), record.Raw...)
		if len(raw) == 0 {
			raw = append([]byte(nil), record.Event.Raw...)
		}
		item := boardsync.InboxItem{
			ID: record.Event.ID, Key: key, Dedupe: record.Dedupe,
			Event: record.Event, Raw: raw, Thread: lane,
		}
		inbox.rows = append(inbox.rows, &memoryInboxRow{
			seq: m.nextSeqLocked(), item: item, lane: lane, state: StatePending,
			availableAt: now, createdAt: now,
		})
		existingDedupe[record.Dedupe] = struct{}{}
		added++
	}
	return InboxPush{Added: added, Skipped: len(cloned) - added}, nil
}

// LeaseInbox implements Store.
func (m *Memory) LeaseInbox(_ context.Context, tenant string, key boardsync.Key, limit int, owner string) ([]boardsync.InboxItem, error) {
	if err := validTenant(tenant); err != nil {
		return nil, err
	}
	if err := expectKey(key); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 10
	}
	owner = leaseOwner(owner)
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	out := make([]boardsync.InboxItem, 0, limit)
	seenLane := make(map[string]struct{})
	for _, row := range m.inboxLocked(tenant, key).rows {
		if row.state != StatePending && row.state != StateLeased {
			continue
		}
		if _, seen := seenLane[row.lane]; seen {
			continue
		}
		seenLane[row.lane] = struct{}{}
		if row.state == StatePending && row.availableAt.After(now) {
			continue
		}
		if row.state == StateLeased && row.leaseUntil.After(now) {
			continue
		}
		token, err := newLeaseToken()
		if err != nil {
			return nil, err
		}
		row.state = StateLeased
		row.leaseOwner = owner
		row.leaseUntil = now.Add(m.cfg.LeaseTTL)
		row.item.Lease = token
		out = append(out, cloneItem(row.item))
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

// AckInbox implements Store.
func (m *Memory) AckInbox(_ context.Context, tenant string, key boardsync.Key, id, lease string, jobErr error) (AckResult, error) {
	if err := validTenant(tenant); err != nil {
		return AckResult{}, err
	}
	if err := expectKey(key); err != nil {
		return AckResult{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	for _, row := range m.inboxLocked(tenant, key).rows {
		if row.item.ID != id {
			continue
		}
		if row.state != StateLeased || row.item.Lease == "" || row.item.Lease != lease || !row.leaseUntil.After(now) {
			return AckResult{}, ErrLeaseLost
		}
		row.item.Lease = ""
		row.leaseOwner = ""
		row.leaseUntil = time.Time{}
		if jobErr == nil {
			row.state = StateDone
			row.completedAt = now
			return AckResult{State: StateDone, Attempts: row.item.Attempts, Acked: true}, nil
		}
		row.item.Attempts++
		row.lastError = ErrorCode(jobErr)
		if boardsync.IsPermanent(jobErr) || row.item.Attempts >= m.cfg.InboundRetry.MaxAttempts {
			row.state = StateDead
			row.completedAt = now
			return AckResult{State: StateDead, Attempts: row.item.Attempts, Dead: true}, nil
		}
		row.state = StatePending
		row.availableAt = now.Add(m.cfg.InboundRetry.Delay(row.item.Attempts))
		return AckResult{State: StatePending, Attempts: row.item.Attempts, Retried: true}, nil
	}
	return AckResult{}, ErrLeaseLost
}

// ReleaseInbox implements Store.
func (m *Memory) ReleaseInbox(_ context.Context, tenant string, key boardsync.Key, id, lease string, delay time.Duration) error {
	if err := validTenant(tenant); err != nil {
		return err
	}
	if err := expectKey(key); err != nil {
		return err
	}
	if delay < 0 {
		delay = 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	for _, row := range m.inboxLocked(tenant, key).rows {
		if row.item.ID != id {
			continue
		}
		if row.state != StateLeased || row.item.Lease != lease || !row.leaseUntil.After(now) {
			return ErrLeaseLost
		}
		row.state = StatePending
		row.item.Lease = ""
		row.availableAt = now.Add(delay)
		row.leaseOwner = ""
		row.leaseUntil = time.Time{}
		return nil
	}
	return ErrLeaseLost
}

// DeadInbox implements Store.
func (m *Memory) DeadInbox(_ context.Context, tenant string, key boardsync.Key, limit int) ([]boardsync.InboxItem, error) {
	if err := validTenant(tenant); err != nil {
		return nil, err
	}
	if err := expectKey(key); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 100
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]boardsync.InboxItem, 0)
	for _, row := range m.inboxLocked(tenant, key).rows {
		if row.state != StateDead {
			continue
		}
		out = append(out, cloneItem(row.item))
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

// RetryInbox implements Store.
func (m *Memory) RetryInbox(_ context.Context, tenant string, key boardsync.Key, id string) error {
	if err := validTenant(tenant); err != nil {
		return err
	}
	if err := expectKey(key); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, row := range m.inboxLocked(tenant, key).rows {
		if row.item.ID != id {
			continue
		}
		if row.state != StateDead {
			return fmt.Errorf("%w: inbox event %q is %s", ErrNotDead, id, row.state)
		}
		row.state = StatePending
		row.item.Attempts = 0
		row.item.Lease = ""
		row.availableAt = m.now()
		row.leaseOwner = ""
		row.leaseUntil = time.Time{}
		row.lastError = ""
		row.completedAt = time.Time{}
		return nil
	}
	return ErrNotDead
}

// InboxDepth implements Store.
func (m *Memory) InboxDepth(_ context.Context, tenant string, key boardsync.Key) (InboxDepth, error) {
	if err := validTenant(tenant); err != nil {
		return InboxDepth{}, err
	}
	if err := expectKey(key); err != nil {
		return InboxDepth{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var depth InboxDepth
	for _, row := range m.inboxLocked(tenant, key).rows {
		switch row.state {
		case StatePending:
			depth.Pending++
		case StateLeased:
			depth.Leased++
		case StateDone:
			depth.Done++
		case StateDead:
			depth.Dead++
		}
	}
	return depth, nil
}

// EnqueueOutbox implements Store.
func (m *Memory) EnqueueOutbox(_ context.Context, tenant string, key boardsync.Key, event boardsync.BoardEvent, message boardsync.Message, targets []boardsync.ResolvedTarget) (EnqueueResult, error) {
	jobs, err := prepareOutbox(tenant, key, event, message, targets)
	if err != nil {
		return EnqueueResult{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	data := m.tenantLocked(tenant)
	now := m.now()
	added := 0
	for _, job := range jobs {
		if _, exists := data.byDelivery[job.id]; exists {
			continue
		}
		eventCopy, err := cloneJSON(job.event)
		if err != nil {
			return EnqueueResult{}, fmt.Errorf("%w: board event %d can't be queued", boardsync.ErrBadRequest, job.event.ID)
		}
		messageCopy, err := cloneJSON(job.message)
		if err != nil {
			return EnqueueResult{}, fmt.Errorf("%w: board message %d can't be queued", boardsync.ErrBadRequest, job.message.ID)
		}
		row := &memoryOutboxRow{
			seq: m.nextSeqLocked(),
			delivery: Delivery{
				ID: job.id, Lane: job.lane, Key: job.key, Event: eventCopy,
				Message: messageCopy, Target: job.target, State: StatePending,
				AvailableAt: now, CreatedAt: now,
			},
			lane: job.lane, targetKey: job.targetKey, state: StatePending,
			availableAt: now, createdAt: now,
		}
		data.outbox = append(data.outbox, row)
		data.byDelivery[job.id] = row
		added++
	}
	return EnqueueResult{Added: added, Skipped: len(targets) - added}, nil
}

// LeaseOutbox implements Store.
func (m *Memory) LeaseOutbox(_ context.Context, tenant string, keys []boardsync.Key, limit int, owner string) ([]Delivery, error) {
	if err := validTenant(tenant); err != nil {
		return nil, err
	}
	owned, err := ownedKeys(keys)
	if err != nil {
		return nil, err
	}
	if len(owned) == 0 {
		return nil, nil
	}
	if limit <= 0 {
		limit = 10
	}
	owner = leaseOwner(owner)
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	data := m.tenantLocked(tenant)
	out := make([]Delivery, 0, limit)
	seenLane := make(map[string]struct{})
	for _, row := range data.outbox {
		if row.state != StatePending && row.state != StateLeased {
			continue
		}
		if _, seen := seenLane[row.lane]; seen {
			continue
		}
		// Rank every lane's head before filtering by ownership: a worker must
		// not jump over another key's earlier job on the same board thread.
		seenLane[row.lane] = struct{}{}
		if _, mine := owned[row.delivery.Key]; !mine {
			continue
		}
		if row.state == StatePending && row.availableAt.After(now) {
			continue
		}
		if row.state == StateLeased && row.leaseUntil.After(now) {
			continue
		}
		token, err := newLeaseToken()
		if err != nil {
			return nil, err
		}
		row.state = StateLeased
		row.leaseOwner = owner
		row.leaseUntil = now.Add(m.cfg.LeaseTTL)
		row.delivery.State = StateLeased
		row.delivery.Lease = token
		out = append(out, cloneDelivery(row.delivery))
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

// AckOutbox implements Store.
func (m *Memory) AckOutbox(_ context.Context, tenant, deliveryID, lease string, jobErr error, copy *Copy) (AckResult, error) {
	if err := validTenant(tenant); err != nil {
		return AckResult{}, err
	}
	if deliveryID == "" || lease == "" {
		return AckResult{}, fmt.Errorf("%w: an ack needs a delivery and a lease", boardsync.ErrBadRequest)
	}
	if copy != nil {
		if err := copy.Validate(); err != nil {
			return AckResult{}, err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	data := m.tenantLocked(tenant)
	row := data.byDelivery[deliveryID]
	if row == nil || row.state != StateLeased || row.delivery.Lease != lease || !row.leaseUntil.After(now) {
		return AckResult{}, ErrLeaseLost
	}
	if copy != nil && jobErr == nil {
		if copy.Key != row.delivery.Key || copy.Target != row.delivery.Target ||
			copy.BoardMessage != row.delivery.Message.ID || copy.BoardThread != row.delivery.Message.Thread {
			return AckResult{}, fmt.Errorf("%w: a copy doesn't belong to this delivery", boardsync.ErrBadRequest)
		}
	}
	row.delivery.Lease = ""
	row.leaseOwner = ""
	row.leaseUntil = time.Time{}
	if jobErr == nil {
		row.state = StateDone
		row.completedAt = now
		row.delivery.State = StateDone
		if copy != nil {
			m.saveCopyLocked(tenant, *copy, now)
		}
		for _, dead := range data.outbox {
			if dead.state == StateDead && dead.clearedAt.IsZero() &&
				dead.delivery.Key == row.delivery.Key && dead.targetKey == row.targetKey {
				dead.clearedAt = now
			}
		}
		return AckResult{State: StateDone, Attempts: row.delivery.Attempts, Acked: true}, nil
	}
	row.delivery.Attempts++
	row.delivery.LastError = ErrorCode(jobErr)
	row.lastError = row.delivery.LastError
	if boardsync.IsPermanent(jobErr) || row.delivery.Attempts >= m.cfg.OutboundRetry.MaxAttempts {
		row.state = StateDead
		row.completedAt = now
		row.delivery.State = StateDead
		row.clearedAt = time.Time{}
		return AckResult{State: StateDead, Attempts: row.delivery.Attempts, Dead: true}, nil
	}
	row.state = StatePending
	row.availableAt = now.Add(m.cfg.OutboundRetry.Delay(row.delivery.Attempts))
	row.delivery.State = StatePending
	return AckResult{State: StatePending, Attempts: row.delivery.Attempts, Retried: true}, nil
}

// ReleaseOutbox implements Store.
func (m *Memory) ReleaseOutbox(_ context.Context, tenant, deliveryID, lease string, delay time.Duration) error {
	if err := validTenant(tenant); err != nil {
		return err
	}
	if deliveryID == "" || lease == "" {
		return fmt.Errorf("%w: a release needs a delivery and a lease", boardsync.ErrBadRequest)
	}
	if delay < 0 {
		delay = 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	row := m.tenantLocked(tenant).byDelivery[deliveryID]
	if row == nil || row.state != StateLeased || row.delivery.Lease != lease || !row.leaseUntil.After(now) {
		return ErrLeaseLost
	}
	row.state = StatePending
	row.availableAt = now.Add(delay)
	row.leaseOwner = ""
	row.leaseUntil = time.Time{}
	row.delivery.State = StatePending
	row.delivery.Lease = ""
	return nil
}

// DeadOutbox implements Store.
func (m *Memory) DeadOutbox(_ context.Context, tenant string, key boardsync.Key, limit int) ([]Delivery, error) {
	if err := validTenant(tenant); err != nil {
		return nil, err
	}
	if key != "" {
		if err := expectKey(key); err != nil {
			return nil, err
		}
	}
	if limit <= 0 {
		limit = 100
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Delivery, 0)
	for _, row := range m.tenantLocked(tenant).outbox {
		if row.state != StateDead {
			continue
		}
		if key != "" && row.delivery.Key != key {
			continue
		}
		delivery := cloneDelivery(row.delivery)
		delivery.Lease = ""
		out = append(out, delivery)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

// RetryOutbox implements Store.
func (m *Memory) RetryOutbox(_ context.Context, tenant, deliveryID string) error {
	if err := validTenant(tenant); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	row := m.tenantLocked(tenant).byDelivery[deliveryID]
	if row == nil || row.state != StateDead {
		return ErrNotDead
	}
	row.state = StatePending
	row.delivery.State = StatePending
	row.delivery.Attempts = 0
	row.delivery.LastError = ""
	row.lastError = ""
	row.availableAt = m.now()
	row.leaseOwner = ""
	row.leaseUntil = time.Time{}
	row.clearedAt = time.Time{}
	row.completedAt = time.Time{}
	return nil
}

// OutboxDepth implements Store.
func (m *Memory) OutboxDepth(_ context.Context, tenant string) (OutboxDepth, error) {
	if err := validTenant(tenant); err != nil {
		return OutboxDepth{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var depth OutboxDepth
	for _, row := range m.tenantLocked(tenant).outbox {
		switch row.state {
		case StatePending:
			depth.Pending++
		case StateLeased:
			depth.Leased++
		case StateDone:
			depth.Done++
		case StateDead:
			depth.Dead++
		}
	}
	return depth, nil
}

// Copy implements Store.
func (m *Memory) Copy(_ context.Context, tenant string, key boardsync.Key, target boardsync.ResolvedTarget, boardMessage uint64) (Copy, bool, error) {
	if err := validTenant(tenant); err != nil {
		return Copy{}, false, err
	}
	if err := expectKey(key); err != nil {
		return Copy{}, false, err
	}
	if err := target.Validate(); err != nil {
		return Copy{}, false, err
	}
	if boardMessage == 0 {
		return Copy{}, false, fmt.Errorf("%w: a copy lookup needs a board message", boardsync.ErrBadRequest)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	copy, ok := m.tenantLocked(tenant).copies[copyIDOf(key, target, boardMessage)]
	return copy, ok, nil
}

// SaveCopy implements Store.
func (m *Memory) SaveCopy(_ context.Context, tenant string, copy Copy) (Copy, error) {
	if err := validTenant(tenant); err != nil {
		return Copy{}, err
	}
	if err := copy.Validate(); err != nil {
		return Copy{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	copy.ID = copyIDOf(copy.Key, copy.Target, copy.BoardMessage)
	m.saveCopyLocked(tenant, copy, m.now())
	return m.tenantLocked(tenant).copies[copy.ID], nil
}

// CopyForMessage implements Store.
func (m *Memory) CopyForMessage(_ context.Context, tenant string, key boardsync.Key, boardMessage uint64) ([]Copy, error) {
	if err := validTenant(tenant); err != nil {
		return nil, err
	}
	if err := expectKey(key); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Copy, 0, 2)
	for _, copy := range m.tenantLocked(tenant).copies {
		if copy.Key == key && copy.BoardMessage == boardMessage {
			out = append(out, copy)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// CopyByExternal implements Store.
func (m *Memory) CopyByExternal(_ context.Context, tenant string, key boardsync.Key, target boardsync.ResolvedTarget, externalMessage string) (Copy, bool, error) {
	if err := validTenant(tenant); err != nil {
		return Copy{}, false, err
	}
	if err := expectKey(key); err != nil {
		return Copy{}, false, err
	}
	if err := target.Validate(); err != nil {
		return Copy{}, false, err
	}
	if target.Key != key {
		return Copy{}, false, fmt.Errorf("%w: target key isn't the lookup key", boardsync.ErrBadRequest)
	}
	if externalMessage == "" || len(externalMessage) > 1200 {
		return Copy{}, false, fmt.Errorf("%w: an external message ID is required", boardsync.ErrBadRequest)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, copy := range m.tenantLocked(tenant).copies {
		if copy.Key == key && copy.ExternalMessage == externalMessage && copy.Target == target {
			return copy, true, nil
		}
	}
	return Copy{}, false, nil
}

// CopyByExternalMessage implements Store.
func (m *Memory) CopyByExternalMessage(_ context.Context, tenant string, key boardsync.Key, externalMessage string) (Copy, bool, error) {
	if err := validTenant(tenant); err != nil {
		return Copy{}, false, err
	}
	if err := expectKey(key); err != nil {
		return Copy{}, false, err
	}
	if externalMessage == "" || len(externalMessage) > 1200 {
		return Copy{}, false, fmt.Errorf("%w: an external message ID is required", boardsync.ErrBadRequest)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var found Copy
	matched := false
	for _, copy := range m.tenantLocked(tenant).copies {
		if copy.Key != key || copy.ExternalMessage != externalMessage {
			continue
		}
		if matched {
			return Copy{}, false, fmt.Errorf("%w: external message %q has several copies", boardsync.ErrBadRequest, externalMessage)
		}
		found, matched = copy, true
	}
	return found, matched, nil
}

// TargetFailures implements Store.
func (m *Memory) TargetFailures(_ context.Context, tenant string, key boardsync.Key, limit int) ([]Delivery, error) {
	if err := validTenant(tenant); err != nil {
		return nil, err
	}
	if err := expectKey(key); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 100
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Delivery, 0)
	for _, row := range m.tenantLocked(tenant).outbox {
		if row.state != StateDead || !row.clearedAt.IsZero() || row.delivery.Key != key {
			continue
		}
		delivery := cloneDelivery(row.delivery)
		delivery.Lease = ""
		out = append(out, delivery)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

// UpsertThread implements Store.
func (m *Memory) UpsertThread(_ context.Context, thread Thread) (Thread, error) {
	if err := thread.Validate(); err != nil {
		return Thread{}, err
	}
	thread.ID = threadIDOf(thread.BoardThread, thread.Key, boardsync.ResolvedTarget{
		Key: thread.Key, Platform: thread.Platform, Channel: thread.Channel,
		Thread: thread.Thread, Subthread: thread.Subthread, Kind: thread.Kind,
	})
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	data := m.tenantLocked(thread.Tenant)
	stored, exists := data.threads[thread.ID]
	if exists {
		stored.Platform = thread.Platform
		stored.Channel = thread.Channel
		stored.Thread = thread.Thread
		stored.Subthread = thread.Subthread
		stored.Kind = thread.Kind
		stored.UpdatedAt = now
		data.threads[thread.ID] = stored
		return stored, nil
	}
	thread.CreatedAt = now
	thread.UpdatedAt = now
	data.threads[thread.ID] = thread
	return thread, nil
}

// Threads implements Store.
func (m *Memory) Threads(_ context.Context, tenant string, boardThread uint64) ([]Thread, error) {
	if err := validTenant(tenant); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Thread, 0)
	for _, thread := range m.tenantLocked(tenant).threads {
		if thread.BoardThread == boardThread {
			out = append(out, thread)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// ThreadByTarget implements Store.
func (m *Memory) ThreadByTarget(_ context.Context, tenant string, key boardsync.Key, target boardsync.ResolvedTarget) (Thread, bool, error) {
	if err := validTenant(tenant); err != nil {
		return Thread{}, false, err
	}
	if err := expectKey(key); err != nil {
		return Thread{}, false, err
	}
	if target.Key != key {
		return Thread{}, false, fmt.Errorf("%w: target key isn't the lookup key", boardsync.ErrBadRequest)
	}
	if err := target.Validate(); err != nil {
		return Thread{}, false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, thread := range m.tenantLocked(tenant).threads {
		if thread.Key != key || thread.Platform != target.Platform ||
			thread.Channel != target.Channel || thread.Thread != target.Thread ||
			thread.Subthread != target.Subthread || thread.Kind != target.Kind {
			continue
		}
		return thread, true, nil
	}
	return Thread{}, false, nil
}

// ThreadSynced implements Store.
func (m *Memory) ThreadSynced(_ context.Context, tenant string, key boardsync.Key, external string) (bool, error) {
	if err := validTenant(tenant); err != nil {
		return false, err
	}
	if err := expectKey(key); err != nil {
		return false, err
	}
	if err := expectLabel(external, "external thread", 1200); err != nil {
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, thread := range m.tenantLocked(tenant).threads {
		if thread.Key != key {
			continue
		}
		native := thread.Thread
		if native == "" {
			native = thread.Channel
		}
		if string(key)+":"+native == external {
			return true, nil
		}
	}
	return false, nil
}

// RemoveThread implements Store.
func (m *Memory) RemoveThread(_ context.Context, tenant, id string) error {
	if err := validTenant(tenant); err != nil {
		return err
	}
	if id == "" {
		return fmt.Errorf("%w: a thread removal needs an ID", boardsync.ErrBadRequest)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.tenantLocked(tenant).threads, id)
	return nil
}

// Cursor implements Store.
func (m *Memory) Cursor(_ context.Context, tenant string, key boardsync.Key, source string) (string, bool, error) {
	if err := validTenant(tenant); err != nil {
		return "", false, err
	}
	if err := expectKey(key); err != nil {
		return "", false, err
	}
	if err := expectLabel(source, "source", 200); err != nil {
		return "", false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	cursor, ok := m.tenantLocked(tenant).cursors[cursorKey(key, source)]
	return cursor, ok, nil
}

// SetCursor implements Store.
func (m *Memory) SetCursor(_ context.Context, tenant string, key boardsync.Key, source, cursor string) error {
	if err := validTenant(tenant); err != nil {
		return err
	}
	if err := expectKey(key); err != nil {
		return err
	}
	if err := expectLabel(source, "source", 200); err != nil {
		return err
	}
	if err := validCursor(cursor); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tenantLocked(tenant).cursors[cursorKey(key, source)] = cursor
	return nil
}

// ClaimNonce implements Store.
func (m *Memory) ClaimNonce(_ context.Context, tenant string, key boardsync.Key, nonce string, ttl time.Duration) (bool, error) {
	if err := validTenant(tenant); err != nil {
		return false, err
	}
	if err := expectKey(key); err != nil {
		return false, err
	}
	if err := expectLabel(nonce, "nonce", 500); err != nil {
		return false, err
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	data := m.tenantLocked(tenant)
	nonceKey := cursorKey(key, nonce)
	if expires, ok := data.nonces[nonceKey]; ok && expires.After(now) {
		return false, nil
	}
	data.nonces[nonceKey] = now.Add(ttl)
	return true, nil
}

// PruneNonces implements Store.
func (m *Memory) PruneNonces(_ context.Context, tenant string, before time.Time) (int64, error) {
	if err := validTenant(tenant); err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	if before.IsZero() {
		before = now
	}
	var removed int64
	for key, expires := range m.tenantLocked(tenant).nonces {
		if !expires.After(before) {
			delete(m.tenantLocked(tenant).nonces, key)
			removed++
		}
	}
	return removed, nil
}

func (m *Memory) saveCopyLocked(tenant string, copy Copy, now time.Time) {
	copy.ID = copyIDOf(copy.Key, copy.Target, copy.BoardMessage)
	copy.Files = append([]string(nil), copy.Files...)
	data := m.tenantLocked(tenant)
	if existing, ok := data.copies[copy.ID]; ok {
		copy.CreatedAt = existing.CreatedAt
	} else {
		copy.CreatedAt = now
	}
	copy.UpdatedAt = now
	data.copies[copy.ID] = copy
}

func cloneItem(item boardsync.InboxItem) boardsync.InboxItem {
	item.Raw = append([]byte(nil), item.Raw...)
	if event, err := cloneJSON(item.Event); err == nil {
		item.Event = event
	}
	return item
}

func cloneDelivery(delivery Delivery) Delivery {
	if event, err := cloneJSON(delivery.Event); err == nil {
		delivery.Event = event
	}
	if message, err := cloneJSON(delivery.Message); err == nil {
		delivery.Message = message
	}
	return delivery
}

func leaseOwner(owner string) string {
	if owner == "" {
		return "worker"
	}
	return owner
}

func cursorKey(key boardsync.Key, source string) string {
	return string(key) + "\x1f" + source
}

var (
	_ Store = (*Memory)(nil)
)
