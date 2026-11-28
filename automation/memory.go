package automation

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"sort"
	"sync"
	"time"

	"github.com/jixoai/keel-board/protocol"
)

// Memory is an in-process Store. It is the executable form of the durable
// contract, used by development and tests; PostgreSQL is the deployment
// implementation. It is safe for concurrent use.
type Memory struct {
	mu      sync.Mutex
	cfg     Config
	now     func() time.Time
	tenants map[string]*memoryTenant
}

type memoryTenant struct {
	watchers   map[string]*Watcher
	deliveries map[string]map[uint64]*Delivery
	schedules  map[string]*Schedule
}

// NewMemory returns an empty in-memory store.
func NewMemory(cfg Config) (*Memory, error) {
	checked, err := cfg.WithDefaults()
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

func (m *Memory) nowLocked() time.Time { return m.now().UTC() }

// LeaseTTL returns the lease the store grants. Workers compare it with their
// action timeout before they run.
func (m *Memory) LeaseTTL() time.Duration { return m.cfg.LeaseTTL }

func (m *Memory) tenantLocked(tenant string) *memoryTenant {
	data := m.tenants[tenant]
	if data == nil {
		data = &memoryTenant{
			watchers:   make(map[string]*Watcher),
			deliveries: make(map[string]map[uint64]*Delivery),
			schedules:  make(map[string]*Schedule),
		}
		m.tenants[tenant] = data
	}
	return data
}

func (m *Memory) watcherLocked(tenant, id string) (*Watcher, bool) {
	data := m.tenants[tenant]
	if data == nil {
		return nil, false
	}
	watcher, ok := data.watchers[id]
	return watcher, ok
}

func (m *Memory) deliveryLaneLocked(tenant, watcherID string) map[uint64]*Delivery {
	data := m.tenantLocked(tenant)
	lane := data.deliveries[watcherID]
	if lane == nil {
		lane = make(map[uint64]*Delivery)
		data.deliveries[watcherID] = lane
	}
	return lane
}

// CreateWatcher implements Store.
func (m *Memory) CreateWatcher(_ context.Context, watcher Watcher) (Watcher, error) {
	watcher = normalizeWatcher(watcher)
	if err := watcher.Validate(); err != nil {
		return Watcher{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	data := m.tenantLocked(watcher.Tenant)
	if _, exists := data.watchers[watcher.ID]; exists {
		return Watcher{}, ErrExists
	}
	now := m.nowLocked()
	stored := cloneWatcher(watcher)
	if stored.Cursor > 0 {
		stored.CursorSet = true
	}
	if stored.CreatedAt.IsZero() {
		stored.CreatedAt = now
	}
	stored.UpdatedAt = now
	data.watchers[stored.ID] = &stored
	return cloneWatcher(stored), nil
}

// Watcher implements Store.
func (m *Memory) Watcher(_ context.Context, tenant, id string) (Watcher, bool, error) {
	if err := expectTenant(tenant); err != nil {
		return Watcher{}, false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	watcher, ok := m.watcherLocked(tenant, id)
	if !ok {
		return Watcher{}, false, nil
	}
	return cloneWatcher(*watcher), true, nil
}

// Watchers implements Store.
func (m *Memory) Watchers(_ context.Context, tenant string) ([]Watcher, error) {
	if err := expectTenant(tenant); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	data := m.tenants[tenant]
	if data == nil {
		return nil, nil
	}
	out := make([]Watcher, 0, len(data.watchers))
	for _, watcher := range data.watchers {
		out = append(out, cloneWatcher(*watcher))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// UpdateWatcher implements Store.
func (m *Memory) UpdateWatcher(_ context.Context, watcher Watcher) (Watcher, error) {
	watcher = normalizeWatcher(watcher)
	if err := watcher.Validate(); err != nil {
		return Watcher{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.watcherLocked(watcher.Tenant, watcher.ID)
	if !ok {
		return Watcher{}, ErrNotFound
	}
	stored := cloneWatcher(watcher)
	stored.Enabled = current.Enabled
	stored.Cursor = current.Cursor
	stored.CursorSet = current.CursorSet
	stored.CreatedAt = current.CreatedAt
	stored.UpdatedAt = m.nowLocked()
	*current = stored
	return cloneWatcher(stored), nil
}

// SetWatcherEnabled implements Store.
func (m *Memory) SetWatcherEnabled(_ context.Context, tenant, id string, enabled bool, cursor *uint64) (Watcher, error) {
	if err := expectTenant(tenant); err != nil {
		return Watcher{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	watcher, ok := m.watcherLocked(tenant, id)
	if !ok {
		return Watcher{}, ErrNotFound
	}
	watcher.Enabled = enabled
	if enabled && cursor != nil {
		if *cursor > watcher.Cursor {
			watcher.Cursor = *cursor
		}
		watcher.CursorSet = true
	}
	watcher.UpdatedAt = m.nowLocked()
	return cloneWatcher(*watcher), nil
}

// DeleteWatcher implements Store.
func (m *Memory) DeleteWatcher(_ context.Context, tenant, id string) error {
	if err := expectTenant(tenant); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	data := m.tenants[tenant]
	if data == nil {
		return ErrNotFound
	}
	if _, ok := data.watchers[id]; !ok {
		return ErrNotFound
	}
	delete(data.watchers, id)
	delete(data.deliveries, id)
	return nil
}

// AdvanceWatcher implements Store.
func (m *Memory) AdvanceWatcher(_ context.Context, tenant, id string, cursor uint64, events []uint64) (int, error) {
	if err := expectTenant(tenant); err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	watcher, ok := m.watcherLocked(tenant, id)
	if !ok {
		return 0, ErrNotFound
	}
	if !watcher.Enabled {
		return 0, ErrWatcherDisabled
	}
	now := m.nowLocked()
	if cursor <= watcher.Cursor {
		// Evaluating an empty board (cursor 0) still marks the cursor as
		// initialized, so event 1 is planned next rather than skipped.
		watcher.CursorSet = true
		watcher.UpdatedAt = now
		return 0, nil
	}
	lane := m.deliveryLaneLocked(tenant, id)
	added := 0
	seen := make(map[uint64]struct{}, len(events))
	for _, seq := range events {
		if seq == 0 || seq <= watcher.Cursor || seq > cursor {
			continue
		}
		if _, duplicate := seen[seq]; duplicate {
			continue
		}
		seen[seq] = struct{}{}
		if _, exists := lane[seq]; exists {
			continue
		}
		lane[seq] = &Delivery{
			Tenant: tenant, WatcherID: id, EventSeq: seq, State: StatePending,
			NextAt: now, CreatedAt: now, UpdatedAt: now,
		}
		added++
	}
	watcher.Cursor = cursor
	watcher.CursorSet = true
	watcher.UpdatedAt = now
	return added, nil
}

// LeaseDeliveries implements Store.
func (m *Memory) LeaseDeliveries(_ context.Context, tenant string, limit int, owner string) ([]Delivery, error) {
	if err := expectTenant(tenant); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 10
	}
	owner = leaseOwner(owner)
	m.mu.Lock()
	defer m.mu.Unlock()
	data := m.tenants[tenant]
	if data == nil {
		return nil, nil
	}
	now := m.nowLocked()
	ids := make([]string, 0, len(data.watchers))
	for id, watcher := range data.watchers {
		if watcher.Enabled {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	out := make([]Delivery, 0, limit)
	for _, id := range ids {
		if len(out) == limit {
			break
		}
		lane := data.deliveries[id]
		seqs := make([]uint64, 0, len(lane))
		for seq := range lane {
			seqs = append(seqs, seq)
		}
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
		for _, seq := range seqs {
			row := lane[seq]
			if row.State != StatePending && row.State != StateLeased {
				continue
			}
			// The first pending or leased row is this watcher's lane head;
			// nothing behind it is offered until it finishes or dies.
			due := (row.State == StatePending && !row.NextAt.After(now)) ||
				(row.State == StateLeased && !row.LeaseUntil.After(now))
			if !due {
				break
			}
			token, err := newLeaseToken()
			if err != nil {
				return nil, err
			}
			row.State = StateLeased
			row.Lease = token
			row.LeaseUntil = now.Add(m.cfg.LeaseTTL)
			row.LeaseOwner = owner
			row.UpdatedAt = now
			leased := cloneDelivery(*row)
			leased.Lease = token
			out = append(out, leased)
			break
		}
	}
	return out, nil
}

// AckDelivery implements Store.
func (m *Memory) AckDelivery(_ context.Context, tenant, watcherID string, eventSeq uint64, lease string, jobErr error) (AckResult, error) {
	if err := expectTenant(tenant); err != nil {
		return AckResult{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	data := m.tenants[tenant]
	if data == nil {
		return AckResult{}, ErrLeaseLost
	}
	lane := data.deliveries[watcherID]
	row := lane[eventSeq]
	now := m.nowLocked()
	if row == nil || row.State != StateLeased || row.Lease != lease || !row.LeaseUntil.After(now) {
		return AckResult{}, ErrLeaseLost
	}
	clearLease(row)
	row.UpdatedAt = now
	if jobErr == nil {
		row.State = StateDone
		completed := now
		row.Completed = &completed
		return AckResult{State: StateDone, Attempts: row.Attempts, Acked: true}, nil
	}
	row.Attempts++
	row.LastError = ErrorCode(jobErr)
	if IsPermanent(jobErr) || row.Attempts >= m.cfg.Retry.MaxAttempts {
		row.State = StateDead
		completed := now
		row.Completed = &completed
		return AckResult{State: StateDead, Attempts: row.Attempts, Dead: true}, nil
	}
	row.State = StatePending
	row.NextAt = now.Add(m.cfg.Retry.Delay(row.Attempts))
	return AckResult{State: StatePending, Attempts: row.Attempts, Retried: true}, nil
}

// ReleaseDelivery implements Store.
func (m *Memory) ReleaseDelivery(_ context.Context, tenant, watcherID string, eventSeq uint64, lease string, delay time.Duration) error {
	if err := expectTenant(tenant); err != nil {
		return err
	}
	if delay < 0 {
		delay = 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	data := m.tenants[tenant]
	if data == nil {
		return ErrLeaseLost
	}
	row := data.deliveries[watcherID][eventSeq]
	now := m.nowLocked()
	if row == nil || row.State != StateLeased || row.Lease != lease || !row.LeaseUntil.After(now) {
		return ErrLeaseLost
	}
	clearLease(row)
	row.State = StatePending
	row.NextAt = now.Add(delay)
	row.UpdatedAt = now
	return nil
}

// RenewDelivery implements Store.
func (m *Memory) RenewDelivery(_ context.Context, tenant, watcherID string, eventSeq uint64, lease string, ttl time.Duration) error {
	if err := expectTenant(tenant); err != nil {
		return err
	}
	if ttl <= 0 {
		ttl = m.cfg.LeaseTTL
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	data := m.tenants[tenant]
	if data == nil {
		return ErrLeaseLost
	}
	row := data.deliveries[watcherID][eventSeq]
	now := m.nowLocked()
	if row == nil || row.State != StateLeased || row.Lease != lease || !row.LeaseUntil.After(now) {
		return ErrLeaseLost
	}
	row.LeaseUntil = now.Add(ttl)
	row.UpdatedAt = now
	return nil
}

// Deliveries implements Store.
func (m *Memory) Deliveries(_ context.Context, tenant, watcherID string, limit int) ([]Delivery, error) {
	return m.listDeliveries(tenant, watcherID, "", limit)
}

// DeadDeliveries implements Store.
func (m *Memory) DeadDeliveries(_ context.Context, tenant, watcherID string, limit int) ([]Delivery, error) {
	return m.listDeliveries(tenant, watcherID, StateDead, limit)
}

func (m *Memory) listDeliveries(tenant, watcherID, state string, limit int) ([]Delivery, error) {
	if err := expectTenant(tenant); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 100
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	data := m.tenants[tenant]
	if data == nil {
		return nil, nil
	}
	lane := data.deliveries[watcherID]
	seqs := make([]uint64, 0, len(lane))
	for seq, row := range lane {
		if state != "" && row.State != state {
			continue
		}
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	out := make([]Delivery, 0, limit)
	for _, seq := range seqs {
		if len(out) == limit {
			break
		}
		out = append(out, cloneDelivery(*lane[seq]))
	}
	return out, nil
}

// RetryDelivery implements Store.
func (m *Memory) RetryDelivery(_ context.Context, tenant, watcherID string, eventSeq uint64) error {
	if err := expectTenant(tenant); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	data := m.tenants[tenant]
	if data == nil {
		return ErrNotDead
	}
	row := data.deliveries[watcherID][eventSeq]
	if row == nil || row.State != StateDead {
		return ErrNotDead
	}
	now := m.nowLocked()
	row.State = StatePending
	row.Attempts = 0
	row.LastError = ""
	row.NextAt = now
	row.Completed = nil
	row.UpdatedAt = now
	return nil
}

// ClearDeliveries implements Store.
func (m *Memory) ClearDeliveries(_ context.Context, tenant, watcherID string) (int64, error) {
	if err := expectTenant(tenant); err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.watcherLocked(tenant, watcherID); !ok {
		return 0, ErrNotFound
	}
	data := m.tenants[tenant]
	lane := data.deliveries[watcherID]
	var removed int64
	for seq, row := range lane {
		if row.State == StateDead {
			delete(lane, seq)
			removed++
		}
	}
	return removed, nil
}

// DeliveryDepth implements Store.
func (m *Memory) DeliveryDepth(_ context.Context, tenant, watcherID string) (Depth, error) {
	if err := expectTenant(tenant); err != nil {
		return Depth{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var depth Depth
	data := m.tenants[tenant]
	if data == nil {
		return depth, nil
	}
	for _, row := range data.deliveries[watcherID] {
		switch row.State {
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

// WatcherStats implements StatsReader.
func (m *Memory) WatcherStats(_ context.Context, tenant, watcherID string) (WatcherStats, error) {
	if err := expectTenant(tenant); err != nil {
		return WatcherStats{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var stats WatcherStats
	var lastErrorAt time.Time
	data := m.tenants[tenant]
	if data == nil {
		return stats, nil
	}
	for _, row := range data.deliveries[watcherID] {
		if row.State == StateDead {
			stats.Dead++
		}
		if row.UpdatedAt.After(stats.LastDeliveryAt) {
			stats.LastDeliveryAt = row.UpdatedAt
		}
		if row.LastError != "" && row.UpdatedAt.After(lastErrorAt) {
			stats.LastError = row.LastError
			lastErrorAt = row.UpdatedAt
		}
	}
	return stats, nil
}

// CreateSchedule implements Store.
func (m *Memory) CreateSchedule(_ context.Context, schedule Schedule) (Schedule, error) {
	if schedule.ID == "" {
		id, err := NewScheduleID()
		if err != nil {
			return Schedule{}, err
		}
		schedule.ID = id
	}
	if schedule.Status == "" {
		schedule.Status = SchedulePending
	}
	if err := schedule.Validate(); err != nil {
		return Schedule{}, err
	}
	if schedule.Status != SchedulePending {
		return Schedule{}, protocol.Invalid("status: a new schedule is pending")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	data := m.tenantLocked(schedule.Tenant)
	if schedule.CreationKey != "" {
		for _, existing := range data.schedules {
			if existing.CreationKey != schedule.CreationKey {
				continue
			}
			if existing.CreationDigest != schedule.CreationDigest {
				return Schedule{}, ErrExists
			}
			replayed := cloneSchedule(*existing)
			replayed.Replayed = true
			return replayed, nil
		}
	}
	if _, exists := data.schedules[schedule.ID]; exists {
		return Schedule{}, ErrExists
	}
	now := m.nowLocked()
	stored := cloneSchedule(schedule)
	stored.Status = SchedulePending
	stored.Sent = 0
	stored.Attempts = 0
	stored.LastError = ""
	stored.CancelReason = ""
	stored.Replayed = false
	stored.Completed = nil
	stored.Lease = ""
	stored.LeaseOwner = ""
	stored.LeaseUntil = time.Time{}
	if stored.CreatedAt.IsZero() {
		stored.CreatedAt = now
	}
	stored.UpdatedAt = now
	data.schedules[stored.ID] = &stored
	return cloneSchedule(stored), nil
}

// Schedule implements Store.
func (m *Memory) Schedule(_ context.Context, tenant, id string) (Schedule, bool, error) {
	if err := expectTenant(tenant); err != nil {
		return Schedule{}, false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	data := m.tenants[tenant]
	if data == nil {
		return Schedule{}, false, nil
	}
	schedule, ok := data.schedules[id]
	if !ok {
		return Schedule{}, false, nil
	}
	return cloneSchedule(*schedule), true, nil
}

// Schedules implements Store.
func (m *Memory) Schedules(_ context.Context, tenant string) ([]Schedule, error) {
	if err := expectTenant(tenant); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	data := m.tenants[tenant]
	if data == nil {
		return nil, nil
	}
	out := make([]Schedule, 0, len(data.schedules))
	for _, schedule := range data.schedules {
		out = append(out, cloneSchedule(*schedule))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].NextRunAt.Equal(out[j].NextRunAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].NextRunAt.Before(out[j].NextRunAt)
	})
	return out, nil
}

// UpdateSchedule implements Store.
func (m *Memory) UpdateSchedule(_ context.Context, schedule Schedule) (Schedule, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.scheduleLocked(schedule.Tenant, schedule.ID)
	if !ok {
		return Schedule{}, ErrNotFound
	}
	now := m.nowLocked()
	if schedule.ExpectedUpdatedAt != nil && !sameInstant(current.UpdatedAt, *schedule.ExpectedUpdatedAt) {
		return Schedule{}, ErrVersionConflict
	}
	if current.LeaseUntil.After(now) {
		return Schedule{}, ErrLeased
	}
	updated := cloneSchedule(schedule)
	updated.Creator = current.Creator
	updated.CreatorKind = current.CreatorKind
	updated.CreatorVia = current.CreatorVia
	updated.CreationKey = current.CreationKey
	updated.CreationDigest = current.CreationDigest
	updated.Status = current.Status
	updated.Sent = current.Sent
	updated.Attempts = current.Attempts
	updated.LastError = current.LastError
	updated.CancelReason = current.CancelReason
	updated.CreatedAt = current.CreatedAt
	updated.Completed = current.Completed
	updated.Lease = ""
	updated.LeaseOwner = ""
	updated.LeaseUntil = time.Time{}
	updated.UpdatedAt = now
	if err := updated.Validate(); err != nil {
		return Schedule{}, err
	}
	*current = updated
	return cloneSchedule(updated), nil
}

func (m *Memory) scheduleLocked(tenant, id string) (*Schedule, bool) {
	data := m.tenants[tenant]
	if data == nil {
		return nil, false
	}
	schedule, ok := data.schedules[id]
	return schedule, ok
}

// SetScheduleEnabled implements Store.
func (m *Memory) SetScheduleEnabled(_ context.Context, tenant, id string, enabled bool, now time.Time) (Schedule, error) {
	if err := expectTenant(tenant); err != nil {
		return Schedule{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	schedule, ok := m.scheduleLocked(tenant, id)
	if !ok {
		return Schedule{}, ErrNotFound
	}
	if now.IsZero() {
		now = m.nowLocked()
	}
	if schedule.LeaseUntil.After(now) {
		return Schedule{}, ErrLeased
	}
	if enabled {
		switch schedule.Status {
		case SchedulePending:
			if schedule.NextRunAt.Before(now) {
				schedule.NextRunAt = now
			}
		case ScheduleDisabled, ScheduleDead:
			schedule.Status = SchedulePending
			schedule.Attempts = 0
			schedule.LastError = ""
			schedule.CancelReason = ""
			schedule.Completed = nil
			if schedule.NextRunAt.Before(now) {
				schedule.NextRunAt = now
			}
		default:
			return Schedule{}, ErrScheduleNotPending
		}
	} else {
		switch schedule.Status {
		case SchedulePending, ScheduleDisabled:
			schedule.Status = ScheduleDisabled
		default:
			return Schedule{}, ErrScheduleNotPending
		}
	}
	clearScheduleLease(schedule)
	schedule.UpdatedAt = now
	return cloneSchedule(*schedule), nil
}

// DeleteSchedule implements Store.
func (m *Memory) DeleteSchedule(_ context.Context, tenant, id string) error {
	if err := expectTenant(tenant); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	data := m.tenants[tenant]
	if data == nil {
		return ErrNotFound
	}
	if _, ok := data.schedules[id]; !ok {
		return ErrNotFound
	}
	delete(data.schedules, id)
	return nil
}

// LeaseSchedules implements Store.
func (m *Memory) LeaseSchedules(_ context.Context, tenant string, limit int, owner string) ([]Schedule, error) {
	if err := expectTenant(tenant); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 10
	}
	owner = leaseOwner(owner)
	m.mu.Lock()
	defer m.mu.Unlock()
	data := m.tenants[tenant]
	if data == nil {
		return nil, nil
	}
	now := m.nowLocked()
	due := make([]*Schedule, 0)
	for _, schedule := range data.schedules {
		if schedule.Status != SchedulePending || schedule.NextRunAt.After(now) {
			continue
		}
		if schedule.LeaseUntil.After(now) {
			continue
		}
		due = append(due, schedule)
	}
	sort.Slice(due, func(i, j int) bool {
		if due[i].NextRunAt.Equal(due[j].NextRunAt) {
			return due[i].ID < due[j].ID
		}
		return due[i].NextRunAt.Before(due[j].NextRunAt)
	})
	if len(due) > limit {
		due = due[:limit]
	}
	out := make([]Schedule, 0, len(due))
	for _, schedule := range due {
		token, err := newLeaseToken()
		if err != nil {
			return nil, err
		}
		schedule.Lease = token
		schedule.LeaseOwner = owner
		schedule.LeaseUntil = now.Add(m.cfg.LeaseTTL)
		schedule.UpdatedAt = now
		leased := cloneSchedule(*schedule)
		leased.Lease = token
		out = append(out, leased)
	}
	return out, nil
}

// AckSchedule implements Store.
func (m *Memory) AckSchedule(_ context.Context, tenant, id, lease string, result ScheduleResult) (Schedule, error) {
	if err := expectTenant(tenant); err != nil {
		return Schedule{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	schedule, ok := m.scheduleLocked(tenant, id)
	now := m.nowLocked()
	if !ok || schedule.Lease != lease || !schedule.LeaseUntil.After(now) {
		return Schedule{}, ErrLeaseLost
	}
	clearScheduleLease(schedule)
	schedule.UpdatedAt = now
	switch result.Outcome {
	case ScheduleSent:
		if result.At.IsZero() {
			result.At = now
		}
		schedule.Sent++
		schedule.Attempts = 0
		schedule.LastError = ""
		done := result.Done || schedule.EverySeconds == 0
		if !done && schedule.Times > 0 && schedule.Sent >= schedule.Times {
			done = true
		}
		if !done && schedule.UntilAt != nil && !result.NextRunAt.Before(*schedule.UntilAt) {
			done = true
		}
		if done {
			schedule.Status = ScheduleDone
			completed := result.At
			schedule.Completed = &completed
		} else {
			schedule.Status = SchedulePending
			schedule.NextRunAt = result.NextRunAt
			if schedule.NextRunAt.IsZero() {
				schedule.NextRunAt = result.At
			}
		}
	case ScheduleRetry:
		schedule.Attempts++
		schedule.LastError = ErrorCode(result.Err)
		if IsPermanent(result.Err) || schedule.Attempts >= m.cfg.Retry.MaxAttempts {
			schedule.Status = ScheduleDead
			completed := now
			schedule.Completed = &completed
		} else {
			schedule.Status = SchedulePending
			schedule.NextRunAt = now.Add(m.cfg.Retry.Delay(schedule.Attempts))
		}
	case ScheduleRevoked:
		schedule.Status = ScheduleCanceled
		schedule.CancelReason = result.Reason
		if schedule.CancelReason == "" {
			schedule.CancelReason = "revoked"
		}
		completed := now
		schedule.Completed = &completed
	default:
		return Schedule{}, protocol.Invalidf("outcome: %q isn't a schedule outcome", result.Outcome)
	}
	return cloneSchedule(*schedule), nil
}

// ReleaseSchedule implements Store.
func (m *Memory) ReleaseSchedule(_ context.Context, tenant, id, lease string, delay time.Duration) error {
	if err := expectTenant(tenant); err != nil {
		return err
	}
	if delay < 0 {
		delay = 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	schedule, ok := m.scheduleLocked(tenant, id)
	now := m.nowLocked()
	if !ok || schedule.Lease != lease || !schedule.LeaseUntil.After(now) {
		return ErrLeaseLost
	}
	clearScheduleLease(schedule)
	if schedule.Status == SchedulePending {
		schedule.NextRunAt = now.Add(delay)
	}
	schedule.UpdatedAt = now
	return nil
}

// RetrySchedule implements Store.
func (m *Memory) RetrySchedule(_ context.Context, tenant, id string) error {
	if err := expectTenant(tenant); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	schedule, ok := m.scheduleLocked(tenant, id)
	if !ok {
		return ErrNotFound
	}
	now := m.nowLocked()
	if schedule.LeaseUntil.After(now) {
		return ErrLeased
	}
	if schedule.Status != ScheduleDead {
		return ErrNotDead
	}
	schedule.Status = SchedulePending
	schedule.Attempts = 0
	schedule.LastError = ""
	schedule.CancelReason = ""
	schedule.Completed = nil
	schedule.NextRunAt = now
	schedule.UpdatedAt = now
	return nil
}

// ClearSchedule implements Store.
func (m *Memory) ClearSchedule(_ context.Context, tenant, id string) error {
	if err := expectTenant(tenant); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	schedule, ok := m.scheduleLocked(tenant, id)
	if !ok {
		return ErrNotFound
	}
	if schedule.Status != ScheduleDead {
		return ErrNotDead
	}
	delete(m.tenants[tenant].schedules, id)
	return nil
}

// clearLease detaches a delivery from its worker.
func clearLease(row *Delivery) {
	row.Lease = ""
	row.LeaseOwner = ""
	row.LeaseUntil = time.Time{}
}

func clearScheduleLease(schedule *Schedule) {
	schedule.Lease = ""
	schedule.LeaseOwner = ""
	schedule.LeaseUntil = time.Time{}
}

// NewLeaseToken returns an unguessable lease token. Storage adapters use it so
// every dialect mints the same shape of ack credential.
func NewLeaseToken() (string, error) { return newLeaseToken() }

func newLeaseToken() (string, error) {
	var raw [24]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}

var _ Store = (*Memory)(nil)
