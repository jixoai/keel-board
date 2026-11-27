package automation

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
)

// Store errors shared by the in-memory and PostgreSQL stores.
var (
	// ErrNotFound means the row doesn't exist in this tenant. It is also the
	// answer for another tenant's row: a caller can't tell the two apart.
	ErrNotFound = errors.New("automation: the row doesn't exist")
	// ErrExists means a watcher or schedule ID is already used in this tenant.
	ErrExists = errors.New("automation: the ID is already used")
	// ErrLeaseLost means an ack names a lease that has expired or been
	// replaced by another worker.
	ErrLeaseLost = errors.New("automation: the lease is not live")
	// ErrNotDead means a retry or clear names a row that isn't dead-lettered.
	ErrNotDead = errors.New("automation: the row is not dead-lettered")
	// ErrWatcherDisabled means a planner tried to advance a disabled watcher.
	ErrWatcherDisabled = errors.New("automation: the watcher is disabled")
	// ErrScheduleNotPending means an enable or update names a finished or
	// canceled schedule.
	ErrScheduleNotPending = errors.New("automation: the schedule is finished or canceled")
	// ErrLeased means a row is being worked on and can't be updated now.
	ErrLeased = errors.New("automation: the row is leased")
	// ErrVersionConflict means an update's expected version isn't current.
	ErrVersionConflict = errors.New("automation: the row changed")
	// ErrPermanent marks a failure that must not be retried.
	ErrPermanent = errors.New("automation: permanent failure")
)

// Config bounds retries and leases. Zero fields take the defaults: five
// attempts, one second doubling to thirty with jitter, and a five-minute lease.
type Config struct {
	Retry    RetryPolicy
	LeaseTTL time.Duration
}

// RetryPolicy decides how long a failed delivery or schedule run waits before
// its next attempt.
type RetryPolicy struct {
	MaxAttempts int           `json:"maxAttempts"`
	BaseDelay   time.Duration `json:"baseDelay"`
	MaxDelay    time.Duration `json:"maxDelay"`
	Jitter      float64       `json:"jitter"`
}

// DefaultLeaseTTL is the lease a worker holds on one delivery or schedule. It
// is comfortably longer than the default watcher action timeout, so a healthy
// action cannot outlive its lease.
const DefaultLeaseTTL = 5 * time.Minute

// DefaultRetry is the policy automation uses when none is configured.
var DefaultRetry = RetryPolicy{
	MaxAttempts: 5,
	BaseDelay:   time.Second,
	MaxDelay:    30 * time.Second,
	Jitter:      0.2,
}

// Validate checks the policy and fills a zero policy with DefaultRetry.
func (p RetryPolicy) Validate() (RetryPolicy, error) {
	if p == (RetryPolicy{}) {
		return DefaultRetry, nil
	}
	if p.MaxAttempts < 1 {
		return p, errors.New("automation: retry policy needs at least one attempt")
	}
	if p.BaseDelay < 0 || p.MaxDelay < 0 {
		return p, errors.New("automation: retry delays can't be negative")
	}
	if p.MaxDelay > 0 && p.MaxDelay < p.BaseDelay {
		return p, errors.New("automation: retry max delay is smaller than its base delay")
	}
	if p.Jitter < 0 || p.Jitter > 1 {
		return p, errors.New("automation: retry jitter is between 0 and 1")
	}
	return p, nil
}

// Delay is how long to wait after attempt n (1 for the first failure).
func (p RetryPolicy) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if p.BaseDelay <= 0 {
		return 0
	}
	delay := p.BaseDelay
	for i := 1; i < attempt && (p.MaxDelay == 0 || delay < p.MaxDelay); i++ {
		delay *= 2
	}
	if p.MaxDelay > 0 && delay > p.MaxDelay {
		delay = p.MaxDelay
	}
	if p.Jitter > 0 && delay > 0 {
		delay += time.Duration(float64(delay) * p.Jitter * rand.Float64())
	}
	return delay
}

// withDefaults validates a configuration and fills zero values.
func (c Config) withDefaults() (Config, error) {
	var err error
	if c.Retry, err = c.Retry.Validate(); err != nil {
		return Config{}, err
	}
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = DefaultLeaseTTL
	}
	return c, nil
}

// Store is the durable automation state. Memory and PostgreSQL implement it
// with the same semantics.
//
// Every method is tenant-scoped and safe for concurrent use. A missing tenant
// is refused rather than treated as a shared namespace.
type Store interface {
	// LeaseTTL is the lease this store grants. A watcher worker refuses an
	// action timeout that isn't shorter, so two workers can't run one row.
	LeaseTTL() time.Duration

	// Watchers. CreateWatcher stores a declaration; a duplicate ID is
	// ErrExists. UpdateWatcher replaces the declarative fields and preserves
	// the enabled flag and cursor.
	CreateWatcher(ctx context.Context, watcher Watcher) (Watcher, error)
	Watcher(ctx context.Context, tenant, id string) (Watcher, bool, error)
	Watchers(ctx context.Context, tenant string) ([]Watcher, error)
	UpdateWatcher(ctx context.Context, watcher Watcher) (Watcher, error)
	// SetWatcherEnabled flips a watcher and, when enabling with a cursor,
	// advances it so a long-disabled watcher starts with future events rather
	// than a backlog.
	SetWatcherEnabled(ctx context.Context, tenant, id string, enabled bool, cursor *uint64) (Watcher, error)
	DeleteWatcher(ctx context.Context, tenant, id string) error

	// AdvanceWatcher records that a planner evaluated up to cursor and queues
	// deliveries for events. The cursor only moves forward; inserting an
	// already-queued event is a no-op, so a crashed plan is safe to repeat.
	AdvanceWatcher(ctx context.Context, tenant, id string, cursor uint64, events []uint64) (int, error)

	// Deliveries. LeaseDeliveries returns at most one due delivery per
	// watcher, so a watcher's events stay ordered while watchers run in
	// parallel. AckDelivery finishes, retries, or dead-letters a leased row.
	LeaseDeliveries(ctx context.Context, tenant string, limit int, owner string) ([]Delivery, error)
	AckDelivery(ctx context.Context, tenant, watcherID string, eventSeq uint64, lease string, jobErr error) (AckResult, error)
	// ReleaseDelivery clears a lease without counting an attempt, so a worker
	// that isn't ready doesn't burn the delivery's retry budget.
	ReleaseDelivery(ctx context.Context, tenant, watcherID string, eventSeq uint64, lease string, delay time.Duration) error
	// RenewDelivery extends a live lease. It is the worker's execution-time
	// recheck and heartbeat: a row another worker has taken answers
	// ErrLeaseLost, so no action starts on a stale lease.
	RenewDelivery(ctx context.Context, tenant, watcherID string, eventSeq uint64, lease string, ttl time.Duration) error
	Deliveries(ctx context.Context, tenant, watcherID string, limit int) ([]Delivery, error)
	DeadDeliveries(ctx context.Context, tenant, watcherID string, limit int) ([]Delivery, error)
	RetryDelivery(ctx context.Context, tenant, watcherID string, eventSeq uint64) error
	// ClearDeliveries removes one watcher's dead letters and returns how many
	// it removed. Live and completed deliveries stay for audit.
	ClearDeliveries(ctx context.Context, tenant, watcherID string) (int64, error)
	DeliveryDepth(ctx context.Context, tenant, watcherID string) (Depth, error)

	// Schedules. CreateSchedule stores a pending plan; a duplicate ID is
	// ErrExists. UpdateSchedule changes a plan that isn't being run.
	CreateSchedule(ctx context.Context, schedule Schedule) (Schedule, error)
	Schedule(ctx context.Context, tenant, id string) (Schedule, bool, error)
	Schedules(ctx context.Context, tenant string) ([]Schedule, error)
	UpdateSchedule(ctx context.Context, schedule Schedule) (Schedule, error)
	SetScheduleEnabled(ctx context.Context, tenant, id string, enabled bool, now time.Time) (Schedule, error)
	DeleteSchedule(ctx context.Context, tenant, id string) error
	// LeaseSchedules returns due pending schedules, each leased to one worker.
	LeaseSchedules(ctx context.Context, tenant string, limit int, owner string) ([]Schedule, error)
	// AckSchedule records one run: sent advances the plan, retry backoffs or
	// dead-letters it, and revoked cancels it with a reason.
	AckSchedule(ctx context.Context, tenant, id, lease string, result ScheduleResult) (Schedule, error)
	ReleaseSchedule(ctx context.Context, tenant, id, lease string, delay time.Duration) error
	RetrySchedule(ctx context.Context, tenant, id string) error
	// ClearSchedule deletes a dead-lettered schedule; a live one is ErrNotDead.
	ClearSchedule(ctx context.Context, tenant, id string) error
}

// Authorizer is the board access seam automation uses. It has the same shape
// as the server's Authorizer, so a deployment passes one policy to both.
type Authorizer interface {
	Authorize(ctx context.Context, tenant string, actor protocol.Actor, channel string, permission protocol.Permission) error
}

// Board is the board log automation reads and writes. *log.Board implements
// it.
type Board interface {
	Head(tenant string) (uint64, error)
	History(tenant string, first, last uint64) ([]log.Event, error)
	ReadContext(ctx context.Context, tenant string, fn func(*log.Snapshot) error) error
	Update(ctx context.Context, tenant string, opts log.UpdateOptions, fn func(*log.Tx) error) error
}

// leaseOwner names a worker in a row's lease; empty means "worker".
func leaseOwner(owner string) string {
	if owner == "" {
		return "worker"
	}
	return owner
}

// expectTenant validates a tenant on a read or write.
func expectTenant(tenant string) error {
	return validTenant(tenant)
}
