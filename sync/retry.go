package sync

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// RetryPolicy decides how a failed outbound delivery or inbound import is
// tried again. The upstream defaults are five tries, one second doubling to
// thirty, with jitter.
type RetryPolicy struct {
	MaxAttempts int           `json:"maxAttempts"`
	BaseDelay   time.Duration `json:"baseDelay"`
	MaxDelay    time.Duration `json:"maxDelay"`
	// Jitter adds up to this fraction of the delay, so many lanes failing at
	// once don't retry in lockstep. 0 disables it.
	Jitter float64 `json:"jitter"`
}

// DefaultRetry is the policy sync uses when none is configured: five tries, 1s
// to 30s, 20% jitter.
var DefaultRetry = RetryPolicy{
	MaxAttempts: 5,
	BaseDelay:   time.Second,
	MaxDelay:    30 * time.Second,
	Jitter:      0.2,
}

// InboundRetry is the policy for a platform event that keeps failing: the
// upstream local board gives up after 10 tries.
var InboundRetry = RetryPolicy{
	MaxAttempts: 10,
	BaseDelay:   time.Second,
	MaxDelay:    30 * time.Second,
	Jitter:      0.2,
}

// Validate checks the policy and fills defaults. A zero policy becomes
// DefaultRetry.
func (p RetryPolicy) Validate() (RetryPolicy, error) {
	if p == (RetryPolicy{}) {
		return DefaultRetry, nil
	}
	if p.MaxAttempts < 1 {
		return p, errors.New("sync: retry policy needs at least one attempt")
	}
	if p.BaseDelay < 0 || p.MaxDelay < 0 {
		return p, errors.New("sync: retry delays can't be negative")
	}
	if p.MaxDelay > 0 && p.MaxDelay < p.BaseDelay {
		return p, errors.New("sync: retry max delay is smaller than its base delay")
	}
	if p.Jitter < 0 || p.Jitter > 1 {
		return p, errors.New("sync: retry jitter is between 0 and 1")
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
	for i := 1; i < attempt && delay < p.MaxDelay; i++ {
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

// sleep is a seam: tests set it to avoid waiting.
var sleep = func(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// ErrOutboxFull means an outbox lane holds its limit of queued jobs.
var ErrOutboxFull = errors.New("sync: outbox lane is full")

// Do runs fn until it succeeds, fails permanently, or the policy runs out.
// A context cancellation stops it. It returns the last error.
func Do(ctx context.Context, policy RetryPolicy, fn func(context.Context) error) error {
	_, err := DoCount(ctx, policy, fn)
	return err
}

// DoCount is Do, and also returns how many attempts it made.
func DoCount(ctx context.Context, policy RetryPolicy, fn func(context.Context) error) (attempts int, err error) {
	policy, policyErr := policy.Validate()
	if policyErr != nil {
		return 0, policyErr
	}
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return attempt - 1, err
		}
		attempts = attempt
		err = fn(ctx)
		if err == nil {
			return attempts, nil
		}
		if IsPermanent(err) || attempt >= policy.MaxAttempts {
			return attempts, err
		}
		if waitErr := sleep(ctx, policy.Delay(attempt)); waitErr != nil {
			return attempts, waitErr
		}
	}
}

// outJob is one queued delivery.
type outJob struct {
	run func(context.Context) error
}

// DeadLetter is a job a retry policy gave up on. It is kept so an operator can
// see what never reached the platform; the next job on the lane still runs.
type DeadLetter struct {
	Lane     string    `json:"lane"`
	Attempts int       `json:"attempts"`
	Err      string    `json:"error"`
	At       time.Time `json:"at"`
}

// DrainResult counts what one Drain run did.
type DrainResult struct {
	Done    int   `json:"done"`
	Retried int   `json:"retried"`
	Dead    int   `json:"dead"`
	Err     error `json:"-"`
}

// Outbox is a per-lane FIFO of outbound jobs with retries. Deliveries on one
// board thread's lane run in order; a job that fails all its tries is
// dead-lettered and the lane moves on, so one bad message never blocks the
// next.
//
// It is in-memory and process-local: a real deployment backs the queue with
// the board log or a durable queue, and calls Drain from its consumer. The
// semantics (lane order, attempts, dead letter) are the part to keep.
type Outbox struct {
	mu      sync.Mutex
	lanes   map[string][]*outJob
	dead    []DeadLetter
	policy  RetryPolicy
	maxLane int
	now     func() time.Time
}

// NewOutbox returns an outbox. A zero policy uses DefaultRetry; maxPerLane <=
// 0 means 256 jobs per lane.
func NewOutbox(policy RetryPolicy, maxPerLane int) *Outbox {
	checked, err := policy.Validate()
	if err != nil {
		checked = DefaultRetry
	}
	if maxPerLane <= 0 {
		maxPerLane = 256
	}
	return &Outbox{lanes: make(map[string][]*outJob), policy: checked, maxLane: maxPerLane, now: time.Now}
}

// WithClock makes dead-letter timestamps deterministic in tests.
func (o *Outbox) WithClock(now func() time.Time) *Outbox {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.now = now
	return o
}

// Enqueue appends a job to lane. The job runs on Drain.
func (o *Outbox) Enqueue(lane string, run func(context.Context) error) error {
	if lane == "" {
		return errors.New("sync: outbox job has no lane")
	}
	if run == nil {
		return errors.New("sync: outbox job has no function")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.lanes[lane]) >= o.maxLane {
		return fmt.Errorf("%w: %s", ErrOutboxFull, lane)
	}
	o.lanes[lane] = append(o.lanes[lane], &outJob{run: run})
	return nil
}

// Len is how many jobs are queued.
func (o *Outbox) Len() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	total := 0
	for _, lane := range o.lanes {
		total += len(lane)
	}
	return total
}

// DeadLetters returns what Drain gave up on.
func (o *Outbox) DeadLetters() []DeadLetter {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]DeadLetter(nil), o.dead...)
}

// Drain runs queued jobs until none are left or ctx is canceled. A canceled
// drain leaves the current job at the front of its lane.
func (o *Outbox) Drain(ctx context.Context) DrainResult {
	result := DrainResult{}
	for {
		if err := ctx.Err(); err != nil {
			result.Err = err
			return result
		}
		lane, job, ok := o.pop()
		if !ok {
			return result
		}
		attempts, err := DoCount(ctx, o.policy, job.run)
		if attempts > 1 {
			result.Retried += attempts - 1
		}
		if err == nil {
			result.Done++
			continue
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			o.pushFront(lane, job)
			result.Err = ctxErr
			return result
		}
		result.Dead++
		o.mu.Lock()
		o.dead = append(o.dead, DeadLetter{Lane: lane, Attempts: attempts, Err: err.Error(), At: o.now()})
		o.mu.Unlock()
	}
}

func (o *Outbox) pop() (string, *outJob, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	lanes := make([]string, 0, len(o.lanes))
	for lane, jobs := range o.lanes {
		if len(jobs) > 0 {
			lanes = append(lanes, lane)
		}
	}
	if len(lanes) == 0 {
		return "", nil, false
	}
	sort.Strings(lanes)
	lane := lanes[0]
	job := o.lanes[lane][0]
	o.lanes[lane] = o.lanes[lane][1:]
	if len(o.lanes[lane]) == 0 {
		delete(o.lanes, lane)
	}
	return lane, job, true
}

func (o *Outbox) pushFront(lane string, job *outJob) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.lanes[lane] = append([]*outJob{job}, o.lanes[lane]...)
}
