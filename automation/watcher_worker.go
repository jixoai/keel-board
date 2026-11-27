package automation

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jixoai/keel-board/log"
)

// WorkResult counts one watcher worker pass.
type WorkResult struct {
	Planned  int `json:"planned"`
	Leased   int `json:"leased"`
	Acked    int `json:"acked"`
	Retried  int `json:"retried"`
	Dead     int `json:"dead"`
	Skipped  int `json:"skipped"`
	Deferred int `json:"deferred"`
	Lost     int `json:"lost"`
}

// WatcherWorkerConfig configures the watcher planner and executor. A worker is
// scoped to one tenant; several processes may serve the same tenant, because
// leasing and delivery identity make duplicate work harmless.
type WatcherWorkerConfig struct {
	Store Store
	// Board is the log the planner reads and actions write through. Required.
	Board Board
	// Actions is the deployment's registered action set. Required.
	Actions *Registry
	// Authorize constrains a bot's board writes. Nil allows them (development
	// only); a deployment supplies the same policy as the HTTP server.
	Authorize Authorizer
	Tenant    string
	// Owner names the worker in a lease; empty means "worker".
	Owner string
	// Limit is the most deliveries a pass leases. Zero means ten.
	Limit int
	// PlanLimit is the most events a pass considers per watcher. Zero means 500.
	PlanLimit int
	// PollInterval is how long Run waits after a pass with no work. Zero means
	// one second.
	PollInterval time.Duration
	// Defer is how long a disabled watcher's delivery is released for. Zero
	// means one second.
	Defer time.Duration
	// ActionTimeout bounds one action run. A timed-out action is a retryable
	// failure; an action that ignores its context can still block this worker,
	// so plugins must honor it. Zero means two minutes. It must be shorter
	// than the store's lease, or two workers could run one delivery; startup
	// refuses that.
	ActionTimeout time.Duration
	// BotAllowlist limits the bot actors a watcher may act as. Empty allows
	// every bot (development); a deployment should list its registered bots.
	BotAllowlist []string
	Logger       *slog.Logger
}

// WatcherWorker matches board events to watchers and runs their actions.
type WatcherWorker struct {
	store         Store
	board         Board
	actions       *Registry
	authorize     Authorizer
	tenant        string
	owner         string
	limit         int
	planLimit     int
	poll          time.Duration
	deferFor      time.Duration
	actionTimeout time.Duration
	leaseTTL      time.Duration
	botAllowlist  []string
	logger        *slog.Logger
}

// NewWatcherWorker validates cfg.
func NewWatcherWorker(cfg WatcherWorkerConfig) (*WatcherWorker, error) {
	if cfg.Store == nil || cfg.Board == nil || cfg.Actions == nil {
		return nil, errors.New("automation: a watcher worker needs a store, board, and action registry")
	}
	if err := expectTenant(cfg.Tenant); err != nil {
		return nil, err
	}
	if cfg.Limit <= 0 {
		cfg.Limit = 10
	}
	if cfg.PlanLimit <= 0 {
		cfg.PlanLimit = 500
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.Defer <= 0 {
		cfg.Defer = time.Second
	}
	if cfg.ActionTimeout <= 0 {
		cfg.ActionTimeout = 2 * time.Minute
	}
	leaseTTL := cfg.Store.LeaseTTL()
	if leaseTTL <= 0 {
		leaseTTL = DefaultLeaseTTL
	}
	if cfg.ActionTimeout >= leaseTTL {
		return nil, fmt.Errorf("automation: action timeout %s must be shorter than the lease %s", cfg.ActionTimeout, leaseTTL)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &WatcherWorker{
		store: cfg.Store, board: cfg.Board, actions: cfg.Actions,
		authorize: cfg.Authorize, tenant: cfg.Tenant, owner: leaseOwner(cfg.Owner),
		limit: cfg.Limit, planLimit: cfg.PlanLimit, poll: cfg.PollInterval,
		deferFor: cfg.Defer, actionTimeout: cfg.ActionTimeout, leaseTTL: leaseTTL,
		botAllowlist: append([]string(nil), cfg.BotAllowlist...), logger: cfg.Logger,
	}, nil
}

// Process plans and executes one batch. A planning failure doesn't stop
// deliveries an earlier pass queued; a delivery failure doesn't stop another
// watcher.
func (w *WatcherWorker) Process(ctx context.Context) (WorkResult, error) {
	planned, planErr := w.plan(ctx)
	exec, execErr := w.execute(ctx)
	exec.Planned = planned
	if planErr != nil && execErr == nil {
		return exec, planErr
	}
	return exec, execErr
}

func (w *WatcherWorker) plan(ctx context.Context) (int, error) {
	watchers, err := w.store.Watchers(ctx, w.tenant)
	if err != nil {
		return 0, err
	}
	head, err := w.board.Head(w.tenant)
	if err != nil {
		return 0, err
	}
	planned := 0
	var firstErr error
	for _, watcher := range watchers {
		if !watcher.Enabled {
			continue
		}
		if !watcher.CursorSet {
			// A new watcher starts with future events: a deployment doesn't get
			// a backlog replay because it declared an automation today.
			if _, err := w.store.AdvanceWatcher(ctx, w.tenant, watcher.ID, head, nil); err != nil {
				if errors.Is(err, ErrWatcherDisabled) || errors.Is(err, ErrNotFound) {
					continue
				}
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			continue
		}
		if watcher.Cursor >= head {
			continue
		}
		target := head
		if w.planLimit > 0 && watcher.Cursor+uint64(w.planLimit) < head {
			target = watcher.Cursor + uint64(w.planLimit)
		}
		events, err := w.board.History(w.tenant, watcher.Cursor+1, target)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		candidates, err := w.candidates(ctx, events)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		matched := make([]uint64, 0, len(candidates))
		bot := BotActor(watcher).ID
		for _, candidate := range candidates {
			if candidate.By == bot {
				// A watcher's own writes never wake it; otherwise an action
				// that posts would feed itself forever.
				continue
			}
			if !watcher.MatchesChannel(candidate.Channel) {
				continue
			}
			if !watcher.Filter.Matches(candidate.Type, candidate.Labels) {
				continue
			}
			matched = append(matched, candidate.Seq)
		}
		added, err := w.store.AdvanceWatcher(ctx, w.tenant, watcher.ID, target, matched)
		if err != nil {
			if errors.Is(err, ErrWatcherDisabled) || errors.Is(err, ErrNotFound) {
				continue
			}
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		planned += added
	}
	return planned, firstErr
}

// candidate is one board event as a watcher's filter sees it.
type candidate struct {
	Seq     uint64
	Type    string
	Channel string
	Labels  map[string]string
	// By is the event's author. A watcher's own writes never wake it, so the
	// planner skips an event its bot made.
	By string
}

// candidates reads the message metadata a channel/label filter needs. One
// snapshot serves the whole page.
func (w *WatcherWorker) candidates(ctx context.Context, events []log.Event) ([]candidate, error) {
	out := make([]candidate, 0, len(events))
	err := w.board.ReadContext(ctx, w.tenant, func(snapshot *log.Snapshot) error {
		for _, event := range events {
			item := candidate{Seq: event.Seq, Type: string(event.Type)}
			author, boardGenerated := eventAuthor(event)
			if id := log.ConcernedMessageID(event); id != 0 {
				if message := snapshot.Message(id); message != nil {
					item.Channel = message.Channel
					item.Labels = eventLabels(event, message)
					// A board-generated fact (task.ready, task.expired) has no
					// writer; falling back to the message's author would
					// suppress a bot's own task forever.
					if author == "" && !boardGenerated {
						author = message.By.Actor
					}
				}
			} else if payload, ok := event.Payload.(*log.ChannelCreatedPayload); ok {
				item.Channel = payload.Channel
			}
			item.By = author
			if item.Channel == "" {
				continue
			}
			out = append(out, item)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// eventLabels is a message's labels as the event left them. A post carries
// its labels; a label change applies its set/unset to the snapshot. Other
// events don't change labels, so the snapshot is their state.
func eventLabels(event log.Event, message *log.Message) map[string]string {
	switch payload := event.Payload.(type) {
	case *log.MessagePostedPayload:
		return cloneLabels(payload.Labels)
	case *log.LabelsChangedPayload:
		labels := cloneLabels(message.Labels)
		if labels == nil {
			labels = map[string]string{}
		}
		for _, key := range payload.Unset {
			delete(labels, key)
		}
		for key, value := range payload.Set {
			labels[key] = value
		}
		return labels
	default:
		return cloneLabels(message.Labels)
	}
}

// eventAuthor is the actor who wrote an event. The bool says the event is a
// board-generated fact with no writer at all (task.ready, task.expired): those
// must not inherit the message's author, or a watcher owned by that author
// would suppress its own task. A message's stored author is the fallback for
// other authorless facts such as receipts; an edit, label, or reaction names
// its own writer so a bot's own changes never wake its watcher.
func eventAuthor(event log.Event) (author string, boardGenerated bool) {
	switch payload := event.Payload.(type) {
	case *log.MessagePostedPayload:
		return payload.By.Actor, false
	case *log.ChannelCreatedPayload:
		return payload.By.Actor, false
	case *log.MessageEditedPayload:
		return payload.By.Actor, false
	case *log.MessageDeletedPayload:
		return payload.By.Actor, false
	case *log.ReactionPayload:
		return payload.By.Actor, false
	case *log.LabelsChangedPayload:
		return payload.By.Actor, false
	case *log.TaskCreatedPayload:
		return payload.By.Actor, false
	case *log.TaskAssignedPayload:
		return payload.By.Actor, false
	case *log.TaskNeedsChangedPayload:
		return payload.By.Actor, false
	case *log.TaskClaimedPayload:
		return payload.By.Actor, false
	case *log.TaskRenewedPayload:
		return payload.By.Actor, false
	case *log.TaskReleasedPayload:
		return payload.By.Actor, false
	case *log.TaskCompletedPayload:
		return payload.By.Actor, false
	case *log.TaskFailedPayload:
		return payload.By.Actor, false
	case *log.TaskBlockedPayload:
		return payload.By.Actor, false
	case *log.TaskUnblockedPayload:
		return payload.By.Actor, false
	case *log.TaskCanceledPayload:
		return payload.By.Actor, false
	case *log.TaskReopenedPayload:
		return payload.By.Actor, false
	case *log.ThreadSyncedPayload:
		return payload.By.Actor, false
	case *log.TaskReadyPayload, *log.TaskExpiredPayload:
		return "", true
	default:
		return "", false
	}
}

// execute leases and runs one batch of deliveries.
func (w *WatcherWorker) execute(ctx context.Context) (WorkResult, error) {
	result := WorkResult{}
	jobs, err := w.store.LeaseDeliveries(ctx, w.tenant, w.limit, w.owner)
	if err != nil {
		return result, err
	}
	result.Leased = len(jobs)
	var firstErr error
	for _, job := range jobs {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		watcher, found, err := w.store.Watcher(ctx, w.tenant, job.WatcherID)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if !found {
			// The watcher was deleted; its rows are gone with it.
			continue
		}
		if !watcher.Enabled {
			result.Deferred++
			if err := w.store.ReleaseDelivery(context.WithoutCancel(ctx), w.tenant, watcher.ID, job.EventSeq, job.Lease, w.deferFor); err != nil {
				if errors.Is(err, ErrLeaseLost) {
					result.Lost++
					continue
				}
				if firstErr == nil {
					firstErr = err
				}
			}
			continue
		}
		// Recheck the lease immediately before the action: a worker whose
		// lease expired while queued must not run it.
		if err := w.store.RenewDelivery(ctx, w.tenant, job.WatcherID, job.EventSeq, job.Lease, w.leaseTTL); err != nil {
			if errors.Is(err, ErrLeaseLost) {
				result.Lost++
				continue
			}
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		actionCtx, cancel := context.WithTimeout(ctx, w.actionTimeout)
		stopHeartbeat, leaseLost := w.startHeartbeat(ctx, job)
		jobErr := w.run(actionCtx, watcher, job)
		cancel()
		stopHeartbeat()
		if leaseLost.Load() {
			// Another worker took the delivery after this lease was lost.
			// Don't ack a row this worker no longer owns.
			result.Lost++
			w.logger.Warn("watcher lease was lost during the action",
				"tenant", w.tenant, "watcher", watcher.ID, "event", job.EventSeq)
			continue
		}
		if err := ctx.Err(); err != nil {
			// The action may not have finished; leave the lease to expire
			// rather than recording a failure it didn't have.
			return result, err
		}
		ack, ackErr := w.store.AckDelivery(context.WithoutCancel(ctx), w.tenant, job.WatcherID, job.EventSeq, job.Lease, jobErr)
		if ackErr != nil {
			if errors.Is(ackErr, ErrLeaseLost) {
				result.Lost++
				continue
			}
			if firstErr == nil {
				firstErr = ackErr
			}
			continue
		}
		switch {
		case ack.Acked:
			result.Acked++
		case ack.Retried:
			result.Retried++
		case ack.Dead:
			result.Dead++
		}
		if jobErr != nil {
			w.logger.Warn("watcher delivery failed",
				"tenant", w.tenant, "watcher", watcher.ID, "event", job.EventSeq,
				"attempts", ack.Attempts, "state", ack.State, "error", ErrorCode(jobErr))
		}
	}
	return result, firstErr
}

// startHeartbeat renews a delivery's lease while its action runs. A lease
// that another worker took stops the heartbeat and marks leaseLost; the worker
// then skips the ack. The goroutine stops when stop is called or ctx ends.
func (w *WatcherWorker) startHeartbeat(ctx context.Context, job Delivery) (stop func(), leaseLost *atomic.Bool) {
	lost := &atomic.Bool{}
	interval := w.leaseTTL / 3
	if interval < 50*time.Millisecond {
		interval = 50 * time.Millisecond
	}
	stopped := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stopped:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := w.store.RenewDelivery(context.WithoutCancel(ctx), w.tenant, job.WatcherID, job.EventSeq, job.Lease, w.leaseTTL)
				if errors.Is(err, ErrLeaseLost) {
					lost.Store(true)
					return
				}
				if err != nil {
					w.logger.Warn("watcher lease renewal failed",
						"tenant", w.tenant, "watcher", job.WatcherID, "event", job.EventSeq,
						"error", ErrorCode(err))
				}
			}
		}
	}()
	return func() {
		close(stopped)
		<-done
	}, lost
}

// run executes one delivery's action. It never writes a delivery error's text
// to a row; the store records ErrorCode instead.
func (w *WatcherWorker) run(ctx context.Context, watcher Watcher, job Delivery) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			// A plugin panic is one delivery's failure, never the worker's or
			// the board's. The panic value stays out of the row: it may carry
			// channel content.
			err = Permanent(errors.New("automation: the watcher action panicked"))
		}
	}()
	events, err := w.board.History(w.tenant, job.EventSeq, job.EventSeq)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return Permanent(errors.New("automation: the matched event no longer exists"))
	}
	event := events[0]
	var message *log.Message
	if id := log.ConcernedMessageID(event); id != 0 {
		err := w.board.ReadContext(ctx, w.tenant, func(snapshot *log.Snapshot) error {
			message = snapshot.Message(id)
			return nil
		})
		if err != nil {
			return err
		}
		if message != nil {
			// Detach from the folded state: the board may fold a later event
			// while the action runs, and an action must never observe or
			// mutate that shared message.
			cloned := deepCopy(*message)
			message = &cloned
		}
	}
	if !BotAllowed(BotActor(watcher).ID, w.botAllowlist) {
		return Permanent(errors.New("automation: the watcher bot is not allowlisted"))
	}
	action, ok := w.actions.Get(watcher.Action.Name)
	if !ok {
		return Permanent(errors.New("automation: the watcher action is not registered on this process"))
	}
	call := &ActionCall{
		tenant: w.tenant, watcher: watcher, event: event, message: message,
		bot: BotActor(watcher), board: w.board, authorize: w.authorize,
	}
	return action.Run(ctx, call)
}

// Run plans and executes until ctx ends. It waits PollInterval only when a
// pass found nothing to do; a busy watcher runs continuously.
func (w *WatcherWorker) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.poll)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		result, err := w.Process(ctx)
		if err != nil && ctx.Err() == nil {
			w.logger.Error("watcher worker pass failed", "tenant", w.tenant, "error", ErrorCode(err))
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if result.Planned > 0 || result.Leased > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
