package syncstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
)

// OutboundWorkerConfig configure one tenant's outbound drains. A lane is a
// board thread, so one worker drains every key's deliveries for that thread
// in order.
type OutboundWorkerConfig struct {
	Store    Store
	Registry *boardsync.Registry
	Runner   *boardsync.Runner
	// Host records message.sync_failed when the outbox gives up. Required.
	Host         boardsync.Host
	Tenant       string
	Owner        string
	Limit        int
	PollInterval time.Duration
	Logger       *slog.Logger
}

// OutboundWorker leases and delivers outbox jobs. A successful post records
// its copy and clears that target's earlier failures; a dead delivery records
// message.sync_failed and does not block the next job on the lane.
type OutboundWorker struct {
	store    Store
	registry *boardsync.Registry
	runner   *boardsync.Runner
	host     boardsync.Host
	tenant   string
	owner    string
	limit    int
	poll     time.Duration
	logger   *slog.Logger
}

// NewOutboundWorker validates cfg.
func NewOutboundWorker(cfg OutboundWorkerConfig) (*OutboundWorker, error) {
	if cfg.Store == nil || cfg.Registry == nil || cfg.Runner == nil || cfg.Host == nil {
		return nil, errors.New("syncstore: an outbound worker needs a store, registry, runner, and host")
	}
	if err := validTenant(cfg.Tenant); err != nil {
		return nil, err
	}
	if cfg.Limit <= 0 {
		cfg.Limit = 10
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &OutboundWorker{
		store: cfg.Store, registry: cfg.Registry, runner: cfg.Runner, host: cfg.Host,
		tenant: cfg.Tenant, owner: leaseOwner(cfg.Owner),
		limit: cfg.Limit, poll: cfg.PollInterval, logger: cfg.Logger,
	}, nil
}

// Process leases and delivers one batch. Deliveries on a lane run in order;
// other lanes proceed. An edit, delete, or reaction reads the copy of the
// board message first, so it changes the platform's copy rather than posting
// twice.
func (w *OutboundWorker) Process(ctx context.Context) (WorkResult, error) {
	result := WorkResult{}
	jobs, err := w.store.LeaseOutbox(ctx, w.tenant, w.limit, w.owner)
	if err != nil {
		return result, err
	}
	result.Leased = len(jobs)
	var firstErr error
	for _, job := range jobs {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		jobErr, copy, skipped := w.deliver(ctx, job)
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if skipped {
			result.Skipped++
		}
		ack, ackErr := w.store.AckOutbox(context.WithoutCancel(ctx), w.tenant, job.ID, job.Lease, jobErr, copy)
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
		if ack.Dead {
			w.recordFailure(ctx, job, jobErr)
		}
		if jobErr != nil {
			w.logger.Warn("sync delivery failed",
				"tenant", w.tenant, "key", job.Key, "delivery", job.ID,
				"lane", job.Lane, "attempts", ack.Attempts,
				"state", ack.State, "error", ErrorCode(jobErr))
		}
	}
	return result, firstErr
}

// Run drains the tenant's outbox until ctx ends.
func (w *OutboundWorker) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.poll)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		result, err := w.Process(ctx)
		if err != nil && ctx.Err() == nil {
			w.logger.Error("sync outbound worker pass failed",
				"tenant", w.tenant, "error", ErrorCode(err))
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err == nil && result.Leased > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// deliver calls the entry's sync function for one job. It returns the job's
// error, an optional receipt copy, and whether it skipped an already-delivered
// post. A missing copy for a change is permanent: retrying can't create one.
func (w *OutboundWorker) deliver(ctx context.Context, job Delivery) (error, *Copy, bool) {
	entry, ok := w.registry.Get(job.Key)
	if !ok {
		return boardsync.Permanent(fmt.Errorf("syncstore: sync entry %q is not registered", job.Key)), nil, false
	}
	switch job.Event.Type {
	case boardsync.BoardMessagePosted, boardsync.BoardThreadSynced:
		// A copy means the post already landed; acking again must not post it
		// a second time.
		if _, found, err := w.store.Copy(ctx, w.tenant, job.Key, job.Target, job.Message.ID); err != nil {
			return err, nil, false
		} else if found {
			return nil, nil, true
		}
	}
	copyID := ""
	switch job.Event.Type {
	case boardsync.BoardMessageEdited, boardsync.BoardMessageDeleted,
		boardsync.BoardReactionAdded, boardsync.BoardReactionRemove:
		found, err := w.copyFor(ctx, job)
		if err != nil {
			return err, nil, false
		}
		copyID = found.ExternalMessage
	}
	receipt, err := w.runner.Outbound(ctx, entry, job.Event, job.Message, &job.Target, copyID)
	if err != nil {
		return err, nil, false
	}
	var copy *Copy
	if job.Event.Type == boardsync.BoardMessagePosted || job.Event.Type == boardsync.BoardThreadSynced {
		copy = copyFromReceipt(job, receipt)
	}
	return nil, copy, false
}

// copyFor finds the platform copy a change addresses. An exact target wins; a
// single copy for the same board message is the fallback for a root that was
// posted to a channel and is now changed through its thread.
func (w *OutboundWorker) copyFor(ctx context.Context, job Delivery) (Copy, error) {
	if copy, found, err := w.store.Copy(ctx, w.tenant, job.Key, job.Target, job.Message.ID); err != nil {
		return Copy{}, err
	} else if found {
		return copy, nil
	}
	copies, err := w.store.CopyForMessage(ctx, w.tenant, job.Key, job.Message.ID)
	if err != nil {
		return Copy{}, err
	}
	if len(copies) == 1 {
		return copies[0], nil
	}
	if len(copies) > 1 {
		return Copy{}, boardsync.Permanent(fmt.Errorf("syncstore: board message %d has several copies for %q", job.Message.ID, job.Key))
	}
	return Copy{}, boardsync.Permanent(fmt.Errorf("syncstore: board message %d has no copy for %q", job.Message.ID, job.Key))
}

// recordFailure writes the board receipt an operator sees after the outbox
// gives up. The error is the content-free class, never a platform body.
func (w *OutboundWorker) recordFailure(ctx context.Context, job Delivery, jobErr error) {
	ref := job.Target.Ref()
	event := boardsync.BoardEvent{
		Type: boardsync.BoardMessageSyncFailed, Thread: job.Message.Thread,
		Message: &job.Message, External: &ref, Error: ErrorCode(jobErr),
	}
	if err := w.host.Record(context.WithoutCancel(ctx), event); err != nil {
		w.logger.Error("sync failure receipt failed",
			"tenant", w.tenant, "key", job.Key, "delivery", job.ID, "error", ErrorCode(err))
	}
}
