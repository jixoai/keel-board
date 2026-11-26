package syncstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
)

// WorkResult counts one worker pass.
type WorkResult struct {
	Leased  int `json:"leased"`
	Acked   int `json:"acked"`
	Skipped int `json:"skipped"`
	Retried int `json:"retried"`
	Dead    int `json:"dead"`
	Lost    int `json:"lost"`
}

// InboundWorkerConfig configure one inbound lane consumer. A worker is scoped
// to one tenant and one key, matching the server's deployment ownership: one
// process holds a key, so an event is never imported twice.
type InboundWorkerConfig struct {
	Store    Store
	Registry *boardsync.Registry
	Runner   *boardsync.Runner
	Tenant   string
	Key      boardsync.Key
	// Owner names the worker in a row's lease; empty means "worker".
	Owner string
	// Limit is the most items a pass leases. Zero means ten.
	Limit int
	// PollInterval is how long Run waits after a pass with no work. Zero means
	// one second.
	PollInterval time.Duration
	Logger       *slog.Logger
}

// InboundWorker leases and imports inbox items. The import is idempotent by
// event ID in the host, so a crash after the host's write and before the ack
// re-imports the same event into the same message.
type InboundWorker struct {
	store    Store
	registry *boardsync.Registry
	runner   *boardsync.Runner
	tenant   string
	key      boardsync.Key
	owner    string
	limit    int
	poll     time.Duration
	logger   *slog.Logger
}

// NewInboundWorker validates cfg.
func NewInboundWorker(cfg InboundWorkerConfig) (*InboundWorker, error) {
	if cfg.Store == nil || cfg.Registry == nil || cfg.Runner == nil {
		return nil, errors.New("syncstore: an inbound worker needs a store, registry, and runner")
	}
	if err := validTenant(cfg.Tenant); err != nil {
		return nil, err
	}
	if err := expectKey(cfg.Key); err != nil {
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
	return &InboundWorker{
		store: cfg.Store, registry: cfg.Registry, runner: cfg.Runner,
		tenant: cfg.Tenant, key: cfg.Key, owner: leaseOwner(cfg.Owner),
		limit: cfg.Limit, poll: cfg.PollInterval, logger: cfg.Logger,
	}, nil
}

// Process leases and imports one batch, then acks each item. A failed import
// is retried on its lane by the store's policy; a dead item no longer blocks
// the next event of that external thread. A lost lease means another worker
// owns the row now; this pass doesn't ack it.
func (w *InboundWorker) Process(ctx context.Context) (WorkResult, error) {
	result := WorkResult{}
	items, err := w.store.LeaseInbox(ctx, w.tenant, w.key, w.limit, w.owner)
	if err != nil {
		return result, err
	}
	result.Leased = len(items)
	var firstErr error
	for _, item := range items {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		entry, ok := w.registry.Get(w.key)
		var jobErr error
		if !ok {
			jobErr = boardsync.Permanent(fmt.Errorf("syncstore: sync entry %q is not registered", w.key))
		} else {
			jobErr = w.runner.Inbound(ctx, entry, item.Event)
		}
		if err := ctx.Err(); err != nil {
			// The import may not have finished; leave the lease to expire
			// rather than recording a failure it didn't have.
			return result, err
		}
		ack, ackErr := w.store.AckInbox(context.WithoutCancel(ctx), w.tenant, w.key, item.ID, item.Lease, jobErr)
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
			w.logger.Warn("sync inbound failed",
				"tenant", w.tenant, "key", w.key, "event", item.ID,
				"thread", item.Thread, "attempts", ack.Attempts,
				"state", ack.State, "error", ErrorCode(jobErr))
		}
	}
	return result, firstErr
}

// Run drains the worker's lane until ctx ends. It waits PollInterval only when
// a pass found nothing to do; a busy lane runs continuously.
func (w *InboundWorker) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.poll)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		result, err := w.Process(ctx)
		if err != nil && ctx.Err() == nil {
			w.logger.Error("sync inbound worker pass failed",
				"tenant", w.tenant, "key", w.key, "error", ErrorCode(err))
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
