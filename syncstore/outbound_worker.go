package syncstore

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
)

// errEntryNotReady means a job's entry isn't registered for this tenant (or
// its provider/platform pair is inconsistent). It is transient: the worker
// releases the job without counting an attempt and never dead-letters it.
var errEntryNotReady = errors.New("syncstore: sync entry is not ready")

// errNoCopy means a change addresses a copy the board never recorded. Edits,
// deletes, and label changes are best-effort: there is nothing to change and
// the delivery is a skip, not a failure.
var errNoCopy = errors.New("syncstore: no copy to change")

// errCopyPending means a reaction arrived before its message's copy was
// recorded. It is transient so the outbox retries after the post's receipt.
var errCopyPending = errors.New("syncstore: the copy is not recorded yet")

// OutboundWorkerConfig configure one tenant's outbound drains. A lane is a
// board thread, so one worker drains its owned keys' deliveries for that
// thread in order. Keys is required: an empty list owns nothing, never "all".
type OutboundWorkerConfig struct {
	Store    Store
	Registry *boardsync.Registry
	Runner   *boardsync.Runner
	// Host records message.synced retries and message.sync_failed. Required.
	Host   boardsync.Host
	Tenant string
	// Keys are the connections this worker owns. Required and non-empty.
	Keys []boardsync.Key
	// Renderer, when set, renders labels.changed edits.
	Renderer boardsync.Renderer
	// Files reads a board message's attachment bytes just before a post. Nil
	// leaves attachments as notes; the message still goes out.
	Files boardsync.FileLoader
	Owner string
	Limit int
	// PollInterval is how long Run waits after a pass with no work. Zero means
	// one second.
	PollInterval time.Duration
	// Defer is how long an unready entry's job is released for. Zero means one
	// second.
	Defer  time.Duration
	Logger *slog.Logger
}

// OutboundWorker leases and delivers outbox jobs. A successful post records
// its copy before its receipt and clears that target's earlier failures; a
// dead delivery records message.sync_failed and does not block the next job on
// the lane.
type OutboundWorker struct {
	store    Store
	registry *boardsync.Registry
	runner   *boardsync.Runner
	host     boardsync.Host
	tenant   string
	keys     []boardsync.Key
	renderer boardsync.Renderer
	files    boardsync.FileLoader
	owner    string
	limit    int
	poll     time.Duration
	deferFor time.Duration
	logger   *slog.Logger

	typingMu  sync.Mutex
	typingOff map[boardsync.Key]bool
}

// NewOutboundWorker validates cfg.
func NewOutboundWorker(cfg OutboundWorkerConfig) (*OutboundWorker, error) {
	if cfg.Store == nil || cfg.Registry == nil || cfg.Runner == nil || cfg.Host == nil {
		return nil, errors.New("syncstore: an outbound worker needs a store, registry, runner, and host")
	}
	if err := validTenant(cfg.Tenant); err != nil {
		return nil, err
	}
	owned, err := ownedKeys(cfg.Keys)
	if err != nil {
		return nil, err
	}
	if len(owned) == 0 {
		return nil, errors.New("syncstore: an outbound worker needs at least one owned key")
	}
	keys := make([]boardsync.Key, 0, len(owned))
	for key := range owned {
		keys = append(keys, key)
	}
	if cfg.Limit <= 0 {
		cfg.Limit = 10
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.Defer <= 0 {
		cfg.Defer = time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &OutboundWorker{
		store: cfg.Store, registry: cfg.Registry, runner: cfg.Runner, host: cfg.Host,
		tenant: cfg.Tenant, keys: keys, renderer: cfg.Renderer, files: cfg.Files,
		owner: leaseOwner(cfg.Owner),
		limit: cfg.Limit, poll: cfg.PollInterval, deferFor: cfg.Defer, logger: cfg.Logger,
		typingOff: make(map[boardsync.Key]bool),
	}, nil
}

// Process leases and delivers one batch. Deliveries on a lane run in order;
// other lanes proceed. An edit, delete, or reaction reads the copy of the
// board message first, so it changes the platform's copy rather than posting
// twice.
func (w *OutboundWorker) Process(ctx context.Context) (WorkResult, error) {
	result := WorkResult{}
	jobs, err := w.store.LeaseOutbox(ctx, w.tenant, w.keys, w.limit, w.owner)
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
		if errors.Is(jobErr, errEntryNotReady) {
			result.Deferred++
			if err := w.store.ReleaseOutbox(context.WithoutCancel(ctx), w.tenant, job.ID, job.Lease, w.deferFor); err != nil {
				if errors.Is(err, ErrLeaseLost) {
					result.Lost++
					continue
				}
				if firstErr == nil {
					firstErr = err
				}
			}
			w.logger.Warn("sync delivery entry is not ready",
				"tenant", w.tenant, "key", job.Key, "delivery", job.ID, "lane", job.Lane)
			continue
		}
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
		if ack.Dead && recordsFailure(job.Event.Type) {
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
	entry, ok := w.registry.Get(w.tenant, job.Key)
	if !ok || entry.Validate() != nil || entry.Tenant != w.tenant || entry.Key != job.Key {
		return errEntryNotReady, nil, false
	}
	switch job.Event.Type {
	case boardsync.BoardMessagePosted, boardsync.BoardThreadSynced:
		// A copy means the post already landed; acking again must not post it
		// a second time. The receipt is retried because it may have failed
		// after the copy was saved.
		if copy, found, err := w.store.Copy(ctx, w.tenant, job.Key, job.Target, job.Message.ID); err != nil {
			return err, nil, false
		} else if found {
			if err := w.recordSynced(ctx, job, copy); err != nil {
				return err, nil, false
			}
			return nil, nil, true
		}
	}
	if job.Event.Type == boardsync.BoardMessageDeleted {
		return w.deleteCopies(ctx, entry, job)
	}
	copyID := ""
	switch job.Event.Type {
	case boardsync.BoardMessageEdited, boardsync.BoardReactionAdded, boardsync.BoardReactionRemove:
		found, err := w.copyForChange(ctx, job)
		if errors.Is(err, errNoCopy) {
			return nil, nil, true
		}
		if errors.Is(err, errCopyPending) {
			return err, nil, false
		}
		if err != nil {
			return err, nil, false
		}
		copyID = found.ExternalMessage
	case boardsync.BoardLabelsChanged:
		// A renderer edits a copy's text; a Labeler needs the copy's external
		// thread too, even for a channel root (the copy is the issue/thread).
		_, isLabeler := entry.Provider.(boardsync.Labeler)
		if w.renderer != nil || isLabeler {
			found, err := w.copyForChange(ctx, job)
			if errors.Is(err, errNoCopy) {
				return nil, nil, true
			}
			if err != nil {
				return err, nil, false
			}
			copyID = found.ExternalMessage
			if isLabeler {
				target := found.Target
				if found.ExternalThread != "" {
					subthread := target.Subthread
					target.Thread = found.ExternalThread
					target.Kind = boardsync.TargetThread
					if subthread != "" && !strings.HasPrefix(subthread, target.Thread+":c:") {
						subthread = ""
					}
					target.Subthread = subthread
				}
				job.Target = target
			}
		}
	}
	if isPostEvent(job.Event.Type) {
		w.showTyping(ctx, entry, job)
	}
	opts := boardsync.OutboundOptions{
		Renderer: w.renderer,
		Files:    w.files,
		CopySink: func(ctx context.Context, req boardsync.CopyRequest) error {
			saved := copyFromRequest(req)
			if saved == nil {
				return nil
			}
			_, err := w.store.SaveCopy(ctx, w.tenant, *saved)
			return err
		},
	}
	receipt, err := w.runner.OutboundWithOptions(ctx, entry, job.Event, job.Message, &job.Target, copyID, opts)
	if err != nil {
		// A change is best-effort: a platform that can't make it (unsupported,
		// bad target) doesn't mark the message undelivered. Transient failures
		// still retry; a reaction's missing copy is retried explicitly.
		if !isPostEvent(job.Event.Type) && boardsync.IsPermanent(err) {
			w.logger.Warn("sync change skipped",
				"tenant", w.tenant, "key", job.Key, "delivery", job.ID,
				"type", job.Event.Type, "error", ErrorCode(err))
			return nil, nil, true
		}
		return err, nil, false
	}
	var copy *Copy
	if job.Event.Type == boardsync.BoardMessagePosted || job.Event.Type == boardsync.BoardThreadSynced {
		copy = copyFromReceipt(job, receipt)
	}
	return nil, copy, false
}

// copyForChange finds the platform copy an edit, delete, or reaction
// addresses. An exact target wins; a root posted to a channel is reached
// through its concrete thread by matching ExternalThread. A missing copy is a
// skip for edits and labels, and a retry for a reaction.
func (w *OutboundWorker) copyForChange(ctx context.Context, job Delivery) (Copy, error) {
	if copied, found, err := w.store.Copy(ctx, w.tenant, job.Key, job.Target, job.Message.ID); err != nil {
		return Copy{}, err
	} else if found {
		return copied, nil
	}
	copies, err := w.store.CopyForMessage(ctx, w.tenant, job.Key, job.Message.ID)
	if err != nil {
		return Copy{}, err
	}
	if job.Target.Thread != "" {
		var rootCopies []Copy
		for _, copied := range copies {
			if copied.Target.Kind == boardsync.TargetChannel && copied.ExternalThread == job.Target.Thread {
				rootCopies = append(rootCopies, copied)
			}
		}
		if len(rootCopies) == 1 {
			return rootCopies[0], nil
		}
	}
	if len(copies) == 1 {
		return copies[0], nil
	}
	if isReaction(job.Event.Type) {
		return Copy{}, errCopyPending
	}
	return Copy{}, errNoCopy
}

// deleteCopies propagates a board delete to every copy of the message. The
// copies are read once per delivery; a second delivery finds the same rows and
// treats a platform's not-found as success.
func (w *OutboundWorker) deleteCopies(ctx context.Context, entry boardsync.Entry, job Delivery) (error, *Copy, bool) {
	copies, err := w.store.CopyForMessage(ctx, w.tenant, job.Key, job.Message.ID)
	if err != nil {
		return err, nil, false
	}
	if len(copies) == 0 {
		return nil, nil, true
	}
	seen := make(map[string]struct{}, len(copies))
	for _, copied := range copies {
		if copied.ExternalMessage == "" {
			continue
		}
		if _, duplicate := seen[copied.ExternalMessage]; duplicate {
			continue
		}
		seen[copied.ExternalMessage] = struct{}{}
		target := copied.Target
		_, err := w.runner.Outbound(ctx, entry, job.Event, job.Message, &target, copied.ExternalMessage)
		if err == nil || errors.Is(err, boardsync.ErrNotFound) {
			// The platform message is gone; forget the copy so another
			// target's delete job doesn't repeat the call.
			if removeErr := w.store.RemoveCopy(ctx, w.tenant, copied.ID); removeErr != nil {
				return removeErr, nil, false
			}
			continue
		}
		if boardsync.IsPermanent(err) {
			w.logger.Warn("sync delete skipped",
				"tenant", w.tenant, "key", job.Key, "delivery", job.ID,
				"copy", copied.ExternalMessage, "error", ErrorCode(err))
			continue
		}
		return err, nil, false
	}
	return nil, nil, true
}

// recordsFailure reports whether a dead delivery is a message failure a
// reader should see. Edits, deletes, and reactions are best-effort and write
// no message.sync_failed/undelivered projection.
func recordsFailure(eventType boardsync.BoardEventType) bool {
	return eventType == boardsync.BoardMessagePosted || eventType == boardsync.BoardThreadSynced
}

func isPostEvent(eventType boardsync.BoardEventType) bool {
	return eventType == boardsync.BoardMessagePosted || eventType == boardsync.BoardThreadSynced
}

func isReaction(eventType boardsync.BoardEventType) bool {
	return eventType == boardsync.BoardReactionAdded || eventType == boardsync.BoardReactionRemove
}

// recordSynced retries the receipt for a copy that already landed. It is the
// side effect a crash between the provider's post and Host.Record leaves
// missing; it never posts to the platform again.
func (w *OutboundWorker) recordSynced(ctx context.Context, job Delivery, copy Copy) error {
	ref := boardsync.ThreadRef{
		Key: copy.Key, Platform: copy.Target.Platform, Channel: copy.Target.Channel,
		Thread: copy.ExternalThread, Subthread: copy.Target.Subthread,
	}
	return w.host.Record(ctx, boardsync.BoardEvent{
		Tenant: w.tenant, Type: boardsync.BoardMessageSynced, Thread: job.Message.Thread,
		Message: &job.Message, External: &ref, ExternalMessage: copy.ExternalMessage,
		Files: copy.Files, FilesSet: copy.FilesSet,
	})
}

// showTyping asks the platform to show a short-lived status in the thread
// before a post. It is best-effort: any failure is logged at debug and the
// message goes out. A platform that answers unsupported is remembered, so the
// worker doesn't ask again for that key.
func (w *OutboundWorker) showTyping(ctx context.Context, entry boardsync.Entry, job Delivery) {
	if job.Target.Kind != boardsync.TargetThread || job.Target.Thread == "" {
		return
	}
	if !entry.Provider.Capabilities().Typing {
		return
	}
	w.typingMu.Lock()
	disabled := w.typingOff[job.Key]
	w.typingMu.Unlock()
	if disabled {
		return
	}
	err := w.runner.Typing(ctx, entry, job.Target.Ref(), "", "")
	if err == nil {
		return
	}
	if boardsync.IsPermanent(err) {
		w.typingMu.Lock()
		w.typingOff[job.Key] = true
		w.typingMu.Unlock()
	}
	w.logger.Debug("sync typing status skipped",
		"tenant", w.tenant, "key", job.Key, "error", ErrorCode(err))
}

// recordFailure writes the board receipt an operator sees after the outbox
// gives up. The error is the content-free class, never a platform body.
func (w *OutboundWorker) recordFailure(ctx context.Context, job Delivery, jobErr error) {
	ref := job.Target.Ref()
	event := boardsync.BoardEvent{
		Tenant: w.tenant, Type: boardsync.BoardMessageSyncFailed, Thread: job.Message.Thread,
		Message: &job.Message, External: &ref, Error: ErrorCode(jobErr),
	}
	if err := w.host.Record(context.WithoutCancel(ctx), event); err != nil {
		w.logger.Error("sync failure receipt failed",
			"tenant", w.tenant, "key", job.Key, "delivery", job.ID, "error", ErrorCode(err))
	}
}
