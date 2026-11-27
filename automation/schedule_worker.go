package automation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
)

// ScheduleWorkResult counts one schedule worker pass.
type ScheduleWorkResult struct {
	Leased   int `json:"leased"`
	Sent     int `json:"sent"`
	Retried  int `json:"retried"`
	Dead     int `json:"dead"`
	Revoked  int `json:"revoked"`
	Lost     int `json:"lost"`
	Deferred int `json:"deferred"`
}

// ScheduleWorkerConfig configures the schedule consumer.
type ScheduleWorkerConfig struct {
	Store Store
	// Board is the log a schedule posts to. Required.
	Board Board
	// Authorize re-checks the creator's channel permission at trigger time.
	// Nil allows the write (development only).
	Authorize Authorizer
	Tenant    string
	Owner     string
	// Limit is the most schedules a pass leases. Zero means ten.
	Limit int
	// PollInterval is how long Run waits after a pass with no work. Zero means
	// one second.
	PollInterval time.Duration
	Logger       *slog.Logger
}

// ScheduleWorker sends due schedules as their creators. A run is idempotent
// by schedule:<id>:<run>, so a crash after the append and before the ack
// replays the same message.
type ScheduleWorker struct {
	store     Store
	board     Board
	authorize Authorizer
	tenant    string
	owner     string
	limit     int
	poll      time.Duration
	logger    *slog.Logger
}

// NewScheduleWorker validates cfg.
func NewScheduleWorker(cfg ScheduleWorkerConfig) (*ScheduleWorker, error) {
	if cfg.Store == nil || cfg.Board == nil {
		return nil, errors.New("automation: a schedule worker needs a store and board")
	}
	if err := expectTenant(cfg.Tenant); err != nil {
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
	return &ScheduleWorker{
		store: cfg.Store, board: cfg.Board, authorize: cfg.Authorize,
		tenant: cfg.Tenant, owner: leaseOwner(cfg.Owner), limit: cfg.Limit,
		poll: cfg.PollInterval, logger: cfg.Logger,
	}, nil
}

// Process leases and runs one batch of due schedules. A permission loss
// cancels the schedule; a send failure backs off and then dead-letters,
// without holding the board's other writes.
func (w *ScheduleWorker) Process(ctx context.Context) (ScheduleWorkResult, error) {
	result := ScheduleWorkResult{}
	schedules, err := w.store.LeaseSchedules(ctx, w.tenant, w.limit, w.owner)
	if err != nil {
		return result, err
	}
	result.Leased = len(schedules)
	var firstErr error
	for _, schedule := range schedules {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		run := w.send(ctx, schedule)
		if err := ctx.Err(); err != nil {
			return result, err
		}
		ack, ackErr := w.store.AckSchedule(context.WithoutCancel(ctx), w.tenant, schedule.ID, schedule.Lease, run)
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
		switch run.Outcome {
		case ScheduleSent:
			result.Sent++
		case ScheduleRetry:
			if ack.Status == ScheduleDead {
				result.Dead++
			} else {
				result.Retried++
			}
		case ScheduleRevoked:
			result.Revoked++
		}
		if run.Outcome != ScheduleSent {
			w.logger.Warn("schedule run did not send",
				"tenant", w.tenant, "schedule", schedule.ID,
				"outcome", string(run.Outcome), "attempts", ack.Attempts,
				"status", ack.Status, "error", ErrorCode(run.Err))
		}
	}
	return result, firstErr
}

// send re-checks the creator's permission and posts the scheduled message.
func (w *ScheduleWorker) send(ctx context.Context, schedule Schedule) ScheduleResult {
	now := time.Now().UTC()
	creator := ActorFromID(schedule.Creator)
	channel := schedule.Channel
	var (
		rootID  *uint64
		replyTo *uint64
	)
	if schedule.Re != nil {
		var found bool
		err := w.board.ReadContext(ctx, w.tenant, func(snapshot *log.Snapshot) error {
			target := snapshot.Message(*schedule.Re)
			if target == nil || target.Deleted {
				return nil
			}
			found = true
			channel = target.Channel
			root := target.ID
			if target.Re != nil {
				root = *target.Re
				replied := target.ID
				replyTo = &replied
			}
			rootID = &root
			return nil
		})
		if err != nil {
			return ScheduleResult{Outcome: ScheduleRetry, At: now, Err: err}
		}
		if !found {
			// A removed reply target is not coming back; cancel rather than
			// retry forever.
			return ScheduleResult{Outcome: ScheduleRevoked, At: now, Reason: "message_deleted"}
		}
	}
	if w.authorize != nil {
		if err := w.authorize.Authorize(ctx, w.tenant, creator, channel, protocol.PermissionPost); err != nil {
			w.logger.Warn("schedule creator lost channel access",
				"tenant", w.tenant, "schedule", schedule.ID,
				"creator", schedule.Creator, "channel", channel)
			return ScheduleResult{Outcome: ScheduleRevoked, At: now, Reason: "revoked"}
		}
		if len(schedule.Labels) > 0 {
			if err := w.authorize.Authorize(ctx, w.tenant, creator, channel, protocol.PermissionLabel); err != nil {
				w.logger.Warn("schedule creator lost label access",
					"tenant", w.tenant, "schedule", schedule.ID,
					"creator", schedule.Creator, "channel", channel)
				return ScheduleResult{Outcome: ScheduleRevoked, At: now, Reason: "revoked"}
			}
		}
	}
	readable := true
	if w.authorize != nil {
		readable = w.authorize.Authorize(ctx, w.tenant, creator, channel, protocol.PermissionRead) == nil
	}
	labels := schedule.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	runNumber := schedule.Sent + 1
	key := scheduleKey(schedule.ID, runNumber)
	digest := scheduleDigest(schedule, channel, runNumber)
	var messageID uint64
	err := w.board.Update(ctx, w.tenant, log.UpdateOptions{
		Idempotency: &log.Idempotency{Key: key, Digest: digest},
	}, func(tx *log.Tx) error {
		if replay, ok := tx.Replay(); ok {
			if len(replay.MessageIDs) == 0 {
				return protocol.NewError(protocol.CodeDuplicateID, "the schedule run already wrote a different message")
			}
			messageID = replay.MessageIDs[0]
			return nil
		}
		offset := 0
		if tx.Actor(creator.ID) == nil {
			tx.Emit(log.EventActorSeen, &log.ActorSeenPayload{
				Actor: creator.ID, Kind: string(creator.Kind), Handle: creator.Handle,
				Name: creator.Name, Platform: "local",
			})
			offset++
		}
		messageID = tx.NextID(offset)
		thread := log.ThreadKey(messageID)
		if rootID != nil {
			thread = log.ThreadKey(*rootID)
		}
		tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
			ID: messageID, Channel: channel, Text: schedule.Text,
			Re: rootID, ReplyTo: replyTo, Thread: thread, Labels: labels,
			By: creator.Actored(), Blind: !readable,
		})
		tx.RecordResult(messageID)
		return nil
	})
	if err != nil {
		if isDuplicateID(err) {
			// The run's idempotency key already landed with another digest:
			// an earlier attempt posted this run, and the schedule was edited
			// before its retry. Count the run as sent so the new content takes
			// effect on the next one, rather than dead-lettering the plan.
			w.logger.Warn("schedule run already wrote a message",
				"tenant", w.tenant, "schedule", schedule.ID, "run", runNumber)
			return completedRun(schedule, now, 0)
		}
		return ScheduleResult{Outcome: ScheduleRetry, At: now, Err: err}
	}
	return completedRun(schedule, now, messageID)
}

// completedRun builds the ack for a posted run: whether the plan is done, and
// the next scheduled time otherwise.
func completedRun(schedule Schedule, now time.Time, messageID uint64) ScheduleResult {
	sent := schedule.Sent + 1
	done := schedule.EverySeconds == 0 || (schedule.Times > 0 && sent >= schedule.Times)
	next := time.Time{}
	if !done {
		next = nextRun(schedule, now)
		if schedule.UntilAt != nil && !next.Before(*schedule.UntilAt) {
			done = true
			next = time.Time{}
		}
	}
	return ScheduleResult{Outcome: ScheduleSent, At: now, MessageID: messageID, NextRunAt: next, Done: done}
}

// isDuplicateID reports the board's 409 duplicate_id, the answer to a keyed
// write whose key already holds a different digest.
func isDuplicateID(err error) bool {
	var boardErr *protocol.Error
	return errors.As(err, &boardErr) && boardErr.Code == protocol.CodeDuplicateID
}

// nextRun skips missed occurrences: enabling a recurring schedule after a long
// pause sends once, not a burst of every missed run.
func nextRun(schedule Schedule, now time.Time) time.Time {
	every := time.Duration(schedule.EverySeconds) * time.Second
	if every <= 0 {
		return time.Time{}
	}
	next := schedule.NextRunAt
	if !next.After(now) {
		delta := now.Sub(next)
		steps := delta / every
		if steps < 1 {
			steps = 1
		}
		next = next.Add(steps * every)
		if !next.After(now) {
			next = next.Add(every)
		}
	}
	return next
}

func scheduleKey(id string, run int) string {
	return "schedule:" + id + ":" + strconv.Itoa(run)
}

// scheduleDigest identifies one run's content. A retry of the same run must
// produce the same value; the run number keeps two runs of identical text
// apart.
func scheduleDigest(schedule Schedule, channel string, run int) string {
	labels := schedule.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	raw, _ := json.Marshal(struct {
		Schedule string            `json:"schedule"`
		Run      int               `json:"run"`
		Creator  string            `json:"creator"`
		Channel  string            `json:"channel"`
		Re       *uint64           `json:"re,omitempty"`
		Text     string            `json:"text"`
		Labels   map[string]string `json:"labels"`
	}{
		Schedule: schedule.ID, Run: run, Creator: schedule.Creator,
		Channel: channel, Re: schedule.Re, Text: schedule.Text, Labels: labels,
	})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// Run drains due schedules until ctx ends.
func (w *ScheduleWorker) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.poll)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		result, err := w.Process(ctx)
		if err != nil && ctx.Err() == nil {
			w.logger.Error("schedule worker pass failed", "tenant", w.tenant, "error", ErrorCode(err))
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if result.Leased > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
