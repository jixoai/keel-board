// Package automation is the durable side of board automation: watchers and
// schedules.
//
// A watcher is a deployed consumer of the board log. It declares a channel or
// subtree, a filter, and the name of a code or plugin action registered at
// deploy time; it never accepts a URL at runtime. The planner reads board
// events in sequence and writes one durable delivery row per matched event.
// A worker leases the oldest delivery of each watcher, runs the action as the
// watcher's bot, and acknowledges the row. A failure retries with backoff and
// then dead-letters, without blocking another watcher or the board's writes.
//
// A schedule is a persisted send plan: a channel or reply target, text,
// labels, and the next due time, optionally recurring. A worker leases a due
// row, re-checks the creator's current permission to post in the target
// channel, and appends an ordinary message under the deterministic idempotency
// key schedule:<id>:<run>. A revoked creator cancels the schedule and records
// why; an at-least-once retry replays the same message instead of posting a
// second one.
//
// Rows are tenant-scoped on every statement. A tenant is supplied by the
// caller (the server's resolved tenant or a worker's configuration), never
// inferred from board content.
package automation

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
)

// Delivery and schedule states.
const (
	StatePending = "pending"
	StateLeased  = "leased"
	StateDone    = "done"
	StateDead    = "dead"

	SchedulePending  = "pending"
	ScheduleDisabled = "disabled"
	ScheduleDone     = "done"
	ScheduleCanceled = "canceled"
	ScheduleDead     = "dead"
)

// Limits on a watcher's or schedule's declaration. They bound what an API
// request and a database row can hold; they are not tuning knobs.
const (
	WatcherIDMax      = 64
	ActionNameMax     = 64
	ActionParamsMax   = 4096
	FilterTypesMax    = 32
	FilterLabelsMax   = 16
	EventTypeMax      = 64
	ScheduleEveryMin  = 1
	ScheduleEveryMax  = 366 * 24 * 60 * 60
	ScheduleTimesMax  = 1_000_000
	ScheduleLabelsMax = 32
)

var watcherIDExpr = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)

// ActionSpec is a deployment-time action: a registered name and bounded
// parameters. Nothing here is executed by name over HTTP; the action registry
// is assembled by the process that runs the worker.
type ActionSpec struct {
	Name   string          `json:"name"`
	Params json.RawMessage `json:"params,omitempty"`
}

// Validate checks the action's shape. It refuses parameter keys that name an
// HTTP target, so a deployment cannot smuggle a runtime URL into a watcher
// while the runtime has no URL-fetching path at all.
func (a ActionSpec) Validate() error {
	if len(a.Name) == 0 || len(a.Name) > ActionNameMax || !actionNameExpr.MatchString(a.Name) {
		return protocol.Invalidf("action.name: %q isn't an action name: one is 1 to %d lowercase letters, numbers, ., _, and -", a.Name, ActionNameMax)
	}
	if len(a.Params) == 0 {
		return nil
	}
	if len(a.Params) > ActionParamsMax {
		return protocol.NewError(protocol.CodeTooLarge, fmt.Sprintf("action.params: at most %d bytes", ActionParamsMax))
	}
	var value any
	if err := json.Unmarshal(a.Params, &value); err != nil {
		return protocol.Invalid("action.params: it isn't JSON")
	}
	object, ok := value.(map[string]any)
	if !ok {
		return protocol.Invalid("action.params: it is a JSON object")
	}
	if err := rejectURLKeys(object, "action.params"); err != nil {
		return err
	}
	return nil
}

var actionNameExpr = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)

// urlKey names an HTTP target a watcher must never carry: the runtime only
// calls registered code. The check is deliberately narrow: values may mention
// a URL as data, but a key that would configure a fetch is refused.
var urlKeys = map[string]struct{}{
	"url": {}, "uri": {}, "href": {}, "endpoint": {},
	"webhook": {}, "webhook_url": {}, "callback": {}, "callback_url": {},
	"action_url": {}, "target_url": {},
}

func rejectURLKeys(value map[string]any, path string) error {
	return walkURLKeys(value, path)
}

func walkURLKeys(value any, path string) error {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			childPath := path + "." + key
			if _, banned := urlKeys[strings.ToLower(key)]; banned {
				return protocol.Invalidf("%s: a watcher action can't declare an HTTP target; register a code or plugin action instead", childPath)
			}
			if err := walkURLKeys(child, childPath); err != nil {
				return err
			}
		}
	case []any:
		for index, child := range typed {
			if err := walkURLKeys(child, fmt.Sprintf("%s[%d]", path, index)); err != nil {
				return err
			}
		}
	}
	return nil
}

// Filter narrows the events a watcher sees. An empty filter matches every
// event on the watcher's channel that names a message. Types are exact event
// types; Labels require every named label to be present.
type Filter struct {
	Types  []string          `json:"types,omitempty"`
	Labels map[string]string `json:"labels,omitempty"`
}

// Validate checks a filter's shape and bounds.
func (f Filter) Validate() error {
	if len(f.Types) > FilterTypesMax {
		return protocol.Invalidf("filter.types: at most %d event types", FilterTypesMax)
	}
	seen := make(map[string]struct{}, len(f.Types))
	for _, value := range f.Types {
		value = strings.TrimSpace(value)
		if value == "" || len(value) > EventTypeMax {
			return protocol.Invalidf("filter.types: %q isn't an event type", value)
		}
		if err := ValidateEventType(value); err != nil {
			return err
		}
		if _, duplicate := seen[value]; duplicate {
			return protocol.Invalidf("filter.types: %q is listed twice", value)
		}
		seen[value] = struct{}{}
	}
	if len(f.Labels) > FilterLabelsMax {
		return protocol.Invalidf("filter.labels: at most %d labels", FilterLabelsMax)
	}
	for key, value := range f.Labels {
		if err := protocol.ValidateLabelKey(key); err != nil {
			return err
		}
		if err := protocol.ValidateLabelValue(value); err != nil {
			return err
		}
	}
	return nil
}

// Matches reports whether an event type with labels passes the filter.
func (f Filter) Matches(eventType string, labels map[string]string) bool {
	if len(f.Types) > 0 {
		found := false
		for _, value := range f.Types {
			if value == eventType {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	for key, want := range f.Labels {
		if got, ok := labels[key]; !ok || got != want {
			return false
		}
	}
	return true
}

// ValidateEventType checks that an event type is one the board writes.
func ValidateEventType(value string) error {
	switch log.EventType(value) {
	case log.EventMessagePosted, log.EventChannelCreated, log.EventMessageEdited,
		log.EventMessageDeleted, log.EventReactionAdded, log.EventReactionRemoved,
		log.EventLabelsChanged, log.EventActorSeen, log.EventTaskCreated,
		log.EventTaskAssigned, log.EventTaskNeedsChange, log.EventTaskReady,
		log.EventTaskClaimed, log.EventTaskRenewed, log.EventTaskReleased,
		log.EventTaskExpired, log.EventTaskCompleted, log.EventTaskFailed,
		log.EventTaskBlocked, log.EventTaskUnblocked, log.EventTaskCanceled,
		log.EventTaskReopened, log.EventThreadSynced, log.EventMessageSynced,
		log.EventMessageFilesSynced, log.EventMessageSyncFailed:
		return nil
	default:
		return protocol.Invalidf("filter.types: %q isn't an event type on this board", value)
	}
}

// Watcher is one deployment-time automation declaration. Cursor is the last
// board sequence the planner evaluated. A watcher created without CursorSet
// starts at the current head the first time the worker sees it.
type Watcher struct {
	Tenant  string     `json:"tenant"`
	ID      string     `json:"id"`
	Channel string     `json:"channel"`
	Filter  Filter     `json:"filter"`
	Action  ActionSpec `json:"action"`
	Bot     string     `json:"bot,omitempty"`
	Enabled bool       `json:"enabled"`
	Cursor  uint64     `json:"cursor"`
	// CursorSet distinguishes "evaluated through 0" (an empty board) from
	// "never evaluated". A new watcher is set at the head, even when that head
	// is 0, so its first event isn't skipped as history.
	CursorSet bool      `json:"cursorSet,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// Validate checks a watcher before it is stored.
func (w Watcher) Validate() error {
	if err := validTenant(w.Tenant); err != nil {
		return err
	}
	if err := ValidateWatcherID(w.ID); err != nil {
		return err
	}
	if err := protocol.ValidateChannelOrSubtree(w.Channel); err != nil {
		return protocol.Invalidf("channel: %s", err.Message)
	}
	if err := w.Filter.Validate(); err != nil {
		return err
	}
	if err := w.Action.Validate(); err != nil {
		return err
	}
	if w.Bot != "" {
		if err := protocol.ValidateActorID(w.Bot); err != nil {
			return protocol.Invalidf("bot: %s", err.Message)
		}
	}
	return nil
}

// ValidateWatcherID checks a watcher ID's shape.
func ValidateWatcherID(id string) error {
	if len(id) == 0 || len(id) > WatcherIDMax || !watcherIDExpr.MatchString(id) {
		return protocol.Invalidf("id: %q isn't a watcher ID: one is 1 to %d lowercase letters, numbers, ., _, and -, starting with a letter or number", id, WatcherIDMax)
	}
	return nil
}

// MatchesChannel reports whether a watcher's channel declaration contains
// channel. A trailing "/" selects the channel and every channel under it.
func (w Watcher) MatchesChannel(channel string) bool {
	if strings.HasSuffix(w.Channel, "/") {
		prefix := strings.TrimSuffix(w.Channel, "/")
		return channel == prefix || strings.HasPrefix(channel, prefix+"/")
	}
	return channel == w.Channel
}

// Delivery is one matched board event queued for one watcher.
type Delivery struct {
	Tenant     string     `json:"tenant"`
	WatcherID  string     `json:"watcherId"`
	EventSeq   uint64     `json:"eventSeq"`
	State      string     `json:"state"`
	Attempts   int        `json:"attempts"`
	NextAt     time.Time  `json:"nextAt"`
	LastError  string     `json:"lastError,omitempty"`
	Lease      string     `json:"-"`
	LeaseOwner string     `json:"-"`
	LeaseUntil time.Time  `json:"-"`
	CreatedAt  time.Time  `json:"createdAt"`
	UpdatedAt  time.Time  `json:"updatedAt"`
	Completed  *time.Time `json:"completedAt,omitempty"`
}

// Depth counts one watcher's deliveries by state.
type Depth struct {
	Pending int `json:"pending"`
	Leased  int `json:"leased"`
	Done    int `json:"done"`
	Dead    int `json:"dead"`
}

// AckResult says what an ack did: committed a success, scheduled a retry, or
// dead-lettered the row.
type AckResult struct {
	State    string `json:"state"`
	Attempts int    `json:"attempts"`
	Acked    bool   `json:"acked"`
	Retried  bool   `json:"retried"`
	Dead     bool   `json:"dead"`
}

// Schedule is a persisted send plan. Text is the message body: it is stored
// so the worker can send it and is never part of a management DTO.
type Schedule struct {
	Tenant       string            `json:"tenant"`
	ID           string            `json:"id"`
	Channel      string            `json:"channel"`
	Re           *uint64           `json:"re,omitempty"`
	Text         string            `json:"-"`
	Labels       map[string]string `json:"labels,omitempty"`
	Creator      string            `json:"creator"`
	NextRunAt    time.Time         `json:"nextRunAt"`
	EverySeconds int               `json:"everySeconds,omitempty"`
	UntilAt      *time.Time        `json:"untilAt,omitempty"`
	Times        int               `json:"times,omitempty"`
	Sent         int               `json:"sent"`
	Status       string            `json:"status"`
	Attempts     int               `json:"attempts"`
	LastError    string            `json:"lastError,omitempty"`
	CancelReason string            `json:"cancelReason,omitempty"`
	Lease        string            `json:"-"`
	LeaseOwner   string            `json:"-"`
	LeaseUntil   time.Time         `json:"-"`
	CreatedAt    time.Time         `json:"createdAt"`
	UpdatedAt    time.Time         `json:"updatedAt"`
	Completed    *time.Time        `json:"completedAt,omitempty"`
}

// Validate checks a schedule before it is stored.
func (s Schedule) Validate() error {
	if err := validTenant(s.Tenant); err != nil {
		return err
	}
	if err := ValidateScheduleID(s.ID); err != nil {
		return err
	}
	if err := protocol.ValidateChannel(s.Channel); err != nil {
		return protocol.Invalidf("channel: %s", err.Message)
	}
	if s.Re != nil && *s.Re == 0 {
		return protocol.Invalid("re: a schedule reply names a message")
	}
	if err := protocol.ValidateText(s.Text); err != nil {
		return protocol.Invalidf("text: %s", err.Message)
	}
	if len(s.Labels) > ScheduleLabelsMax {
		return protocol.Invalidf("labels: at most %d labels", ScheduleLabelsMax)
	}
	if err := protocol.ValidateNewLabels(s.Labels); err != nil {
		return err
	}
	if err := protocol.ValidateActorID(s.Creator); err != nil {
		return protocol.Invalidf("creator: %s", err.Message)
	}
	if s.NextRunAt.IsZero() {
		return protocol.Invalid("nextRunAt: a schedule needs a next run time")
	}
	if s.EverySeconds < 0 || s.EverySeconds > ScheduleEveryMax {
		return protocol.Invalidf("everySeconds: 1 to %d seconds, or 0 for one run", ScheduleEveryMax)
	}
	if s.EverySeconds > 0 && s.EverySeconds < ScheduleEveryMin {
		return protocol.Invalidf("everySeconds: at least %d second", ScheduleEveryMin)
	}
	if s.Times < 0 || s.Times > ScheduleTimesMax {
		return protocol.Invalidf("times: 0 to %d, or 0 for no limit", ScheduleTimesMax)
	}
	if s.Times > 1 && s.EverySeconds == 0 {
		return protocol.Invalid("times: more than one run needs an everySeconds interval")
	}
	if s.UntilAt != nil && !s.UntilAt.After(s.NextRunAt) {
		return protocol.Invalid("untilAt: it is after nextRunAt")
	}
	switch s.Status {
	case "", SchedulePending, ScheduleDisabled, ScheduleDone, ScheduleCanceled, ScheduleDead:
	default:
		return protocol.Invalid("status: it isn't a schedule status")
	}
	return nil
}

// ValidateScheduleID checks a schedule ID's shape: a UUID string.
func ValidateScheduleID(id string) error {
	if len(id) != 36 || id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' {
		return protocol.Invalidf("id: %q isn't a schedule ID (a UUID)", id)
	}
	for i, c := range id {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			continue
		}
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') {
			continue
		}
		return protocol.Invalidf("id: %q isn't a schedule ID (a UUID)", id)
	}
	return nil
}

// Schedule outcomes an ack can report.
type ScheduleOutcome string

const (
	ScheduleSent    ScheduleOutcome = "sent"
	ScheduleRetry   ScheduleOutcome = "retry"
	ScheduleRevoked ScheduleOutcome = "revoked"
)

// ScheduleResult is what one run did. Error is never persisted as text; the
// store records its stable class instead.
type ScheduleResult struct {
	Outcome ScheduleOutcome
	At      time.Time
	// MessageID is the board message the run posted (also on a replay).
	MessageID uint64
	// NextRunAt is when a recurring schedule runs next; ignored when Done.
	NextRunAt time.Time
	// Done ends a one-shot, a spent schedule, or one past until_at.
	Done bool
	// Reason is the recorded cancellation reason for Outcome revoked.
	Reason string
	// Err is the failure a retry reports. It is classified, never stored whole.
	Err error
}

// NewWatcherID returns a random watcher ID.
func NewWatcherID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("automation: generating a watcher ID: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}

// NewScheduleID returns a random version-4 UUID string.
func NewScheduleID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("automation: generating a schedule ID: %w", err)
	}
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", raw[0:4], raw[4:6], raw[6:8], raw[8:10], raw[10:16]), nil
}

// IsPermanent reports whether a failure must not be retried.
func IsPermanent(err error) bool {
	if err == nil {
		return false
	}
	var marked permanentError
	return errors.As(err, &marked) || errors.Is(err, ErrPermanent)
}

// Permanent marks an error as not retryable. The wrapped error's text is
// preserved for the caller, never persisted.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err: err}
}

type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

// ErrorCode reduces an error to a stable, content-free class for a row, a
// log line, or a DTO. A raw error may carry a message body or a credential;
// none of that is stored here.
func ErrorCode(err error) string {
	if err == nil {
		return ""
	}
	var boardErr *protocol.Error
	if errors.As(err, &boardErr) {
		switch boardErr.Status {
		case 400:
			return "invalid"
		case 401:
			return "unauthorized"
		case 403:
			return "forbidden"
		case 404:
			return "not_found"
		case 409:
			return "conflict"
		case 429:
			return "rate_limited"
		default:
			return "failed"
		}
	}
	switch {
	case IsPermanent(err):
		return "permanent"
	case errors.Is(err, ErrNotFound):
		return "not_found"
	case errors.Is(err, ErrLeaseLost):
		return "lease_lost"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, log.ErrHeadMoved):
		return "conflict"
	default:
		return "failed"
	}
}

// validTenant refuses an empty or unbounded tenant, so a caller can't
// accidentally use "" as a tenant-wide namespace.
func validTenant(tenant string) error {
	if tenant == "" || len(tenant) > 200 || !protocol.Storable(tenant) {
		return protocol.Invalid("tenant: it is empty, longer than 200 bytes, or not storable")
	}
	return nil
}

// cloneWatcher, cloneDelivery, and cloneSchedule detach a stored value from
// the caller and from the store's maps.
func cloneWatcher(w Watcher) Watcher {
	w.Filter.Types = append([]string(nil), w.Filter.Types...)
	if w.Filter.Labels != nil {
		w.Filter.Labels = cloneLabels(w.Filter.Labels)
	}
	if w.Action.Params != nil {
		w.Action.Params = append(json.RawMessage(nil), w.Action.Params...)
	}
	return w
}

func cloneDelivery(d Delivery) Delivery {
	if d.Completed != nil {
		completed := *d.Completed
		d.Completed = &completed
	}
	return d
}

func cloneSchedule(s Schedule) Schedule {
	if s.Re != nil {
		re := *s.Re
		s.Re = &re
	}
	if s.Labels != nil {
		s.Labels = cloneLabels(s.Labels)
	}
	if s.UntilAt != nil {
		until := *s.UntilAt
		s.UntilAt = &until
	}
	if s.Completed != nil {
		completed := *s.Completed
		s.Completed = &completed
	}
	return s
}

func cloneLabels(labels map[string]string) map[string]string {
	if labels == nil {
		return nil
	}
	out := make(map[string]string, len(labels))
	for key, value := range labels {
		out[key] = value
	}
	return out
}
