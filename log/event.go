// Package log is the board's append-only log: one ordered stream of events per
// tenant, folded into messages, threads, channels, actors, and tasks.
//
// A board is derived from its log; nothing is stored twice. Appends are
// conditional (an expected head, an idempotency key and digest), and every
// mutation runs under one lock, so a read-decide-append transaction is atomic
// in this process. A PostgreSQL implementation would take the same shape as a
// serializable transaction: see store.Postgres.
package log

import (
	"time"

	"github.com/jixoai/keel-board/protocol"
)

// EventType is the type of an event in the board log.
type EventType string

const (
	EventMessagePosted   EventType = "message.posted"
	EventChannelCreated  EventType = "channel.created"
	EventMessageEdited   EventType = "message.edited"
	EventMessageDeleted  EventType = "message.deleted"
	EventLabelsChanged   EventType = "labels.changed"
	EventActorSeen       EventType = "actor.seen"
	EventTaskCreated     EventType = "task.created"
	EventTaskAssigned    EventType = "task.assigned"
	EventTaskNeedsChange EventType = "task.needs_changed"
	EventTaskReady       EventType = "task.ready"
	EventTaskClaimed     EventType = "task.claimed"
	EventTaskRenewed     EventType = "task.renewed"
	EventTaskReleased    EventType = "task.released"
	EventTaskExpired     EventType = "task.expired"
	EventTaskCompleted   EventType = "task.completed"
	EventTaskFailed      EventType = "task.failed"
	EventTaskBlocked     EventType = "task.blocked"
	EventTaskUnblocked   EventType = "task.unblocked"
	EventTaskCanceled    EventType = "task.canceled"
	EventTaskReopened    EventType = "task.reopened"
)

// Event is one fact in the board log.
type Event struct {
	// Seq is the event's 1-based position: its cursor.
	Seq uint64
	// Type is what happened.
	Type EventType
	// Payload is the fact itself, one of the *Payload types below.
	Payload any
	// At is when the board recorded it.
	At time.Time
}

// ConcernedMessageID returns the message an event changes, or 0 for one that
// concerns no message.
func ConcernedMessageID(event Event) uint64 {
	switch payload := event.Payload.(type) {
	case *MessagePostedPayload:
		return payload.ID
	case *ChannelCreatedPayload:
		return 0
	case *ActorSeenPayload:
		return 0
	case *MessageEditedPayload:
		return payload.ID
	case *MessageDeletedPayload:
		return payload.ID
	case *LabelsChangedPayload:
		return payload.ID
	case interface{ messageID() uint64 }:
		return payload.messageID()
	}
	return 0
}

// ActorSeenPayload records who an actor is, so views can name it.
type ActorSeenPayload struct {
	Actor    string  `json:"actor"`
	Kind     string  `json:"kind"`
	Handle   string  `json:"handle"`
	Name     string  `json:"name"`
	Avatar   *string `json:"avatar"`
	Via      *Via    `json:"via,omitempty"`
	Platform string  `json:"platform,omitempty"`
	Host     bool    `json:"host,omitempty"`
	Verified bool    `json:"verified,omitempty"`
}

func (*ActorSeenPayload) messageID() uint64 { return 0 }

// Via is who an actor with a profile acts through.
type Via struct {
	Name        string  `json:"name"`
	Environment *string `json:"environment"`
}

// MessagePostedPayload starts or replies to a thread.
type MessagePostedPayload struct {
	// ID is the event sequence of this post, and the message's ID.
	ID      uint64            `json:"id"`
	Channel string            `json:"channel"`
	Text    string            `json:"text"`
	Re      *uint64           `json:"re"`
	ReplyTo *uint64           `json:"replyTo"`
	Thread  string            `json:"thread"`
	Labels  map[string]string `json:"labels"`
	NoSync  bool              `json:"noSync"`
	By      Author            `json:"by"`
	From    *string           `json:"from"`
	Blind   bool              `json:"blind,omitempty"`
	// Along is how many events right after this post, in its append, are
	// about it: what made a new thread a task.
	Along int `json:"along,omitempty"`
}

func (p *MessagePostedPayload) messageID() uint64 { return p.ID }

// Author is a write's provenance, as the protocol states it.
type Author = protocol.Author

// ChannelCreatedPayload makes a channel before anything posts to it.
type ChannelCreatedPayload struct {
	Channel string `json:"channel"`
	By      Author `json:"by"`
}

func (*ChannelCreatedPayload) messageID() uint64 { return 0 }

// MessageEditedPayload changes a message's text.
type MessageEditedPayload struct {
	ID     uint64  `json:"id"`
	Thread string  `json:"thread"`
	Text   string  `json:"text"`
	By     Author  `json:"by"`
	From   *string `json:"from"`
}

func (p *MessageEditedPayload) messageID() uint64 { return p.ID }

// MessageDeletedPayload turns a message into a tombstone.
type MessageDeletedPayload struct {
	ID     uint64  `json:"id"`
	Thread string  `json:"thread"`
	By     Author  `json:"by"`
	From   *string `json:"from"`
}

func (p *MessageDeletedPayload) messageID() uint64 { return p.ID }

// LabelsChangedPayload sets and unsets labels on a message.
type LabelsChangedPayload struct {
	ID     uint64            `json:"id"`
	Thread string            `json:"thread"`
	Set    map[string]string `json:"set"`
	Unset  []string          `json:"unset"`
	By     Author            `json:"by"`
}

func (p *LabelsChangedPayload) messageID() uint64 { return p.ID }

// TaskRef is the message a task event changes: its root's ID and thread key.
type TaskRef struct {
	ID     uint64 `json:"id"`
	Thread string `json:"thread"`
}

// TaskCreatedPayload makes a thread a task.
type TaskCreatedPayload struct {
	ID     uint64   `json:"id"`
	Thread string   `json:"thread"`
	Needs  []uint64 `json:"needs"`
	For    *string  `json:"for"`
	By     Author   `json:"by"`
}

func (p *TaskCreatedPayload) messageID() uint64 { return p.ID }

// TaskAssignedPayload keeps a task for one of its owner's actors, or, with a
// nil For, returns it to the pool.
type TaskAssignedPayload struct {
	ID     uint64  `json:"id"`
	Thread string  `json:"thread"`
	For    *string `json:"for"`
	By     Author  `json:"by"`
}

func (p *TaskAssignedPayload) messageID() uint64 { return p.ID }

// TaskNeedsChangedPayload replaces what a task waits for.
type TaskNeedsChangedPayload struct {
	ID     uint64   `json:"id"`
	Thread string   `json:"thread"`
	Needs  []uint64 `json:"needs"`
	By     Author   `json:"by"`
}

func (p *TaskNeedsChangedPayload) messageID() uint64 { return p.ID }

// TaskReadyPayload says every task the task needs has completed.
type TaskReadyPayload struct {
	ID     uint64 `json:"id"`
	Thread string `json:"thread"`
}

func (p *TaskReadyPayload) messageID() uint64 { return p.ID }

// TaskClaimedPayload takes a lease on a task. Token is the SHA-256 of the
// claim's secret, hex: no view shows it.
type TaskClaimedPayload struct {
	ID     uint64 `json:"id"`
	Thread string `json:"thread"`
	Epoch  uint64 `json:"epoch"`
	Until  string `json:"until"`
	Token  string `json:"token"`
	By     Author `json:"by"`
}

func (p *TaskClaimedPayload) messageID() uint64 { return p.ID }

// TaskRenewedPayload extends a claim's lease.
type TaskRenewedPayload struct {
	ID     uint64 `json:"id"`
	Thread string `json:"thread"`
	Epoch  uint64 `json:"epoch"`
	Until  string `json:"until"`
	By     Author `json:"by"`
}

func (p *TaskRenewedPayload) messageID() uint64 { return p.ID }

// TaskReleasedPayload hands a task back, ready again.
type TaskReleasedPayload struct {
	ID     uint64 `json:"id"`
	Thread string `json:"thread"`
	Epoch  uint64 `json:"epoch"`
	By     Author `json:"by"`
}

func (p *TaskReleasedPayload) messageID() uint64 { return p.ID }

// TaskExpiredPayload says a claim's lease ran out.
type TaskExpiredPayload struct {
	ID     uint64 `json:"id"`
	Thread string `json:"thread"`
	Epoch  uint64 `json:"epoch"`
}

func (p *TaskExpiredPayload) messageID() uint64 { return p.ID }

// TaskCompletedPayload ends a claim with a result.
type TaskCompletedPayload struct {
	ID     uint64  `json:"id"`
	Thread string  `json:"thread"`
	Epoch  uint64  `json:"epoch"`
	Result *string `json:"result"`
	By     Author  `json:"by"`
}

func (p *TaskCompletedPayload) messageID() uint64 { return p.ID }

// TaskFailedPayload ends a claim with a reason.
type TaskFailedPayload struct {
	ID     uint64 `json:"id"`
	Thread string `json:"thread"`
	Epoch  uint64 `json:"epoch"`
	Reason string `json:"reason"`
	By     Author `json:"by"`
}

func (p *TaskFailedPayload) messageID() uint64 { return p.ID }

// TaskBlockedPayload holds a task for a person, ending any claim. Kind
// `edited` is the board's own: it means the root was edited by someone who
// doesn't own the task.
type TaskBlockedPayload struct {
	ID     uint64  `json:"id"`
	Thread string  `json:"thread"`
	Kind   *string `json:"kind"`
	Reason string  `json:"reason"`
	By     Author  `json:"by"`
}

func (p *TaskBlockedPayload) messageID() uint64 { return p.ID }

// TaskUnblockedPayload lifts a hold.
type TaskUnblockedPayload struct {
	ID     uint64 `json:"id"`
	Thread string `json:"thread"`
	By     Author `json:"by"`
}

func (p *TaskUnblockedPayload) messageID() uint64 { return p.ID }

// TaskCanceledPayload calls a task off. Reason `deleted` is the board's: its
// root was deleted.
type TaskCanceledPayload struct {
	ID     uint64 `json:"id"`
	Thread string `json:"thread"`
	Reason string `json:"reason"`
	By     Author `json:"by"`
}

func (p *TaskCanceledPayload) messageID() uint64 { return p.ID }

// TaskReopenedPayload makes a failed task ready again.
type TaskReopenedPayload struct {
	ID     uint64 `json:"id"`
	Thread string `json:"thread"`
	By     Author `json:"by"`
}

func (p *TaskReopenedPayload) messageID() uint64 { return p.ID }
