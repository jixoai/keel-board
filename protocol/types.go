// Package protocol is the board's wire contract: the JSON shapes clients send
// and receive, the identifiers and limits they carry, and the error codes that
// map to HTTP statuses.
//
// It transposes the wire layer of vercel/board (Apache-2.0) to Go. The
// semantics live one layer down, in log/ and server/; this package only says
// how a board is spoken to.
package protocol

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"time"
)

// Ref is a message ID or cursor as clients pass it: a string of 1 to 128
// characters. A board assigns decimal positions, but clients must never parse
// or count with them.
type Ref = string

// ActorID identifies a principal ("local:user:julian") or one of its profiles
// ("local:user:julian+claude").
type ActorID = string

// ActorKind is what an actor is.
type ActorKind string

const (
	ActorPerson     ActorKind = "person"
	ActorAgent      ActorKind = "agent"
	ActorDeployment ActorKind = "deployment"
	ActorWorkflow   ActorKind = "workflow"
	ActorExternal   ActorKind = "external"
)

// Permission is one thing a caller may do in a channel. read covers messages,
// threads and the event feed; post starts threads; label changes labels and
// claims tasks; manage is the board's own bookkeeping.
type Permission string

const (
	PermissionRead   Permission = "read"
	PermissionPost   Permission = "post"
	PermissionReact  Permission = "react"
	PermissionLabel  Permission = "label"
	PermissionSync   Permission = "sync"
	PermissionManage Permission = "manage"
)

// Permissions is every permission, in the order a board lists them.
var Permissions = []Permission{
	PermissionRead, PermissionPost, PermissionReact,
	PermissionLabel, PermissionSync, PermissionManage,
}

// Actor is who a write is by, as the server resolved it from a credential.
// Profile is nil when the caller acts as its principal itself.
type Actor struct {
	// ID is the full actor ID, including "+profile" when Profile is set.
	ID        ActorID
	Principal ActorID
	Profile   *string
	Kind      ActorKind
	Handle    string
	Name      string
	Avatar    *string
	// Via is who an agent acts through, for display.
	Via *Via
	// Platform names a platform user's platform ("slack"), when any.
	Platform string
}

// Actored returns the actor's author record.
func (a Actor) Actored() Author {
	return Author{Actor: a.ID, Principal: a.Principal, Profile: a.Profile}
}

// Via is who an actor with a profile acts through: an agent's person or
// project, with a deployment's environment when it has one.
type Via struct {
	Name        string  `json:"name"`
	Environment *string `json:"environment"`
}

// Author is the provenance a write carries: the actor, its principal, and the
// profile the write was made as.
type Author struct {
	Actor     ActorID `json:"actor"`
	Principal string  `json:"principal"`
	Profile   *string `json:"profile"`
}

// ActorView is an actor as clients see it.
type ActorView struct {
	Kind     ActorKind `json:"kind"`
	Handle   string    `json:"handle"`
	Name     string    `json:"name"`
	Avatar   *string   `json:"avatar"`
	Via      *Via      `json:"via,omitempty"`
	Platform string    `json:"platform,omitempty"`
	Verified bool      `json:"verified,omitempty"`
}

// Attachment is a file on a message. Phase 1 keeps the wire shape so a view
// is stable, but the board stores no files and refuses IDs it never issued.
type Attachment struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Type string `json:"type"`
	Size int64  `json:"size"`
}

// BlockView is a schema-checked data block in a message. Phase 1 registers no
// schemas, so every message carries an empty list.
type BlockView struct {
	Schema  string         `json:"schema"`
	Version int            `json:"version"`
	Data    map[string]any `json:"data"`
}

// LinkView is a platform message a board message links to. Phase 1 never
// records links, so the list is empty.
type LinkView struct {
	URL         string  `json:"url"`
	Message     *string `json:"message"`
	Snapshot    *string `json:"snapshot"`
	Unavailable string  `json:"unavailable,omitempty"`
}

// ClaimView is a task's live claim, without its token.
type ClaimView struct {
	Actor ActorID `json:"actor"`
	Until string  `json:"until"`
}

// HoldView is a task held for a person.
type HoldView struct {
	Kind   *string `json:"kind"`
	Reason string  `json:"reason"`
}

// TaskView is a task as clients see it. It is carried on its root message.
type TaskView struct {
	State   string     `json:"state"`
	Owner   string     `json:"owner"`
	By      string     `json:"by"`
	For     *string    `json:"for"`
	Needs   []Ref      `json:"needs"`
	Epoch   uint64     `json:"epoch"`
	Claim   *ClaimView `json:"claim"`
	Blocked *HoldView  `json:"blocked"`
	Result  *string    `json:"result"`
	Reason  *string    `json:"reason"`
}

// MessageView is a message as clients see it. IDs are refs, version grows
// with each change, revision changes only with edits and deletes.
type MessageView struct {
	ID          Ref               `json:"id"`
	Channel     string            `json:"channel"`
	Text        string            `json:"text"`
	Re          *Ref              `json:"re"`
	ReplyTo     *Ref              `json:"replyTo"`
	Thread      string            `json:"thread"`
	Labels      map[string]string `json:"labels"`
	Reactions   map[string][]Ref  `json:"reactions"`
	NoSync      bool              `json:"noSync"`
	From        *string           `json:"from"`
	CreatedAt   string            `json:"createdAt"`
	EditedAt    *string           `json:"editedAt"`
	Deleted     bool              `json:"deleted"`
	Undelivered map[string]string `json:"undelivered"`
	Attachments []Attachment      `json:"attachments"`
	Links       []LinkView        `json:"links"`
	Version     uint64            `json:"version"`
	Revision    uint64            `json:"revision"`
	Task        *TaskView         `json:"task,omitempty"`
	Blind       bool              `json:"blind,omitempty"`
	Blocks      []BlockView       `json:"blocks"`
	Origin      any               `json:"origin"`
	// By and Author are the same view. `by` is the historical field clients
	// read; `author` was added later for the same content.
	By     *ActorView `json:"by"`
	Author *ActorView `json:"author"`
}

// ThreadSummary is a thread at a glance: its root, reply count, and latest
// reply.
type ThreadSummary struct {
	Message MessageView  `json:"message"`
	Replies int          `json:"replies"`
	Last    *MessageView `json:"last"`
}

// ThreadView is a whole thread: its root, the replies asked for, and the
// board cursor the read reflects.
type ThreadView struct {
	Message   MessageView   `json:"message"`
	Replies   []MessageView `json:"replies"`
	Cursor    Ref           `json:"cursor"`
	Schedules []any         `json:"schedules,omitempty"`
}

// ChannelView is a channel and what the board counts in it. LastIndex is a
// channel's latest message position, as a number: channels are bookkeeping,
// and upstream leaves it numeric.
type ChannelView struct {
	Path      string `json:"path"`
	LastIndex uint64 `json:"lastIndex"`
	Messages  int    `json:"messages"`
	Threads   int    `json:"threads"`
}

// ChannelsPage is a page of channels. Next is the `after` of the page that
// follows, or null.
type ChannelsPage struct {
	Channels []ChannelView `json:"channels"`
	Cursor   Ref           `json:"cursor"`
	Next     *string       `json:"next"`
}

// MessagesPage is one channel's messages and the cursor the read reflects.
type MessagesPage struct {
	Messages []MessageView `json:"messages"`
	Cursor   Ref           `json:"cursor"`
}

// TasksPage is tasks as thread summaries, oldest first. Cursor reads the next
// page, as `after`.
type TasksPage struct {
	Tasks  []ThreadSummary `json:"tasks"`
	Cursor Ref             `json:"cursor"`
}

// TaskClaim is what a claim answers once: epoch and token go on every write
// the claim makes, until the lease ends.
type TaskClaim struct {
	Epoch uint64 `json:"epoch"`
	Token string `json:"token"`
	Until string `json:"until"`
}

// EventView is a board event as subscribers receive it.
type EventView struct {
	Cursor    Ref            `json:"cursor"`
	Type      string         `json:"type"`
	CreatedAt string         `json:"createdAt"`
	Payload   map[string]any `json:"payload"`
	Message   *MessageView   `json:"message,omitempty"`
	Actor     *EventActor    `json:"actor,omitempty"`
}

// EventActor is who did an event, with its ID.
type EventActor struct {
	ID ActorID `json:"id"`
	ActorView
}

// Pages and bodies that are only ever read by a client.
type (
	// PostRequest is POST /messages.
	PostRequest struct {
		Channel     *string           `json:"channel"`
		Re          *Ref              `json:"re"`
		Text        string            `json:"text"`
		Labels      map[string]string `json:"labels"`
		NoSync      bool              `json:"noSync"`
		ID          string            `json:"id"`
		After       *Ref              `json:"after"`
		Sync        []any             `json:"sync"`
		Attachments []string          `json:"attachments"`
	}

	// LabelRequest is POST /messages/{id}/labels. A nil Set or Unset means
	// the field was absent; a `set` of {} is present but empty.
	LabelRequest struct {
		Set   map[string]string  `json:"set"`
		Unset []string           `json:"unset"`
		If    map[string]*string `json:"if"`
	}

	// CreateChannelRequest is POST /channels.
	CreateChannelRequest struct {
		Path string `json:"path"`
	}

	// CreateTaskRequest is POST /tasks.
	CreateTaskRequest struct {
		Channel     string            `json:"channel"`
		Re          *Ref              `json:"re"`
		Text        *string           `json:"text"`
		Labels      map[string]string `json:"labels"`
		NoSync      bool              `json:"noSync"`
		ID          string            `json:"id"`
		Attachments []string          `json:"attachments"`
		Needs       []Ref             `json:"needs"`
		For         *string           `json:"for"`
		Claim       bool              `json:"claim"`
		Lease       *float64          `json:"lease"`
		Token       string            `json:"token"`
	}

	// ClaimRequest is POST /tasks/:id/claim.
	ClaimRequest struct {
		Lease *float64 `json:"lease"`
		Token string   `json:"token"`
	}

	// ClaimReadyRequest is POST /tasks/claim.
	ClaimReadyRequest struct {
		Channel string            `json:"channel"`
		Label   map[string]string `json:"label"`
		From    []string          `json:"from"`
		Lease   *float64          `json:"lease"`
		Wait    *float64          `json:"wait"`
		Token   string            `json:"token"`
	}

	// RenewRequest is POST /tasks/:id/renew.
	RenewRequest struct {
		Epoch uint64   `json:"epoch"`
		Token string   `json:"token"`
		Lease *float64 `json:"lease"`
	}

	// ClaimedRequest is the body of release, complete, and fail.
	ClaimedRequest struct {
		Epoch  uint64  `json:"epoch"`
		Token  string  `json:"token"`
		Result *string `json:"result"`
		Reason *string `json:"reason"`
	}

	// BlockRequest is POST /tasks/:id/block.
	BlockRequest struct {
		Reason string  `json:"reason"`
		Kind   *string `json:"kind"`
		Epoch  *uint64 `json:"epoch"`
		Token  *string `json:"token"`
	}

	// AssignRequest is POST /tasks/:id/assign. `for` is required, and null
	// returns the task to the pool, so presence is tracked.
	AssignRequest struct {
		For NullableString `json:"for"`
	}

	// NeedsRequest is POST /tasks/:id/needs. The field is required; an empty
	// array makes the task wait for nothing.
	NeedsRequest struct {
		Needs *[]Ref `json:"needs"`
	}

	// CancelRequest is POST /tasks/:id/cancel.
	CancelRequest struct {
		Reason string `json:"reason"`
	}
)

// NullableString is a JSON string that may be null, and remembers whether it
// was present at all.
type NullableString struct {
	Set   bool
	Value *string
}

// UnmarshalJSON implements json.Unmarshaler.
func (n *NullableString) UnmarshalJSON(data []byte) error {
	n.Set = true
	if string(data) == "null" {
		n.Value = nil
		return nil
	}
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	n.Value = &value
	return nil
}

// Time format used on the wire: ISO 8601 in UTC, with milliseconds.
const wireTimeLayout = "2006-01-02T15:04:05.000Z"

// FormatTime returns t as the board writes an instant, in UTC.
func FormatTime(t time.Time) string {
	return t.UTC().Format(wireTimeLayout)
}

// FormatTimePtr returns a nil-safe instant.
func FormatTimePtr(t *time.Time) *string {
	if t == nil {
		return nil
	}
	s := FormatTime(*t)
	return &s
}

// ParseTime parses an ISO instant with an offset, as a client sends one.
func ParseTime(value string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t, err = time.Parse(time.RFC3339Nano, value)
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("time %q isn't an ISO time with an offset", value)
	}
	return t, nil
}

var decimalRef = regexp.MustCompile(`^(0|[1-9]\d{0,15})$`)

// ParseRef parses a decimal ref (a message ID or a cursor) on this board. A
// ref of 0, or one that isn't decimal, names nothing here.
func ParseRef(value string) (uint64, bool) {
	if len(value) == 0 || !decimalRef.MatchString(value) {
		return 0, false
	}
	n, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// FormatRef returns a message ID or cursor as clients see it.
func FormatRef(id uint64) Ref { return strconv.FormatUint(id, 10) }

// RefOf returns a pointer to the ref of id, for optional fields.
func RefOf(id uint64) *Ref {
	ref := FormatRef(id)
	return &ref
}
