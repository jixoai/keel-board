// Package sync is the platform-agnostic half of two-way sync between a board
// and its external threads.
//
// The package transposes the sync model of vercel/board (Apache-2.0) into Go.
// A sync entry is one platform account plus one function; the function is
// called for every event in both directions and decides what happens by
// calling Import, Deliver, or Start. The package keeps:
//
//   - the normalized event a provider emits and a function reads;
//   - external thread and target IDs, prefixed with a sync key;
//   - the dedupe, marker, retry, inbox and outbox primitives the engine needs;
//   - the provider contract implemented by Slack, GitHub, Linear, and the
//     generic Chat SDK bridge.
//
// It performs no network I/O and knows no platform. Adapters own HTTP,
// credentials and payloads; the board owns the durable log. The seams are
// Host (the board service) and Provider (the platform).
package sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// Platform names a platform an adapter speaks: "slack", "github", "linear",
// or a custom Chat SDK platform.
type Platform string

// Key names one sync entry in a Registry: "slack", "github", "acme". It is the
// connection, not the platform, so two Slack workspaces can be "slack" and
// "acme". A key prefixes every external ID made through its entry.
type Key string

// MaxKeyLen is the longest sync key.
const MaxKeyLen = 64

// KeyedID names a platform user under the entry's key: a provider emits
// "slack:user:U0456", and entry "slack-eu" imports it as
// "slack-eu:user:U0456". A user belongs to the connection, not the platform,
// so two Slack workspaces never share actors.
func KeyedID(key Key, platform Platform, id string) string {
	if strings.HasPrefix(id, string(key)+":") {
		return id
	}
	prefix := string(platform) + ":"
	if strings.HasPrefix(id, prefix) {
		return string(key) + ":" + strings.TrimPrefix(id, prefix)
	}
	return string(key) + ":" + id
}

// Valid reports whether k is shaped like a sync key: 1 to 64 lowercase
// letters, numbers, "-", and "_", starting with a letter or number.
func (k Key) Valid() bool {
	if len(k) == 0 || len(k) > MaxKeyLen {
		return false
	}
	for i, c := range []byte(k) {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case (c == '-' || c == '_') && i > 0:
		default:
			return false
		}
	}
	return true
}

// Direction is which way a sync function is being called.
type Direction uint8

const (
	// DirectionIn is a platform event coming onto the board.
	DirectionIn Direction = iota + 1
	// DirectionOut is a board event going to a platform.
	DirectionOut
)

// String returns "in" or "out".
func (d Direction) String() string {
	switch d {
	case DirectionIn:
		return "in"
	case DirectionOut:
		return "out"
	default:
		return "unknown"
	}
}

// EventKind is the broad kind of a normalized platform event.
type EventKind string

const (
	// KindMessage is a new platform message or comment.
	KindMessage EventKind = "message"
	// KindEdited is an edit of a platform message.
	KindEdited EventKind = "edited"
	// KindDeleted is a deletion of a platform message.
	KindDeleted EventKind = "deleted"
	// KindReaction is a reaction added or removed.
	KindReaction EventKind = "reaction"
	// KindLifecycle is a platform lifecycle event: a PR opening, a review.
	KindLifecycle EventKind = "lifecycle"
)

// Valid reports whether k is one of the five event kinds.
func (k EventKind) Valid() bool {
	switch k {
	case KindMessage, KindEdited, KindDeleted, KindReaction, KindLifecycle:
		return true
	default:
		return false
	}
}

// Narrow event types. They are the values a sync function sees as Type and
// are not closed: a provider may add its own lifecycle types.
const (
	TypeMessage        = "message"
	TypeComment        = "comment"
	TypeMessageEdited  = "message.edited"
	TypeMessageDeleted = "message.deleted"
	TypeReactionAdded  = "reaction.added"
	TypeReactionRemove = "reaction.removed"
)

// Author is a platform user, as the platform names them. ID is native, before
// the entry key is applied: "slack:user:U0456", "github:user:41898282".
type Author struct {
	ID     string `json:"id"`
	Handle string `json:"handle"`
	Name   string `json:"name"`
	Avatar string `json:"avatar,omitempty"`
	Bot    bool   `json:"bot,omitempty"`
}

// Validate checks the fields an import needs.
func (a Author) Validate() error {
	if a.ID == "" || strings.ContainsAny(a.ID, " \t\r\n") {
		return fmt.Errorf("%w: author ID is empty or has whitespace", ErrBadRequest)
	}
	if len(a.ID) > 300 {
		return fmt.Errorf("%w: author ID is longer than 300 bytes", ErrBadRequest)
	}
	if a.Handle == "" && a.Name == "" {
		return fmt.Errorf("%w: author %q has no handle or name", ErrBadRequest, a.ID)
	}
	return nil
}

// DisplayName is Name, or Handle when the platform gave no name.
func (a Author) DisplayName() string {
	if a.Name != "" {
		return a.Name
	}
	return a.Handle
}

// File is a platform file on a message: enough to fetch it later, never the
// bytes.
type File struct {
	Type          string            `json:"type,omitempty"`
	Name          string            `json:"name,omitempty"`
	MIMEType      string            `json:"mimeType,omitempty"`
	Size          int64             `json:"size,omitempty"`
	URL           string            `json:"url,omitempty"`
	FetchMetadata map[string]string `json:"fetchMetadata,omitempty"`
}

// LinkFile is a file in a link snapshot: linked, not copied.
type LinkFile struct {
	Name string `json:"name"`
	URL  string `json:"url,omitempty"`
}

// LinkSnapshot is what a linked platform message said when the link arrived.
type LinkSnapshot struct {
	Author    string     `json:"author"`
	Text      string     `json:"text"`
	CreatedAt string     `json:"createdAt,omitempty"`
	Files     []LinkFile `json:"files,omitempty"`
}

// Link is a platform message linked from another platform message.
type Link struct {
	URL       string        `json:"url"`
	Thread    string        `json:"thread,omitempty"`
	Message   string        `json:"message,omitempty"`
	Snapshot  *LinkSnapshot `json:"snapshot,omitempty"`
	Available bool          `json:"available,omitempty"`
	Why       string        `json:"why,omitempty"`
}

// Lifecycle is a platform lifecycle event: a PR opening, a review, a CI run.
type Lifecycle struct {
	// Type is the narrow name: "pr.opened", "review.submitted", "issue.created".
	Type string `json:"type"`
	// Title, URL and Body describe the subject; Body is what a root it opens says.
	Title string `json:"title"`
	URL   string `json:"url,omitempty"`
	Body  string `json:"body,omitempty"`
	// Labels and Unset reconcile the thread root's labels.
	Labels map[string]string `json:"labels,omitempty"`
	Unset  []string          `json:"unset,omitempty"`
	// Owns lists label key prefixes the platform owns on the root: keys under
	// them that Labels doesn't have are unset.
	Owns []string `json:"owns,omitempty"`
	// Reply is a reply to add to the thread, such as "merged by ada".
	Reply string `json:"reply,omitempty"`
	// CreateRoot creates the thread root if the board hasn't seen the subject.
	CreateRoot bool `json:"createRoot,omitempty"`
	// Retitled says the subject's title or body changed: a root the platform
	// opened says the new one.
	Retitled bool `json:"retitled,omitempty"`
}

// Event is a platform event normalized by an adapter and read by a sync
// function. Channel, Thread, and Subthread are native IDs: the key is on Key,
// not baked into them.
//
// A message's Thread is the conversation it belongs to (a Slack thread, a
// GitHub PR), and Subthread is the precise sub-thread when the provider
// grouped one (a GitHub review thread). A message on a thread root has
// IsRoot true.
type Event struct {
	// ID is the inbox event ID. It is stable across retries and is the
	// idempotency key for writes made by the import.
	ID       string    `json:"id"`
	Key      Key       `json:"key"`
	Platform Platform  `json:"platform"`
	Kind     EventKind `json:"kind"`
	// Type is the narrow name: "message", "comment", "message.edited",
	// "reaction.added", "pr.opened".
	Type string `json:"type"`

	Channel   string `json:"channel"`
	Thread    string `json:"thread"`
	Subthread string `json:"subthread,omitempty"`

	Message string `json:"message,omitempty"`
	IsRoot  bool   `json:"isRoot,omitempty"`
	Text    string `json:"text,omitempty"`
	Author  Author `json:"author"`
	// Context is where in the subject the message is: "src/auth.ts:42".
	Context string `json:"context,omitempty"`

	Emoji string `json:"emoji,omitempty"`
	Added bool   `json:"added,omitempty"`

	Files     []File   `json:"files,omitempty"`
	Links     []Link   `json:"links,omitempty"`
	Mentioned []Author `json:"mentioned,omitempty"`

	Lifecycle *Lifecycle `json:"lifecycle,omitempty"`

	// Raw is the platform's payload, kept for debugging and for a root
	// override that needs it.
	Raw json.RawMessage `json:"raw,omitempty"`
}

// MaxTextBytes bounds a normalized text. Adapters clamp what a platform sends;
// the board is the final judge.
const MaxTextBytes = 256 << 10

// MaxRawBytes bounds a raw platform payload kept in an event.
const MaxRawBytes = 2 << 20

// Validate checks what every import relies on. It does not interpret the
// platform's payload.
func (e *Event) Validate() error {
	if e.ID == "" {
		return fmt.Errorf("%w: event ID is empty", ErrBadRequest)
	}
	if len(e.ID) > 300 {
		return fmt.Errorf("%w: event ID is longer than 300 bytes", ErrBadRequest)
	}
	if !e.Key.Valid() {
		return fmt.Errorf("%w: %q isn't a sync key", ErrBadRequest, e.Key)
	}
	if e.Platform == "" {
		return fmt.Errorf("%w: platform is empty", ErrBadRequest)
	}
	if !e.Kind.Valid() {
		return fmt.Errorf("%w: %q isn't an event kind", ErrBadRequest, e.Kind)
	}
	if e.Channel == "" || e.Thread == "" {
		return fmt.Errorf("%w: event has no channel or thread", ErrBadRequest)
	}
	if len(e.Text) > MaxTextBytes {
		return fmt.Errorf("%w: text is longer than %d bytes", ErrBadRequest, MaxTextBytes)
	}
	if len(e.Raw) > MaxRawBytes {
		return fmt.Errorf("%w: raw payload is longer than %d bytes", ErrBadRequest, MaxRawBytes)
	}
	switch e.Kind {
	case KindMessage:
		if e.Message == "" {
			return fmt.Errorf("%w: message event has no message ID", ErrBadRequest)
		}
		if err := e.Author.Validate(); err != nil {
			return err
		}
		if e.Text == "" && len(e.Files) == 0 {
			return fmt.Errorf("%w: message has no text and no files", ErrBadRequest)
		}
	case KindEdited:
		if e.Message == "" || e.Author.ID == "" {
			return fmt.Errorf("%w: edited event needs a message and an author", ErrBadRequest)
		}
	case KindDeleted:
		if e.Message == "" {
			return fmt.Errorf("%w: deleted event needs a message", ErrBadRequest)
		}
	case KindReaction:
		if e.Message == "" || e.Emoji == "" || e.Author.ID == "" {
			return fmt.Errorf("%w: reaction needs a message, an emoji, and an author", ErrBadRequest)
		}
		if e.Added && e.Type != TypeReactionAdded {
			return fmt.Errorf("%w: an added reaction is %q", ErrBadRequest, TypeReactionAdded)
		}
		if !e.Added && e.Type != TypeReactionRemove {
			return fmt.Errorf("%w: a removed reaction is %q", ErrBadRequest, TypeReactionRemove)
		}
	case KindLifecycle:
		if e.Lifecycle == nil || e.Lifecycle.Type == "" {
			return fmt.Errorf("%w: lifecycle event has no type", ErrBadRequest)
		}
		if e.Author.ID == "" {
			return fmt.Errorf("%w: lifecycle event has no author", ErrBadRequest)
		}
		if e.Lifecycle.Title == "" && len(e.Lifecycle.Labels) == 0 {
			return fmt.Errorf("%w: lifecycle event has no title and no labels", ErrBadRequest)
		}
	}
	return nil
}

// ThreadRef returns the event's conversation as a ThreadRef.
func (e Event) ThreadRef() ThreadRef {
	return ThreadRef{Key: e.Key, Platform: e.Platform, Channel: e.Channel, Thread: e.Thread, Subthread: e.Subthread}
}

// ThreadRef is one external conversation, named by parts. Thread is native and
// includes what the platform needs to find it ("C0123:1727.9",
// "acme/www:123"); an empty Thread is a channel target, not a thread.
type ThreadRef struct {
	Key       Key      `json:"key"`
	Platform  Platform `json:"platform"`
	Channel   string   `json:"channel"`
	Thread    string   `json:"thread,omitempty"`
	Subthread string   `json:"subthread,omitempty"`
}

// IsChannel reports whether the ref names a platform channel, not a thread.
func (r ThreadRef) IsChannel() bool { return r.Thread == "" }

// Native returns the native thread ID, or the channel ID for a channel target.
func (r ThreadRef) Native() string {
	if r.Thread != "" {
		return r.Thread
	}
	return r.Channel
}

// External returns the key-prefixed external ID: "slack:C0123:1727.9".
func (r ThreadRef) External() string { return string(r.Key) + ":" + r.Native() }

// ChannelID returns the key-prefixed channel ID: "slack:C0123".
func (r ThreadRef) ChannelID() string { return string(r.Key) + ":" + r.Channel }

// SubthreadID returns the key-prefixed sub-thread ID, or "" when there is none.
func (r ThreadRef) SubthreadID() string {
	if r.Subthread == "" {
		return ""
	}
	return string(r.Key) + ":" + r.Subthread
}

// WithSubthread returns the ref pointed at its sub-thread.
func (r ThreadRef) WithSubthread() ThreadRef {
	if r.Subthread == "" {
		return r
	}
	r.Thread = r.Subthread
	r.Subthread = ""
	return r
}

// TargetKind is whether a sync target is a channel or an existing thread.
type TargetKind string

const (
	// TargetChannel starts a new thread in a platform channel.
	TargetChannel TargetKind = "channel"
	// TargetThread joins an existing platform thread.
	TargetThread TargetKind = "thread"
)

// Valid reports whether k is a target kind.
func (k TargetKind) Valid() bool { return k == TargetChannel || k == TargetThread }

// ResolvedTarget is a sync target a provider has read: key-prefixed and with
// its channel and kind worked out. Thread is native and empty for a channel.
type ResolvedTarget struct {
	Key       Key        `json:"key"`
	Platform  Platform   `json:"platform"`
	Channel   string     `json:"channel"`
	Thread    string     `json:"thread,omitempty"`
	Subthread string     `json:"subthread,omitempty"`
	Kind      TargetKind `json:"kind"`
}

// Validate checks that the target is internally consistent.
func (t ResolvedTarget) Validate() error {
	if !t.Key.Valid() {
		return fmt.Errorf("%w: target has no valid sync key", ErrBadRequest)
	}
	if t.Platform == "" {
		return fmt.Errorf("%w: target has no platform", ErrBadRequest)
	}
	if t.Channel == "" {
		return fmt.Errorf("%w: target has no platform channel", ErrBadRequest)
	}
	if !t.Kind.Valid() {
		return fmt.Errorf("%w: %q isn't a target kind", ErrBadRequest, t.Kind)
	}
	if t.Kind == TargetChannel && t.Thread != "" {
		return fmt.Errorf("%w: a channel target has no thread", ErrBadRequest)
	}
	if t.Kind == TargetThread && t.Thread == "" {
		return fmt.Errorf("%w: a thread target has no thread ID", ErrBadRequest)
	}
	if t.Kind == TargetChannel && t.Subthread != "" {
		return fmt.Errorf("%w: a channel target has no sub-thread", ErrBadRequest)
	}
	if t.Subthread != "" && !strings.HasPrefix(t.Subthread, t.Thread) {
		return fmt.Errorf("%w: a sub-thread must extend its thread", ErrBadRequest)
	}
	return nil
}

// Ref turns the target into a ThreadRef.
func (t ResolvedTarget) Ref() ThreadRef {
	return ThreadRef{Key: t.Key, Platform: t.Platform, Channel: t.Channel, Thread: t.Thread, Subthread: t.Subthread}
}

// External returns the key-prefixed external ID of the target.
func (t ResolvedTarget) External() string { return t.Ref().External() }

// OutboundFile is a file to send with a message.
type OutboundFile struct {
	Data     []byte `json:"-"`
	Filename string `json:"filename"`
	MIMEType string `json:"mimeType,omitempty"`
}

// Sender is who an outbound message is from, for a platform that shows a
// sender per message (Slack's username and icon). Attribution is the end of
// Name that says whose word it is (": via support"), which a platform that
// cuts names short keeps whole.
type Sender struct {
	Name        string `json:"name"`
	Avatar      string `json:"avatar,omitempty"`
	Attribution string `json:"attribution,omitempty"`
}

// OutboundMessage is what a provider is asked to post or edit: Markdown that
// the platform converts to its own format, optional files, and an optional
// sender.
type OutboundMessage struct {
	Markdown string         `json:"markdown"`
	Files    []OutboundFile `json:"-"`
	Sender   *Sender        `json:"sender,omitempty"`
	// External is set when the text was written on another platform: markup in
	// it that notifies people here is the platform's to keep inert.
	External bool `json:"external,omitempty"`
	// BoardMessage and BoardThread name what this copy came from, for a marker
	// a provider writes into a platform comment.
	BoardMessage uint64 `json:"boardMessage,omitempty"`
	BoardThread  uint64 `json:"boardThread,omitempty"`
}

// Posted is a provider's answer to Post: native IDs, without the sync key.
type Posted struct {
	// Thread is the native thread that received the message. A root creates it.
	Thread  string   `json:"thread"`
	Message string   `json:"message"`
	Files   []string `json:"files,omitempty"`
}

// Validate checks the answer a provider must return.
func (p Posted) Validate() error {
	if p.Thread == "" || p.Message == "" {
		return fmt.Errorf("%w: provider answered with no thread or message ID", ErrBadRequest)
	}
	return nil
}

// Receipt records that a board message exists on a platform, or its files do.
type Receipt struct {
	Target   ResolvedTarget `json:"-"`
	Thread   ThreadRef      `json:"thread"`
	Message  string         `json:"message"`
	Files    []string       `json:"files,omitempty"`
	FilesSet bool           `json:"filesSet,omitempty"`
}

// Origin is where a board message came from: a platform's own IDs.
type Origin struct {
	Channel   string `json:"channel,omitempty"`
	Thread    string `json:"thread"`
	Subthread string `json:"subthread,omitempty"`
	Message   string `json:"message"`
	Context   string `json:"context,omitempty"`
}

// AttachmentRef is a board attachment as the outbound side needs it.
type AttachmentRef struct {
	Name     string `json:"name"`
	Type     string `json:"type,omitempty"`
	Size     int64  `json:"size,omitempty"`
	MIMEType string `json:"mimeType,omitempty"`
}

// Message is a board message as a sync function and a provider's outbound
// side see it.
type Message struct {
	ID        uint64            `json:"id"`
	Thread    uint64            `json:"thread"`
	Channel   string            `json:"channel"`
	Text      string            `json:"text"`
	Labels    map[string]string `json:"labels,omitempty"`
	Actor     string            `json:"actor"`
	ActorName string            `json:"actorName,omitempty"`
	// Verified says a credential named the author, so a platform that shows a
	// sender per message may show them. An unverified author posts under the
	// app with a label in the text.
	Verified bool `json:"verified,omitempty"`
	// From is the sync key the message came through, empty for a message
	// written on the board. It is what keeps an import from echoing back.
	From        Key             `json:"from,omitempty"`
	Origin      *Origin         `json:"origin,omitempty"`
	Deleted     bool            `json:"deleted,omitempty"`
	NoSync      bool            `json:"noSync,omitempty"`
	Attachments []AttachmentRef `json:"attachments,omitempty"`
}

// CameFrom reports whether delivering the message to target would send it back
// to where it came from: the whole sync key for an import, or the exact
// platform thread.
func (m Message) CameFrom(target ThreadRef) bool {
	if m.From != "" && m.From == target.Key {
		return true
	}
	if m.Origin != nil && m.Origin.Thread != "" && m.Origin.Thread == target.External() {
		return true
	}
	return false
}

// TargetsForDelivery returns the targets a message may go to: every target but
// where it came from.
func TargetsForDelivery(m Message, targets []ThreadRef) []ThreadRef {
	out := make([]ThreadRef, 0, len(targets))
	for _, target := range targets {
		if m.CameFrom(target) {
			continue
		}
		out = append(out, target)
	}
	return out
}

// BoardEventType is the board log event a sync function is being offered.
type BoardEventType string

const (
	BoardMessagePosted  BoardEventType = "message.posted"
	BoardMessageEdited  BoardEventType = "message.edited"
	BoardMessageDeleted BoardEventType = "message.deleted"
	BoardReactionAdded  BoardEventType = "reaction.added"
	BoardReactionRemove BoardEventType = "reaction.removed"
	BoardLabelsChanged  BoardEventType = "labels.changed"
	BoardThreadSynced   BoardEventType = "thread.synced"

	// Receipts sync records on the board.
	BoardMessageSynced      BoardEventType = "message.synced"
	BoardMessageFilesSynced BoardEventType = "message.files_synced"
	BoardMessageSyncFailed  BoardEventType = "message.sync_failed"
)

// BoardEvent is one board log fact offered to sync, or one sync records.
type BoardEvent struct {
	// Tenant is the board the fact belongs to. Sync carries it through to
	// receipts so a host can't write into another tenant's log.
	Tenant  string         `json:"tenant,omitempty"`
	ID      uint64         `json:"id"`
	Type    BoardEventType `json:"type"`
	Thread  uint64         `json:"thread"`
	Message *Message       `json:"message,omitempty"`
	// Before is the message before a labels.changed.
	Before *Message `json:"before,omitempty"`
	Emoji  string   `json:"emoji,omitempty"`
	Actor  string   `json:"actor,omitempty"`
	// Target is a thread.synced's platform target.
	Target *ThreadRef `json:"target,omitempty"`
	Cause  string     `json:"cause,omitempty"`
	// External is a message.synced receipt's platform thread.
	External *ThreadRef `json:"external,omitempty"`
	// ExternalMessage is a message.synced receipt's platform message ID.
	ExternalMessage string   `json:"externalMessage,omitempty"`
	Files           []string `json:"files,omitempty"`
	FilesSet        bool     `json:"filesSet,omitempty"`
	Error           string   `json:"error,omitempty"`
	NoSync          bool     `json:"noSync,omitempty"`
}

// ImportOptions are the overrides a sync function passes to Import.
type ImportOptions struct {
	// Text replaces the imported message's text.
	Text *string `json:"text,omitempty"`
	// Labels merge into the thread root's labels.
	Labels map[string]string `json:"labels,omitempty"`
	// Unset removes label keys from the root, after Labels.
	Unset []string `json:"unset,omitempty"`
	// NoSync keeps an imported reply on the board: no other platform gets it.
	NoSync bool `json:"noSync,omitempty"`
	// Root shapes a root the import creates. It runs synchronously.
	Root RootOverride `json:"-"`
}

// RootDraft is the default root an import would create.
type RootDraft struct {
	Text   string            `json:"text"`
	Labels map[string]string `json:"labels,omitempty"`
	Author Author            `json:"author"`
	Raw    json.RawMessage   `json:"raw,omitempty"`
}

// RootPatch is what a RootOverride changes.
type RootPatch struct {
	Text   *string           `json:"text,omitempty"`
	Labels map[string]string `json:"labels,omitempty"`
}

// RootOverride shapes a root a lifecycle event or a recovered thread creates.
type RootOverride func(RootDraft) (RootPatch, error)

// ImportRequest is one inbound import, as a Host applies it.
type ImportRequest struct {
	Tenant  string `json:"tenant"`
	Key     Key    `json:"key"`
	EventID string `json:"eventId"`
	// Channel is the board channel a new root lands in. Empty means only a
	// thread the board already syncs can receive the event.
	Channel   string        `json:"channel,omitempty"`
	Event     Event         `json:"event"`
	Overrides ImportOptions `json:"overrides,omitempty"`
}

// ImportResult is what an import touched. Zero means the event didn't land.
type ImportResult struct {
	Root        uint64 `json:"root,omitempty"`
	Message     uint64 `json:"message,omitempty"`
	CreatedRoot bool   `json:"createdRoot,omitempty"`
}

// RenderInput is what a renderer sees: the message, who it is from, and the
// platform target it goes to.
type RenderInput struct {
	Message Message
	Author  Author
	Label   string
	Target  ResolvedTarget
}

// Renderer makes the Markdown a platform receives. It may read labels.
type Renderer func(RenderInput) (string, error)

// DefaultRender is the upstream default: "**<label>**: <text>".
func DefaultRender(in RenderInput) (string, error) {
	label := in.Label
	if label == "" {
		label = in.Author.DisplayName()
	}
	if label == "" {
		label = "board"
	}
	return "**" + label + "**: " + in.Message.Text, nil
}

// DeliverOptions are the options a sync function passes to Deliver.
type DeliverOptions struct {
	Renderer Renderer `json:"-"`
}

// CopyRequest is a successful post a host may persist before it records the
// message.synced receipt. Persisting it first is what lets a retry after a
// receipt failure skip the platform post.
type CopyRequest struct {
	Tenant  string
	Message Message
	Target  ResolvedTarget
	Receipt Receipt
}

// CopySink persists a copy. A Runner calls it after a provider answers and
// before Host.Record; a failure is returned so the outbox retries the job.
type CopySink func(context.Context, CopyRequest) error

// OutboundOptions are the runner-level options a consumer supplies for one
// outbound call.
type OutboundOptions struct {
	// Renderer renders Markdown for every Deliver in the call.
	Renderer Renderer
	// CopySink persists a post's copy before the receipt is written.
	CopySink CopySink
}

// Errors a provider may return. The registry maps them to HTTP answers and the
// outbox decides whether to retry.
var (
	// ErrUnsupported means the platform can't do what was asked (no edits, no
	// typing, no channel roots). It is permanent.
	ErrUnsupported = errors.New("sync: unsupported")
	// ErrUnauthorized means a webhook's credential didn't verify, or a token
	// is missing. It is permanent.
	ErrUnauthorized = errors.New("sync: unauthorized")
	// ErrBadRequest means a payload or target is malformed. It is permanent.
	ErrBadRequest = errors.New("sync: bad request")
	// ErrNotFound means a channel or message doesn't exist. It is permanent.
	ErrNotFound = errors.New("sync: not found")
	// ErrRateLimited means the platform asked to slow down. It is retryable.
	ErrRateLimited = errors.New("sync: rate limited")
	// ErrCallFinished means a sync function used its call after returning.
	ErrCallFinished = errors.New("sync: call already finished")
	// ErrPermanent marks an error a retry can't fix.
	ErrPermanent = errors.New("sync: permanent")
)

// Permanent wraps err so a retry policy gives up at once.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrPermanent, err)
}

// IsPermanent reports whether err is one a retry can't fix.
func IsPermanent(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrPermanent) {
		return true
	}
	return errors.Is(err, ErrUnsupported) || errors.Is(err, ErrUnauthorized) ||
		errors.Is(err, ErrBadRequest) || errors.Is(err, ErrNotFound) ||
		errors.Is(err, ErrCallFinished)
}

// StorableText cleans a platform text for a board that stores UTF-8 without
// NULs, and clamps it. Adapters call it; a Host may call it again.
func StorableText(text string, max int) string {
	if max <= 0 {
		max = MaxTextBytes
	}
	if strings.IndexByte(text, 0) >= 0 {
		text = strings.ReplaceAll(text, "\x00", "")
	}
	if !utf8.ValidString(text) {
		text = strings.ToValidUTF8(text, "\uFFFD")
	}
	if len(text) <= max {
		return text
	}
	const ellipsis = "…"
	budget := max - len(ellipsis)
	if budget < 0 {
		budget = 0
	}
	for budget > 0 && !utf8.RuneStart(text[budget]) {
		budget--
	}
	return text[:budget] + ellipsis
}

// ClampID bounds a platform ID kept as an external ID part.
func ClampID(id string) string {
	if len(id) > 300 {
		return id[:300]
	}
	return id
}
