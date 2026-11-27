package chatsdk

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	boardsync "github.com/jixoai/keel-board/sync"
)

// Bridge contract version 1. A sidecar and the board exchange normalized
// events and delivery requests over signed HTTP; neither embeds the other's
// runtime. The JSON shapes below are the whole contract: docs/BRIDGE.md and
// docs/bridge-v1.schema.json describe them for a sidecar implementation.
const (
	// BridgeVersion is the only contract version this package speaks.
	BridgeVersion = "1"
	// SignatureScheme is the signature header's version prefix.
	SignatureScheme = "v1"

	// HeaderTimestamp carries the Unix second the request was signed at.
	HeaderTimestamp = "X-Keel-Board-Timestamp"
	// HeaderNonce carries a per-request nonce, unique in the replay window.
	HeaderNonce = "X-Keel-Board-Nonce"
	// HeaderSignature carries "v1=" plus the lowercase hex HMAC-SHA256.
	HeaderSignature = "X-Keel-Board-Signature"
)

// Contract limits. A sidecar and the board enforce the same bounds so a
// malformed or oversized delivery is refused before it is stored.
const (
	// MaxBodyBytes caps an inbound events body and an outbound request body.
	MaxBodyBytes = 1 << 20
	// MaxEvents caps one inbound batch.
	MaxEvents = 100
	// MaxResponseBytes caps a sidecar's answer.
	MaxResponseBytes = 64 << 10
	// MinNonceBytes and MaxNonceBytes bound a nonce. A UUID fits.
	MinNonceBytes = 8
	MaxNonceBytes = 128
)

// InboundBatch is a sidecar's normalized event push.
type InboundBatch struct {
	// Version, when set, must be "1". The signature's v1 prefix is
	// authoritative; this field lets a sidecar state the contract it used.
	Version string `json:"version,omitempty"`
	// Events is never nil: a push with no batch is a malformed request, not
	// an empty success.
	Events []InboundEvent `json:"events"`
}

// InboundEvent is one normalized event with the occurrence identity a retry
// keeps. Dedupe is the inbox identity; Event.ID is filled from it.
type InboundEvent struct {
	Dedupe string          `json:"dedupe"`
	Event  boardsync.Event `json:"event"`
}

// DeliverKind is the outbound action a delivery asks for.
type DeliverKind string

// Delivery kinds.
const (
	DeliverMessage  DeliverKind = "message"
	DeliverEdit     DeliverKind = "edit"
	DeliverDelete   DeliverKind = "delete"
	DeliverReaction DeliverKind = "reaction"
)

// Valid reports whether k is one of the four delivery kinds.
func (k DeliverKind) Valid() bool {
	switch k {
	case DeliverMessage, DeliverEdit, DeliverDelete, DeliverReaction:
		return true
	default:
		return false
	}
}

// OutboundBody is the content of a delivery. Files are not part of v1: a
// sidecar that needs them can fetch nothing from the board and the board
// refuses a file-bearing message rather than inventing bytes.
type OutboundBody struct {
	Markdown     string            `json:"markdown,omitempty"`
	Sender       *boardsync.Sender `json:"sender,omitempty"`
	External     bool              `json:"external,omitempty"`
	BoardMessage uint64            `json:"boardMessage,omitempty"`
	BoardThread  uint64            `json:"boardThread,omitempty"`
}

// DeliverRequest is a board-to-sidecar post, edit, delete, or reaction.
type DeliverRequest struct {
	// DeliveryID is stable across retries of one delivery, so a sidecar can
	// answer idempotently.
	DeliveryID string                   `json:"deliveryId"`
	Key        boardsync.Key            `json:"key"`
	Target     boardsync.ResolvedTarget `json:"target"`
	Kind       DeliverKind              `json:"kind"`
	Body       OutboundBody             `json:"body"`
	// Message is the native message an edit, delete, or reaction names.
	Message string `json:"message,omitempty"`
	Emoji   string `json:"emoji,omitempty"`
	Added   bool   `json:"added,omitempty"`
}

// Validate checks a delivery before it is signed.
func (r DeliverRequest) Validate() error {
	if strings.TrimSpace(r.DeliveryID) == "" || len(r.DeliveryID) > 300 {
		return fmt.Errorf("%w: delivery needs an ID of at most 300 bytes", boardsync.ErrBadRequest)
	}
	if err := checkWireString("deliveryId", r.DeliveryID, 300); err != nil {
		return fmt.Errorf("%w: %v", boardsync.ErrBadRequest, err)
	}
	if !r.Key.Valid() {
		return fmt.Errorf("%w: delivery key %q isn't a sync key", boardsync.ErrBadRequest, r.Key)
	}
	if err := r.Target.Validate(); err != nil {
		return err
	}
	if err := checkWireString("target.channel", r.Target.Channel, 300); err != nil {
		return fmt.Errorf("%w: %v", boardsync.ErrBadRequest, err)
	}
	if err := checkWireString("target.thread", r.Target.Thread, 300); err != nil {
		return fmt.Errorf("%w: %v", boardsync.ErrBadRequest, err)
	}
	if err := checkWireString("target.subthread", r.Target.Subthread, 300); err != nil {
		return fmt.Errorf("%w: %v", boardsync.ErrBadRequest, err)
	}
	if err := checkWireString("message", r.Message, 300); err != nil {
		return fmt.Errorf("%w: %v", boardsync.ErrBadRequest, err)
	}
	if err := checkWireString("emoji", r.Emoji, 300); err != nil {
		return fmt.Errorf("%w: %v", boardsync.ErrBadRequest, err)
	}
	if r.Body.Sender != nil {
		if err := checkWireString("sender.name", r.Body.Sender.Name, 300); err != nil {
			return fmt.Errorf("%w: %v", boardsync.ErrBadRequest, err)
		}
		if err := checkWireString("sender.avatar", r.Body.Sender.Avatar, 2000); err != nil {
			return fmt.Errorf("%w: %v", boardsync.ErrBadRequest, err)
		}
		if err := checkWireString("sender.attribution", r.Body.Sender.Attribution, 300); err != nil {
			return fmt.Errorf("%w: %v", boardsync.ErrBadRequest, err)
		}
	}
	if r.Target.Key != r.Key {
		return fmt.Errorf("%w: delivery target belongs to %q, not %q", boardsync.ErrBadRequest, r.Target.Key, r.Key)
	}
	if !r.Kind.Valid() {
		return fmt.Errorf("%w: %q isn't a delivery kind", boardsync.ErrBadRequest, r.Kind)
	}
	if len(r.Body.Markdown) > boardsync.MaxTextBytes {
		return fmt.Errorf("%w: delivery markdown is longer than %d bytes", boardsync.ErrBadRequest, boardsync.MaxTextBytes)
	}
	switch r.Kind {
	case DeliverMessage:
		if strings.TrimSpace(r.Body.Markdown) == "" {
			return fmt.Errorf("%w: a message delivery has no markdown", boardsync.ErrBadRequest)
		}
	case DeliverEdit:
		if r.Message == "" {
			return fmt.Errorf("%w: an edit needs the native message ID", boardsync.ErrBadRequest)
		}
		if strings.TrimSpace(r.Body.Markdown) == "" {
			return fmt.Errorf("%w: an edit has no markdown", boardsync.ErrBadRequest)
		}
	case DeliverDelete:
		if r.Message == "" {
			return fmt.Errorf("%w: a delete needs the native message ID", boardsync.ErrBadRequest)
		}
	case DeliverReaction:
		if r.Message == "" || r.Emoji == "" {
			return fmt.Errorf("%w: a reaction needs a message and an emoji", boardsync.ErrBadRequest)
		}
	}
	return nil
}

// ResolveRequest asks a sidecar to resolve a channel name ("#support") or a
// native target ("C0123:1727.9"). Exactly one field is set.
type ResolveRequest struct {
	Name   string `json:"name,omitempty"`
	Target string `json:"target,omitempty"`
}

// Validate checks a resolve request.
func (r ResolveRequest) Validate() error {
	if (r.Name == "") == (r.Target == "") {
		return fmt.Errorf("%w: resolve needs exactly one of name or target", boardsync.ErrBadRequest)
	}
	if len(r.Name) > 500 || len(r.Target) > 500 {
		return fmt.Errorf("%w: resolve input is longer than 500 bytes", boardsync.ErrBadRequest)
	}
	return nil
}

// ResolveResponse is a sidecar's answer. A name resolution sets Channel; a
// target resolution sets a complete Target.
type ResolveResponse struct {
	Channel string                    `json:"channel,omitempty"`
	Target  *boardsync.ResolvedTarget `json:"target,omitempty"`
}

// ErrorCode is the closed set of bridge failures.
type ErrorCode string

// Bridge error codes. The first three are permanent; rate_limited and
// transient are retryable.
const (
	ErrorUnsupported ErrorCode = "unsupported"
	ErrorAuth        ErrorCode = "auth"
	ErrorPermanent   ErrorCode = "permanent"
	ErrorRateLimited ErrorCode = "rate_limited"
	ErrorTransient   ErrorCode = "transient"
)

// Valid reports whether c is a bridge error code.
func (c ErrorCode) Valid() bool {
	switch c {
	case ErrorUnsupported, ErrorAuth, ErrorPermanent, ErrorRateLimited, ErrorTransient:
		return true
	default:
		return false
	}
}

// HTTPStatus is the status a sidecar returns for the code.
func (c ErrorCode) HTTPStatus() int {
	switch c {
	case ErrorAuth:
		return 401
	case ErrorRateLimited:
		return 429
	case ErrorTransient:
		return 503
	case ErrorUnsupported, ErrorPermanent:
		return 422
	default:
		return 500
	}
}

// BridgeError is the JSON error body both directions use.
type BridgeError struct {
	Code       ErrorCode `json:"code"`
	Message    string    `json:"message,omitempty"`
	RetryAfter int       `json:"retryAfter,omitempty"`
}

// Error implements error.
func (e *BridgeError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message == "" {
		return "chatsdk bridge: " + string(e.Code)
	}
	return "chatsdk bridge: " + string(e.Code) + ": " + e.Message
}

// Unwrap maps a code onto the sync sentinel that decides retryability and an
// HTTP answer.
func (e *BridgeError) Unwrap() error {
	if e == nil {
		return nil
	}
	switch e.Code {
	case ErrorAuth:
		return boardsync.ErrUnauthorized
	case ErrorUnsupported:
		return boardsync.ErrUnsupported
	case ErrorPermanent:
		return boardsync.ErrPermanent
	case ErrorRateLimited:
		return boardsync.ErrRateLimited
	default:
		return nil
	}
}

// DecodeInbound validates a sidecar's normalized event batch for one entry and
// returns the records the board would store. It pins the entry key and
// platform, clamps text, and rejects a record whose identity is inconsistent
// with the batch or the entry.
func DecodeInbound(body []byte, key boardsync.Key, platform boardsync.Platform) ([]boardsync.Record, error) {
	if len(body) == 0 {
		return nil, fmt.Errorf("%w: bridge events body is empty", boardsync.ErrBadRequest)
	}
	if len(body) > MaxBodyBytes {
		return nil, fmt.Errorf("%w: bridge events body is larger than %d bytes", boardsync.ErrBadRequest, MaxBodyBytes)
	}
	var batch InboundBatch
	if err := json.Unmarshal(body, &batch); err != nil {
		return nil, fmt.Errorf("%w: bridge events body: %v", boardsync.ErrBadRequest, err)
	}
	if batch.Version != "" && batch.Version != BridgeVersion {
		return nil, fmt.Errorf("%w: bridge contract version %q isn't %q", boardsync.ErrBadRequest, batch.Version, BridgeVersion)
	}
	if batch.Events == nil {
		return nil, fmt.Errorf("%w: bridge events body has no events array", boardsync.ErrBadRequest)
	}
	if len(batch.Events) > MaxEvents {
		return nil, fmt.Errorf("%w: bridge batch has %d events, more than %d", boardsync.ErrBadRequest, len(batch.Events), MaxEvents)
	}
	seen := make(map[string]struct{}, len(batch.Events))
	records := make([]boardsync.Record, 0, len(batch.Events))
	for i, inbound := range batch.Events {
		if inbound.Dedupe == "" || len(inbound.Dedupe) > 300 {
			return nil, fmt.Errorf("%w: event %d needs a dedupe ID of at most 300 bytes", boardsync.ErrBadRequest, i)
		}
		if err := checkWireString("dedupe", inbound.Dedupe, 300); err != nil {
			return nil, fmt.Errorf("%w: event %d: %v", boardsync.ErrBadRequest, i, err)
		}
		if _, duplicate := seen[inbound.Dedupe]; duplicate {
			return nil, fmt.Errorf("%w: event %d repeats dedupe %q in one batch", boardsync.ErrBadRequest, i, inbound.Dedupe)
		}
		seen[inbound.Dedupe] = struct{}{}
		event := inbound.Event
		if event.ID != "" && event.ID != inbound.Dedupe {
			return nil, fmt.Errorf("%w: event %d has ID %q but dedupe %q", boardsync.ErrBadRequest, i, event.ID, inbound.Dedupe)
		}
		event.ID = inbound.Dedupe
		if event.Key != "" && event.Key != key {
			return nil, fmt.Errorf("%w: event %d belongs to key %q, not this entry's %q", boardsync.ErrBadRequest, i, event.Key, key)
		}
		event.Key = key
		if event.Platform != "" && event.Platform != platform {
			return nil, fmt.Errorf("%w: event %d belongs to platform %q, not this entry's %q", boardsync.ErrBadRequest, i, event.Platform, platform)
		}
		event.Platform = platform
		if strings.TrimSpace(event.Type) == "" {
			return nil, fmt.Errorf("%w: event %d has no type", boardsync.ErrBadRequest, i)
		}
		if event.Author.ID == "" {
			// Deletion events in particular may omit an author in the
			// platform payload. Without one the bridge can't tell whether the
			// event is its own, so it refuses rather than risking an echo.
			return nil, fmt.Errorf("%w: event %d has no author", boardsync.ErrBadRequest, i)
		}
		if err := checkEventStrings(&event); err != nil {
			return nil, fmt.Errorf("%w: event %d: %v", boardsync.ErrBadRequest, i, err)
		}
		cleanEvent(&event)
		if err := rejectStoredNUL(event); err != nil {
			return nil, fmt.Errorf("%w: event %d: %v", boardsync.ErrBadRequest, i, err)
		}
		record := boardsync.Record{Dedupe: inbound.Dedupe, Event: event}
		if err := record.Validate(); err != nil {
			return nil, fmt.Errorf("%w: event %d: %v", boardsync.ErrBadRequest, i, err)
		}
		records = append(records, record)
	}
	return records, nil
}

// rejectStoredNUL walks the event's JSON form and refuses a NUL anywhere. The
// inbox's event column is jsonb, and PostgreSQL rejects a \u0000 escape even
// though Go can carry it in a string; refusing it here keeps a bad sidecar
// event a 400 instead of a 500 from the store.
func rejectStoredNUL(event boardsync.Event) error {
	encoded, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("event isn't JSON: %v", err)
	}
	var value any
	if err := json.Unmarshal(encoded, &value); err != nil {
		return fmt.Errorf("event isn't JSON: %v", err)
	}
	if containsNUL(value) {
		return fmt.Errorf("event contains a NUL that jsonb can't store")
	}
	return nil
}

func containsNUL(value any) bool {
	switch typed := value.(type) {
	case string:
		return strings.IndexByte(typed, 0) >= 0
	case []any:
		for _, item := range typed {
			if containsNUL(item) {
				return true
			}
		}
	case map[string]any:
		for key, item := range typed {
			if strings.IndexByte(key, 0) >= 0 || containsNUL(item) {
				return true
			}
		}
	}
	return false
}

// checkEventStrings refuses an ID that can't be stored or indexed safely. A
// NUL, a control character, invalid UTF-8, or an overlong ID would otherwise
// reach PostgreSQL and turn a bad event into a 500 instead of a 400.
func checkEventStrings(event *boardsync.Event) error {
	checks := []struct {
		name  string
		value string
		max   int
	}{
		{"type", event.Type, 200},
		{"channel", event.Channel, 300},
		{"thread", event.Thread, 300},
		{"subthread", event.Subthread, 300},
		{"message", event.Message, 300},
		{"author.id", event.Author.ID, 300},
		{"author.avatar", event.Author.Avatar, 2000},
		{"emoji", event.Emoji, 300},
		{"context", event.Context, 1000},
	}
	for _, check := range checks {
		if err := checkWireString(check.name, check.value, check.max); err != nil {
			return err
		}
	}
	if event.Lifecycle != nil {
		if err := checkWireString("lifecycle.type", event.Lifecycle.Type, 200); err != nil {
			return err
		}
		if err := checkWireString("lifecycle.url", event.Lifecycle.URL, 2000); err != nil {
			return err
		}
		for key, value := range event.Lifecycle.Labels {
			if err := checkWireString("lifecycle.labels key", key, 200); err != nil {
				return err
			}
			if err := checkWireString("lifecycle.labels value", value, 1000); err != nil {
				return err
			}
		}
		for _, value := range event.Lifecycle.Unset {
			if err := checkWireString("lifecycle.unset", value, 200); err != nil {
				return err
			}
		}
		for _, value := range event.Lifecycle.Owns {
			if err := checkWireString("lifecycle.owns", value, 200); err != nil {
				return err
			}
		}
	}
	for _, mention := range event.Mentioned {
		if err := checkWireString("mentioned.id", mention.ID, 300); err != nil {
			return err
		}
	}
	for _, file := range event.Files {
		if err := checkWireString("file.name", file.Name, 300); err != nil {
			return err
		}
		if err := checkWireString("file.url", file.URL, 2000); err != nil {
			return err
		}
	}
	for _, link := range event.Links {
		if err := checkWireString("link.url", link.URL, 2000); err != nil {
			return err
		}
		if err := checkWireString("link.thread", link.Thread, 300); err != nil {
			return err
		}
	}
	return nil
}

// checkWireString accepts an empty optional string and enforces the bounds for
// a value the board stores outside a JSON document.
func checkWireString(name, value string, max int) error {
	if value == "" {
		return nil
	}
	if len(value) > max || !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
		return fmt.Errorf("%s is invalid or longer than %d bytes", name, max)
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("%s has a control character", name)
		}
	}
	return nil
}

// cleanEvent applies the same bounding every native adapter applies: no NULs,
// valid UTF-8, and a text that fits the board's column.
func cleanEvent(event *boardsync.Event) {
	event.Text = boardsync.StorableText(event.Text, boardsync.MaxTextBytes)
	event.Author.Handle = boardsync.StorableText(event.Author.Handle, 300)
	event.Author.Name = boardsync.StorableText(event.Author.Name, 300)
	if event.Lifecycle != nil {
		event.Lifecycle.Title = boardsync.StorableText(event.Lifecycle.Title, boardsync.MaxTextBytes)
		event.Lifecycle.Body = boardsync.StorableText(event.Lifecycle.Body, boardsync.MaxTextBytes)
		event.Lifecycle.Reply = boardsync.StorableText(event.Lifecycle.Reply, boardsync.MaxTextBytes)
	}
}

// ErrorCodeOf returns a bridge code for an error, for a response body or a log.
func ErrorCodeOf(err error) ErrorCode {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, boardsync.ErrUnauthorized):
		return ErrorAuth
	case errors.Is(err, boardsync.ErrUnsupported):
		return ErrorUnsupported
	case errors.Is(err, boardsync.ErrRateLimited):
		return ErrorRateLimited
	case boardsync.IsPermanent(err):
		return ErrorPermanent
	default:
		return ErrorTransient
	}
}
