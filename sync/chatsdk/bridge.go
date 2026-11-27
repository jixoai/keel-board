package chatsdk

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

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
	if !r.Key.Valid() {
		return fmt.Errorf("%w: delivery key %q isn't a sync key", boardsync.ErrBadRequest, r.Key)
	}
	if err := r.Target.Validate(); err != nil {
		return err
	}
	if r.Target.Key != r.Key {
		return fmt.Errorf("%w: delivery target belongs to %q, not %q", boardsync.ErrBadRequest, r.Target.Key, r.Key)
	}
	if !r.Kind.Valid() {
		return fmt.Errorf("%w: %q isn't a delivery kind", boardsync.ErrBadRequest, r.Kind)
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
		cleanEvent(&event)
		record := boardsync.Record{Dedupe: inbound.Dedupe, Event: event}
		if err := record.Validate(); err != nil {
			return nil, fmt.Errorf("%w: event %d: %v", boardsync.ErrBadRequest, i, err)
		}
		records = append(records, record)
	}
	return records, nil
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
