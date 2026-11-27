// Package audit keeps the board's bounded operational audit: sync webhook
// acceptance and refusal, inbox and outbox dead letters, retries, and bridge
// failures.
//
// An audit row is deliberately narrow. It names a kind, a sync connection key,
// a platform, an external target, a delivery ID, a bounded status, an attempt
// count, and a small detail object of status/error codes. It never carries a
// raw request body, a message body, a token, a credential, or a secret. The
// validation here refuses fields shaped like free text; a caller cannot store
// one by accident.
//
// The board log (board_events) stays the domain audit: who changed what is a
// fact there. This package is the operational trace beside it.
//
// Phase 0 writes: webhook acceptance and refusal, bridge failures, source
// read/join, inbox and outbox dead letters, and manual inbox/outbox retries.
// Per-attempt outbound
// success and inbound-processed rows are deliberately not written: the board
// log's message.synced/message.sync_failed receipts carry the delivery trace,
// and per-attempt rows would grow with every delivery. Those remain a Phase 1
// decision, not a claim this table makes.
//
// PostgreSQL rows are tenant-scoped twice: every statement filters tenant_id
// and every transaction sets keel.tenant_id, which the table's FORCE RLS policy
// checks. A transaction that loses its tenant setting sees nothing. There is no
// bypass setting on this table: the audit never needs a cross-tenant read.
package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"
	"unicode/utf8"
)

// Kinds of audit facts. They are stable tokens, not sentences.
const (
	KindWebhookAccepted = "sync.webhook.accepted"
	KindWebhookRejected = "sync.webhook.rejected"
	KindInboxDead       = "sync.inbox.dead"
	KindInboxRetry      = "sync.inbox.retry"
	KindOutboxDead      = "sync.outbox.dead"
	KindOutboxRetry     = "sync.outbox.retry"
	KindBridgeRejected  = "sync.bridge.rejected"

	// KindSourceRead and KindSourceJoin are written by the on-demand source
	// routes (§14.10): who read or joined what connection, with a bounded
	// status and no target or message body.
	KindSourceRead = "sync.source.read"
	KindSourceJoin = "sync.source.join"
)

// Status values shared by writers.
const (
	StatusOK       = "ok"
	StatusAccepted = "accepted"
	StatusRejected = "rejected"
	StatusFailed   = "failed"
	StatusDead     = "dead"
	StatusRetried  = "retried"
)

// maxDetailEntries bounds one row's detail object.
const maxDetailEntries = 16

// maxDetailToken bounds one detail string value. A detail value is a token or
// a closed code, never a sentence or a body.
const maxDetailToken = 128

// Event is one audit fact. Tenant scopes the row and is not exposed by the
// read API; At is zero for "now".
type Event struct {
	ID         uint64         `json:"id"`
	Tenant     string         `json:"-"`
	At         time.Time      `json:"at"`
	Kind       string         `json:"kind"`
	Key        string         `json:"key,omitempty"`
	Platform   string         `json:"platform,omitempty"`
	Target     string         `json:"target,omitempty"`
	DeliveryID string         `json:"deliveryId,omitempty"`
	Status     string         `json:"status,omitempty"`
	Attempts   int            `json:"attempts,omitempty"`
	Detail     map[string]any `json:"detail,omitempty"`
}

// Store persists audit rows and reads a tenant's newest ones.
type Store interface {
	// Append stores one row. It returns the row with its assigned ID and time.
	Append(ctx context.Context, event Event) (Event, error)
	// Recent returns a tenant's newest rows, newest first.
	Recent(ctx context.Context, tenant string, limit int) ([]Event, error)
}

// ErrInvalid is returned for a row that would carry something the audit must
// not hold.
var ErrInvalid = errors.New("audit: invalid event")

// Validate refuses a row whose fields are empty, unbounded, or shaped like
// free text. Kind and Tenant are required; every other text field is optional.
func (e Event) Validate() error {
	if err := validTenant(e.Tenant); err != nil {
		return err
	}
	if !validToken(e.Kind, 64) {
		return fmt.Errorf("%w: kind %q isn't a bounded kind", ErrInvalid, e.Kind)
	}
	if e.Key != "" && !validToken(e.Key, 128) {
		return fmt.Errorf("%w: key %q isn't a bounded key", ErrInvalid, e.Key)
	}
	if e.Platform != "" && !validToken(e.Platform, 64) {
		return fmt.Errorf("%w: platform %q isn't a bounded platform", ErrInvalid, e.Platform)
	}
	if e.Status != "" && !validToken(e.Status, 64) {
		return fmt.Errorf("%w: status %q isn't a bounded status", ErrInvalid, e.Status)
	}
	if e.DeliveryID != "" && !validToken(e.DeliveryID, 200) {
		return fmt.Errorf("%w: delivery ID isn't a bounded token", ErrInvalid)
	}
	if e.Target != "" && !validTarget(e.Target) {
		return fmt.Errorf("%w: target isn't a bounded target", ErrInvalid)
	}
	if e.Attempts < 0 || e.Attempts > 1_000_000 {
		return fmt.Errorf("%w: attempts %d is out of range", ErrInvalid, e.Attempts)
	}
	if len(e.Detail) > maxDetailEntries {
		return fmt.Errorf("%w: detail has %d entries", ErrInvalid, len(e.Detail))
	}
	for key, value := range e.Detail {
		if !validDetailKey(key) {
			return fmt.Errorf("%w: detail key %q isn't a status or error code name", ErrInvalid, key)
		}
		if !validDetailValue(value) {
			return fmt.Errorf("%w: detail %q carries an unbounded value", ErrInvalid, key)
		}
	}
	return nil
}

func validTenant(tenant string) error {
	if tenant == "" || len(tenant) > 200 || !utf8.ValidString(tenant) {
		return fmt.Errorf("%w: tenant is empty or longer than 200 bytes", ErrInvalid)
	}
	for _, r := range tenant {
		if r == 0 || r < 0x20 || r == 0x7f {
			return fmt.Errorf("%w: tenant has a control character", ErrInvalid)
		}
	}
	return nil
}

// validToken accepts an identifier-like token: letters, digits, '.', '_', '-',
// ':' and '/', starting with a letter. It refuses whitespace, quotes, and
// anything long enough to be opaque content.
func validToken(value string, max int) bool {
	if value == "" || len(value) > max || !utf8.ValidString(value) {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
		case c >= '0' && c <= '9', c == '.', c == '_', c == '-', c == ':', c == '/':
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// validTarget bounds an external target such as "slack:C123:T456" or
// "github:acme/web#12". A target is a platform identifier: a whitespace or quote
// makes it a body or a header, and it is refused.
func validTarget(target string) bool {
	return validBoundedToken(target, 512)
}

// validBoundedToken accepts an identifier-like token: letters, digits, '.', '_',
// '-', ':', '/', '#' and '@', with at least one letter or digit. Whitespace,
// quotes, '=', and anything that could be free text are refused. It is the
// shape of a platform ID, a delivery ID, an error code, or a target.
func validBoundedToken(value string, max int) bool {
	if value == "" || len(value) > max || !utf8.ValidString(value) {
		return false
	}
	alnum := false
	for i := 0; i < len(value); i++ {
		c := value[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
			alnum = true
		case c == '.', c == '_', c == '-', c == ':', c == '/', c == '#', c == '@':
		default:
			return false
		}
	}
	return alnum
}

// SafeToken reports whether value has the bounded shape a detail string
// accepts. A writer whose value comes from an external system uses it to omit
// the field rather than have the whole row refused.
func SafeToken(value string) bool {
	return validBoundedToken(value, maxDetailToken)
}

// SafeDeliveryID reports whether value has the bounded shape the DeliveryID
// field accepts. A delivery ID must start with a letter, so a numeric message
// ID can never stand in for one.
func SafeDeliveryID(value string) bool {
	return validToken(value, 200)
}

// SafeTarget reports whether value has the bounded shape the Target field
// accepts. A writer whose target comes from a platform omits it rather than
// losing the whole audit row.
func SafeTarget(value string) bool {
	return value != "" && validTarget(value)
}

func validDetailKey(key string) bool {
	if key == "" || len(key) > 32 {
		return false
	}
	for i := 0; i < len(key); i++ {
		c := key[i]
		switch {
		case c >= 'a' && c <= 'z':
		case c == '_':
			if i == 0 {
				return false
			}
		case c >= '0' && c <= '9':
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// validDetailValue accepts only bounded scalars: a short token or closed code,
// an integer in a sane range, or a bool. Maps, slices, sentences, and long
// strings are refused, so a caller can't smuggle a payload, a body, or a
// secret into detail.
func validDetailValue(value any) bool {
	switch typed := value.(type) {
	case nil:
		return true
	case string:
		return validBoundedToken(typed, maxDetailToken)
	case bool:
		return true
	case int:
		return typed >= -1_000_000_000 && typed <= 1_000_000_000
	case int32:
		return true
	case int64:
		return typed >= -1_000_000_000 && typed <= 1_000_000_000
	case float64:
		return !math.IsNaN(typed) && !math.IsInf(typed, 0) && math.Abs(typed) <= 1_000_000_000
	case json.Number:
		number, err := typed.Int64()
		return err == nil && number >= -1_000_000_000 && number <= 1_000_000_000
	default:
		return false
	}
}

// prepare normalizes an event before it is stored: fills the time, copies the
// detail, and validates.
func prepare(event Event) (Event, error) {
	if event.At.IsZero() {
		event.At = time.Now().UTC()
	}
	event.At = event.At.Truncate(time.Microsecond)
	if event.Detail != nil {
		copied := make(map[string]any, len(event.Detail))
		for key, value := range event.Detail {
			copied[key] = value
		}
		event.Detail = copied
	}
	if err := event.Validate(); err != nil {
		return Event{}, err
	}
	return event, nil
}

func cloneEvents(events []Event) []Event {
	out := make([]Event, len(events))
	for i, event := range events {
		out[i] = event
		if event.Detail != nil {
			copied := make(map[string]any, len(event.Detail))
			for key, value := range event.Detail {
				copied[key] = value
			}
			out[i].Detail = copied
		}
	}
	return out
}
