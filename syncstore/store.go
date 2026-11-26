// Package syncstore is the durable side of sync: a PostgreSQL or in-memory
// store for sync entries, the inbound inbox, thread/copy mappings, the outbound
// lane, source cursors, and bridge nonces, plus the workers and the webhook
// handler that drive them.
//
// It sits above sync: rows carry sync.Record, sync.Event, sync.BoardEvent and
// sync.ResolvedTarget, and the workers call sync.Runner. It never speaks a
// platform and never holds a credential. A webhook is verified and normalized
// by a sync.Provider before this package stores it; an outbound delivery is
// made by a sync.Provider through the Runner.
//
// The two rules the store keeps are the ones the engine needs:
//
//   - Store before acknowledging. PushInbox commits every kept record before
//     the webhook handler writes its 2xx. A failed push is a 5xx, so the
//     platform sends the event again.
//   - One lane per conversation. The inbox leases at most one item per external
//     thread, and the outbox at most one job per board thread, so a lane is
//     ordered while lanes run in parallel. A completed or dead item no longer
//     blocks its lane.
//
// Rows are tenant-scoped on every statement. A tenant is supplied by the
// caller (the server's resolved tenant or a worker's configuration), never
// inferred from a platform payload.
package syncstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	boardsync "github.com/jixoai/keel-board/sync"
)

// Row states shared by the inbox and the outbox.
const (
	StatePending = "pending"
	StateLeased  = "leased"
	StateDone    = "done"
	StateDead    = "dead"
)

// ErrLeaseLost is returned by an ack whose lease isn't the row's live one:
// another worker leased the row after this one's lease expired.
var ErrLeaseLost = boardsync.ErrLeaseLost

// ErrNotDead is returned when a retry names a row that isn't dead-lettered.
var ErrNotDead = errors.New("syncstore: the row is not dead-lettered")

// Config bounds the store's retries and leases. Zero fields take the sync
// defaults: InboundRetry for inbound imports, DefaultRetry for outbound
// deliveries, and one minute for a lease.
type Config struct {
	InboundRetry  boardsync.RetryPolicy
	OutboundRetry boardsync.RetryPolicy
	LeaseTTL      time.Duration
}

// withDefaults validates the configuration and fills zero values.
func (c Config) withDefaults() (Config, error) {
	if c.InboundRetry == (boardsync.RetryPolicy{}) {
		c.InboundRetry = boardsync.InboundRetry
	}
	if c.OutboundRetry == (boardsync.RetryPolicy{}) {
		c.OutboundRetry = boardsync.DefaultRetry
	}
	var err error
	if c.InboundRetry, err = c.InboundRetry.Validate(); err != nil {
		return Config{}, fmt.Errorf("syncstore: invalid inbound retry policy: %w", err)
	}
	if c.OutboundRetry, err = c.OutboundRetry.Validate(); err != nil {
		return Config{}, fmt.Errorf("syncstore: invalid outbound retry policy: %w", err)
	}
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = time.Minute
	}
	return c, nil
}

// Entry is one configured sync connection: a key belongs to one tenant and
// names a platform. It carries no secret; a deployment resolves credentials
// separately. Disabled entries keep their history but accept no new webhook.
//
// On a new row Enabled is stored as given, so a deployment registering a live
// connection must set it true. A later UpsertEntry preserves the stored flag:
// turning an entry off is SetEntryEnabled's job, not a restart's.
type Entry struct {
	Tenant    string             `json:"tenant"`
	Key       boardsync.Key      `json:"key"`
	Platform  boardsync.Platform `json:"platform"`
	Enabled   bool               `json:"enabled"`
	CreatedAt time.Time          `json:"createdAt"`
	UpdatedAt time.Time          `json:"updatedAt"`
}

// Validate checks an entry before it is stored.
func (e Entry) Validate() error {
	if err := validTenant(e.Tenant); err != nil {
		return err
	}
	if !e.Key.Valid() {
		return fmt.Errorf("%w: %q isn't a sync key", boardsync.ErrBadRequest, e.Key)
	}
	if strings.TrimSpace(string(e.Platform)) == "" || len(e.Platform) > 64 {
		return fmt.Errorf("%w: sync entry %q has no usable platform", boardsync.ErrBadRequest, e.Key)
	}
	return nil
}

// Thread maps one board thread to an external target. Thread and Subthread are
// native IDs; ThreadID is deterministic, so re-attaching a target is one row.
type Thread struct {
	Tenant      string               `json:"tenant"`
	ID          string               `json:"id"`
	BoardThread uint64               `json:"boardThread"`
	Key         boardsync.Key        `json:"key"`
	Platform    boardsync.Platform   `json:"platform"`
	Channel     string               `json:"channel"`
	Thread      string               `json:"thread,omitempty"`
	Subthread   string               `json:"subthread,omitempty"`
	Kind        boardsync.TargetKind `json:"kind"`
	CreatedAt   time.Time            `json:"createdAt"`
	UpdatedAt   time.Time            `json:"updatedAt"`
}

// Validate checks a thread mapping.
func (t Thread) Validate() error {
	if err := validTenant(t.Tenant); err != nil {
		return err
	}
	if t.BoardThread == 0 {
		return fmt.Errorf("%w: a thread mapping needs a board thread", boardsync.ErrBadRequest)
	}
	if !t.Key.Valid() {
		return fmt.Errorf("%w: a thread mapping has no valid sync key", boardsync.ErrBadRequest)
	}
	target := boardsync.ResolvedTarget{
		Key: t.Key, Platform: t.Platform, Channel: t.Channel,
		Thread: t.Thread, Subthread: t.Subthread, Kind: t.Kind,
	}
	if err := target.Validate(); err != nil {
		return err
	}
	return nil
}

// Copy maps one board message to its platform copy on one target.
type Copy struct {
	ID              string                   `json:"id"`
	BoardMessage    uint64                   `json:"boardMessage"`
	BoardThread     uint64                   `json:"boardThread"`
	Key             boardsync.Key            `json:"key"`
	Target          boardsync.ResolvedTarget `json:"target"`
	ExternalThread  string                   `json:"externalThread"`
	ExternalMessage string                   `json:"externalMessage"`
	CreatedAt       time.Time                `json:"createdAt"`
	UpdatedAt       time.Time                `json:"updatedAt"`
}

// Validate checks a copy before it is stored.
func (c *Copy) Validate() error {
	if c.BoardMessage == 0 || c.BoardThread == 0 {
		return fmt.Errorf("%w: a copy needs a board message and thread", boardsync.ErrBadRequest)
	}
	if err := c.Target.Validate(); err != nil {
		return err
	}
	if c.Key == "" {
		c.Key = c.Target.Key
	}
	if c.Key != c.Target.Key {
		return fmt.Errorf("%w: a copy's key isn't its target's", boardsync.ErrBadRequest)
	}
	if c.ExternalThread == "" || c.ExternalMessage == "" {
		return fmt.Errorf("%w: a copy needs external thread and message IDs", boardsync.ErrBadRequest)
	}
	return nil
}

// Delivery is one queued outbound delivery, or one dead-lettered failure.
type Delivery struct {
	ID          string                   `json:"id"`
	Lane        string                   `json:"lane"`
	Key         boardsync.Key            `json:"key"`
	Event       boardsync.BoardEvent     `json:"event"`
	Message     boardsync.Message        `json:"message"`
	Target      boardsync.ResolvedTarget `json:"target"`
	Attempts    int                      `json:"attempts"`
	State       string                   `json:"state"`
	Lease       string                   `json:"-"`
	LastError   string                   `json:"lastError,omitempty"`
	AvailableAt time.Time                `json:"availableAt"`
	CreatedAt   time.Time                `json:"createdAt"`
}

// InboxPush is what a push added; records it recognized are skipped.
type InboxPush struct {
	Added   int `json:"added"`
	Skipped int `json:"skipped"`
}

// EnqueueResult is what an outbound enqueue added; a fact already queued for a
// target is skipped.
type EnqueueResult struct {
	Added   int `json:"added"`
	Skipped int `json:"skipped"`
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

// InboxDepth counts a key's inbox rows by state.
type InboxDepth struct {
	Pending int `json:"pending"`
	Leased  int `json:"leased"`
	Done    int `json:"done"`
	Dead    int `json:"dead"`
}

// OutboxDepth counts a tenant's outbox rows by state.
type OutboxDepth struct {
	Pending int `json:"pending"`
	Leased  int `json:"leased"`
	Done    int `json:"done"`
	Dead    int `json:"dead"`
}

// Store is the durable sync state. Memory and PostgreSQL implement it with
// the same semantics; a caller may run either behind the workers and handler.
//
// All methods are safe for concurrent use. Tenant and key arguments identify
// the rows a caller may touch; a missing or empty tenant is refused rather
// than treated as a shared namespace.
type Store interface {
	// Entries. UpsertEntry inserts a connection or updates its platform while
	// preserving an operator's enabled flag.
	UpsertEntry(ctx context.Context, entry Entry) (Entry, error)
	Entry(ctx context.Context, tenant string, key boardsync.Key) (Entry, bool, error)
	Entries(ctx context.Context, tenant string) ([]Entry, error)
	SetEntryEnabled(ctx context.Context, tenant string, key boardsync.Key, enabled bool) (bool, error)

	// Inbound. PushInbox validates the whole batch, then stores every new
	// dedupe or none. LeaseInbox returns at most one due item per external
	// lane. AckInbox removes a finished item from the lane (it stays as a done
	// row for dedupe) or schedules a failed one.
	PushInbox(ctx context.Context, tenant string, key boardsync.Key, records []boardsync.Record) (InboxPush, error)
	LeaseInbox(ctx context.Context, tenant string, key boardsync.Key, limit int, owner string) ([]boardsync.InboxItem, error)
	AckInbox(ctx context.Context, tenant string, key boardsync.Key, id, lease string, jobErr error) (AckResult, error)
	// ReleaseInbox clears a lease without counting an attempt, so a worker
	// that isn't ready for its key doesn't burn the item's retry budget.
	ReleaseInbox(ctx context.Context, tenant string, key boardsync.Key, id, lease string, delay time.Duration) error
	DeadInbox(ctx context.Context, tenant string, key boardsync.Key, limit int) ([]boardsync.InboxItem, error)
	RetryInbox(ctx context.Context, tenant string, key boardsync.Key, id string) error
	InboxDepth(ctx context.Context, tenant string, key boardsync.Key) (InboxDepth, error)

	// Outbound. EnqueueOutbox turns one board fact and its targets into one
	// deterministic job per target. LeaseOutbox returns at most one due job per
	// board-thread lane, but only for the caller's keys: lane heads are ranked
	// across every key, so a worker never jumps over another key's earlier job.
	EnqueueOutbox(ctx context.Context, tenant string, key boardsync.Key, event boardsync.BoardEvent, message boardsync.Message, targets []boardsync.ResolvedTarget) (EnqueueResult, error)
	LeaseOutbox(ctx context.Context, tenant string, keys []boardsync.Key, limit int, owner string) ([]Delivery, error)
	AckOutbox(ctx context.Context, tenant, deliveryID, lease string, jobErr error, copy *Copy) (AckResult, error)
	// ReleaseOutbox is ReleaseInbox for a delivery: no attempt is counted and
	// the lane stays ordered.
	ReleaseOutbox(ctx context.Context, tenant, deliveryID, lease string, delay time.Duration) error
	DeadOutbox(ctx context.Context, tenant string, key boardsync.Key, limit int) ([]Delivery, error)
	RetryOutbox(ctx context.Context, tenant, deliveryID string) error
	OutboxDepth(ctx context.Context, tenant string) (OutboxDepth, error)
	Copy(ctx context.Context, tenant string, key boardsync.Key, target boardsync.ResolvedTarget, boardMessage uint64) (Copy, bool, error)
	CopyForMessage(ctx context.Context, tenant string, key boardsync.Key, boardMessage uint64) ([]Copy, error)
	// SaveCopy persists a post's copy. The runner calls it before it records
	// message.synced, so a receipt retry finds the copy and doesn't post again.
	SaveCopy(ctx context.Context, tenant string, copy Copy) (Copy, error)
	TargetFailures(ctx context.Context, tenant string, key boardsync.Key, limit int) ([]Delivery, error)

	// Threads. ThreadByTarget is the reverse of a thread mapping; ThreadSynced
	// answers the Gate's key-prefixed external thread.
	UpsertThread(ctx context.Context, thread Thread) (Thread, error)
	Threads(ctx context.Context, tenant string, boardThread uint64) ([]Thread, error)
	ThreadByTarget(ctx context.Context, tenant string, key boardsync.Key, target boardsync.ResolvedTarget) (Thread, bool, error)
	ThreadSynced(ctx context.Context, tenant string, key boardsync.Key, external string) (bool, error)
	RemoveThread(ctx context.Context, tenant, id string) error

	// Source cursors and bridge nonces.
	Cursor(ctx context.Context, tenant string, key boardsync.Key, source string) (string, bool, error)
	SetCursor(ctx context.Context, tenant string, key boardsync.Key, source, cursor string) error
	ClaimNonce(ctx context.Context, tenant string, key boardsync.Key, nonce string, ttl time.Duration) (bool, error)
	PruneNonces(ctx context.Context, tenant string, before time.Time) (int64, error)
}

// validTenant refuses an empty or unbounded tenant, so a caller can't
// accidentally use "" as a tenant-wide namespace.
func validTenant(tenant string) error {
	if tenant == "" || len(tenant) > 200 || !utf8.ValidString(tenant) {
		return fmt.Errorf("%w: tenant is empty or longer than 200 bytes", boardsync.ErrBadRequest)
	}
	for _, r := range tenant {
		if r == 0 || r < 0x20 || r == 0x7f {
			return fmt.Errorf("%w: tenant has a control character", boardsync.ErrBadRequest)
		}
	}
	return nil
}

// ownedKeys validates a worker's key set and returns it as a set. An empty set
// means the worker owns nothing, never "all keys": a missing configuration must
// not let one key's worker burn another key's deliveries.
func ownedKeys(keys []boardsync.Key) (map[boardsync.Key]struct{}, error) {
	owned := make(map[boardsync.Key]struct{}, len(keys))
	for _, key := range keys {
		if err := expectKey(key); err != nil {
			return nil, err
		}
		owned[key] = struct{}{}
	}
	return owned, nil
}

// prepareRecords validates a webhook's records and fills their event identity.
// Nothing is stored unless every record is valid, so a malformed record makes
// the webhook answer 400/500 rather than importing part of a delivery.
func prepareRecords(key boardsync.Key, records []boardsync.Record) ([]boardsync.Record, error) {
	if !key.Valid() {
		return nil, fmt.Errorf("%w: %q isn't a sync key", boardsync.ErrBadRequest, key)
	}
	prepared := make([]boardsync.Record, len(records))
	for i := range records {
		record := records[i]
		if record.Event.Key == "" {
			record.Event.Key = key
		}
		if record.Event.Key != key {
			return nil, fmt.Errorf("%w: record key %q isn't the entry key %q", boardsync.ErrBadRequest, record.Event.Key, key)
		}
		if err := record.Validate(); err != nil {
			return nil, fmt.Errorf("%w: %v", boardsync.ErrBadRequest, err)
		}
		if _, err := json.Marshal(record.Event); err != nil {
			return nil, fmt.Errorf("%w: event %q isn't JSON", boardsync.ErrBadRequest, record.Event.ID)
		}
		if len(record.Raw) > 0 && !json.Valid(record.Raw) {
			return nil, fmt.Errorf("%w: record %q has a raw payload that isn't JSON", boardsync.ErrBadRequest, record.Event.ID)
		}
		prepared[i] = record
	}
	return prepared, nil
}

// deliverable reports whether a board event is one sync delivers. Receipts are
// board facts sync writes, never things it sends to a platform.
func deliverable(eventType boardsync.BoardEventType) bool {
	switch eventType {
	case boardsync.BoardMessagePosted, boardsync.BoardMessageEdited,
		boardsync.BoardMessageDeleted, boardsync.BoardReactionAdded,
		boardsync.BoardReactionRemove, boardsync.BoardLabelsChanged,
		boardsync.BoardThreadSynced:
		return true
	default:
		return false
	}
}

// preparedDelivery is one enqueue's validated job.
type preparedDelivery struct {
	id        string
	lane      string
	key       boardsync.Key
	event     boardsync.BoardEvent
	message   boardsync.Message
	target    boardsync.ResolvedTarget
	targetKey string
}

// prepareOutbox validates a board fact and its targets. It applies echo
// suppression and NoSync before returning jobs, so an outbox row is never
// queued for the platform a message came from.
func prepareOutbox(tenant string, key boardsync.Key, event boardsync.BoardEvent, message boardsync.Message, targets []boardsync.ResolvedTarget) ([]preparedDelivery, error) {
	if err := validTenant(tenant); err != nil {
		return nil, err
	}
	if !key.Valid() {
		return nil, fmt.Errorf("%w: %q isn't a sync key", boardsync.ErrBadRequest, key)
	}
	if event.Tenant == "" {
		event.Tenant = tenant
	}
	if event.Tenant != tenant {
		return nil, fmt.Errorf("%w: event tenant %q isn't the delivery tenant %q", boardsync.ErrBadRequest, event.Tenant, tenant)
	}
	if !deliverable(event.Type) {
		return nil, fmt.Errorf("%w: sync can't deliver %q", boardsync.ErrBadRequest, event.Type)
	}
	if event.ID == 0 {
		return nil, fmt.Errorf("%w: a delivery needs the board event ID", boardsync.ErrBadRequest)
	}
	if message.ID == 0 {
		return nil, fmt.Errorf("%w: a delivery needs the board message ID", boardsync.ErrBadRequest)
	}
	if event.Thread != 0 && message.Thread != 0 && event.Thread != message.Thread {
		return nil, fmt.Errorf("%w: event thread %d isn't message thread %d", boardsync.ErrBadRequest, event.Thread, message.Thread)
	}
	if event.Thread == 0 {
		event.Thread = message.Thread
	}
	if message.Thread == 0 {
		message.Thread = event.Thread
	}
	if event.Thread == 0 || message.Thread == 0 {
		return nil, fmt.Errorf("%w: a delivery needs a board thread", boardsync.ErrBadRequest)
	}
	if message.NoSync || event.NoSync {
		return nil, nil
	}
	// The stored event doesn't need a second copy of the message; the runner
	// reads the message beside it.
	event.Message = nil

	kept := make([]boardsync.ResolvedTarget, 0, len(targets))
	for _, target := range targets {
		if message.CameFrom(target.Ref()) {
			continue
		}
		kept = append(kept, target)
	}
	jobs := make([]preparedDelivery, 0, len(kept))
	seen := make(map[string]struct{}, len(kept))
	for _, target := range kept {
		if target.Key != key {
			return nil, fmt.Errorf("%w: target key %q isn't the delivery key %q", boardsync.ErrBadRequest, target.Key, key)
		}
		if err := target.Validate(); err != nil {
			return nil, err
		}
		targetKey := targetKeyOf(target)
		if _, duplicate := seen[targetKey]; duplicate {
			continue
		}
		seen[targetKey] = struct{}{}
		jobs = append(jobs, preparedDelivery{
			id:        DeliveryID(tenant, key, event, target),
			lane:      strconv.FormatUint(event.Thread, 10),
			key:       key,
			event:     event,
			message:   message,
			target:    target,
			targetKey: targetKey,
		})
	}
	return jobs, nil
}

// DeliveryID is the deterministic identity of one board fact for one target.
// Enqueueing the same fact twice, or a worker retrying after a crash, names
// the same outbox row. event.ID is the board's monotone seq, so two edits to
// one message are two deliveries while a redelivery is one.
func DeliveryID(tenant string, key boardsync.Key, event boardsync.BoardEvent, target boardsync.ResolvedTarget) string {
	return "delivery_" + boardsync.Hash(keyParts(
		tenant, string(key), strconv.FormatUint(event.ID, 10), string(event.Type),
		targetKeyOf(target), event.Emoji, event.Cause,
	))
}

func targetKeyOf(target boardsync.ResolvedTarget) string {
	return keyParts(
		string(target.Key), string(target.Platform), target.Channel,
		target.Thread, target.Subthread, string(target.Kind),
	)
}

// keyParts encodes parts unambiguously: each part is its byte length, a colon,
// and the bytes. A platform ID containing a separator can't make two different
// targets share a delivery or copy identity.
func keyParts(parts ...string) string {
	var b strings.Builder
	for _, part := range parts {
		b.WriteString(strconv.Itoa(len(part)))
		b.WriteByte(':')
		b.WriteString(part)
	}
	return b.String()
}

func copyIDOf(key boardsync.Key, target boardsync.ResolvedTarget, boardMessage uint64) string {
	return "copy_" + boardsync.Hash(keyParts(
		string(key), targetKeyOf(target), strconv.FormatUint(boardMessage, 10),
	))
}

func threadIDOf(boardThread uint64, key boardsync.Key, target boardsync.ResolvedTarget) string {
	return "thread_" + boardsync.Hash(keyParts(
		strconv.FormatUint(boardThread, 10), string(key), targetKeyOf(target),
	))
}

// ErrorCode reduces an error to a stable, content-free class. Store rows,
// board receipts, and logs use it instead of err.Error(): a platform error may
// carry a response body or a request's text, which must not be persisted.
func ErrorCode(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, boardsync.ErrRateLimited):
		return "rate_limited"
	case errors.Is(err, boardsync.ErrUnauthorized):
		return "unauthorized"
	case errors.Is(err, boardsync.ErrNotFound):
		return "not_found"
	case errors.Is(err, boardsync.ErrUnsupported):
		return "unsupported"
	case errors.Is(err, boardsync.ErrBadRequest):
		return "bad_request"
	case errors.Is(err, boardsync.ErrPermanent):
		return "permanent"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	default:
		return "failed"
	}
}

// copyFromRequest builds the copy a successful post records. It returns nil
// when the provider's answer carries no external message (an edit, a no-op
// labels change, or a thread target's thread.synced).
func copyFromRequest(req boardsync.CopyRequest) *Copy {
	if req.Receipt.Message == "" {
		return nil
	}
	externalThread := req.Receipt.Thread.Thread
	if externalThread == "" {
		externalThread = req.Receipt.Thread.Native()
	}
	if externalThread == "" {
		externalThread = req.Target.Thread
	}
	copy := &Copy{
		BoardMessage:    req.Message.ID,
		BoardThread:     req.Message.Thread,
		Key:             req.Target.Key,
		Target:          req.Target,
		ExternalThread:  externalThread,
		ExternalMessage: req.Receipt.Message,
	}
	copy.ID = copyIDOf(copy.Key, copy.Target, copy.BoardMessage)
	return copy
}

func copyFromReceipt(job Delivery, receipt boardsync.Receipt) *Copy {
	return copyFromRequest(boardsync.CopyRequest{
		Tenant: "", Message: job.Message, Target: job.Target, Receipt: receipt,
	})
}

func cloneJSON[T any](value T) (T, error) {
	var out T
	raw, err := json.Marshal(value)
	if err != nil {
		return out, err
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, err
	}
	return out, nil
}

func expectKey(key boardsync.Key) error {
	if !key.Valid() {
		return fmt.Errorf("%w: %q isn't a sync key", boardsync.ErrBadRequest, key)
	}
	return nil
}

func expectLabel(value, name string, max int) error {
	if value == "" || len(value) > max || !utf8.ValidString(value) {
		return fmt.Errorf("%w: %s is empty, invalid, or longer than %d bytes", boardsync.ErrBadRequest, name, max)
	}
	for _, r := range value {
		if r == 0 || r < 0x20 || r == 0x7f {
			return fmt.Errorf("%w: %s has a control character", boardsync.ErrBadRequest, name)
		}
	}
	return nil
}

// validCursor checks an opaque source cursor. Newlines are allowed (a cursor
// may be a JSON blob), a NUL and invalid UTF-8 are not.
func validCursor(cursor string) error {
	if cursor == "" || len(cursor) > 8192 || !utf8.ValidString(cursor) || strings.IndexByte(cursor, 0) >= 0 {
		return fmt.Errorf("%w: cursor is empty, invalid, or longer than 8192 bytes", boardsync.ErrBadRequest)
	}
	return nil
}
