package log

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/store"
)

// ErrHeadMoved reports a conditional append whose expected head isn't the
// log's head anymore. The caller reads again and decides again.
var ErrHeadMoved = store.ErrHeadMoved

// Clock returns the board's notion of now. Tests substitute one to advance
// leases.
type Clock func() time.Time

// Options configure a board.
type Options struct {
	// Store persists the raw log. Default: an in-memory store.
	Store store.EventStore
	// Now is the board's clock. Default: time.Now.
	Now Clock
}

// Board is one process's view of many tenants' logs.
type Board struct {
	mu      sync.Mutex
	store   store.EventStore
	now     Clock
	tenants map[string]*tenant
}

type tenant struct {
	state  *State
	events []Event
	idem   map[string]idemEntry
	// notify is closed and replaced after each append, waking waiters.
	notify chan struct{}
}

type idemEntry struct {
	digest     string
	messageIDs []uint64
	first      uint64
	last       uint64
}

// New returns a board reading and writing through opts.Store.
func New(opts Options) *Board {
	if opts.Store == nil {
		opts.Store = store.NewMemory()
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Board{store: opts.Store, now: opts.Now, tenants: make(map[string]*tenant)}
}

// SetNow replaces the board's clock. It is for tests and the board's own
// start-up wiring, never for a request.
func (b *Board) SetNow(now Clock) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if now == nil {
		now = time.Now
	}
	b.now = now
}

// ensure returns a tenant, loading and folding its log the first time.
// The caller holds b.mu.
func (b *Board) ensure(tenantName string) (*tenant, error) {
	if tenantName == "" {
		tenantName = "default"
	}
	if existing, ok := b.tenants[tenantName]; ok {
		return existing, nil
	}
	records, err := b.store.Load(context.Background(), tenantName)
	if err != nil {
		return nil, err
	}
	state := NewState()
	events := make([]Event, 0, len(records))
	idem := make(map[string]idemEntry)
	// A keyed batch's metadata rides its first record; the posts it made are
	// the message events up to its last.
	active := ""
	for _, record := range records {
		event, err := decodeEvent(record)
		if err != nil {
			return nil, fmt.Errorf("log: replaying %s #%d: %w", record.Type, record.Seq, err)
		}
		state.Apply(event)
		events = append(events, event)
		if record.RequestKey != "" {
			last := record.RequestLast
			if last < record.Seq {
				last = record.Seq
			}
			idem[record.RequestKey] = idemEntry{digest: record.Digest, first: record.Seq, last: last}
			active = record.RequestKey
		}
		if active != "" {
			entry := idem[active]
			if posted, ok := event.Payload.(*MessagePostedPayload); ok && posted.ID == event.Seq {
				entry.messageIDs = append(entry.messageIDs, event.Seq)
				idem[active] = entry
			}
			if event.Seq >= entry.last {
				active = ""
			}
		}
	}
	created := &tenant{state: state, events: events, idem: idem, notify: make(chan struct{})}
	b.tenants[tenantName] = created
	return created, nil
}

// Update runs fn against a transaction and commits what it emits, atomically:
// either every emitted event lands or none does. The board is locked for the
// whole read-decide-append, so two writers can't both decide from the same
// state.
//
// With opts.Idempotency set, a key already seen with the same digest answers
// the first result and fn should return early after calling tx.Replay; a key
// seen with another digest is 409 duplicate_id before fn runs.
func (b *Board) Update(ctx context.Context, tenantName string, opts UpdateOptions, fn func(*Tx) error) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	current, err := b.ensure(tenantName)
	if err != nil {
		return err
	}
	tx := &Tx{
		board:  b,
		tenant: normalizeTenant(tenantName),
		snap:   &Snapshot{state: current.state, now: b.now()},
		head:   current.state.Head,
	}
	if opts.Idempotency != nil && opts.Idempotency.Key != "" {
		tx.idem = opts.Idempotency
		if entry, ok := current.idem[opts.Idempotency.Key]; ok {
			if entry.digest != opts.Idempotency.Digest {
				return protocol.NewError(protocol.CodeDuplicateID, "that id was already used for a different write")
			}
			tx.replay = &AppendResult{
				FirstSeq:   entry.first,
				LastSeq:    entry.last,
				Replayed:   true,
				MessageIDs: append([]uint64(nil), entry.messageIDs...),
			}
		}
	}
	if err := fn(tx); err != nil {
		return err
	}
	if tx.replay != nil {
		return nil
	}
	if len(tx.events) == 0 {
		return nil
	}
	records := make([]store.Record, len(tx.events))
	for i, event := range tx.events {
		payload, err := json.Marshal(event.Payload)
		if err != nil {
			return fmt.Errorf("log: encoding %s #%d: %w", event.Type, event.Seq, err)
		}
		records[i] = store.Record{Seq: event.Seq, Type: string(event.Type), Payload: payload, At: event.At}
	}
	if tx.idem != nil && len(records) > 0 {
		records[0].RequestKey = tx.idem.Key
		records[0].Digest = tx.idem.Digest
		records[0].RequestLast = tx.events[len(tx.events)-1].Seq
	}
	if err := b.store.Append(ctx, normalizeTenant(tenantName), records, tx.head); err != nil {
		if errors.Is(err, store.ErrHeadMoved) {
			return ErrHeadMoved
		}
		return err
	}
	for _, event := range tx.events {
		current.state.Apply(event)
		current.events = append(current.events, event)
	}
	if tx.idem != nil && tx.idem.Key != "" {
		current.idem[tx.idem.Key] = idemEntry{
			digest:     tx.idem.Digest,
			messageIDs: append([]uint64(nil), tx.results...),
			first:      tx.events[0].Seq,
			last:       tx.events[len(tx.events)-1].Seq,
		}
	}
	close(current.notify)
	current.notify = make(chan struct{})
	return nil
}

// UpdateOptions are a write's conditions.
type UpdateOptions struct {
	// Idempotency makes a retry answer the first write. Nil for no key.
	Idempotency *Idempotency
}

// Idempotency is a request's key and the digest of what it asks for. The same
// key with the same digest is a retry; with another digest it's 409
// duplicate_id.
type Idempotency struct {
	Key    string
	Digest string
}

// AppendResult says where a write landed, and what a replay answered.
type AppendResult struct {
	// FirstSeq and LastSeq bound the events the first write appended.
	FirstSeq uint64
	LastSeq  uint64
	// Replayed is true when the idempotency key had already landed.
	Replayed bool
	// MessageIDs are the posts the write made (or made the first time).
	MessageIDs []uint64
}

// AppendRequest is a conditional append.
type AppendRequest struct {
	// Tenant is the board. Empty means the default board.
	Tenant string
	// Channel is the channel the write lands in, for the log's own records.
	Channel string
	// Events are the facts to append. Their Seq is assigned here.
	Events []Event
	// RequestKey and Digest make the append idempotent. Empty key: no
	// idempotency.
	RequestKey string
	Digest     string
	// ExpectHead, when set, makes the append land only while the log's head
	// is still that value: a compare-and-swap.
	ExpectHead *uint64
}

// Append appends events conditionally. The same RequestKey with the same
// Digest answers the first result without appending; another digest is 409
// duplicate_id.
func (b *Board) Append(ctx context.Context, req AppendRequest) (AppendResult, error) {
	var result AppendResult
	err := b.Update(ctx, req.Tenant, UpdateOptions{Idempotency: idempotencyOf(req)}, func(tx *Tx) error {
		if replay, ok := tx.Replay(); ok {
			result = replay
			return nil
		}
		if req.ExpectHead != nil && tx.Head() != *req.ExpectHead {
			return ErrHeadMoved
		}
		for _, event := range req.Events {
			seq := tx.EmitRaw(event.Type, event.Payload)
			if posted, ok := event.Payload.(*MessagePostedPayload); ok {
				_ = posted
				tx.RecordResult(seq)
			}
		}
		result = tx.Result()
		return nil
	})
	return result, err
}

func idempotencyOf(req AppendRequest) *Idempotency {
	if req.RequestKey == "" {
		return nil
	}
	return &Idempotency{Key: req.RequestKey, Digest: req.Digest}
}

// Read runs fn against the folded board as it is now. No write can land while
// fn runs.
func (b *Board) Read(tenantName string, fn func(*Snapshot) error) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	current, err := b.ensure(tenantName)
	if err != nil {
		return err
	}
	return fn(&Snapshot{state: current.state, now: b.now()})
}

// Head returns a tenant's head: the seq of its latest event, 0 when empty.
func (b *Board) Head(tenantName string) (uint64, error) {
	var head uint64
	err := b.Read(tenantName, func(snapshot *Snapshot) error {
		head = snapshot.Head()
		return nil
	})
	return head, err
}

// History returns events with FirstSeq <= Seq <= LastSeq, in order.
func (b *Board) History(tenantName string, first, last uint64) ([]Event, error) {
	var out []Event
	b.mu.Lock()
	defer b.mu.Unlock()
	current, err := b.ensure(tenantName)
	if err != nil {
		return nil, err
	}
	for _, event := range current.events {
		if event.Seq < first {
			continue
		}
		if event.Seq > last {
			break
		}
		out = append(out, event)
	}
	return out, nil
}

// Wait blocks until the tenant's head moves past after, or ctx ends. It reads
// nothing, so a caller that wants the events calls History after it returns.
func (b *Board) Wait(ctx context.Context, tenantName string, after uint64) error {
	for {
		b.mu.Lock()
		current, err := b.ensure(tenantName)
		if err != nil {
			b.mu.Unlock()
			return err
		}
		if current.state.Head > after {
			b.mu.Unlock()
			return nil
		}
		notify := current.notify
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notify:
		}
	}
}

// SnapshotOf wraps a state as a read-only view. It is for a feed that folds
// events as it goes, so each event's message is as that event left it.
func SnapshotOf(state *State, now time.Time) *Snapshot {
	return &Snapshot{state: state, now: now}
}

// Snapshot is a read-only view of the folded board. A caller must not mutate
// a Message or Task it receives.
type Snapshot struct {
	state *State
	now   time.Time
}

// Head returns the board's latest seq.
func (s *Snapshot) Head() uint64 { return s.state.Head }

// Now returns the board's clock as this read saw it.
func (s *Snapshot) Now() time.Time { return s.now }

// Message returns a message by ID.
func (s *Snapshot) Message(id uint64) *Message { return s.state.Messages[id] }

// RootOf returns the thread's root for any message in it.
func (s *Snapshot) RootOf(id uint64) *Message {
	message := s.state.Messages[id]
	if message == nil {
		return nil
	}
	if message.Re == nil {
		return message
	}
	return s.state.Messages[*message.Re]
}

// Replies returns a thread root's replies, in log order, without tombstones.
func (s *Snapshot) Replies(root uint64) []*Message {
	ids := s.state.Replies[root]
	out := make([]*Message, 0, len(ids))
	for _, id := range ids {
		if message := s.state.Messages[id]; message != nil && !message.Deleted {
			out = append(out, message)
		}
	}
	return out
}

// AllReplies returns a thread root's replies, tombstones included.
func (s *Snapshot) AllReplies(root uint64) []*Message {
	ids := s.state.Replies[root]
	out := make([]*Message, 0, len(ids))
	for _, id := range ids {
		if message := s.state.Messages[id]; message != nil {
			out = append(out, message)
		}
	}
	return out
}

// MessagesInChannel returns a channel's messages, roots and replies, oldest
// first.
func (s *Snapshot) MessagesInChannel(channel string) []*Message {
	out := make([]*Message, 0)
	for _, message := range s.state.Messages {
		if message.Channel == channel {
			out = append(out, message)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Channel returns a channel by path.
func (s *Snapshot) Channel(path string) *Channel { return s.state.Channels[path] }

// Channels returns every channel, sorted by path.
func (s *Snapshot) Channels() []*Channel {
	out := make([]*Channel, 0, len(s.state.Channels))
	for _, channel := range s.state.Channels {
		out = append(out, channel)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

// Tasks returns every task's root, oldest first.
func (s *Snapshot) Tasks() []*Message {
	out := make([]*Message, 0)
	for _, message := range s.state.Messages {
		if message.Task != nil {
			out = append(out, message)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Actor returns an actor's record.
func (s *Snapshot) Actor(id string) *ActorRecord { return s.state.Actors[id] }

// ActorsByHandle returns the actor IDs that go by a handle, case-insensitive.
func (s *Snapshot) ActorsByHandle(handle string) []string {
	return append([]string(nil), s.state.ByHandle[lower(handle)]...)
}

// Tx is a write transaction. It reads the committed board and buffers facts
// to append; nothing it emits is visible to readers until it commits.
type Tx struct {
	board   *Board
	tenant  string
	snap    *Snapshot
	head    uint64
	events  []Event
	idem    *Idempotency
	replay  *AppendResult
	results []uint64
}

// Head returns the log's head as this transaction read it. Every event it
// emits lands after it.
func (tx *Tx) Head() uint64 { return tx.head }

// Now returns the transaction's time, fixed when it began.
func (tx *Tx) Now() time.Time { return tx.snap.now }

// NextID returns the seq the offset-th event to be emitted will have, where
// offset 0 is the first. It lets a write know a new root's ID before emitting
// its post.
func (tx *Tx) NextID(offset int) uint64 { return tx.head + uint64(offset) + 1 }

// Emit buffers an event and returns the seq it will land at. A message.posted
// payload's ID is filled in with that seq when it's zero.
func (tx *Tx) Emit(eventType EventType, payload any) uint64 {
	return tx.EmitRaw(eventType, payload)
}

// EmitRaw is Emit for a caller that already has an Event type.
func (tx *Tx) EmitRaw(eventType EventType, payload any) uint64 {
	seq := tx.head + uint64(len(tx.events)) + 1
	if posted, ok := payload.(*MessagePostedPayload); ok && posted.ID == 0 {
		posted.ID = seq
	}
	tx.events = append(tx.events, Event{Seq: seq, Type: eventType, Payload: payload, At: tx.snap.now})
	return seq
}

// RecordResult records a message ID the write made, so an idempotent replay
// can answer it.
func (tx *Tx) RecordResult(ids ...uint64) {
	tx.results = append(tx.results, ids...)
}

// Replay says whether the write's idempotency key already landed. When it did,
// the caller answers the result and emits nothing.
func (tx *Tx) Replay() (AppendResult, bool) {
	if tx.replay == nil {
		return AppendResult{}, false
	}
	return *tx.replay, true
}

// Result is the result the write will answer.
func (tx *Tx) Result() AppendResult {
	result := AppendResult{Replayed: tx.replay != nil, MessageIDs: append([]uint64(nil), tx.results...)}
	if tx.replay != nil {
		return *tx.replay
	}
	if len(tx.events) > 0 {
		result.FirstSeq = tx.events[0].Seq
		result.LastSeq = tx.events[len(tx.events)-1].Seq
	}
	return result
}

// Events returns the events this write will append, in order.
func (tx *Tx) Events() []Event { return append([]Event(nil), tx.events...) }

// Preview returns a message as this write will leave it, by folding the
// write's own events onto the committed message. A post this write makes is
// created; a write that changes nothing returns the committed message.
func (tx *Tx) Preview(id uint64) (*Message, bool) {
	mini := NewState()
	if base := tx.snap.Message(id); base != nil {
		mini.Messages[id] = CloneMessage(base)
		if base.Re != nil {
			mini.Messages[*base.Re] = CloneMessage(tx.snap.Message(*base.Re))
		}
	}
	for _, event := range tx.events {
		if ConcernedMessageID(event) != id {
			continue
		}
		mini.Apply(event)
	}
	message := mini.Messages[id]
	return message, message != nil
}

// Read gives the transaction the committed board, without its own events.
func (tx *Tx) Read() *Snapshot { return tx.snap }

func (tx *Tx) Message(id uint64) *Message            { return tx.snap.Message(id) }
func (tx *Tx) RootOf(id uint64) *Message             { return tx.snap.RootOf(id) }
func (tx *Tx) Replies(root uint64) []*Message        { return tx.snap.Replies(root) }
func (tx *Tx) Tasks() []*Message                     { return tx.snap.Tasks() }
func (tx *Tx) Channels() []*Channel                  { return tx.snap.Channels() }
func (tx *Tx) Channel(path string) *Channel          { return tx.snap.Channel(path) }
func (tx *Tx) Actor(id string) *ActorRecord          { return tx.snap.Actor(id) }
func (tx *Tx) ActorsByHandle(handle string) []string { return tx.snap.ActorsByHandle(handle) }

func normalizeTenant(name string) string {
	if name == "" {
		return "default"
	}
	return name
}

// decodeEvent rebuilds an event from its persisted record.
func decodeEvent(record store.Record) (Event, error) {
	event := Event{Seq: record.Seq, Type: EventType(record.Type), At: record.At}
	var payload any
	switch event.Type {
	case EventActorSeen:
		payload = &ActorSeenPayload{}
	case EventChannelCreated:
		payload = &ChannelCreatedPayload{}
	case EventMessagePosted:
		payload = &MessagePostedPayload{}
	case EventMessageEdited:
		payload = &MessageEditedPayload{}
	case EventMessageDeleted:
		payload = &MessageDeletedPayload{}
	case EventLabelsChanged:
		payload = &LabelsChangedPayload{}
	case EventTaskCreated:
		payload = &TaskCreatedPayload{}
	case EventTaskAssigned:
		payload = &TaskAssignedPayload{}
	case EventTaskNeedsChange:
		payload = &TaskNeedsChangedPayload{}
	case EventTaskReady:
		payload = &TaskReadyPayload{}
	case EventTaskClaimed:
		payload = &TaskClaimedPayload{}
	case EventTaskRenewed:
		payload = &TaskRenewedPayload{}
	case EventTaskReleased:
		payload = &TaskReleasedPayload{}
	case EventTaskExpired:
		payload = &TaskExpiredPayload{}
	case EventTaskCompleted:
		payload = &TaskCompletedPayload{}
	case EventTaskFailed:
		payload = &TaskFailedPayload{}
	case EventTaskBlocked:
		payload = &TaskBlockedPayload{}
	case EventTaskUnblocked:
		payload = &TaskUnblockedPayload{}
	case EventTaskCanceled:
		payload = &TaskCanceledPayload{}
	case EventTaskReopened:
		payload = &TaskReopenedPayload{}
	default:
		return Event{}, fmt.Errorf("unknown event type %q", record.Type)
	}
	if err := json.Unmarshal(record.Payload, payload); err != nil {
		return Event{}, err
	}
	event.Payload = payload
	return event, nil
}
