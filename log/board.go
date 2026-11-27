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

// defaultFreshness bounds how long a shared store's cached head is trusted
// before a read asks the store. It is the staleness a multi-process board
// accepts for non-streaming reads; one indexed head lookup per tenant per
// window, not one per request.
const defaultFreshness = time.Second

// storeTimeout bounds one store read (a tenant's first load, a catch-up page,
// a head check). Writes use the caller's context.
const storeTimeout = 5 * time.Second

// Options configure a board.
type Options struct {
	// Store persists the raw log. Default: an in-memory store.
	Store store.EventStore
	// Now is the board's clock. Default: time.Now.
	Now Clock
	// Freshness bounds how stale a cached view of a store that implements
	// store.HeadStore may be. Default: time.Second. A negative value disables
	// the check (a board that owns its tenant's writes).
	Freshness time.Duration
}

// Board is one process's view of many tenants' logs. Reads are served from the
// cache; a store that reports a head (store.HeadStore, as PostgreSQL does) is
// asked at most once per Freshness window, so several processes sharing a log
// see each other's writes within that bound.
type Board struct {
	mu        sync.Mutex
	store     store.EventStore
	now       Clock
	freshness time.Duration
	tenants   map[string]*tenant
}

type tenant struct {
	state  *State
	events []Event
	idem   map[string]idemEntry
	// active is the keyed batch whose records are still being folded: a page
	// boundary can split one, so it carries across apply calls. A stored
	// RequestResult answers the batch, so it doesn't start an active scan.
	active string
	// checked is when the cache last compared itself with the store's head.
	checked time.Time
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
	if opts.Freshness == 0 {
		opts.Freshness = defaultFreshness
	}
	return &Board{
		store:     opts.Store,
		now:       opts.Now,
		freshness: opts.Freshness,
		tenants:   make(map[string]*tenant),
	}
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

// ensure returns a tenant, loading and folding its log the first time. A cold
// load runs outside b.mu, so one tenant's first read doesn't block another
// tenant's reads. The caller does not hold b.mu.
func (b *Board) ensure(ctx context.Context, tenantName string) (*tenant, error) {
	name := normalizeTenant(tenantName)
	b.mu.Lock()
	if existing, ok := b.tenants[name]; ok {
		b.mu.Unlock()
		return existing, nil
	}
	b.mu.Unlock()

	loadCtx, cancel := context.WithTimeout(ctx, storeTimeout)
	records, err := b.store.Load(loadCtx, name)
	cancel()
	if err != nil {
		return nil, err
	}
	created := newTenant()
	if err := created.apply(records); err != nil {
		return nil, err
	}
	created.checked = time.Now()

	b.mu.Lock()
	defer b.mu.Unlock()
	if existing, ok := b.tenants[name]; ok {
		// Another goroutine loaded this tenant first; its cache is as good.
		return existing, nil
	}
	b.tenants[name] = created
	return created, nil
}

// newTenant returns an empty tenant log.
func newTenant() *tenant {
	return &tenant{
		state:  NewState(),
		idem:   make(map[string]idemEntry),
		notify: make(chan struct{}),
	}
}

// apply folds records into the tenant's log, in seq order. It is idempotent
// for records already folded, so a catch-up that overlaps the head changes
// nothing. A keyed batch's metadata rides its first record; the posts it made
// are the message events up to its last.
func (t *tenant) apply(records []store.Record) error {
	changed := false
	for _, record := range records {
		if record.Seq <= t.state.Head {
			continue
		}
		if record.Seq != t.state.Head+1 {
			return fmt.Errorf("log: record #%d after head #%d: the log has a gap", record.Seq, t.state.Head)
		}
		event, err := decodeEvent(record)
		if err != nil {
			return fmt.Errorf("log: replaying %s #%d: %w", record.Type, record.Seq, err)
		}
		t.state.Apply(event)
		t.events = append(t.events, event)
		if record.RequestKey != "" {
			last := record.RequestLast
			if last < record.Seq {
				last = record.Seq
			}
			entry := idemEntry{digest: record.Digest, first: record.Seq, last: last}
			// A store may keep the batch's answer with the key. It names every
			// post the batch made, including a task adopted from an existing
			// thread, whose batch has no message.posted event to scan.
			answered := false
			if raw := record.RequestResult; len(raw) > 0 {
				var batch store.BatchResult
				if err := json.Unmarshal(raw, &batch); err != nil {
					return fmt.Errorf("log: replaying request %q: %w", record.RequestKey, err)
				}
				if batch.FirstSeq != 0 || batch.LastSeq != 0 || len(batch.MessageIDs) > 0 {
					answered = true
					if batch.FirstSeq != 0 {
						entry.first = batch.FirstSeq
					}
					if batch.LastSeq != 0 {
						entry.last = batch.LastSeq
					}
					entry.messageIDs = append([]uint64(nil), batch.MessageIDs...)
				}
			}
			t.idem[record.RequestKey] = entry
			if answered {
				t.active = ""
			} else {
				t.active = record.RequestKey
			}
		}
		if t.active != "" {
			entry := t.idem[t.active]
			if posted, ok := event.Payload.(*MessagePostedPayload); ok && posted.ID == event.Seq {
				if !containsMessageID(entry.messageIDs, event.Seq) {
					entry.messageIDs = append(entry.messageIDs, event.Seq)
					t.idem[t.active] = entry
				}
			}
			if event.Seq >= entry.last {
				t.active = ""
			}
		}
		changed = true
	}
	if changed {
		t.wake()
	}
	return nil
}

// wake releases everyone waiting on this tenant's log.
func (t *tenant) wake() {
	close(t.notify)
	t.notify = make(chan struct{})
}

// errRequestReplayed reports that the store held the write's request key
// although this process's cache did not: the cache was refreshed and the
// update must run once more, now seeing the key and replaying it.
var errRequestReplayed = errors.New("log: the store replayed the request")

// errStoreHeadMoved reports an append the store refused because another
// process moved the head: the cache was refreshed and fn must decide again.
var errStoreHeadMoved = errors.New("log: the store's head moved")

// Update runs fn against a transaction and commits what it emits, atomically:
// either every emitted event lands or none does. The board is locked for the
// whole read-decide-append, so two writers in this process can't both decide
// from the same state.
//
// With opts.Idempotency set, a key already seen with the same digest answers
// the first result and fn should return early after calling tx.Replay; a key
// seen with another digest is 409 duplicate_id before fn runs.
//
// A write can be decided again once when another process got there first: the
// cache is caught up and fn runs again against the fresh board (a keyed retry
// then takes its replay path). fn therefore must decide only from tx and must
// not have side effects outside it.
func (b *Board) Update(ctx context.Context, tenantName string, opts UpdateOptions, fn func(*Tx) error) error {
	current, err := b.ensure(ctx, tenantName)
	if err != nil {
		return err
	}
	replayed, moved := false, false
	for attempt := 0; attempt < 3; attempt++ {
		b.mu.Lock()
		err := b.update(ctx, tenantName, current, opts, fn)
		b.mu.Unlock()
		switch {
		case errors.Is(err, errRequestReplayed):
			if replayed {
				return fmt.Errorf("log: the store replayed a request the refreshed log doesn't know")
			}
			replayed = true
		case errors.Is(err, errStoreHeadMoved):
			if moved {
				return ErrHeadMoved
			}
			moved = true
		default:
			return err
		}
	}
	return fmt.Errorf("log: the store kept changing under the write")
}

// update is one read-decide-append. The caller holds b.mu and passes the
// tenant ensure returned. Update may call it again after a store-level race,
// so fn can run more than once.
func (b *Board) update(ctx context.Context, tenantName string, current *tenant, opts UpdateOptions, fn func(*Tx) error) error {
	tenant := normalizeTenant(tenantName)
	if err := b.refreshStaleLocked(ctx, tenant, current); err != nil {
		return err
	}
	tx := &Tx{
		board:  b,
		tenant: tenant,
		snap:   &Snapshot{state: current.state, now: b.now()},
		head:   current.state.Head,
	}
	if opts.Idempotency != nil && opts.Idempotency.Key != "" {
		tx.idem = opts.Idempotency
		if entry, ok := current.idem[opts.Idempotency.Key]; ok {
			if entry.digest != opts.Idempotency.Digest {
				return duplicateIDError()
			}
			// A cache entry from before request results were stored may lack
			// the message IDs; ask the store before answering a retry.
			if len(entry.messageIDs) == 0 {
				if looker, ok := b.store.(store.RequestStore); ok {
					stored, found, err := looker.Request(ctx, tenant, opts.Idempotency.Key)
					if err != nil {
						return err
					}
					if found {
						if stored.Digest != opts.Idempotency.Digest {
							return duplicateIDError()
						}
						mergeBatch(&entry, stored.Result)
						current.idem[opts.Idempotency.Key] = entry
					}
				}
			}
			tx.replay = replayOf(entry)
		} else if looker, ok := b.store.(store.RequestStore); ok {
			// The store may know a write this process's cache predates.
			stored, found, err := looker.Request(ctx, tenant, opts.Idempotency.Key)
			if err != nil {
				return err
			}
			if found {
				if stored.Digest != opts.Idempotency.Digest {
					return duplicateIDError()
				}
				if err := b.refreshLocked(ctx, tenant, current); err != nil {
					return err
				}
				entry, ok := current.idem[opts.Idempotency.Key]
				if !ok {
					entry = idemEntry{digest: stored.Digest}
				}
				mergeBatch(&entry, stored.Result)
				current.idem[opts.Idempotency.Key] = entry
				tx.replay = replayOf(entry)
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
		records[i] = store.Record{
			Seq:     event.Seq,
			Type:    string(event.Type),
			Channel: channelOf(tx, event),
			Payload: payload,
			At:      event.At,
		}
	}
	result := tx.Result()
	if tx.idem != nil {
		records[0].RequestKey = tx.idem.Key
		records[0].Digest = tx.idem.Digest
		records[0].RequestLast = result.LastSeq
		raw, err := json.Marshal(store.BatchResult{
			FirstSeq:   result.FirstSeq,
			LastSeq:    result.LastSeq,
			MessageIDs: result.MessageIDs,
		})
		if err != nil {
			return fmt.Errorf("log: encoding request %q: %w", tx.idem.Key, err)
		}
		records[0].RequestResult = raw
	}
	outcome, err := b.store.Append(ctx, tenant, records, tx.head)
	switch {
	case err == nil:
	case errors.Is(err, store.ErrDuplicateRequest):
		return duplicateIDError()
	case errors.Is(err, store.ErrHeadMoved):
		// Another process moved the head while this write decided. Catch the
		// cache up and let Update run fn once more against the fresh board.
		if refreshErr := b.refreshLocked(ctx, tenant, current); refreshErr != nil {
			return refreshErr
		}
		return errStoreHeadMoved
	default:
		return err
	}
	if outcome.Replayed {
		// Another process landed the key between the lookup and the append.
		// Fold what the store has, keep its answer, and retry so fn replays.
		if err := b.refreshLocked(ctx, tenant, current); err != nil {
			return err
		}
		if tx.idem != nil {
			entry, ok := current.idem[tx.idem.Key]
			if !ok {
				entry = idemEntry{digest: tx.idem.Digest}
			}
			mergeBatch(&entry, outcome.BatchResult)
			current.idem[tx.idem.Key] = entry
		}
		return errRequestReplayed
	}
	for _, event := range tx.events {
		current.state.Apply(event)
		current.events = append(current.events, event)
	}
	if tx.idem != nil {
		current.idem[tx.idem.Key] = idemEntry{
			digest:     tx.idem.Digest,
			messageIDs: append([]uint64(nil), tx.results...),
			first:      result.FirstSeq,
			last:       result.LastSeq,
		}
	}
	current.checked = time.Now()
	current.wake()
	return nil
}

// mergeBatch fills a cache entry's missing answer from a batch result.
func mergeBatch(entry *idemEntry, batch store.BatchResult) {
	if len(entry.messageIDs) == 0 && len(batch.MessageIDs) > 0 {
		entry.messageIDs = append([]uint64(nil), batch.MessageIDs...)
	}
	if entry.first == 0 {
		entry.first = batch.FirstSeq
	}
	if entry.last == 0 {
		entry.last = batch.LastSeq
	}
}

// containsMessageID reports whether ids already names want.
func containsMessageID(ids []uint64, want uint64) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

// duplicateIDError is the 409 an idempotency key reused for another write
// answers.
func duplicateIDError() *protocol.Error {
	return protocol.NewError(protocol.CodeDuplicateID, "that id was already used for a different write")
}

// replayOf turns a landed request's entry into the answer a retry gets.
func replayOf(entry idemEntry) *AppendResult {
	return &AppendResult{
		FirstSeq:   entry.first,
		LastSeq:    entry.last,
		Replayed:   true,
		Digest:     entry.digest,
		MessageIDs: append([]uint64(nil), entry.messageIDs...),
	}
}

// channelOf is the channel an event concerns, when it concerns one. Message
// and channel events name it; a task event names its root, which this write
// may have posted earlier in the same batch.
func channelOf(tx *Tx, event Event) string {
	switch payload := event.Payload.(type) {
	case *MessagePostedPayload:
		return payload.Channel
	case *ChannelCreatedPayload:
		return payload.Channel
	}
	id := ConcernedMessageID(event)
	if id == 0 {
		return ""
	}
	for index := len(tx.events) - 1; index >= 0; index-- {
		if posted, ok := tx.events[index].Payload.(*MessagePostedPayload); ok && posted.ID == id {
			return posted.Channel
		}
	}
	if message := tx.snap.Message(id); message != nil {
		return message.Channel
	}
	return ""
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
	// Digest is what the keyed write asked for, as its key recorded it.
	Digest string
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
// fn runs. Store reads are bounded by storeTimeout.
func (b *Board) Read(tenantName string, fn func(*Snapshot) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), storeTimeout)
	defer cancel()
	return b.ReadContext(ctx, tenantName, fn)
}

// ReadContext is Read with a caller's context. A shared store's head is checked
// at most once per Freshness window, so a read sees another process's writes
// within that bound.
func (b *Board) ReadContext(ctx context.Context, tenantName string, fn func(*Snapshot) error) error {
	current, err := b.ensure(ctx, tenantName)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.refreshStaleLocked(ctx, normalizeTenant(tenantName), current); err != nil {
		return err
	}
	return fn(&Snapshot{state: current.state, now: b.now()})
}

// Head returns a tenant's head: the seq of its latest event, 0 when empty.
func (b *Board) Head(tenantName string) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), storeTimeout)
	defer cancel()
	current, err := b.ensure(ctx, tenantName)
	if err != nil {
		return 0, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.refreshStaleLocked(ctx, normalizeTenant(tenantName), current); err != nil {
		return 0, err
	}
	return current.state.Head, nil
}

// ReplayKey answers what an idempotent write under key landed, for an early
// read that tells a retry from a new write without writing. It refreshes the
// tenant from the store and asks a store that remembers requests, so a cold
// cache or another process's write answers the same result; it is not a map
// lookup alone.
func (b *Board) ReplayKey(ctx context.Context, tenantName, key string) (AppendResult, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, storeTimeout)
		defer cancel()
	}
	current, err := b.ensure(ctx, tenantName)
	if err != nil {
		return AppendResult{}, false, err
	}
	tenant := normalizeTenant(tenantName)
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.refreshNowLocked(ctx, tenant, current); err != nil {
		return AppendResult{}, false, err
	}
	entry, entryFound := current.idem[key]
	if entryFound && len(entry.messageIDs) > 0 {
		return appendResultOf(entry), true, nil
	}
	// A cache entry may lack its answer; so may a cache that predates the
	// write. The store remembers both.
	if looker, ok := b.store.(store.RequestStore); ok {
		stored, found, err := looker.Request(ctx, tenant, key)
		if err != nil {
			return AppendResult{}, false, err
		}
		if found {
			if !entryFound {
				entry = idemEntry{digest: stored.Digest}
			}
			if entry.digest == "" {
				entry.digest = stored.Digest
			}
			mergeBatch(&entry, stored.Result)
			current.idem[key] = entry
			return appendResultOf(entry), true, nil
		}
	}
	if entryFound {
		return appendResultOf(entry), true, nil
	}
	return AppendResult{}, false, nil
}

// appendResultOf turns a cache entry into the answer a retry gets.
func appendResultOf(entry idemEntry) AppendResult {
	return AppendResult{
		FirstSeq:   entry.first,
		LastSeq:    entry.last,
		Replayed:   true,
		Digest:     entry.digest,
		MessageIDs: append([]uint64(nil), entry.messageIDs...),
	}
}

// History returns events with FirstSeq <= Seq <= LastSeq, in order.
func (b *Board) History(tenantName string, first, last uint64) ([]Event, error) {
	ctx, cancel := context.WithTimeout(context.Background(), storeTimeout)
	defer cancel()
	current, err := b.ensure(ctx, tenantName)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.refreshStaleLocked(ctx, normalizeTenant(tenantName), current); err != nil {
		return nil, err
	}
	var out []Event
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
//
// A store another process may write to (store.Waiter) is asked to wait too;
// when it reports a change this board catches up from the store first, so a
// PostgreSQL-backed board's feed sees other processes' writes. The store's
// wait never holds a write lock.
func (b *Board) Wait(ctx context.Context, tenantName string, after uint64) error {
	tenant := normalizeTenant(tenantName)
	current, err := b.ensure(ctx, tenantName)
	if err != nil {
		return err
	}
	for {
		b.mu.Lock()
		if current.state.Head > after {
			b.mu.Unlock()
			return nil
		}
		notify := current.notify
		head := current.state.Head
		b.mu.Unlock()

		if waiter, ok := b.store.(store.Waiter); ok {
			if err := waiter.Wait(ctx, tenant, head); err != nil {
				return err
			}
			b.mu.Lock()
			err := b.refreshLocked(ctx, tenant, current)
			b.mu.Unlock()
			if err != nil {
				return err
			}
			continue
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notify:
		}
	}
}

// Refresh makes the cache compare with the store now, ignoring the freshness
// window. A feed calls it before reading its cursor: a client's Last-Event-ID
// must not be clamped below events the store already holds just because this
// process's cache is a little behind.
func (b *Board) Refresh(ctx context.Context, tenantName string) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, storeTimeout)
		defer cancel()
	}
	current, err := b.ensure(ctx, tenantName)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.refreshNowLocked(ctx, normalizeTenant(tenantName), current)
}

// refreshStaleLocked refreshes a tenant when its cache is older than the
// freshness window. A store without a head check is left alone: its writes are
// this process's own.
func (b *Board) refreshStaleLocked(ctx context.Context, tenant string, current *tenant) error {
	if b.freshness < 0 {
		return nil
	}
	_, hasHead := b.store.(store.HeadStore)
	if !hasHead {
		if _, ok := b.store.(store.IntegrityStore); !ok {
			return nil
		}
	}
	if time.Since(current.checked) < b.freshness {
		return nil
	}
	return b.refreshNowLocked(ctx, tenant, current)
}

// refreshNowLocked checks the store's head and folds anything past the cache.
// The caller holds b.mu.
func (b *Board) refreshNowLocked(ctx context.Context, tenant string, current *tenant) error {
	head, hasHead := uint64(0), false
	if header, ok := b.store.(store.HeadStore); ok {
		value, err := header.Head(ctx, tenant)
		if err != nil {
			return err
		}
		head, hasHead = value, true
	} else if checker, ok := b.store.(store.IntegrityStore); ok {
		integrity, err := checker.Integrity(ctx, tenant)
		if err != nil {
			return err
		}
		head, hasHead = integrity.Head, true
	}
	if hasHead {
		if head < current.state.Head {
			return fmt.Errorf("%w: projection at #%d, the log's head row at #%d", ErrProjectionAhead, current.state.Head, head)
		}
		if head == current.state.Head {
			current.checked = time.Now()
			return nil
		}
	}
	return b.refreshLocked(ctx, tenant, current)
}

// refreshLocked folds records the store has gained since the tenant's head: a
// write by another process, or a key this process hadn't loaded. The caller
// holds b.mu.
func (b *Board) refreshLocked(ctx context.Context, tenant string, current *tenant) (err error) {
	defer func() {
		if err == nil {
			current.checked = time.Now()
		}
	}()
	const page = 1000
	for {
		records, readErr := b.recordsAfter(ctx, tenant, current.state.Head, page)
		if readErr != nil {
			return readErr
		}
		if len(records) == 0 {
			return nil
		}
		if applyErr := current.apply(records); applyErr != nil {
			return applyErr
		}
		if len(records) < page {
			return nil
		}
	}
}

// recordsAfter reads a tenant's records after seq, a page at a time. A store
// that can page does; one that can't is read whole.
func (b *Board) recordsAfter(ctx context.Context, tenant string, after uint64, limit int) ([]store.Record, error) {
	if historian, ok := b.store.(store.HistoryStore); ok {
		return historian.History(ctx, tenant, after, limit)
	}
	records, err := b.store.Load(ctx, tenant)
	if err != nil {
		return nil, err
	}
	out := make([]store.Record, 0, limit)
	for _, record := range records {
		if record.Seq <= after {
			continue
		}
		if len(out) == limit {
			break
		}
		out = append(out, record)
	}
	return out, nil
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

// AllMessages returns every message, roots and replies, oldest first.
func (s *Snapshot) AllMessages() []*Message {
	out := make([]*Message, 0, len(s.state.Messages))
	for _, message := range s.state.Messages {
		out = append(out, message)
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
// payload with no ID is copied with the event's seq filled in; the caller's
// payload is never mutated, so a refused append leaves its request as it was.
func (tx *Tx) Emit(eventType EventType, payload any) uint64 {
	return tx.EmitRaw(eventType, payload)
}

// EmitRaw is Emit for a caller that already has an Event type.
func (tx *Tx) EmitRaw(eventType EventType, payload any) uint64 {
	seq := tx.head + uint64(len(tx.events)) + 1
	if posted, ok := payload.(*MessagePostedPayload); ok && posted.ID == 0 {
		copy := *posted
		copy.ID = seq
		payload = &copy
	}
	tx.events = append(tx.events, Event{
		Seq: seq, Type: eventType, Payload: payload,
		// The stores keep microseconds; a live fold and a replay agree.
		At: tx.snap.now.Truncate(time.Microsecond),
	})
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
	if tx.idem != nil {
		result.Digest = tx.idem.Digest
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
	case EventReactionAdded, EventReactionRemoved:
		payload = &ReactionPayload{}
	case EventLabelsChanged:
		payload = &LabelsChangedPayload{}
	case EventThreadSynced:
		payload = &ThreadSyncedPayload{}
	case EventMessageSynced:
		payload = &MessageSyncedPayload{}
	case EventMessageFilesSynced:
		payload = &MessageFilesSyncedPayload{}
	case EventMessageSyncFailed:
		payload = &MessageSyncFailedPayload{}
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
		// An event kind this build doesn't know stays in the log for the
		// version that does; folding skips it.
		event.Payload = append(json.RawMessage(nil), record.Payload...)
		return event, nil
	}
	if err := json.Unmarshal(record.Payload, payload); err != nil {
		return Event{}, err
	}
	event.Payload = payload
	return event, nil
}
