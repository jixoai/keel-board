package log

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jixoai/keel-board/store"
)

// ErrProjectionAhead reports a cached projection whose head is past the
// durable log's: events can't disappear, so the cache is corrupt. Writes and
// reads fail instead of serving it; Rebuild replaces it from the log.
var ErrProjectionAhead = errors.New("log: the projection is ahead of the durable log")

// ErrLogInconsistent reports a store whose head row and event log disagree.
// It is the store's error, re-exported so callers can test one sentinel.
var ErrLogInconsistent = store.ErrLogInconsistent

// ProjectionReport is one tenant's projection check: the durable log it read,
// the projection's watermark, and what a full replay folds. Consistent is true
// only when the projection is at the log's head and hashes to the same value.
type ProjectionReport struct {
	Tenant string `json:"tenant"`
	// LogHead is the newest event seq the check read.
	LogHead uint64 `json:"logHead"`
	// HeadRow is the store's durable head. Stores without a separate head row
	// report LogHead.
	HeadRow uint64 `json:"headRow"`
	// ProjectionHead is the cached projection's watermark.
	ProjectionHead uint64 `json:"projectionHead"`
	// Digest hashes the cached projection; ReplayDigest hashes a replay of the
	// log up to ProjectionHead; StoreDigest hashes the projection of the whole
	// log.
	Digest       string `json:"digest"`
	ReplayDigest string `json:"replayDigest"`
	StoreDigest  string `json:"storeDigest"`
	// Consistent reports whether the projection equals the full replay at the
	// log's head.
	Consistent bool `json:"consistent"`
	// Problems says what is wrong, in a stable order, when it isn't.
	Problems []string `json:"problems,omitempty"`
}

// ProjectionDigest returns a stable SHA-256 over a folded board: every
// message, reply, channel, actor, task, label, claim, attachment, origin and
// sync fact, plus the head. Two states with the same content hash the same
// regardless of map iteration order.
func ProjectionDigest(state *State) (string, error) {
	if state == nil {
		return "", errors.New("log: no projection to hash")
	}
	doc := projectionDocument{Head: state.Head}

	ids := make([]uint64, 0, len(state.Messages))
	for id := range state.Messages {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	doc.Messages = make([]*Message, 0, len(ids))
	for _, id := range ids {
		doc.Messages = append(doc.Messages, canonicalMessage(state.Messages[id]))
	}

	roots := make([]uint64, 0, len(state.Replies))
	for root := range state.Replies {
		roots = append(roots, root)
	}
	sort.Slice(roots, func(i, j int) bool { return roots[i] < roots[j] })
	doc.Replies = make([]projectionReplies, 0, len(roots))
	for _, root := range roots {
		doc.Replies = append(doc.Replies, projectionReplies{
			Root:    root,
			Replies: append([]uint64(nil), state.Replies[root]...),
		})
	}

	paths := make([]string, 0, len(state.Channels))
	for path := range state.Channels {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	doc.Channels = make([]*Channel, 0, len(paths))
	for _, path := range paths {
		doc.Channels = append(doc.Channels, state.Channels[path])
	}

	actors := make([]string, 0, len(state.Actors))
	for id := range state.Actors {
		actors = append(actors, id)
	}
	sort.Strings(actors)
	doc.Actors = make([]*ActorRecord, 0, len(actors))
	for _, id := range actors {
		doc.Actors = append(doc.Actors, state.Actors[id])
	}

	doc.ByHandle = make(map[string][]string, len(state.ByHandle))
	for handle, ids := range state.ByHandle {
		doc.ByHandle[handle] = append([]string(nil), ids...)
	}

	encoded, err := json.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("log: hashing the projection: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// projectionDocument is the canonical projection: collections are sorted, so
// encoding/json's map ordering never decides a digest.
type projectionDocument struct {
	Head     uint64              `json:"head"`
	Messages []*Message          `json:"messages"`
	Replies  []projectionReplies `json:"replies"`
	Channels []*Channel          `json:"channels"`
	Actors   []*ActorRecord      `json:"actors"`
	ByHandle map[string][]string `json:"byHandle"`
}

type projectionReplies struct {
	Root    uint64   `json:"root"`
	Replies []uint64 `json:"replies"`
}

// canonicalMessage copies a message and puts its times in UTC. A PostgreSQL
// replay may hand back the same instant in a local location; the digest must
// not care.
func canonicalMessage(message *Message) *Message {
	if message == nil {
		return nil
	}
	copy := CloneMessage(message)
	copy.CreatedAt = copy.CreatedAt.UTC()
	if copy.EditedAt != nil {
		edited := copy.EditedAt.UTC()
		copy.EditedAt = &edited
	}
	if copy.Task != nil && copy.Task.Claim != nil {
		claim := *copy.Task.Claim
		claim.Until = claim.Until.UTC()
		copy.Task.Claim = &claim
	}
	return copy
}

// CheckProjection compares the cached projection with a full replay of the
// durable log. It does not catch the cache up or repair it: a projection that
// lags is reported as inconsistent, and Rebuild replaces it. A store failure is
// an error; a corrupt projection or log is a report.
func (b *Board) CheckProjection(ctx context.Context, tenantName string) (ProjectionReport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	current, err := b.ensure(ctx, tenantName)
	if err != nil {
		return ProjectionReport{}, err
	}
	tenant := normalizeTenant(tenantName)
	b.mu.Lock()
	defer b.mu.Unlock()

	records, err := b.store.Load(ctx, tenant)
	if err != nil {
		return ProjectionReport{}, err
	}
	integrity, err := b.integrityLocked(ctx, tenant)
	if err != nil {
		return ProjectionReport{}, err
	}
	return analyzeProjection(tenant, current.state, records, integrity), nil
}

// projectionRebuildTimeout bounds one rebuild's store reads and catch-up, so
// a hung store can't hold the board's lock indefinitely.
const projectionRebuildTimeout = 5 * time.Minute

// Rebuild replaces a tenant's projection with a replay of its durable log. It
// loads and folds the log outside the board lock, then takes the lock only to
// catch up what landed meanwhile, verify the log, and swap. A failed or
// interrupted rebuild leaves the old projection serving; a retry is safe.
// Events are never written or deleted, a repeated rebuild is idempotent, and
// the cache's pointer is kept so feeds waiting on it wake to the new state.
func (b *Board) Rebuild(ctx context.Context, tenantName string) (ProjectionReport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	// Report every completed attempt (successful or not) after the board lock
	// the function may hold is released: the deferred unlock runs first. A
	// panicking observer is recovered; instrumentation never fails a rebuild.
	started := time.Now()
	defer func() {
		pointer := b.observer.Load()
		if pointer == nil {
			return
		}
		defer func() { _ = recover() }()
		(*pointer).Rebuilt(time.Since(started))
	}()
	ctx, cancel := context.WithTimeout(ctx, projectionRebuildTimeout)
	defer cancel()
	tenant := normalizeTenant(tenantName)

	records, err := b.store.Load(ctx, tenant)
	if err != nil {
		return ProjectionReport{}, err
	}
	rebuilt := newTenant()
	if err := rebuilt.apply(records); err != nil {
		problem := fmt.Sprintf("the log does not replay: %v", err)
		return ProjectionReport{Tenant: tenant, Problems: []string{problem}},
			fmt.Errorf("%w: %s", ErrLogInconsistent, problem)
	}
	// Hash once before taking the lock: a projection that can't be hashed is
	// not a rebuild we want to serve.
	if _, err := ProjectionDigest(rebuilt.state); err != nil {
		return ProjectionReport{}, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	current := b.tenants[tenant]
	if current == nil {
		current = newTenant()
		b.tenants[tenant] = current
	}

	integrity, err := b.integrityLocked(ctx, tenant)
	if err != nil {
		return ProjectionReport{}, err
	}
	if integrity != nil && !integrity.Consistent() {
		// The durable log itself is broken. A replay would silently leave out
		// an event the head row names, so keep the old projection serving and
		// report the log failure; only restoring the log or its head can fix
		// it.
		report := analyzeProjection(tenant, current.state, records, integrity)
		return report, fmt.Errorf("%w: %s", ErrLogInconsistent, strings.Join(report.Problems, "; "))
	}

	// Writes in this process were blocked while the log was loaded; catch up
	// those (and any other process's) events before swapping. The target is
	// fixed by the integrity read, so sustained writers can't keep this loop
	// going past one observed head.
	target := ^uint64(0)
	if integrity != nil {
		target = integrity.MaxSeq
	}
	tail, err := b.catchUpLocked(ctx, tenant, rebuilt, target)
	if err != nil {
		if errors.Is(err, ErrLogInconsistent) {
			return ProjectionReport{Tenant: tenant, Problems: []string{err.Error()}}, err
		}
		return ProjectionReport{}, err
	}
	records = append(records, tail...)
	if integrity != nil && rebuilt.state.Head < integrity.MaxSeq {
		report := analyzeProjection(tenant, current.state, records, integrity)
		return report, fmt.Errorf("%w: the log ends at #%d but the head row names #%d",
			ErrLogInconsistent, rebuilt.state.Head, integrity.MaxSeq)
	}

	digest, err := ProjectionDigest(rebuilt.state)
	if err != nil {
		return ProjectionReport{}, err
	}
	current.state = rebuilt.state
	current.events = rebuilt.events
	current.idem = rebuilt.idem
	current.active = rebuilt.active
	// Force the next read to compare with the store: an event another process
	// wrote after the integrity read is caught up rather than hidden by
	// freshness.
	current.checked = time.Time{}
	current.wake()

	return rebuiltReport(tenant, current.state, records, integrity, digest), nil
}

// catchUpLocked folds events after a rebuilt state's head, through target
// (^uint64(0) means until the store has no more), and returns the records it
// folded. The caller holds the board lock; target bounds the work.
func (b *Board) catchUpLocked(ctx context.Context, tenant string, rebuilt *tenant, target uint64) ([]store.Record, error) {
	const page = 1000
	var tail []store.Record
	for rebuilt.state.Head < target {
		records, err := b.recordsAfter(ctx, tenant, rebuilt.state.Head, page)
		if err != nil {
			return tail, err
		}
		if len(records) == 0 {
			return tail, nil
		}
		if err := rebuilt.apply(records); err != nil {
			return tail, fmt.Errorf("%w: the log does not replay: %v", ErrLogInconsistent, err)
		}
		tail = append(tail, records...)
		if len(records) < page {
			return tail, nil
		}
	}
	return tail, nil
}

// rebuiltReport builds a rebuild's report from the state just installed. The
// replay, the projection and the whole log are the same state, so one digest
// covers all three; validateLog still checks the records are contiguous.
func rebuiltReport(tenant string, state *State, records []store.Record, integrity *store.Integrity, digest string) ProjectionReport {
	report := ProjectionReport{
		Tenant:         tenant,
		LogHead:        state.Head,
		HeadRow:        state.Head,
		ProjectionHead: state.Head,
		Digest:         digest,
		ReplayDigest:   digest,
		StoreDigest:    digest,
	}
	addProblem := func(format string, args ...any) {
		report.Problems = append(report.Problems, fmt.Sprintf(format, args...))
	}

	logHead, logErr := validateLog(records)
	report.LogHead = logHead
	if logErr != nil {
		addProblem("%v", logErr)
	}
	if integrity != nil {
		report.HeadRow = integrity.Head
		if !integrity.Consistent() {
			addProblem("the store's head row is #%d but its newest event is #%d", integrity.Head, integrity.MaxSeq)
		}
	}
	if state.Head != logHead {
		addProblem("the rebuilt projection is at #%d but the log ends at #%d", state.Head, logHead)
	}
	report.Consistent = len(report.Problems) == 0
	return report
}

// integrityLocked reads the store's durable head facts, when it reports them.
func (b *Board) integrityLocked(ctx context.Context, tenant string) (*store.Integrity, error) {
	if checker, ok := b.store.(store.IntegrityStore); ok {
		integrity, err := checker.Integrity(ctx, tenant)
		if err != nil {
			return nil, err
		}
		return &integrity, nil
	}
	if header, ok := b.store.(store.HeadStore); ok {
		head, err := header.Head(ctx, tenant)
		if err != nil {
			return nil, err
		}
		// A head-only store has no separate log-end query, so its head is the
		// best end it can report. Leaving MaxSeq zero would claim every
		// non-empty log is corrupt.
		return &store.Integrity{Head: head, MaxSeq: head}, nil
	}
	return nil, nil
}

// analyzeProjection builds a report for one projection and log snapshot.
func analyzeProjection(tenant string, state *State, records []store.Record, integrity *store.Integrity) ProjectionReport {
	report := ProjectionReport{Tenant: tenant, ProjectionHead: state.Head}
	addProblem := func(format string, args ...any) {
		report.Problems = append(report.Problems, fmt.Sprintf(format, args...))
	}

	logHead, logErr := validateLog(records)
	report.LogHead = logHead
	if logErr != nil {
		addProblem("%v", logErr)
	}

	if integrity != nil {
		report.HeadRow = integrity.Head
		if !integrity.Consistent() {
			addProblem("the store's head row is #%d but its newest event is #%d", integrity.Head, integrity.MaxSeq)
		}
		if integrity.MaxSeq > logHead {
			addProblem("the log advanced during the check: read #%d, the store reports #%d", logHead, integrity.MaxSeq)
		}
	} else {
		report.HeadRow = logHead
	}

	if digest, err := ProjectionDigest(state); err != nil {
		addProblem("%v", err)
	} else {
		report.Digest = digest
	}

	full, fullErr := foldRecords(records, ^uint64(0))
	if fullErr != nil {
		addProblem("the log does not replay: %v", fullErr)
	} else if digest, err := ProjectionDigest(full); err != nil {
		addProblem("%v", err)
	} else {
		report.StoreDigest = digest
	}

	switch {
	case state.Head > logHead:
		addProblem("the projection is ahead of the log: #%d vs #%d", state.Head, logHead)
	case state.Head < logHead:
		addProblem("the projection lags the log by %d event(s): #%d vs #%d", logHead-state.Head, state.Head, logHead)
	}
	if state.Head <= logHead && state.Head <= uint64(len(records)) {
		prefix, err := foldRecords(records, state.Head)
		if err != nil {
			addProblem("the log up to #%d does not replay: %v", state.Head, err)
		} else if digest, err := ProjectionDigest(prefix); err != nil {
			addProblem("%v", err)
		} else {
			report.ReplayDigest = digest
			if report.Digest != "" && report.Digest != digest {
				addProblem("the projection digest %s differs from the replay digest %s", report.Digest, digest)
			}
		}
	}

	report.Consistent = len(report.Problems) == 0
	return report
}

// validateLog checks a loaded log's seqs are contiguous from 1 and returns its
// head, which is the last seq that was contiguous.
func validateLog(records []store.Record) (uint64, error) {
	expected := uint64(1)
	for _, record := range records {
		if record.Seq != expected {
			return expected - 1, fmt.Errorf("log: event #%d follows #%d: the log has a gap", record.Seq, expected-1)
		}
		expected++
	}
	return expected - 1, nil
}

// foldRecords replays a loaded log into a fresh state, through seq at most
// through. It refuses a gap or an undecodable known event; an unknown kind
// stays in the log and folds away.
func foldRecords(records []store.Record, through uint64) (*State, error) {
	state := NewState()
	for _, record := range records {
		if record.Seq > through {
			break
		}
		if record.Seq != state.Head+1 {
			return nil, fmt.Errorf("log: event #%d follows #%d: the log has a gap", record.Seq, state.Head)
		}
		event, err := decodeEvent(record)
		if err != nil {
			return nil, fmt.Errorf("replaying %s #%d: %w", record.Type, record.Seq, err)
		}
		state.Apply(event)
	}
	return state, nil
}
