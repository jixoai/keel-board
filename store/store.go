// Package store keeps a board's raw log. It knows nothing about messages or
// tasks: an event's type and payload are opaque bytes here, and the log package
// above folds them. (Channel is a derived index column the log fills; the store
// never interprets it.)
//
// EventStore is the seam a PostgreSQL implementation plugs into. Any store must
// make an append all-or-nothing and must reject one whose expected head has
// moved, so the log's conditional writes stay conditional. A keyed append is
// idempotent at the store too: the same request key with the same digest
// answers the first result without writing, and another digest is refused, so
// two processes can't both land one request.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"
)

// ErrHeadMoved is returned by an append whose expected head isn't the store's
// current head anymore: another append landed first.
var ErrHeadMoved = errors.New("store: the log moved on")

// ErrDuplicateRequest is returned by an append whose request key already
// landed with another digest: 409 duplicate_id to the caller.
var ErrDuplicateRequest = errors.New("store: the request key was used for another write")

// HistoryDefaultLimit is what History reads when its limit is zero or less.
const HistoryDefaultLimit = 1000

// Record is one persisted event.
type Record struct {
	// Seq is the event's 1-based position in its tenant's log.
	Seq uint64
	// Type is the event's type, such as "message.posted".
	Type string
	// Channel is the channel the event concerns, when it concerns one. It is a
	// derived index column: the store never interprets it.
	Channel string
	// Payload is the event's JSON payload, as the log wrote it.
	Payload []byte
	// At is when the board recorded it.
	At time.Time
	// RequestKey, Digest, RequestLast, and RequestResult describe the
	// idempotent batch this record starts: its key, the digest of what it
	// asked for, the seq of the batch's last record, and the answer a retry
	// gets. They are set on the batch's first record only, so (tenant,
	// request_key) can be unique.
	RequestKey    string
	Digest        string
	RequestLast   uint64
	RequestResult []byte
}

// BatchResult is a keyed batch's answer: where it landed, and the posts it
// made. A retry answers it again.
type BatchResult struct {
	FirstSeq   uint64   `json:"firstSeq"`
	LastSeq    uint64   `json:"lastSeq"`
	MessageIDs []uint64 `json:"messageIds"`
}

// AppendResult says what an append did. Replayed is true when the request key
// had already landed with the same digest, in which case BatchResult is the
// first write's answer and nothing was written.
type AppendResult struct {
	Replayed bool
	BatchResult
}

// Request is a keyed append the store has already landed.
type Request struct {
	// Digest is what the first write asked for.
	Digest string
	// Result is the answer the first write gave.
	Result BatchResult
}

// EventStore persists a board's events.
type EventStore interface {
	// Load returns every record of a tenant's log, ordered by Seq. A tenant
	// with none returns an empty slice, not an error.
	Load(ctx context.Context, tenant string) ([]Record, error)
	// Append writes records atomically. It succeeds only while expectedHead
	// is the tenant's current head: seqs start at 1, so a tenant with no
	// records has head 0. It returns ErrHeadMoved otherwise. A batch whose
	// first record carries a RequestKey is idempotent: the same key with the
	// same digest returns the first result with Replayed set and writes
	// nothing; another digest returns ErrDuplicateRequest.
	Append(ctx context.Context, tenant string, records []Record, expectedHead uint64) (AppendResult, error)
}

// HistoryStore is an EventStore that can read a tenant's log in pages. It is
// how a process catches up with writes another process made without reading a
// whole log.
type HistoryStore interface {
	// History returns records with Seq > after, ordered by Seq, at most limit
	// of them. limit <= 0 uses HistoryDefaultLimit.
	History(ctx context.Context, tenant string, after uint64, limit int) ([]Record, error)
}

// Waiter is a store whose writes can arrive from another process: Wait blocks
// until the tenant's head passes after, or ctx ends. It never holds a database
// lock while it waits.
type Waiter interface {
	Wait(ctx context.Context, tenant string, after uint64) error
}

// RequestStore is a store that can answer an idempotent request it has already
// landed. A process whose cache predates the write uses it to answer a retry
// without appending.
type RequestStore interface {
	Request(ctx context.Context, tenant, key string) (Request, bool, error)
}

// Memory is an in-process EventStore for tests and development. It keeps every
// tenant's log in memory and is safe for concurrent use.
type Memory struct {
	mu       sync.Mutex
	byTenant map[string][]Record
	requests map[string]map[string]memoryRequest
	notify   map[string]chan struct{}
}

type memoryRequest struct {
	digest string
	result BatchResult
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory {
	return &Memory{
		byTenant: make(map[string][]Record),
		requests: make(map[string]map[string]memoryRequest),
		notify:   make(map[string]chan struct{}),
	}
}

// Load implements EventStore.
func (m *Memory) Load(_ context.Context, tenant string) ([]Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return copyRecords(m.byTenant[tenant]), nil
}

// Append implements EventStore.
func (m *Memory) Append(_ context.Context, tenant string, records []Record, expectedHead uint64) (AppendResult, error) {
	if len(records) == 0 {
		return AppendResult{}, nil
	}
	key, digest := requestOf(records)
	m.mu.Lock()
	defer m.mu.Unlock()
	if key != "" {
		if stored, ok := m.requests[tenant][key]; ok {
			if stored.digest != digest {
				return AppendResult{}, ErrDuplicateRequest
			}
			return AppendResult{Replayed: true, BatchResult: copyResult(stored.result)}, nil
		}
	}
	current := m.byTenant[tenant]
	if uint64(len(current)) != expectedHead {
		return AppendResult{}, ErrHeadMoved
	}
	for index, record := range records {
		if record.Seq != uint64(len(current))+uint64(index)+1 {
			return AppendResult{}, ErrHeadMoved
		}
	}
	for _, record := range records {
		record.Payload = append([]byte(nil), record.Payload...)
		record.RequestResult = append([]byte(nil), record.RequestResult...)
		current = append(current, record)
	}
	m.byTenant[tenant] = current
	result := batchResultOf(records)
	if key != "" {
		if m.requests[tenant] == nil {
			m.requests[tenant] = make(map[string]memoryRequest)
		}
		m.requests[tenant][key] = memoryRequest{digest: digest, result: copyResult(result)}
	}
	if channel, ok := m.notify[tenant]; ok {
		close(channel)
		delete(m.notify, tenant)
	}
	return AppendResult{BatchResult: result}, nil
}

// History implements HistoryStore.
func (m *Memory) History(_ context.Context, tenant string, after uint64, limit int) ([]Record, error) {
	if limit <= 0 {
		limit = HistoryDefaultLimit
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	records := m.byTenant[tenant]
	out := make([]Record, 0)
	for _, record := range records {
		if record.Seq <= after {
			continue
		}
		if len(out) == limit {
			break
		}
		out = append(out, record)
	}
	return copyRecords(out), nil
}

// Wait implements Waiter.
func (m *Memory) Wait(ctx context.Context, tenant string, after uint64) error {
	for {
		m.mu.Lock()
		if uint64(len(m.byTenant[tenant])) > after {
			m.mu.Unlock()
			return nil
		}
		notify, ok := m.notify[tenant]
		if !ok {
			notify = make(chan struct{})
			m.notify[tenant] = notify
		}
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notify:
		}
	}
}

// Request implements RequestStore.
func (m *Memory) Request(_ context.Context, tenant, key string) (Request, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	stored, ok := m.requests[tenant][key]
	if !ok {
		return Request{}, false, nil
	}
	return Request{Digest: stored.digest, Result: copyResult(stored.result)}, true, nil
}

// requestOf returns the key and digest a batch writes, if any. The log sets
// them on the batch's first record.
func requestOf(records []Record) (string, string) {
	if len(records) == 0 {
		return "", ""
	}
	return records[0].RequestKey, records[0].Digest
}

// batchResultOf computes a batch's answer from its records and the log's
// result note on the first one.
func batchResultOf(records []Record) BatchResult {
	result := BatchResult{FirstSeq: records[0].Seq, LastSeq: records[len(records)-1].Seq}
	if raw := records[0].RequestResult; len(raw) > 0 {
		var stored BatchResult
		if err := json.Unmarshal(raw, &stored); err != nil {
			return result
		}
		result.MessageIDs = append([]uint64(nil), stored.MessageIDs...)
	}
	return result
}

func copyResult(result BatchResult) BatchResult {
	result.MessageIDs = append([]uint64(nil), result.MessageIDs...)
	return result
}

func copyRecords(records []Record) []Record {
	out := make([]Record, len(records))
	for i, record := range records {
		out[i] = record
		out[i].Payload = append([]byte(nil), record.Payload...)
		out[i].RequestResult = append([]byte(nil), record.RequestResult...)
	}
	return out
}

var (
	_ EventStore   = (*Memory)(nil)
	_ HistoryStore = (*Memory)(nil)
	_ Waiter       = (*Memory)(nil)
	_ RequestStore = (*Memory)(nil)
)

// checkBatch verifies a batch's seqs start right after expectedHead and are
// contiguous. Stores call it before writing.
func checkBatch(records []Record, expectedHead uint64) error {
	if len(records) == 0 {
		return nil
	}
	if records[0].Seq != expectedHead+1 {
		return ErrHeadMoved
	}
	for index := 1; index < len(records); index++ {
		if records[index].Seq != records[index-1].Seq+1 {
			return ErrHeadMoved
		}
	}
	return nil
}
