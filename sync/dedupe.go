package sync

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"strings"
	"sync"
	"time"
)

// MaxDedupeLen is the longest event identity a platform may produce; upstream
// inbox IDs are capped at 300 characters.
const MaxDedupeLen = 300

// Hash returns the base64url SHA-256 of s, without padding. It is what an
// identity uses when a text is longer than an ID may be.
func Hash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// EscapePart escapes a part of a composite identity, so "|" and "\" in an ID
// can't make two identities read the same.
func EscapePart(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return strings.ReplaceAll(s, `|`, `\|`)
}

// compact joins a prefix and parts and keeps the result within MaxDedupeLen.
// Long composites are replaced by their hash: the identity stays stable and
// the prefix still says what kind it is.
func compact(prefix string, parts ...string) string {
	escaped := make([]string, len(parts))
	for i, part := range parts {
		escaped[i] = EscapePart(part)
	}
	raw := prefix + ":" + strings.Join(escaped, "|")
	if len(raw) <= MaxDedupeLen {
		return raw
	}
	return prefix + ":" + Hash(raw)
}

// MessageDedupe is the identity of one platform message.
func MessageDedupe(key Key, thread, message string) string {
	return compact("message:"+string(key), thread, message)
}

// EditedDedupe is the identity of one edit. The text digest makes a retry the
// same edit and an edit back to earlier text a new one; editedAt, when the
// platform knows it, separates two edits that leave the same text.
func EditedDedupe(key Key, thread, message, text string, editedAt time.Time) string {
	stamp := ""
	if !editedAt.IsZero() {
		stamp = editedAt.UTC().Format(time.RFC3339Nano)
	}
	return compact("edited:"+string(key), thread, message, Hash(text), stamp)
}

// DeletedDedupe is the identity of one deletion.
func DeletedDedupe(key Key, thread, message string) string {
	return compact("deleted:"+string(key), thread, message)
}

// ReactionDedupe is the identity of one reaction occurrence. occurrence must
// be the platform's event ID or timestamp: a redelivery keeps it, and an add,
// remove, and add again get three different ones.
func ReactionDedupe(key Key, thread, message, emoji, user string, added bool, occurrence string) string {
	verb := "removed"
	if added {
		verb = "added"
	}
	return compact("reaction:"+string(key), thread, message, emoji, user, verb, occurrence)
}

// LifecycleDedupe is the identity of one lifecycle event. delivery is the
// platform's delivery ID where it has one; subject separates several records
// a single delivery can carry (a workflow run for two PRs).
func LifecycleDedupe(key Key, delivery, typeName, subject string) string {
	return compact("lifecycle:"+string(key), delivery, typeName, subject)
}

// EventDedupe is the identity of a whole platform delivery, used when a
// provider emits one record for a webhook.
func EventDedupe(key Key, delivery string) string {
	return compact("event:"+string(key), delivery)
}

// DedupeStore remembers identities long enough to drop a redelivery. The board
// inbox is the durable implementation; this interface is for adapters and
// tests that need a local one.
type DedupeStore interface {
	// Seen reports whether id was marked and still is.
	Seen(ctx context.Context, id string) (bool, error)
	// Mark records id.
	Mark(ctx context.Context, id string) error
}

// MemoryDedupe is an in-memory DedupeStore with a TTL and a bound. It is not
// the durable inbox: a restart forgets it, which is why a provider's identity
// must also be stable.
type MemoryDedupe struct {
	mu   sync.Mutex
	seen map[string]time.Time
	ttl  time.Duration
	max  int
	now  func() time.Time
}

// NewMemoryDedupe returns a store that forgets an ID after ttl and keeps at
// most max IDs. Zero ttl keeps IDs for the process's life; max <= 0 means 4096.
func NewMemoryDedupe(ttl time.Duration, max int) *MemoryDedupe {
	if max <= 0 {
		max = 4096
	}
	return &MemoryDedupe{seen: make(map[string]time.Time), ttl: ttl, max: max, now: time.Now}
}

// WithClock makes the store's time deterministic in tests.
func (m *MemoryDedupe) WithClock(now func() time.Time) *MemoryDedupe {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = now
	return m
}

// Seen reports whether id is marked and not expired.
func (m *MemoryDedupe) Seen(_ context.Context, id string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	at, ok := m.seen[id]
	if !ok {
		return false, nil
	}
	if m.ttl > 0 && m.now().Sub(at) > m.ttl {
		delete(m.seen, id)
		return false, nil
	}
	return true, nil
}

// Mark records id, dropping expired entries and the oldest when full.
func (m *MemoryDedupe) Mark(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ttl > 0 {
		now := m.now()
		for key, at := range m.seen {
			if now.Sub(at) > m.ttl {
				delete(m.seen, key)
			}
		}
	}
	if len(m.seen) >= m.max {
		m.evictOldest()
	}
	m.seen[id] = m.now()
	return nil
}

// SeenOrMark checks and marks in one lock, so two concurrent redeliveries
// can't both win.
func (m *MemoryDedupe) SeenOrMark(ctx context.Context, id string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if at, ok := m.seen[id]; ok && (m.ttl <= 0 || m.now().Sub(at) <= m.ttl) {
		return true, nil
	}
	if m.ttl > 0 {
		now := m.now()
		for key, at := range m.seen {
			if now.Sub(at) > m.ttl {
				delete(m.seen, key)
			}
		}
	}
	if len(m.seen) >= m.max {
		m.evictOldest()
	}
	m.seen[id] = m.now()
	return false, nil
}

func (m *MemoryDedupe) evictOldest() {
	var oldestKey string
	var oldest time.Time
	for key, at := range m.seen {
		if oldestKey == "" || at.Before(oldest) {
			oldestKey, oldest = key, at
		}
	}
	if oldestKey != "" {
		delete(m.seen, oldestKey)
	}
}
