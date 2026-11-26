package board

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	// LeaseDefault is the lease a claim gets when it asks for none.
	LeaseDefault = 15 * time.Minute
	// LeaseMin and LeaseMax bound a requested lease.
	LeaseMin = 30 * time.Second
	LeaseMax = time.Hour

	// MaxNeeds is the most prerequisites one task waits for. vercel/board
	// caps this at 20; Keel's board keeps 32.
	MaxNeeds = 32
	// MaxNote is the longest result, reason, or hold reason.
	MaxNote = 1000
	// MaxNeedsWalk is how many tasks a cycle check may visit before it
	// refuses a change it cannot check.
	MaxNeedsWalk = 500

	// LabelKeyMax and LabelValueMax bound a label. A key is ASCII; a value
	// is measured in runes.
	LabelKeyMax   = 64
	LabelValueMax = 256

	// HoldKindMax is the longest hold kind.
	HoldKindMax = 64

	// ReservedLabelPrefix is the namespace the board writes itself. Callers
	// cannot set or unset a key under it.
	ReservedLabelPrefix = "board."
	// StateLabel is the label a task's root wears, with its state as value.
	StateLabel = "board.task"
	// EditedHoldKind is the board's own hold kind for a root edited by
	// someone other than the task's owner. No request may name it.
	EditedHoldKind = "edited"
)

var (
	// An actor ID is "<origin>:<who>": no whitespace, and a colon only after
	// the origin. What it names is a credential's to say.
	actorIDPattern = regexp.MustCompile(`^[^\s:]+:\S+$`)
	// A profile is the part of an actor ID after its owner's ID and a "+".
	profilePattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)
	// A label key is 1 to 64 lowercase letters, numbers, ".", "_", and "-",
	// starting with a letter or number.
	labelKeyPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]*$`)
	// A hold kind is 1 to 64 lowercase letters, numbers, ".", "_", and "-".
	holdKindPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]*$`)
)

// ValidActorID reports whether id is shaped like an actor ID.
func ValidActorID(id string) bool { return actorIDPattern.MatchString(id) }

// ValidProfile reports whether profile is a lowercase profile name of 1 to 32
// characters.
func ValidProfile(profile string) bool {
	return len(profile) <= 32 && profilePattern.MatchString(profile)
}

// ForActor builds the assignee ID for one of owner's profiles: the owner's ID,
// a "+", and the profile.
func ForActor(owner ActorID, profile string) (ActorID, error) {
	if !owner.Valid() {
		return "", taskErrorf(CodeInvalidActor, "%q isn't an actor ID", owner)
	}
	if !ValidProfile(profile) {
		return "", taskErrorf(CodeInvalidProfile, "%q isn't a profile: one is 1 to 32 lowercase letters, numbers, and -", profile)
	}
	return ActorID(string(owner) + "+" + profile), nil
}

// normalizeLease applies the default and checks the bounds.
func normalizeLease(lease time.Duration) (time.Duration, error) {
	if lease == 0 {
		return LeaseDefault, nil
	}
	if lease < LeaseMin || lease > LeaseMax {
		return 0, taskErrorf(CodeInvalidLease, "a lease is %s to %s", LeaseMin, LeaseMax)
	}
	return lease, nil
}

// validateFor checks that an assignee belongs to owner.
func validateFor(owner, forActor ActorID) error {
	if !owner.Valid() {
		return taskErrorf(CodeInvalidActor, "%q isn't an actor ID", owner)
	}
	if forActor == "" {
		return nil
	}
	prefix := string(owner) + "+"
	if !strings.HasPrefix(string(forActor), prefix) || !ValidProfile(string(forActor)[len(prefix):]) {
		return taskErrorf(CodeInvalidProfile, "a task is assigned to one of its owner's profiles, %s+<profile>, got %q", owner, forActor)
	}
	return nil
}

// normalizeLabels validates caller labels and returns a copy. Keys are visited
// in order, so the first bad label in an error is deterministic.
func normalizeLabels(labels map[string]string) (map[string]string, error) {
	if len(labels) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make(map[string]string, len(labels))
	for _, key := range keys {
		value := labels[key]
		if err := validateLabel(key, value); err != nil {
			return nil, err
		}
		out[key] = value
	}
	return out, nil
}

func validateLabel(key, value string) error {
	if strings.HasPrefix(key, ReservedLabelPrefix) {
		return taskErrorf(CodeReservedLabel, "labels under %s are the board's own: only its task writes set them", ReservedLabelPrefix)
	}
	if len(key) == 0 || len(key) > LabelKeyMax || !labelKeyPattern.MatchString(key) {
		return taskErrorf(CodeInvalidLabel, "%q isn't a label key: one is 1 to %d lowercase letters, numbers, ., _, and -, starting with a letter or number", key, LabelKeyMax)
	}
	if !utf8.ValidString(value) || strings.ContainsRune(value, 0) || utf8.RuneCountInString(value) > LabelValueMax {
		return taskErrorf(CodeInvalidLabel, "a label value is at most %d characters, and this one is invalid or too long", LabelValueMax)
	}
	if strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return taskErrorf(CodeInvalidLabel, "a label value has a control character")
	}
	return nil
}

// validateNote checks a result, reason, or hold reason.
func validateNote(what, note string) error {
	if strings.TrimSpace(note) == "" {
		return taskErrorf(CodeInvalidNote, "%s is empty", what)
	}
	if utf8.RuneCountInString(note) > MaxNote {
		return taskErrorf(CodeInvalidNote, "%s is at most %d characters", what, MaxNote)
	}
	if !utf8.ValidString(note) || strings.ContainsRune(note, 0) {
		return taskErrorf(CodeInvalidNote, "%s is invalid", what)
	}
	return nil
}

// validateHoldKind checks a hold kind. The board's own "edited" kind is
// refused: only the board places it.
func validateHoldKind(kind string) error {
	if kind == "" {
		return nil
	}
	if kind == EditedHoldKind {
		return taskErrorf(CodeInvalidHoldKind, "%q is the board's own hold kind", kind)
	}
	if len(kind) > HoldKindMax || !holdKindPattern.MatchString(kind) {
		return taskErrorf(CodeInvalidHoldKind, "%q isn't a hold kind: one is 1 to %d lowercase letters, numbers, ., _, and -", kind, HoldKindMax)
	}
	return nil
}

// normalizeNeeds deduplicates prerequisites and checks their shape. A zero ID
// is not a task; a task cannot wait for itself.
func normalizeNeeds(needs []uint64, self uint64) ([]uint64, error) {
	if len(needs) == 0 {
		return nil, nil
	}
	seen := make(map[uint64]struct{}, len(needs))
	out := make([]uint64, 0, len(needs))
	for _, id := range needs {
		if id == 0 {
			return nil, taskErrorf(CodeInvalidNeeds, "#0 isn't a task")
		}
		if self != 0 && id == self {
			return nil, taskErrorf(CodeInvalidNeeds, "#%d is the task itself: it can't wait for itself", id)
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	if len(out) > MaxNeeds {
		return nil, taskErrorf(CodeInvalidNeeds, "a task waits for at most %d tasks", MaxNeeds)
	}
	return out, nil
}

// ValidateNeeds deduplicates and checks prerequisites against the board's
// tasks. Every ID must name a task, and a canceled task can never complete, so
// it is refused. A failed task is allowed: it may be reopened.
func ValidateNeeds(needs []uint64, tasks map[uint64]Task, self uint64) ([]uint64, error) {
	out, err := normalizeNeeds(needs, self)
	if err != nil {
		return nil, err
	}
	for _, id := range out {
		need, ok := tasks[id]
		if !ok {
			return nil, taskErrorf(CodeInvalidNeeds, "#%d isn't a task", id)
		}
		if need.State == StateCanceled {
			return nil, taskErrorf(CodeInvalidNeeds, "#%d was canceled: it can never complete", id)
		}
	}
	return out, nil
}

// ValidTokenHash reports whether hash is SHA-256 hex.
func ValidTokenHash(hash string) bool {
	if len(hash) != sha256.Size*2 {
		return false
	}
	for _, c := range hash {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// HashToken returns the SHA-256 of token, hex encoded. This is the form a
// claim stores.
func HashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// NewToken returns 32 random bytes in base64url: 43 characters, the shape a
// claim token has on the wire.
func NewToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

// VerifyClaim checks that epoch and token are the live claim's identity. It
// does not look at the clock: use VerifyClaimAt, or a transition, to also
// require an unexpired lease. The token is compared with the stored hash in
// constant time, and a failure is always claim_lost.
func VerifyClaim(t Task, epoch uint64, token string) error {
	if t.State != StateClaimed || t.Claim == nil || t.Claim.Epoch != epoch {
		return taskErrorf(CodeClaimLost, "this claim on #%d is over: claim it again", t.ID)
	}
	want := HashToken(token)
	if len(t.Claim.TokenHash) != len(want) || !constantTimeEqual(t.Claim.TokenHash, want) {
		return taskErrorf(CodeClaimLost, "this claim on #%d is over: claim it again", t.ID)
	}
	return nil
}

// VerifyClaimAt is VerifyClaim plus the lease: a claim whose lease ended is
// over, even before anything expired it.
func VerifyClaimAt(t Task, epoch uint64, token string, now time.Time) error {
	if err := VerifyClaim(t, epoch, token); err != nil {
		return err
	}
	if !now.Before(t.Claim.Until) {
		return taskErrorf(CodeClaimLost, "this claim on #%d is over: claim it again", t.ID)
	}
	return nil
}

func constantTimeEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
