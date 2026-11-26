package protocol

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Board limits, as the upstream protocol states them. The board also holds
// its own limits on a task; these are the shape of a request.
const (
	ChannelPathMax     = 2048
	ChannelPatternMax  = ChannelPathMax + 1
	MessageTextMax     = 10_000
	LabelKeyMax        = 64
	LabelValueMax      = 256
	ActorIDMax         = 256
	RefMax             = 128
	ClientIDMax        = 200
	TaskNeedsMax       = 20
	TaskNoteMax        = 1000
	HoldKindMax        = 64
	LeaseDefaultSecs   = 900
	LeaseMinSecs       = 30
	LeaseMaxSecs       = 3600
	ClaimTokenChars    = 43
	MaxLabelConditions = 16
)

const (
	// ReservedLabelPrefix is the namespace the board writes itself. No
	// caller's write sets or unsets a key under it.
	ReservedLabelPrefix = "board."
	// TaskLabel is the label a task's root wears, with its state as value.
	TaskLabel = "board.task"
	// EditedHoldKind is the board's own hold kind for a root edited by
	// someone other than the task's owner. No request names it.
	EditedHoldKind = "edited"
)

var (
	channelSegment  = `[a-z0-9][a-z0-9_-]*`
	channelPathExpr = regexp.MustCompile(`^` + channelSegment + `(?:/` + channelSegment + `)*$`)
	channelSubExpr  = regexp.MustCompile(`^` + channelSegment + `(?:/` + channelSegment + `)*/?$`)
	labelKeyExpr    = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]*$`)
	actorIDExpr     = regexp.MustCompile(`^[^\s:]+:\S+$`)
	profileExpr     = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)
	holdKindExpr    = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]*$`)
	claimTokenExpr  = regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`)
	clientKeyIDExpr = regexp.MustCompile(`^key:[^\s:]+:[0-9a-f]{32}$`)
)

// Storable reports whether a string is one a board keeps: valid UTF-8
// without a NUL. A lone surrogate can't occur in valid UTF-8, which is what a
// Go string is.
func Storable(value string) bool {
	return utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

// ValidateChannel reports whether path is a channel path: lowercase
// segments joined by "/", up to ChannelPathMax characters.
func ValidateChannel(path string) *Error {
	if len(path) == 0 || len(path) > ChannelPathMax || !channelPathExpr.MatchString(path) {
		return Invalidf("%q isn't a channel path: one is lowercase letters, numbers, _, and -, joined by /", path)
	}
	return nil
}

// ValidateChannelOrSubtree reports whether value names a channel, or with a
// trailing "/" a channel and every channel under it.
func ValidateChannelOrSubtree(value string) *Error {
	if len(value) == 0 || len(value) > ChannelPatternMax || !channelSubExpr.MatchString(value) {
		return Invalidf("%q isn't a channel: name a channel, or a channel and its subtree (work/)", value)
	}
	return nil
}

// ValidateText validates a message's text as a request sends it: it must say
// something, be at most MessageTextMax characters, and be storable. An empty
// text is only allowed with attachments, which phase 1 doesn't serve.
func ValidateText(text string) *Error {
	if strings.TrimSpace(text) == "" {
		return Invalid("text: message text is empty")
	}
	if utf8.RuneCountInString(text) > MessageTextMax {
		return Errorf(CodeTextTooLong, "text: a message's text is at most %d characters", MessageTextMax)
	}
	if !Storable(text) {
		return Invalid("text: message text has a NUL character or an invalid sequence")
	}
	return nil
}

// ValidateStoredText validates a text that is already on the board.
func ValidateStoredText(text string) *Error {
	if strings.TrimSpace(text) == "" {
		return Invalid("text: message text is empty")
	}
	if utf8.RuneCountInString(text) > MessageTextMax {
		return Errorf(CodeTextTooLong, "text: a message's text is at most %d characters", MessageTextMax)
	}
	if !Storable(text) {
		return Invalid("text: message text has a NUL character or an invalid sequence")
	}
	return nil
}

// ValidateLabelKey reports whether key is a label key.
func ValidateLabelKey(key string) *Error {
	if len(key) == 0 || len(key) > LabelKeyMax || !labelKeyExpr.MatchString(key) {
		return Invalidf("%q isn't a label key: one is 1 to %d lowercase letters, numbers, ., _, and -, starting with a letter or number", key, LabelKeyMax)
	}
	return nil
}

// ValidateLabelValue reports whether value is a label value a write may set.
func ValidateLabelValue(value string) *Error {
	if utf8.RuneCountInString(value) > LabelValueMax {
		return Invalidf("a label value is at most %d characters, and this one has %d", LabelValueMax, utf8.RuneCountInString(value))
	}
	if !Storable(value) {
		return Invalid("a label value has a NUL character or an invalid sequence")
	}
	if strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return Invalid("a label value has a control character")
	}
	return nil
}

// ValidateNewLabels validates a set of labels a write would set. Keys under
// board. are the board's own and are refused with reserved_label; callers
// never set or unset them.
func ValidateNewLabels(labels map[string]string) *Error {
	for key, value := range labels {
		if strings.HasPrefix(key, ReservedLabelPrefix) {
			return NewError(CodeReservedLabel, fmt.Sprintf("labels under %s are the board's own: only its task writes set them", ReservedLabelPrefix))
		}
		if err := ValidateLabelKey(key); err != nil {
			return err
		}
		if err := ValidateLabelValue(value); err != nil {
			return err
		}
	}
	return nil
}

// ValidateActorID reports whether id is shaped like an actor ID:
// "<origin>:<who>", with no whitespace and no second colon in the origin.
func ValidateActorID(id string) *Error {
	if len(id) == 0 || len(id) > ActorIDMax || !actorIDExpr.MatchString(id) {
		return Invalidf("%q isn't an actor ID: one is <origin>:<who>, with no whitespace", id)
	}
	return nil
}

// ValidProfile reports whether profile is a lowercase profile name.
func ValidProfile(profile string) bool { return profileExpr.MatchString(profile) }

// ResolveAssignee returns the actor ID a task is assigned to: one of its
// owner's profiles, "<owner>+<profile>", or nil for the pool. An actor of
// another principal is refused.
func ResolveAssignee(owner string, forActor *string) (*string, *Error) {
	if forActor == nil {
		return nil, nil
	}
	if *forActor == "" {
		return nil, Invalid("for: name one of the owner's profiles, or null for anyone who may claim it")
	}
	prefix := owner + "+"
	if !strings.HasPrefix(*forActor, prefix) || !ValidProfile(strings.TrimPrefix(*forActor, prefix)) {
		return nil, Invalidf("for: a task is assigned to one of its owner's profiles, %s+<profile>, got %s", owner, *forActor)
	}
	return forActor, nil
}

// ValidateRef validates a message ref as a request sends it. It only checks
// the shape; whether it names something is a read's to say.
func ValidateRef(value string) *Error {
	if len(value) == 0 || len(value) > RefMax {
		return Invalidf("%q isn't a message ID: one is 1 to %d characters", value, RefMax)
	}
	return nil
}

// ValidateClientID validates a post's or a batch's idempotency key. UUIDv8
// IDs, sync IDs, and key: IDs under another principal are the board's and
// sync's own.
func ValidateClientID(id string) *Error {
	if len(id) == 0 || len(id) > ClientIDMax {
		return Invalidf("id: an id is 1 to %d characters", ClientIDMax)
	}
	if !Storable(id) {
		return Invalid("id: id has a NUL character or an invalid sequence")
	}
	if strings.HasPrefix(id, "sync:") {
		return Invalid("id: sync: IDs are sync's own")
	}
	return nil
}

// ValidClaimToken reports whether token is 32 bytes in base64url.
func ValidClaimToken(token string) bool { return claimTokenExpr.MatchString(token) }

// ValidateClaimToken validates a claim token a caller brings.
func ValidateClaimToken(token string) *Error {
	if !ValidClaimToken(token) {
		return Invalid("token: a claim token is 32 bytes in base64url: 43 characters")
	}
	return nil
}

// ValidateLease checks a requested lease in seconds and returns it, applying
// the default for none. A board gives what's asked up to its cap.
func ValidateLease(seconds *float64) (int, *Error) {
	if seconds == nil {
		return LeaseDefaultSecs, nil
	}
	value := *seconds
	if value < LeaseMinSecs || value > LeaseMaxSecs {
		return 0, Invalidf("lease: a lease is %d to %d seconds", LeaseMinSecs, LeaseMaxSecs)
	}
	return int(value), nil
}

// ValidateNote checks a task's result or reason.
func ValidateNote(what, note string) *Error {
	if strings.TrimSpace(note) == "" {
		return Invalidf("%s: it is empty", what)
	}
	if utf8.RuneCountInString(note) > TaskNoteMax {
		return Invalidf("%s: a task's %s is at most %d characters", what, what, TaskNoteMax)
	}
	if !Storable(note) {
		return Invalidf("%s: it has a NUL character or an invalid sequence", what)
	}
	return nil
}

// ValidateHoldKind checks a hold's kind. `edited` is the board's own.
func ValidateHoldKind(kind string) *Error {
	if kind == "" {
		return nil
	}
	if kind == EditedHoldKind {
		return Invalid("kind: `edited` is the board's own kind, for a task whose root someone else edited: name another")
	}
	if len(kind) > HoldKindMax || !holdKindExpr.MatchString(kind) {
		return Invalidf("kind: %q isn't a hold kind: one is 1 to %d lowercase letters, numbers, ., _, and -", kind, HoldKindMax)
	}
	return nil
}

// ValidateNeedsShape checks that every ref is shaped like one and that no task
// waits for itself. Whether each names a task is checked against the board.
func ValidateNeedsShape(refs []string, self *uint64) *Error {
	if len(refs) > TaskNeedsMax {
		return Errorf(CodeInvalidNeeds, "needs: a task waits for at most %d tasks", TaskNeedsMax)
	}
	for _, ref := range refs {
		if err := ValidateRef(ref); err != nil {
			return Errorf(CodeInvalidNeeds, "needs: %q isn't a task", ref)
		}
		id, ok := ParseRef(ref)
		if !ok || id == 0 {
			return Errorf(CodeInvalidNeeds, "needs: %q isn't a task", ref)
		}
		if self != nil && id == *self {
			return Errorf(CodeInvalidNeeds, "needs: #%s is the task itself: it can't wait for itself", ref)
		}
	}
	return nil
}

// ValidateOwnerFrom validates one of a claim's `from` owners: an ID, never a
// handle, since several actors can share a handle.
func ValidateOwnerFrom(value string) *Error {
	if !actorIDExpr.MatchString(value) {
		return NewError(CodeInvalidFrom, fmt.Sprintf("from: %s is a handle, which several can share: name the owner by ID", value))
	}
	return nil
}

// ValidTaskState reports whether state is one of the seven task states.
func ValidTaskState(state string) bool {
	switch state {
	case "waiting", "ready", "claimed", "blocked", "completed", "failed", "canceled":
		return true
	}
	return false
}

// OpenTaskStates are the states a task can still leave by being worked.
var OpenTaskStates = []string{"waiting", "ready", "claimed", "blocked"}
