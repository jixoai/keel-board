package protocol

import (
	"fmt"
	"regexp"
	"strconv"
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
	TaskKeyMax         = 64
	TaskBatchMax       = 100
	HoldKindMax        = 64
	LeaseDefaultSecs   = 900
	LeaseMinSecs       = 30
	LeaseMaxSecs       = 3600
	ClaimTokenChars    = 43
	MaxLabelConditions = 16

	// AttachmentBytesMax is the most a request may upload in one body. It
	// sits under a typical 4.5 MB function body limit.
	AttachmentBytesMax = 4 * 1024 * 1024
	// DirectUploadBytesMax is the most a signed direct upload may carry.
	DirectUploadBytesMax = 100 * 1024 * 1024
	// AttachmentsPerMessageMax is how many files one message carries.
	AttachmentsPerMessageMax = 10
	// AttachmentNameMax is how long a stored file name may be.
	AttachmentNameMax = 255
	// EmojiMax is how long a reaction's name may be.
	EmojiMax = 64
	// ThreadsLimitDefault and ThreadsLimitMax bound a thread page.
	ThreadsLimitDefault = 50
	ThreadsLimitMax     = 500
	// ActorsByIdMax is how many actors one read may name.
	ActorsByIdMax = 100
	// BatchRequestMax is how long a batch's request digest may be.
	BatchRequestMax = 64
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
// text is only allowed with attachments.
func ValidateText(text string) *Error {
	return ValidateMessageText(text, false)
}

// ValidateMessageText validates a message's text, allowing it to be empty when
// the message carries attachments. Upstream lets a file-only message say
// nothing; every other write still needs text.
func ValidateMessageText(text string, hasAttachments bool) *Error {
	if strings.TrimSpace(text) == "" && !hasAttachments {
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

var (
	attachmentIDExpr   = regexp.MustCompile(`^(?:[A-Za-z0-9_-]{22}|att_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$`)
	emojiExpr          = regexp.MustCompile(`^[a-z0-9_+-]+$`)
	mediaTypeExpr      = regexp.MustCompile(`^[a-z0-9][a-z0-9!#$&^_.+-]*/[a-z0-9][a-z0-9!#$&^_.+-]*$`)
	threadPositionExpr = regexp.MustCompile(`^([1-9][0-9]{0,15})\.([1-9][0-9]{0,15})$`)
)

// reservedEmoji are names a JavaScript object already has. A board that keys
// reactions by name refuses them, as upstream does.
var reservedEmoji = map[string]bool{
	"constructor": true,
	"prototype":   true,
	"__proto__":   true,
	"tostring":    true,
	"valueof":     true,
}

// ValidateRevision validates a message's `revision` precondition: the positive
// revision the caller read.
func ValidateRevision(revision *uint64) *Error {
	if revision == nil {
		return nil
	}
	if *revision == 0 {
		return Invalid("revision: a message's revision is a positive number")
	}
	return nil
}

// ValidateEmoji validates a reaction's name: a short name such as thumbs_up,
// 1 to EmojiMax lowercase letters, numbers, _ and -, and not a reserved name.
func ValidateEmoji(name string) *Error {
	if len(name) == 0 || len(name) > EmojiMax || !emojiExpr.MatchString(name) {
		return Invalidf("emoji: %q isn't an emoji name: one is 1 to %d lowercase letters, numbers, _ and -", name, EmojiMax)
	}
	if reservedEmoji[name] {
		return Invalidf("emoji: %s is a reserved name", name)
	}
	return nil
}

// emojiAliases maps the platform names the board used to accept to the Chat
// SDK names it stores, as upstream's table does for the common ones. A name
// it doesn't know is its own.
var emojiAliases = map[string]string{
	"+1":                     "thumbs_up",
	"thumbsup":               "thumbs_up",
	"-1":                     "thumbs_down",
	"thumbsdown":             "thumbs_down",
	"white_check_mark":       "check",
	"heavy_check_mark":       "check",
	"ballot_box_with_check":  "check",
	"x":                      "x",
	"heavy_multiplication_x": "x",
	"red_circle":             "red_circle",
	"large_blue_circle":      "large_blue_circle",
	"rotating_light":         "rotating_light",
	"ship":                   "ship",
	"tada":                   "tada",
	"fire":                   "fire",
	"rocket":                 "rocket",
	"heart":                  "heart",
	"joy":                    "joy",
	"eyes":                   "eyes",
	"clap":                   "clap",
	"pray":                   "pray",
	"warning":                "warning",
}

// CanonicalEmoji returns an emoji's stored name: the Chat SDK name for a
// platform alias, and the name itself otherwise.
func CanonicalEmoji(name string) string {
	if canonical, ok := emojiAliases[name]; ok {
		return canonical
	}
	return name
}

// NormalizeEmoji validates a reaction's emoji as a request sends it (with or
// without colons) and returns its canonical stored name.
func NormalizeEmoji(value string) (string, *Error) {
	trimmed := strings.TrimSpace(value)
	trimmed = strings.TrimSuffix(strings.TrimPrefix(trimmed, ":"), ":")
	name := strings.ToLower(trimmed)
	if err := ValidateEmoji(name); err != nil {
		return "", err
	}
	return CanonicalEmoji(name), nil
}

// ValidateAttachmentID reports whether id is shaped like one this board issues:
// 22 base64url characters (128 random bits), or an att_ UUID.
func ValidateAttachmentID(id string) *Error {
	if len(id) == 0 || len(id) > 128 || !attachmentIDExpr.MatchString(id) {
		return Invalidf("%q isn't an attachment ID", id)
	}
	return nil
}

// ValidateAttachmentIDs validates the files a write binds: at most
// AttachmentsPerMessageMax, each shaped like an ID. Duplicates are allowed and
// bound once.
func ValidateAttachmentIDs(ids []string) *Error {
	if len(ids) > AttachmentsPerMessageMax {
		return Invalidf("attachments: a message carries at most %d", AttachmentsPerMessageMax)
	}
	for _, id := range ids {
		if err := ValidateAttachmentID(id); err != nil {
			return err
		}
	}
	return nil
}

// ValidateFileName checks a request's file name as upstream does: the last
// path segment is the name, a dotfile is refused, and so is one that holds
// invisible formatting characters (bidi overrides, zero-width, line and
// paragraph separators). It doesn't rewrite the name; the server refuses it.
func ValidateFileName(name string) *Error {
	base := name
	if index := strings.LastIndexAny(base, `/\`); index >= 0 {
		base = base[index+1:]
	}
	if !utf8.ValidString(base) {
		return NewError(CodeInvalidAttachment, "attachment names must be valid UTF-8")
	}
	if strings.HasPrefix(base, ".") {
		return NewError(CodeInvalidAttachment, fmt.Sprintf("attachment names can't start with a dot: %q", base))
	}
	for _, r := range base {
		if unicode.Is(unicode.Cf, r) || unicode.Is(unicode.Zl, r) || unicode.Is(unicode.Zp, r) {
			return NewError(CodeInvalidAttachment, fmt.Sprintf("attachment names can't hold invisible formatting characters: %q", base))
		}
	}
	return nil
}

// SanitizeFileName turns a request's file name into one the board stores: the
// last path segment, without control or invisible formatting characters, no
// leading dot, at most AttachmentNameMax characters. An empty result is
// "file". A name ValidateFileName refuses isn't silently rewritten; this is
// for names that passed it and for the default.
func SanitizeFileName(name string) string {
	if index := strings.LastIndexAny(name, `/\`); index >= 0 {
		name = name[index+1:]
	}
	clean := strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || unicode.Is(unicode.Zl, r) || unicode.Is(unicode.Zp, r) {
			return -1
		}
		return r
	}, name)
	clean = strings.TrimSpace(strings.TrimLeft(strings.TrimSpace(clean), "."))
	if runes := []rune(clean); len(runes) > AttachmentNameMax {
		clean = string(runes[:AttachmentNameMax])
	}
	if clean == "" {
		return "file"
	}
	return clean
}

// MediaType returns a request's content type as the board stores it, falling
// back to application/octet-stream for anything that isn't one.
func MediaType(value string) string {
	value = strings.ToLower(strings.TrimSpace(strings.SplitN(value, ";", 2)[0]))
	if len(value) > 255 || !mediaTypeExpr.MatchString(value) {
		return "application/octet-stream"
	}
	return value
}

// ValidateTaskKey validates a key a batch's tasks name each other by.
func ValidateTaskKey(key string) *Error {
	if len(key) == 0 || len(key) > TaskKeyMax {
		return Invalidf("key: a task's key is 1 to %d characters", TaskKeyMax)
	}
	if !Storable(key) {
		return Invalid("key: it has a NUL character or an invalid sequence")
	}
	return nil
}

// ValidateBatchRequest validates a batch's `request`: the digest of what it
// asked for, as a board in front of this one gave it.
func ValidateBatchRequest(request string) *Error {
	if len(request) == 0 || len(request) > BatchRequestMax {
		return Invalidf("request: a batch's request is 1 to %d characters", BatchRequestMax)
	}
	if !Storable(request) {
		return Invalid("request: it has a NUL character or an invalid sequence")
	}
	return nil
}

// FormatThreadPosition returns a thread page's `older` cursor: the position of
// the oldest thread shown, as "<activity>.<root>".
func FormatThreadPosition(activity, root uint64) string {
	return fmt.Sprintf("%d.%d", activity, root)
}

// ParseThreadPosition parses a thread page's `older` cursor.
func ParseThreadPosition(value string) (activity, root uint64, ok bool) {
	found := threadPositionExpr.FindStringSubmatch(value)
	if found == nil {
		return 0, 0, false
	}
	activity, err := strconv.ParseUint(found[1], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	root, err = strconv.ParseUint(found[2], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return activity, root, true
}
