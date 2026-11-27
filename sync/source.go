package sync

import "context"

// SourceReader is the optional provider surface an on-demand source read
// uses. It is not a sync: it reads what a platform already holds, writes
// nothing to the board, and subscribes to nothing. A provider that can't read
// its platform simply doesn't implement it; the HTTP layer answers 501.
type SourceReader interface {
	// ReadSource reads a channel, thread, issue, or project by target. The
	// provider decides what target means; the answer is normalized.
	ReadSource(ctx context.Context, key Key, target string, opts ReadOptions) (SourceRead, error)
}

// SourceJoiner is the optional provider surface an opt-in join uses. Joining
// usually announces the app in a channel and makes the platform start sending
// its events, so an adapter must require explicit configuration first.
type SourceJoiner interface {
	JoinSource(ctx context.Context, key Key, target string) (SourceJoin, error)
}

const (
	// DefaultSourceLimit is how many items one source read returns when the
	// caller names no limit.
	DefaultSourceLimit = 50
	// MaxSourceLimit bounds one source read. Providers may cap lower.
	MaxSourceLimit = 200
)

// ReadOptions page a source read. Limit defaults to DefaultSourceLimit and is
// capped at MaxSourceLimit; After is an opaque cursor from a previous read's
// Next, meaningful only to the provider that issued it.
//
// A provider must not synthesize a cursor. When it has to trim a page locally
// and can't offer a safe continuation, it leaves Next empty; More=true with
// Next=="" means "the platform has more, but restart with a larger limit"
// rather than "this page was the end". Callers must not treat an empty Next as
// the end while More is true.
type ReadOptions struct {
	Limit int
	After string
}

// LimitFor returns Limit bounded to [1, max], using def when Limit isn't
// positive. A provider passes its own cap when it has one lower than the
// board's.
func (o ReadOptions) LimitFor(def, max int) int {
	if max <= 0 {
		max = MaxSourceLimit
	}
	if def <= 0 || def > max {
		def = max
	}
	limit := o.Limit
	if limit <= 0 {
		limit = def
	}
	if limit > max {
		limit = max
	}
	return limit
}

// SourceRead is one normalized on-demand platform read. Kind is the adapter's
// own shape ("channel", "thread", "team", "project", "issue"); Channel, Name,
// and Thread locate what was read. Nothing here is stored on the board.
type SourceRead struct {
	Kind     string          `json:"kind"`
	Channel  string          `json:"channel"`
	Name     string          `json:"name,omitempty"`
	Thread   string          `json:"thread,omitempty"`
	Messages []SourceMessage `json:"messages"`
	More     bool            `json:"more"`
	Next     string          `json:"next,omitempty"`
}

// SourceMessage is one message, issue, or comment a source read returned,
// oldest first. Author is what the platform shows; AuthorID is the
// connection-prefixed identity the board would store for the same author.
type SourceMessage struct {
	ID        string `json:"id"`
	Thread    string `json:"thread"`
	Author    string `json:"author"`
	AuthorID  string `json:"authorId,omitempty"`
	Text      string `json:"text"`
	CreatedAt string `json:"createdAt,omitempty"`
	Replies   int    `json:"replies,omitempty"`
	Files     []File `json:"files,omitempty"`
}

// SourceJoin is the answer to an opt-in join. Joined is false when the app was
// already a member and nothing changed.
type SourceJoin struct {
	Channel string `json:"channel"`
	Name    string `json:"name,omitempty"`
	Joined  bool   `json:"joined"`
}
