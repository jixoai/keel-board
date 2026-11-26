package access

import (
	"sort"
	"strings"

	"github.com/jixoai/keel-board/protocol"
)

// PatternKind is what a channel pattern covers.
type PatternKind string

const (
	// PatternAll covers every channel, including ones that don't exist yet.
	PatternAll PatternKind = "all"
	// PatternExact covers one channel.
	PatternExact PatternKind = "exact"
	// PatternSubtree covers a channel and everything under it.
	PatternSubtree PatternKind = "subtree"
)

const patternShape = "write *, a channel, or a channel and its subtree (feedback/)"

// ChannelPattern is a compiled channel pattern: `*`, an exact channel
// (`feedback`), or a channel and its subtree (`feedback/`). Its zero value
// covers nothing useful; build one with ParsePattern.
type ChannelPattern struct {
	Kind PatternKind
	Path string
}

// ParsePattern compiles a channel pattern. Any `*` but a lone `*` is an
// InvalidCan that says what to write instead.
func ParsePattern(value string) (ChannelPattern, error) {
	if value == "*" {
		return ChannelPattern{Kind: PatternAll}, nil
	}
	if len(value) > protocol.ChannelPatternMax {
		return ChannelPattern{}, invalidCanf("invalid channel pattern: at most %d characters", protocol.ChannelPatternMax)
	}
	if star := strings.IndexByte(value, '*'); star >= 0 {
		before := strings.TrimRight(value[:star], "/")
		hint := "write * for every channel"
		if before != "" {
			if err := protocol.ValidateChannel(before); err == nil {
				hint = "write " + before + "/ for " + before + " and its subtree"
			} else {
				hint = patternShape
			}
		}
		return ChannelPattern{}, invalidCanf("invalid channel pattern %q: %s", value, hint)
	}
	subtree := strings.HasSuffix(value, "/")
	path := value
	if subtree {
		path = strings.TrimSuffix(value, "/")
	}
	if path == "" {
		return ChannelPattern{}, invalidCanf("invalid channel pattern %q: %s", value, patternShape)
	}
	if err := protocol.ValidateChannel(path); err != nil {
		return ChannelPattern{}, invalidCanf("invalid channel pattern %q: %s", value, patternShape)
	}
	if subtree {
		return ChannelPattern{Kind: PatternSubtree, Path: path}, nil
	}
	return ChannelPattern{Kind: PatternExact, Path: path}, nil
}

// String returns the pattern as it is written.
func (p ChannelPattern) String() string {
	switch p.Kind {
	case PatternAll:
		return "*"
	case PatternSubtree:
		return p.Path + "/"
	case PatternExact:
		return p.Path
	default:
		return ""
	}
}

// Covers reports whether the pattern covers channel: `*` covers every
// channel, an exact pattern only itself, and a subtree pattern its root and
// everything under it.
func (p ChannelPattern) Covers(channel string) bool {
	switch p.Kind {
	case PatternAll:
		return true
	case PatternExact:
		return p.Path == channel
	case PatternSubtree:
		return channel == p.Path || strings.HasPrefix(channel, p.Path+"/")
	default:
		return false
	}
}

// specificity ranks patterns that cover one channel: an exact channel first,
// then the longest subtree, then `*`.
func (p ChannelPattern) specificity() int {
	switch p.Kind {
	case PatternExact:
		return 1 << 30
	case PatternSubtree:
		return len(p.Path)
	case PatternAll:
		return -1
	default:
		return -2
	}
}

// sortPatterns sorts patterns from most to least specific.
func sortPatterns(patterns []ChannelPattern) {
	sort.SliceStable(patterns, func(i, j int) bool {
		return patterns[i].specificity() > patterns[j].specificity()
	})
}
