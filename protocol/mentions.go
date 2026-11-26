package protocol

import (
	"regexp"
	"sort"
	"strings"
)

// A mention is prose's `@[<actor id>]` (an exact mention) or `@handle`. Code
// is not prose: a mention inside a code span or fence names no one. This is a
// smaller parser than upstream's Markdown-aware one: it blanks fenced and
// inline code, then reads exact mentions before handles.

// MentionKind is what a mention token names.
type MentionKind string

const (
	MentionActor  MentionKind = "actor"
	MentionHandle MentionKind = "handle"
)

// Mention is one mention in a text, deduplicated in prose order.
type Mention struct {
	Kind  MentionKind
	Value string
}

var (
	exactMentionExpr  = regexp.MustCompile(`@\[([^\]\s]+)\]`)
	handleMentionExpr = regexp.MustCompile(`(?i)@([a-z0-9](?:[a-z0-9._-]*[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]*[a-z0-9])?)?)`)
	inlineCodeExpr    = regexp.MustCompile("`[^`\\n]*`")
	fenceLineExpr     = regexp.MustCompile("^ {0,3}(`{3,}|~{3,})(.*)$")
)

// proseOnly blanks code so a mention inside it never counts.
func proseOnly(text string) string {
	prose := blankFences(text)
	return inlineCodeExpr.ReplaceAllStringFunc(prose, func(value string) string {
		return strings.Repeat(" ", len(value))
	})
}

// blankFences replaces each fenced code block with spaces, keeping offsets.
// A fence opens with up to three spaces and three or more backticks or tildes,
// and closes with at least as many of the same character.
func blankFences(text string) string {
	lines := strings.SplitAfter(text, "\n")
	blanked := make([]string, len(lines))
	open := byte(0)
	openLength := 0
	for at, line := range lines {
		body := strings.TrimSuffix(line, "\n")
		if open == 0 {
			found := fenceLineExpr.FindStringSubmatch(body)
			if found != nil {
				fence := found[1]
				open = fence[0]
				openLength = len(fence)
				blanked[at] = strings.Repeat(" ", len(body)) + strings.TrimPrefix(line, body)
				continue
			}
			blanked[at] = line
			continue
		}
		blanked[at] = strings.Repeat(" ", len(body)) + strings.TrimPrefix(line, body)
		trimmed := strings.TrimSpace(body)
		if len(trimmed) >= openLength && strings.Trim(trimmed, string(open)) == "" {
			open = 0
			openLength = 0
		}
	}
	return strings.Join(blanked, "")
}

// ExtractMentions returns the mentions in text, deduplicated, in prose order.
func ExtractMentions(text string) []Mention {
	prose := proseOnly(text)
	type foundMention struct {
		index   int
		mention Mention
	}
	matches := make([]foundMention, 0)
	// Exact mentions first, so their handle-shaped contents aren't read again.
	exact := exactMentionExpr.FindAllStringSubmatchIndex(prose, -1)
	for _, match := range exact {
		start := match[0]
		if start > 0 && !isMentionBoundary(prose[start-1]) {
			continue
		}
		matches = append(matches, foundMention{index: start, mention: Mention{Kind: MentionActor, Value: prose[match[2]:match[3]]}})
	}
	// Blank exact mentions before looking for handles.
	remaining := exactMentionExpr.ReplaceAllStringFunc(prose, func(value string) string {
		return strings.Repeat(" ", len(value))
	})
	for _, match := range handleMentionExpr.FindAllStringSubmatchIndex(remaining, -1) {
		start, end := match[0], match[1]
		if start > 0 && !isMentionBoundary(remaining[start-1]) {
			continue
		}
		if end < len(remaining) && !isHandleEnd(remaining[end:]) {
			continue
		}
		matches = append(matches, foundMention{index: start, mention: Mention{Kind: MentionHandle, Value: strings.ToLower(remaining[match[2]:match[3]])}})
	}
	sort.SliceStable(matches, func(i, j int) bool { return matches[i].index < matches[j].index })
	seen := make(map[string]bool, len(matches))
	mentions := make([]Mention, 0, len(matches))
	for _, entry := range matches {
		key := string(entry.mention.Kind) + ":" + entry.mention.Value
		if seen[key] {
			continue
		}
		seen[key] = true
		mentions = append(mentions, entry.mention)
	}
	return mentions
}

// isMentionBoundary reports whether a byte before an '@' lets it start a
// mention: a word character, a previous '@', a dot, or a dash doesn't.
func isMentionBoundary(previous byte) bool {
	switch {
	case previous >= 'a' && previous <= 'z',
		previous >= 'A' && previous <= 'Z',
		previous >= '0' && previous <= '9':
		return false
	case previous == '_' || previous == '@' || previous == '.' || previous == '-':
		return false
	}
	return true
}

// isHandleEnd reports whether what follows a handle lets it end there: a colon
// joins it to more (`@slack:U0456`), and so do a word character, a slash, and a
// dot before a word character.
func isHandleEnd(rest string) bool {
	if rest == "" {
		return true
	}
	next := rest[0]
	switch {
	case next >= 'a' && next <= 'z',
		next >= 'A' && next <= 'Z',
		next >= '0' && next <= '9':
		return false
	case next == '_' || next == '/' || next == '-':
		return false
	case next == ':':
		return len(rest) == 1 || rest[1] == ' ' || rest[1] == '\t' || rest[1] == '\n'
	case next == '.':
		return len(rest) == 1 || isMentionBoundary(rest[1]) || rest[1] == ' '
	}
	return true
}

// ValidateMentions checks a text's exact mentions: each `@[...]` must hold an
// actor ID. Handles stay as prose and name no one the board can verify; a
// value that names an actor resolves elsewhere.
func ValidateMentions(text string) *Error {
	for _, mention := range ExtractMentions(text) {
		if mention.Kind != MentionActor {
			continue
		}
		if err := ValidateActorID(mention.Value); err != nil {
			return Invalidf("mentions: @[%s] isn't an actor ID", mention.Value)
		}
	}
	return nil
}

// MentionTerms returns what a text mentions as a `mentions` filter matches it:
// actor IDs as written, handles lowercased.
func MentionTerms(text string) []string {
	terms := make([]string, 0)
	seen := make(map[string]bool)
	for _, mention := range ExtractMentions(text) {
		term := mention.Value
		if mention.Kind == MentionHandle {
			term = strings.ToLower(term)
		}
		if !seen[term] {
			seen[term] = true
			terms = append(terms, term)
		}
	}
	return terms
}

// MentionsMatch reports whether text mentions any term a filter names. An
// actor ID (it holds a colon) also matches its profiles, which are
// `<id>+<profile>`; a handle matches profiles written `@handle/profile`. An
// exact term always matches.
func MentionsMatch(text string, wanted []string) bool {
	terms := MentionTerms(text)
	for _, want := range wanted {
		for _, term := range terms {
			if term == want {
				return true
			}
			if strings.Contains(want, ":") {
				if strings.HasPrefix(term, want+"+") {
					return true
				}
			} else if strings.HasPrefix(term, want+"/") {
				return true
			}
		}
	}
	return false
}

// MentionsMatchExact reports whether text mentions one of the terms exactly.
// It is what a `mentions=me` filter uses: the caller's own actor and handle,
// without the profiles under either.
func MentionsMatchExact(text string, wanted []string) bool {
	terms := MentionTerms(text)
	for _, want := range wanted {
		for _, term := range terms {
			if term == want {
				return true
			}
		}
	}
	return false
}

// MentionIDs returns the exact actor IDs a text mentions.
func MentionIDs(text string) []string {
	ids := make([]string, 0)
	for _, mention := range ExtractMentions(text) {
		if mention.Kind == MentionActor {
			ids = append(ids, mention.Value)
		}
	}
	return ids
}
