package github

import (
	"regexp"
	"strings"

	boardsync "github.com/jixoai/keel-board/sync"
)

// githubLogin is the shape GitHub accepts as a user or bot login. A board
// mention that isn't one stays text rather than becoming a broken native
// mention.
var githubLogin = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9-]{0,38})$`)

// githubMention finds an @login in prose. The preceding character must not be
// part of an email local part or a URL's authority, so "mail me@host" and
// "github.com/@x" keep their meaning.
var githubMention = regexp.MustCompile(`(^|[^A-Za-z0-9_.%+-])@([A-Za-z0-9](?:[A-Za-z0-9-]{0,38}))`)

// nativeMentions turns a board mention under this entry's key into GitHub's
// own @login. A token for another connection stays exactly as written: this
// adapter can't speak for that user, and a literal token notifies no one.
func nativeMentions(text string, key boardsync.Key) string {
	return boardsync.ReplaceMentions(text, key, func(id string) (string, bool) {
		if !githubLogin.MatchString(id) {
			return "", false
		}
		return "@" + id, true
	})
}

// boardMentions turns GitHub @logins into the board's exact token so an
// imported comment mentions the platform user rather than a local actor that
// happens to share the handle. Code spans are untouched. An `@org/team` team
// mention is left as written: it names a team, not a user, and splitting it
// would invent a user.
func boardMentions(text string, key boardsync.Key) string {
	if text == "" || key == "" || !strings.Contains(text, "@") {
		return text
	}
	return boardsync.MapProse(text, func(prose string) string {
		matches := githubMention.FindAllStringSubmatchIndex(prose, -1)
		if len(matches) == 0 {
			return prose
		}
		var out strings.Builder
		last := 0
		for _, match := range matches {
			start, end := match[0], match[1]
			loginStart, loginEnd := match[4], match[5]
			if end < len(prose) && prose[end] == '/' {
				continue
			}
			out.WriteString(prose[last:start])
			out.WriteString(prose[match[2]:match[3]])
			out.WriteString("@[")
			out.WriteString(string(key))
			out.WriteString(":user:")
			out.WriteString(prose[loginStart:loginEnd])
			out.WriteString("]")
			last = end
		}
		if last == 0 {
			return prose
		}
		out.WriteString(prose[last:])
		return out.String()
	})
}
