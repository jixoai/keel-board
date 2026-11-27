package sync

import "strings"

// MapProse applies fn to the parts of text that are prose, leaving inline code
// spans and fenced code blocks untouched. A platform's markup inside code is
// literal text, not a mention or a command; a converter that rewrites it is
// wrong. An unclosed backtick run is prose, because a truncated message isn't
// a code span.
func MapProse(text string, fn func(string) string) string {
	if text == "" || fn == nil {
		return text
	}
	var out strings.Builder
	var prose strings.Builder
	out.Grow(len(text))
	flush := func() {
		if prose.Len() > 0 {
			out.WriteString(fn(prose.String()))
			prose.Reset()
		}
	}
	for i := 0; i < len(text); {
		if text[i] == '`' {
			run := backtickRun(text[i:])
			if end := closingBackticks(text, i+run, run); end >= 0 {
				flush()
				out.WriteString(text[i : end+run])
				i = end + run
				continue
			}
		}
		prose.WriteByte(text[i])
		i++
	}
	flush()
	return out.String()
}

func backtickRun(text string) int {
	run := 0
	for run < len(text) && text[run] == '`' {
		run++
	}
	return run
}

func closingBackticks(text string, from, length int) int {
	for i := from; i < len(text); {
		if text[i] != '`' {
			i++
			continue
		}
		run := backtickRun(text[i:])
		if run == length {
			return i
		}
		i += run
	}
	return -1
}

// ReplaceMentions rewrites the board's explicit mention token for one sync key
// and leaves every other token exactly as written. Other keys' mentions are
// inert text on a platform that doesn't own them: fabricating a native mention
// for a foreign user ID is worse than showing the token, which cannot notify
// the wrong person. Code spans and fenced blocks are left alone.
//
// The token shape is "@[<key>:user:<native id>]". fn returns the native
// mention and whether the platform can make one; a false answer keeps the
// token, which is the documented cross-platform fallback.
func ReplaceMentions(text string, key Key, fn func(id string) (string, bool)) string {
	if text == "" || key == "" || fn == nil || !strings.Contains(text, "@[") {
		return text
	}
	prefix := string(key) + ":user:"
	return MapProse(text, func(prose string) string {
		var out strings.Builder
		for i := 0; i < len(prose); {
			if strings.HasPrefix(prose[i:], "@[") {
				if end := strings.IndexByte(prose[i+2:], ']'); end >= 0 {
					inner := prose[i+2 : i+2+end]
					if strings.HasPrefix(inner, prefix) && !strings.ContainsAny(inner, " \t\r\n[") {
						id := strings.TrimPrefix(inner, prefix)
						if id != "" {
							if replacement, ok := fn(id); ok {
								out.WriteString(replacement)
								i += 2 + end + 1
								continue
							}
						}
					}
				}
			}
			out.WriteByte(prose[i])
			i++
		}
		return out.String()
	})
}
