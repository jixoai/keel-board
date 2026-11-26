package sync

import (
	"regexp"
	"strconv"
	"strings"
)

// A marker is a hidden HTML comment a provider adds to a platform comment it
// posts, and reads before importing one. It is a second line of defence
// against echo, beside the platform's bot user ID: the marker survives a
// dashboard that renames the app, and names the board message that made the
// comment.
//
// Format:
//
//	<!-- keel-board:sync key=slack message=812 thread=slack:C0123:1727.9 -->
//
// Slack and Linear don't use markers: their text is visible and their bot
// identity is enough. GitHub comments do.
const markerName = "keel-board:sync"

var markerPattern = regexp.MustCompile(`(?s)<!--\s*` + regexp.QuoteMeta(markerName) + `\s+(.*?)\s*-->`)

// Marker identifies the board message a posted platform comment came from.
type Marker struct {
	Key     Key    `json:"key"`
	Message uint64 `json:"message,omitempty"`
	Thread  string `json:"thread,omitempty"`
}

// String returns the hidden comment.
func (m Marker) String() string {
	var b strings.Builder
	b.WriteString("<!-- ")
	b.WriteString(markerName)
	b.WriteString(" key=")
	b.WriteString(string(m.Key))
	if m.Message != 0 {
		b.WriteString(" message=")
		b.WriteString(strconv.FormatUint(m.Message, 10))
	}
	if m.Thread != "" {
		b.WriteString(" thread=")
		b.WriteString(sanitizeMarkerValue(m.Thread))
	}
	b.WriteString(" -->")
	return b.String()
}

// Valid reports whether the marker names a sync key.
func (m Marker) Valid() bool { return m.Key.Valid() }

// WithMarker appends a marker to a body on its own lines.
func WithMarker(body string, m Marker) string {
	if !m.Valid() {
		return body
	}
	body = strings.TrimRight(body, "\n")
	if body == "" {
		return m.String()
	}
	return body + "\n\n" + m.String()
}

// ParseMarker returns the first valid marker in text.
func ParseMarker(text string) (Marker, bool) {
	for _, match := range markerPattern.FindAllStringSubmatch(text, -1) {
		marker, ok := parseMarkerFields(match[1])
		if ok {
			return marker, true
		}
	}
	return Marker{}, false
}

// StripMarker removes every marker comment and the blank lines around it.
func StripMarker(text string) string {
	text = markerPattern.ReplaceAllString(text, "")
	text = strings.ReplaceAll(text, "\r\n", "\n")
	for strings.Contains(text, "\n\n\n") {
		text = strings.ReplaceAll(text, "\n\n\n", "\n\n")
	}
	return strings.TrimSpace(text)
}

// IsOwnMarker reports whether text carries a marker for key, which means the
// comment was written by this entry and must not be imported back.
func IsOwnMarker(text string, key Key) bool {
	marker, ok := ParseMarker(text)
	return ok && marker.Key == key
}

func parseMarkerFields(fields string) (Marker, bool) {
	var marker Marker
	for _, field := range strings.Fields(fields) {
		name, value, found := strings.Cut(field, "=")
		if !found {
			continue
		}
		switch name {
		case "key":
			marker.Key = Key(value)
		case "message":
			id, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return Marker{}, false
			}
			marker.Message = id
		case "thread":
			marker.Thread = value
		}
	}
	return marker, marker.Valid()
}

func sanitizeMarkerValue(value string) string {
	value = strings.ReplaceAll(value, "-->", "->")
	value = strings.Map(func(r rune) rune {
		switch r {
		case ' ', '\t', '\r', '\n':
			return -1
		default:
			return r
		}
	}, value)
	if len(value) > 300 {
		return value[:300]
	}
	return value
}
