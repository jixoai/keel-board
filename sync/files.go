package sync

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"
	"unicode"
)

// FileLoadResult is what the board returns for one outbound message's
// attachments: the bytes a platform can upload, and notes for the ones it
// can't carry. Notes are rendered into the delivered text, so a file problem
// never loses the message.
type FileLoadResult struct {
	Files []OutboundFile
	Notes []string
}

// FileLoader reads a board message's attachment bytes for delivery. A loader
// bounds what it reads: a provider's Post receives at most the files the
// loader returns. Returning an error means the board couldn't read its own
// store; the delivery fails and the outbox retries, because nothing has been
// posted yet.
type FileLoader func(ctx context.Context, tenant string, message Message) (FileLoadResult, error)

// FileFetchTimeout is the most an import waits for one file. A slow platform
// degrades to a note; it never holds the inbound lane for long.
const FileFetchTimeout = 10 * time.Second

// FileNote is one line a delivered message carries when an attachment didn't
// come over. The name is untrusted: it is made inert for the platform's text
// syntax before it is returned.
func FileNote(name string, size int64, reason string) string {
	note := "📎 " + noteText(name)
	if size > 0 {
		note += " (" + HumanBytes(size) + ")"
	}
	if reason = strings.TrimSpace(reason); reason != "" {
		note += ": " + noteText(reason)
	}
	return note
}

// AppendFileNotes appends attachment notes to a message's markdown. An empty
// body becomes the notes; otherwise they are a separate paragraph so a note
// cannot be read as part of the author's last sentence.
func AppendFileNotes(markdown string, notes []string) string {
	if len(notes) == 0 {
		return markdown
	}
	body := strings.TrimRight(markdown, "\n")
	if body == "" {
		return strings.Join(notes, "\n")
	}
	return body + "\n\n" + strings.Join(notes, "\n")
}

// noteText makes a file name or a reason inert in a platform's markdown: no
// angle-bracket controls (Slack), no link or table syntax, no code fences, and
// no invisible formatting. Keeping ordinary letters (including non-Latin
// ones) is the point; only the characters a text format gives meaning to are
// dropped.
func noteText(text string) string {
	text = StorableText(text, 300)
	text = strings.Map(func(r rune) rune {
		switch r {
		case '\n', '\r', '\t':
			return ' '
		case '<', '>', '&', '`', '[', ']', '|', '{', '}':
			return -1
		}
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || unicode.Is(unicode.Zl, r) || unicode.Is(unicode.Zp, r) {
			return -1
		}
		return r
	}, text)
	text = strings.Join(strings.Fields(text), " ")
	if text == "" {
		return "file"
	}
	return text
}

// HumanBytes renders a size for a note: "12.3 KB".
func HumanBytes(size int64) string {
	const unit = 1024
	if size < unit {
		return fmt.Sprintf("%d B", size)
	}
	div, exp := int64(unit), 0
	for n := size / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(size)/float64(div), "KMGTPE"[exp])
}

// FileURLSafe reports whether raw is an absolute https URL without credentials
// on one of the allowed hosts. An empty allowlist refuses every URL. A literal
// IP is refused even when it is listed: a host allowlist names services, and a
// cloud metadata address is never one. It is the board's second check: an
// adapter fetches only its own platform's URLs, and the board fetches only
// what an operator listed.
func FileURLSafe(raw string, hosts map[string]struct{}) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" || len(raw) > 2048 || len(hosts) == 0 {
		return false
	}
	parsed, err := url.Parse(raw)
	if err != nil || !strings.EqualFold(parsed.Scheme, "https") || parsed.Host == "" {
		return false
	}
	if parsed.User != nil {
		return false
	}
	if port := parsed.Port(); port != "" && port != "443" {
		return false
	}
	host := strings.ToLower(parsed.Hostname())
	if net.ParseIP(host) != nil {
		return false
	}
	_, allowed := hosts[host]
	return allowed
}

// FileFailureNote says why a file didn't come over without carrying a platform
// body or a URL. err must be a classified sync error or a transport failure.
func FileFailureNote(name string, size int64, err error) string {
	return FileNote(name, size, fileFailureReason(err))
}

// fileFailureReason classifies a fetch failure for a note.
func fileFailureReason(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return "timed out"
	case errors.Is(err, ErrUnsupported):
		return "not available to this connection"
	case errors.Is(err, ErrUnauthorized):
		return "not authorized to fetch it"
	case errors.Is(err, ErrNotFound):
		return "no longer on the platform"
	case errors.Is(err, ErrRateLimited):
		return "the platform is rate limiting file downloads"
	case errors.Is(err, ErrBadRequest):
		return "the platform refused the download"
	default:
		return "the download failed"
	}
}
