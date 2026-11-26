// Package slack is the Slack adapter skeleton: signature verification, webhook
// normalization, and chat.postMessage-style calls, with no real network in
// tests and no Vercel Connect dependency.
package slack

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
)

// SignatureHeader is Slack's signature header.
const SignatureHeader = "X-Slack-Signature"

// TimestampHeader is Slack's request timestamp header, part of the signed
// payload.
const TimestampHeader = "X-Slack-Request-Timestamp"

// DefaultTolerance is how far a request's timestamp may be from now. Slack
// itself recommends five minutes.
const DefaultTolerance = 5 * time.Minute

// Sign returns the `v0=` signature for a timestamp and body.
func Sign(secret []byte, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte("v0:"))
	mac.Write([]byte(timestamp))
	mac.Write([]byte(":"))
	mac.Write(body)
	return "v0=" + hex.EncodeToString(mac.Sum(nil))
}

// VerifySignature checks Slack's signing secret. timestamp is the request's
// X-Slack-Request-Timestamp and signature its X-Slack-Signature. A timestamp
// outside tolerance is refused, so a captured request can't be replayed
// forever.
func VerifySignature(secret []byte, timestamp, signature string, body []byte, now time.Time, tolerance time.Duration) error {
	if len(secret) == 0 {
		return fmt.Errorf("%w: slack signing secret is unset", boardsync.ErrUnauthorized)
	}
	if signature == "" || timestamp == "" {
		return fmt.Errorf("%w: slack request has no signature or timestamp", boardsync.ErrUnauthorized)
	}
	seconds, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return fmt.Errorf("%w: slack timestamp isn't a number", boardsync.ErrUnauthorized)
	}
	if tolerance <= 0 {
		tolerance = DefaultTolerance
	}
	skew := now.Sub(time.Unix(seconds, 0))
	if skew < 0 {
		skew = -skew
	}
	if skew > tolerance {
		return fmt.Errorf("%w: slack request timestamp is outside the %s window", boardsync.ErrUnauthorized, tolerance)
	}
	expected := Sign(secret, timestamp, body)
	if !hmac.Equal([]byte(expected), []byte(signature)) {
		return fmt.Errorf("%w: slack signature didn't verify", boardsync.ErrUnauthorized)
	}
	return nil
}

// verifyRequest reads Slack's headers and verifies the request.
func verifyRequest(secret boardsync.Secret, req *boardsync.WebhookRequest, now time.Time, tolerance time.Duration) error {
	if secret.Empty() {
		return fmt.Errorf("%w: slack signing secret is unset", boardsync.ErrUnauthorized)
	}
	if req == nil || req.Header == nil {
		return fmt.Errorf("%w: slack webhook has no headers", boardsync.ErrUnauthorized)
	}
	return VerifySignature([]byte(secret.Reveal()), req.Header.Get(TimestampHeader), req.Header.Get(SignatureHeader), req.Body, now, tolerance)
}

// isSlackUserID reports whether id is a Slack user ID: U… or Enterprise Grid's
// W…. A bot's B… ID isn't a user.
func isSlackUserID(id string) bool {
	if len(id) < 2 || (id[0] != 'U' && id[0] != 'W') {
		return false
	}
	for _, c := range []byte(id[1:]) {
		if (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '_' {
			return false
		}
	}
	return true
}

// slackMention renders a board mention of a Slack user as `<@U0456>`. Only an
// ID is enough; anything else stays text, so a mention can't close and open
// Slack markup.
func slackMention(actor string) (string, bool) {
	const prefix = "slack:user:"
	if !strings.HasPrefix(actor, prefix) {
		return "", false
	}
	id := strings.TrimPrefix(actor, prefix)
	if !isSlackUserID(id) {
		return "", false
	}
	return "<@" + id + ">", true
}

var errMalformed = fmt.Errorf("%w: malformed slack payload", boardsync.ErrBadRequest)

func decodeJSON(body []byte, target any) error {
	if len(body) == 0 {
		return errMalformed
	}
	if err := jsonUnmarshal(body, target); err != nil {
		return fmt.Errorf("%w: %v", boardsync.ErrBadRequest, err)
	}
	return nil
}

// jsonUnmarshal is a seam so tests can keep encoding/json's errors out of the
// adapter's API.
func jsonUnmarshal(body []byte, target any) error { return json.Unmarshal(body, target) }
