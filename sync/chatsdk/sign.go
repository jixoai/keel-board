package chatsdk

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
)

// DefaultWindow is the timestamp skew and nonce replay window: five minutes,
// the same window the native webhook adapters use.
const DefaultWindow = 5 * time.Minute

// SignV1 returns the v1 signature for a request:
// HMAC-SHA256(secret, timestamp + nonce + body), lowercase hex after "v1=".
func SignV1(secret boardsync.Secret, timestamp int64, nonce string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret.Reveal()))
	mac.Write([]byte(strconv.FormatInt(timestamp, 10)))
	mac.Write([]byte(nonce))
	mac.Write(body)
	return SignatureScheme + "=" + hex.EncodeToString(mac.Sum(nil))
}

// VerifyV1 checks the timestamp, nonce shape, and signature of a bridge
// request. The nonce's uniqueness is the durable store's job; this function is
// stateless. A missing, malformed, stale, or future timestamp and a malformed
// nonce are all ErrUnauthorized, as is a bad signature, so a caller learns
// only that the request didn't authenticate.
func VerifyV1(secret boardsync.Secret, timestampHeader, nonceHeader, signatureHeader string, body []byte, now time.Time, window time.Duration) error {
	if secret.Empty() {
		return fmt.Errorf("%w: bridge secret is unset", boardsync.ErrUnauthorized)
	}
	if window <= 0 {
		window = DefaultWindow
	}
	seconds, err := strconv.ParseInt(strings.TrimSpace(timestampHeader), 10, 64)
	if err != nil || seconds <= 0 {
		return fmt.Errorf("%w: %s isn't a Unix second", boardsync.ErrUnauthorized, HeaderTimestamp)
	}
	skew := now.Sub(time.Unix(seconds, 0))
	if skew < 0 {
		skew = -skew
	}
	if skew > window {
		return fmt.Errorf("%w: bridge timestamp is outside the %s window", boardsync.ErrUnauthorized, window)
	}
	if err := validNonce(nonceHeader); err != nil {
		return fmt.Errorf("%w: %v", boardsync.ErrUnauthorized, err)
	}
	prefix := SignatureScheme + "="
	if !strings.HasPrefix(signatureHeader, prefix) {
		return fmt.Errorf("%w: bridge signature isn't %s", boardsync.ErrUnauthorized, prefix)
	}
	got, err := hex.DecodeString(strings.TrimPrefix(signatureHeader, prefix))
	if err != nil || len(got) != sha256.Size {
		return fmt.Errorf("%w: bridge signature is malformed", boardsync.ErrUnauthorized)
	}
	mac := hmac.New(sha256.New, []byte(secret.Reveal()))
	mac.Write([]byte(strconv.FormatInt(seconds, 10)))
	mac.Write([]byte(nonceHeader))
	mac.Write(body)
	if !hmac.Equal(got, mac.Sum(nil)) {
		return fmt.Errorf("%w: bridge signature didn't verify", boardsync.ErrUnauthorized)
	}
	return nil
}

// validNonce accepts the visible ASCII range without spaces: a header value
// must survive transport byte-for-byte, and a nonce is not user text.
func validNonce(nonce string) error {
	if len(nonce) < MinNonceBytes || len(nonce) > MaxNonceBytes {
		return fmt.Errorf("nonce must be %d to %d bytes", MinNonceBytes, MaxNonceBytes)
	}
	for i := 0; i < len(nonce); i++ {
		if nonce[i] < 0x21 || nonce[i] > 0x7e {
			return fmt.Errorf("nonce has a non-printable or whitespace byte")
		}
	}
	return nil
}

// VerifyRequestV1 verifies the bridge headers of one inbound HTTP request's
// header map. It is a small seam so both the board handler and a sidecar can
// share the exact check.
func VerifyRequestV1(secret boardsync.Secret, header http.Header, body []byte, now time.Time, window time.Duration) error {
	return VerifyV1(secret, header.Get(HeaderTimestamp), header.Get(HeaderNonce), header.Get(HeaderSignature), body, now, window)
}
