// Package linear is the Linear adapter skeleton: webhook signature
// verification, issue/comment normalization, and GraphQL calls.
package linear

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
)

// SignatureHeader is Linear's HMAC header.
const SignatureHeader = "Linear-Signature"

// LegacySignatureHeader is the header's older name.
const LegacySignatureHeader = "X-Linear-Signature"

// DeliveryHeader is Linear's webhook delivery ID.
const DeliveryHeader = "Linear-Delivery"

// TimestampHeader is Linear's send-time header, in milliseconds. It is not
// signed; the body's webhookTimestamp is authoritative when present.
const TimestampHeader = "Linear-Timestamp"

// DefaultTolerance bounds a webhook's own timestamp. Linear sends
// webhookTimestamp in milliseconds.
const DefaultTolerance = 5 * time.Minute

// Sign returns the hex HMAC-SHA256 of body.
func Sign(secret []byte, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// VerifySignature checks Linear-Signature (or X-Linear-Signature) against the
// raw body. Both sides are decoded first, so the comparison is over the MAC
// bytes and constant time; uppercase hex is accepted as long as it names the
// right MAC.
func VerifySignature(secret []byte, signature string, body []byte) error {
	if len(secret) == 0 {
		return fmt.Errorf("%w: linear signing secret is unset", boardsync.ErrUnauthorized)
	}
	if signature == "" {
		return fmt.Errorf("%w: linear request has no signature", boardsync.ErrUnauthorized)
	}
	provided, err := hex.DecodeString(signature)
	if err != nil || len(provided) != sha256.Size {
		return fmt.Errorf("%w: linear signature is malformed", boardsync.ErrUnauthorized)
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	if !hmac.Equal(mac.Sum(nil), provided) {
		return fmt.Errorf("%w: linear signature didn't verify", boardsync.ErrUnauthorized)
	}
	return nil
}

// VerifyTimestamp checks the payload's webhookTimestamp, in milliseconds,
// against now. A zero timestamp is allowed for older payloads.
func VerifyTimestamp(timestamp int64, now time.Time, tolerance time.Duration) error {
	if timestamp == 0 {
		return nil
	}
	if tolerance <= 0 {
		tolerance = DefaultTolerance
	}
	at := time.UnixMilli(timestamp)
	skew := now.Sub(at)
	if skew < 0 {
		skew = -skew
	}
	if skew > tolerance {
		return fmt.Errorf("%w: linear webhook is outside the %s window", boardsync.ErrUnauthorized, tolerance)
	}
	return nil
}

func signatureFromHeaders(get func(string) string) string {
	if value := get(SignatureHeader); value != "" {
		return value
	}
	return get(LegacySignatureHeader)
}

// timestampFromHeaders reads Linear-Timestamp, in milliseconds (a seconds
// value from an older sender is accepted too). The header is unsigned, so it
// is only a fallback when the signed body omitted webhookTimestamp.
func timestampFromHeaders(get func(string) string) int64 {
	raw := get(TimestampHeader)
	if raw == "" {
		return 0
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		return 0
	}
	if value < 1_000_000_000_000 { // seconds, not milliseconds
		value *= 1000
	}
	return value
}
