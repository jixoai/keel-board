// Package linear is the Linear adapter skeleton: webhook signature
// verification, issue/comment normalization, and GraphQL calls.
package linear

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
)

// SignatureHeader is Linear's HMAC header.
const SignatureHeader = "Linear-Signature"

// LegacySignatureHeader is the header's older name.
const LegacySignatureHeader = "X-Linear-Signature"

// DeliveryHeader is Linear's webhook delivery ID.
const DeliveryHeader = "Linear-Delivery"

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
// raw body.
func VerifySignature(secret []byte, signature string, body []byte) error {
	if len(secret) == 0 {
		return fmt.Errorf("%w: linear signing secret is unset", boardsync.ErrUnauthorized)
	}
	if signature == "" {
		return fmt.Errorf("%w: linear request has no signature", boardsync.ErrUnauthorized)
	}
	expected := Sign(secret, body)
	if !hmac.Equal([]byte(expected), []byte(signature)) {
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
