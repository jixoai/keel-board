// Package github is the GitHub adapter: webhook signature verification,
// issue/PR/comment/review/CI normalization, and REST calls.
package github

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	boardsync "github.com/jixoai/keel-board/sync"
)

// SignatureHeader is GitHub's HMAC header.
const SignatureHeader = "X-Hub-Signature-256"

// EventHeader names the webhook event.
const EventHeader = "X-GitHub-Event"

// DeliveryHeader is GitHub's unique delivery ID.
const DeliveryHeader = "X-GitHub-Delivery"

// Sign returns "sha256=" plus the hex HMAC-SHA256 of body.
func Sign(secret []byte, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// VerifySignature checks X-Hub-Signature-256 against the raw body. A sha1
// signature is refused: this adapter only accepts GitHub's current header.
func VerifySignature(secret []byte, signature string, body []byte) error {
	if len(secret) == 0 {
		return fmt.Errorf("%w: github webhook secret is unset", boardsync.ErrUnauthorized)
	}
	if signature == "" {
		return fmt.Errorf("%w: github request has no signature", boardsync.ErrUnauthorized)
	}
	if strings.HasPrefix(signature, "sha1=") {
		return fmt.Errorf("%w: github sha1 signatures are not accepted", boardsync.ErrUnauthorized)
	}
	if !strings.HasPrefix(signature, "sha256=") {
		return fmt.Errorf("%w: github signature has no sha256= prefix", boardsync.ErrUnauthorized)
	}
	expected := Sign(secret, body)
	if !hmac.Equal([]byte(expected), []byte(signature)) {
		return fmt.Errorf("%w: github signature didn't verify", boardsync.ErrUnauthorized)
	}
	return nil
}
