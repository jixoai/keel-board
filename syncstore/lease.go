package syncstore

import (
	"crypto/rand"
	"encoding/base64"
)

// NewLeaseToken returns an unguessable lease token for a storage adapter.
func NewLeaseToken() (string, error) { return newLeaseToken() }

// newLeaseToken returns an unguessable lease token. The token is an ack
// credential for one row while it is leased; it is not a platform secret and
// is never logged.
func newLeaseToken() (string, error) {
	var raw [24]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}
