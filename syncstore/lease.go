package syncstore

import (
	"crypto/rand"
	"encoding/base64"
	"hash/fnv"
)

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

// advisoryLockKey is the per-tenant/per-operation PostgreSQL advisory lock a
// lease transaction takes. Leasing is a read-then-write and must be serialized
// across workers; a lock per tenant and lane kind keeps unrelated keys and
// tenants parallel. A collision only serializes two unrelated leases.
func advisoryLockKey(parts ...string) int64 {
	hash := fnv.New64a()
	for _, part := range parts {
		_, _ = hash.Write([]byte(part))
		_, _ = hash.Write([]byte{0})
	}
	return int64(hash.Sum64())
}
