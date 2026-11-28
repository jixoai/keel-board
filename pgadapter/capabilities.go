package pgadapter

import "github.com/jixoai/keel-board/storage"

// postgresCapabilities is what every PostgreSQL adapter in this package
// guarantees. The adapter owns its schema and its leases are serialized with
// advisory locks, so both are cross-process safe. Wait polls the head at
// waitPoll instead of using LISTEN/NOTIFY, so ListenNotify stays false rather
// than claiming a push wakeup the adapter does not have.
var postgresCapabilities = storage.Capabilities{
	Durable:            true,
	CrossProcessLeases: true,
	Migrations:         true,
	ListenNotify:       false,
}

// Capabilities implements storage.Reporter.
func (p *SyncStore) Capabilities() storage.Capabilities { return postgresCapabilities }

// Capabilities implements storage.Reporter.
func (p *AuditStore) Capabilities() storage.Capabilities { return postgresCapabilities }

// Capabilities implements storage.Reporter.
func (p *Automation) Capabilities() storage.Capabilities { return postgresCapabilities }
