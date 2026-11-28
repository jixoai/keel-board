package pglogstore

import "github.com/jixoai/keel-board/storage"

// Capabilities implements storage.Reporter. The log is durable and shared
// between processes, and the adapter owns its schema. Wait polls the head
// instead of using LISTEN/NOTIFY.
func (p *LogStore) Capabilities() storage.Capabilities {
	return storage.Capabilities{
		Durable:            true,
		CrossProcessLeases: true,
		Migrations:         true,
		ListenNotify:       false,
	}
}
