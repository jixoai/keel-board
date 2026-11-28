package store

import "github.com/jixoai/keel-board/storage"

// Capabilities implements storage.Reporter. The in-memory log is not durable,
// is not shared between processes, owns no schema, and wakes waiters in-process
// rather than through a database notification.
func (m *Memory) Capabilities() storage.Capabilities {
	return storage.Capabilities{ListenNotify: true}
}
