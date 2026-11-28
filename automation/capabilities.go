package automation

import "github.com/jixoai/keel-board/storage"

// Capabilities implements storage.Reporter. The in-memory automation store is
// not durable, is not shared between processes, owns no schema, and has no push
// wakeups.
func (m *Memory) Capabilities() storage.Capabilities {
	return storage.Capabilities{}
}
