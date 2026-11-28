package conformance_test

import (
	"testing"

	"github.com/jixoai/keel-board/audit"
	"github.com/jixoai/keel-board/automation"
	"github.com/jixoai/keel-board/conformance"
	"github.com/jixoai/keel-board/store"
	"github.com/jixoai/keel-board/syncstore"
)

// TestStorageMemory proves the storage conformance suite is dialect-neutral:
// the in-memory adapters pass the same suite the PostgreSQL adapter runs. It
// declares no capabilities because the bundle's members differ (the log wakes
// waiters in-process; the others do not), and an undeclared capability must
// stay explicit instead of being assumed.
func TestStorageMemory(t *testing.T) {
	log := store.NewMemory()
	conformance.RunStorage(t, conformance.StorageOptions{
		Name: "memory",
		Store: func(t *testing.T) store.EventStore {
			return log
		},
		SyncStore: func(t *testing.T) syncstore.Store {
			memory, err := syncstore.NewMemory(syncstore.Config{})
			if err != nil {
				t.Fatalf("sync memory: %v", err)
			}
			return memory
		},
		Audit: func(t *testing.T) audit.Store { return audit.NewMemory() },
		Automation: func(t *testing.T) automation.Store {
			memory, err := automation.NewMemory(automation.Config{})
			if err != nil {
				t.Fatalf("automation memory: %v", err)
			}
			return memory
		},
	})
}
