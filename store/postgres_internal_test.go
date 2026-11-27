package store

import "testing"

// TestMigrationLockIDIsSchemaDerived keeps migrations on different schemas
// from serializing on one global advisory lock.
func TestMigrationLockIDIsSchemaDerived(t *testing.T) {
	if migrationLockID("") != defaultMigrationLockID {
		t.Fatalf("an empty schema should use the default lock")
	}
	first := migrationLockID("keel_board_a")
	if first != migrationLockID("keel_board_a") {
		t.Fatalf("the same schema should derive the same lock")
	}
	if first == migrationLockID("keel_board_b") {
		t.Fatalf("two schemas should derive different locks")
	}
}
