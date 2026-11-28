package pgadapter_test

import (
	"os/exec"
	"strings"
	"testing"
)

// TestStorageBoundary proves the domain, server and sync packages depend on the
// storage interfaces, not on the PostgreSQL driver. The PostgreSQL adapter is
// the only production dependency on pgx; this test fails if a domain import
// starts pulling it in again.
func TestStorageBoundary(t *testing.T) {
	domain := []string{
		"github.com/jixoai/keel-board/store",
		"github.com/jixoai/keel-board/syncstore",
		"github.com/jixoai/keel-board/audit",
		"github.com/jixoai/keel-board/automation",
		"github.com/jixoai/keel-board/server",
		"github.com/jixoai/keel-board/sync",
		"github.com/jixoai/keel-board/sync/github",
		"github.com/jixoai/keel-board/sync/linear",
		"github.com/jixoai/keel-board/sync/slack",
		"github.com/jixoai/keel-board/sync/chatsdk",
		"github.com/jixoai/keel-board/log",
		"github.com/jixoai/keel-board/board",
		"github.com/jixoai/keel-board/protocol",
		"github.com/jixoai/keel-board/access",
		"github.com/jixoai/keel-board/blob",
		"github.com/jixoai/keel-board/metrics",
		"github.com/jixoai/keel-board/conformance",
	}
	for _, pkg := range domain {
		out, err := exec.Command("go", "list", "-deps", pkg).Output()
		if err != nil {
			t.Fatalf("go list -deps %s: %v", pkg, err)
		}
		for _, line := range strings.Split(string(out), "\n") {
			if strings.Contains(line, "github.com/jackc/pgx") {
				t.Errorf("%s depends on %s; the PostgreSQL driver belongs only in pgadapter", pkg, line)
			}
		}
	}

	// The adapter itself must still be the package that owns pgx.
	out, err := exec.Command("go", "list", "-deps", "github.com/jixoai/keel-board/pgadapter").Output()
	if err != nil {
		t.Fatalf("go list -deps pgadapter: %v", err)
	}
	if !strings.Contains(string(out), "github.com/jackc/pgx/v5") {
		t.Fatal("pgadapter no longer depends on pgx; the boundary moved without updating this proof")
	}
}
