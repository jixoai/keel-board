package pgadapter_test

import (
	"os/exec"
	"strings"
	"testing"
)

// TestStorageBoundary proves every non-adapter package in the module depends on
// the storage interfaces, not on the PostgreSQL driver. The package list is
// discovered, not hand-maintained, so a new subpackage cannot slip past it.
// `cmd` and `scripts` are deployment/test harnesses, not the domain; they may
// assemble the adapter directly and are excluded with the adapter itself.
func TestStorageBoundary(t *testing.T) {
	out, err := exec.Command("go", "list", "github.com/jixoai/keel-board/...").Output()
	if err != nil {
		t.Fatalf("go list module packages: %v", err)
	}
	packages := strings.Fields(string(out))
	if len(packages) < 10 {
		t.Fatalf("discovered only %d packages; the module list looks wrong", len(packages))
	}
	for _, pkg := range packages {
		if pkg == "github.com/jixoai/keel-board/pgadapter" ||
			strings.HasPrefix(pkg, "github.com/jixoai/keel-board/pgadapter/") ||
			strings.Contains(pkg, "/cmd/") ||
			strings.Contains(pkg, "/scripts/") {
			continue
		}
		deps, err := exec.Command("go", "list", "-deps", pkg).Output()
		if err != nil {
			t.Fatalf("go list -deps %s: %v", pkg, err)
		}
		for _, line := range strings.Split(string(deps), "\n") {
			if strings.Contains(line, "github.com/jackc/pgx") {
				t.Errorf("%s depends on %s; the PostgreSQL driver belongs only in pgadapter", pkg, line)
			}
		}
	}

	// The adapter itself must still be the package that owns pgx.
	out, err = exec.Command("go", "list", "-deps", "github.com/jixoai/keel-board/pgadapter").Output()
	if err != nil {
		t.Fatalf("go list -deps pgadapter: %v", err)
	}
	if !strings.Contains(string(out), "github.com/jackc/pgx/v5") {
		t.Fatal("pgadapter no longer depends on pgx; the boundary moved without updating this proof")
	}
}
