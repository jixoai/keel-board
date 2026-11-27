package main

import (
	"context"
	"strings"
	"testing"

	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/store"
)

// TestRebuildNeedsDSN checks the command refuses to pretend an in-memory
// projection is a durable rebuild.
func TestRebuildNeedsDSN(t *testing.T) {
	err := rebuild(nil)
	if err == nil || !strings.Contains(err.Error(), "--dsn") {
		t.Fatalf("rebuild without a DSN: %v", err)
	}
}

// TestRebuildLogReplaysEveryTenant proves the offline rebuild is idempotent,
// scoped per tenant, and does not write events.
func TestRebuildLogReplaysEveryTenant(t *testing.T) {
	ctx := context.Background()
	events := store.NewMemory()
	board := log.New(log.Options{Store: events})
	author := protocol.Author{Actor: "local:user:alice", Principal: "local:user:alice"}
	for _, tenant := range []string{"a", "b"} {
		for i := 0; i < 2; i++ {
			err := board.Update(ctx, tenant, log.UpdateOptions{}, func(tx *log.Tx) error {
				id := tx.NextID(0)
				tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
					ID: id, Channel: "work", Text: "hello", Thread: log.ThreadKey(id),
					By: author, Labels: map[string]string{},
				})
				tx.RecordResult(id)
				return nil
			})
			if err != nil {
				t.Fatalf("seeding %s: %v", tenant, err)
			}
		}
	}
	before, err := events.Load(ctx, "a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	reports, err := rebuildLog(ctx, events, []string{"a", "b"})
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if len(reports) != 2 {
		t.Fatalf("reports: %+v", reports)
	}
	digests := make(map[string]string, 2)
	for _, report := range reports {
		if !report.Consistent {
			t.Fatalf("tenant %s: %+v", report.Tenant, report)
		}
		if report.Digest == "" || report.Digest != report.ReplayDigest || report.Digest != report.StoreDigest {
			t.Fatalf("tenant %s digests: %+v", report.Tenant, report)
		}
		digests[report.Tenant] = report.Digest
	}

	// A second run is the same rebuild: same digests, same log.
	again, err := rebuildLog(ctx, events, []string{"a", "b"})
	if err != nil {
		t.Fatalf("second rebuild: %v", err)
	}
	for _, report := range again {
		if !report.Consistent || report.Digest != digests[report.Tenant] {
			t.Fatalf("tenant %s isn't idempotent: %+v", report.Tenant, report)
		}
	}
	after, err := events.Load(ctx, "a")
	if err != nil {
		t.Fatalf("load after: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("a rebuild changed tenant a's log: %d -> %d", len(before), len(after))
	}

	// Selecting one tenant leaves the other out of the report.
	one, err := rebuildLog(ctx, events, []string{"a"})
	if err != nil || len(one) != 1 || one[0].Tenant != "a" {
		t.Fatalf("one tenant: %+v, %v", one, err)
	}
}

// TestStringListFlag checks the repeatable --tenant flag refuses an empty
// value rather than rebuilding the default tenant by accident.
func TestStringListFlag(t *testing.T) {
	var list stringList
	if err := list.Set("a"); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := list.Set("b"); err != nil {
		t.Fatalf("set: %v", err)
	}
	if got := list.String(); got != "a,b" {
		t.Fatalf("list: %q", got)
	}
	if err := list.Set("  "); err == nil {
		t.Fatal("an empty tenant should be refused")
	}
}
