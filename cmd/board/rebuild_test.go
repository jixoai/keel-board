package main

import (
	"context"
	"errors"
	"fmt"
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

// failingTenantStore fails one tenant's Load, as an unreadable log would.
type failingTenantStore struct {
	*store.Memory
	fail string
}

func (s *failingTenantStore) Load(ctx context.Context, tenant string) ([]store.Record, error) {
	if tenant == s.fail {
		return nil, errors.New("injected load failure")
	}
	return s.Memory.Load(ctx, tenant)
}

// TestRebuildLogReturnsPartialReports proves a later tenant's failure still
// leaves the earlier rebuilt results available to print.
func TestRebuildLogReturnsPartialReports(t *testing.T) {
	ctx := context.Background()
	memory := store.NewMemory()
	board := log.New(log.Options{Store: memory})
	author := protocol.Author{Actor: "local:user:alice", Principal: "local:user:alice"}
	if err := board.Update(ctx, "a", log.UpdateOptions{}, func(tx *log.Tx) error {
		id := tx.NextID(0)
		tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
			ID: id, Channel: "work", Text: "hello", Thread: log.ThreadKey(id),
			By: author, Labels: map[string]string{},
		})
		return nil
	}); err != nil {
		t.Fatalf("seeding: %v", err)
	}

	reports, err := rebuildLog(ctx, &failingTenantStore{Memory: memory, fail: "b"}, []string{"a", "b"})
	if err == nil {
		t.Fatal("an unreadable tenant should fail the rebuild")
	}
	if len(reports) != 1 || reports[0].Tenant != "a" || !reports[0].Consistent {
		t.Fatalf("partial reports: %+v", reports)
	}
}

// gapStore serves a log with a missing seq, so it cannot replay.
type gapStore struct{}

func (gapStore) Load(context.Context, string) ([]store.Record, error) {
	record := func(seq uint64) store.Record {
		return store.Record{
			Seq: seq, Type: string(log.EventMessagePosted), Channel: "work",
			Payload: []byte(fmt.Sprintf(`{"id":%d,"channel":"work","text":"one","thread":"1","labels":{},"by":{"actor":"local:user:alice","principal":"local:user:alice"}}`, seq)),
		}
	}
	return []store.Record{record(1), record(3)}, nil
}

func (gapStore) Append(context.Context, string, []store.Record, uint64) (store.AppendResult, error) {
	return store.AppendResult{}, store.ErrLogInconsistent
}

// TestRebuildLogReportsABrokenLog proves a gap is classified as an
// inconsistent log and still yields a per-tenant report with the reason.
func TestRebuildLogReportsABrokenLog(t *testing.T) {
	reports, err := rebuildLog(context.Background(), gapStore{}, []string{"broken"})
	if !errors.Is(err, log.ErrLogInconsistent) {
		t.Fatalf("gap rebuild error: %v", err)
	}
	if len(reports) != 1 || reports[0].Tenant != "broken" || len(reports[0].Problems) == 0 {
		t.Fatalf("broken-log report: %+v", reports)
	}
}
