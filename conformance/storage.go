// Storage conformance is the dialect-neutral side of the storage boundary: it
// asserts the append log, sync leases and health, automation leases and audit
// through the domain interfaces only. It imports no database driver, so a new
// dialect (PostgreSQL today) opts in by supplying factories and declares its
// capabilities.
package conformance

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jixoai/keel-board/audit"
	"github.com/jixoai/keel-board/automation"
	"github.com/jixoai/keel-board/storage"
	"github.com/jixoai/keel-board/store"
	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/syncstore"
)

// StorageOptions configure one storage conformance run. Every factory returns
// a handle to the same durable backend; the suite creates its own tenants.
type StorageOptions struct {
	// Name labels the implementation in subtest names. Required.
	Name string
	// Store is the board log. Required.
	Store func(t *testing.T) store.EventStore
	// SyncStore is the sync entry, inbox and outbox store. Required.
	SyncStore func(t *testing.T) syncstore.Store
	// Audit is the operational audit store. Required.
	Audit func(t *testing.T) audit.Store
	// Automation is the watcher and delivery store. Required.
	Automation func(t *testing.T) automation.Store
	// Capabilities, when set, is what every adapter in this bundle must
	// declare. The suite does not assume a backend has a capability it does
	// not report.
	Capabilities *storage.Capabilities
	// Reporters are the adapters whose Capabilities method is checked against
	// Capabilities. Optional.
	Reporters []storage.Reporter
}

var storageTenantSeq atomic.Uint64

func storageTenant() string {
	return fmt.Sprintf("conf%d_%d", time.Now().UnixNano(), storageTenantSeq.Add(1))
}

// RunStorage runs the storage contract over opts. It fails on the first
// contract violation.
func RunStorage(t *testing.T, opts StorageOptions) {
	t.Helper()
	if opts.Store == nil || opts.SyncStore == nil || opts.Audit == nil || opts.Automation == nil {
		t.Fatal("conformance: all four storage factories are required")
	}
	if opts.Name == "" {
		opts.Name = "storage"
	}
	t.Run(opts.Name+"/capabilities_are_explicit", func(t *testing.T) { testStorageCapabilities(t, opts) })
	t.Run(opts.Name+"/append_log_contract", func(t *testing.T) {
		Run(t, Options{Name: opts.Name + "/log", Store: opts.Store})
	})
	t.Run(opts.Name+"/sync_lease_and_health", func(t *testing.T) { testStorageSync(t, opts) })
	t.Run(opts.Name+"/automation_lease", func(t *testing.T) { testStorageAutomation(t, opts) })
	t.Run(opts.Name+"/audit", func(t *testing.T) { testStorageAudit(t, opts) })
}

func testStorageCapabilities(t *testing.T, opts StorageOptions) {
	t.Helper()
	// The zero value must refuse every capability with a typed error, so an
	// adapter that forgets to declare one cannot silently look capable.
	zero := storage.Capabilities{}
	for _, capability := range []storage.Capability{
		storage.Durable, storage.CrossProcessLeases, storage.Migrations, storage.ListenNotify,
	} {
		if err := zero.Require(capability); !errors.Is(err, storage.ErrUnsupported) {
			t.Fatalf("zero capabilities Require(%s) = %v, want ErrUnsupported", capability, err)
		}
	}
	if opts.Capabilities == nil {
		return
	}
	for _, capability := range []storage.Capability{
		storage.Durable, storage.CrossProcessLeases, storage.Migrations, storage.ListenNotify,
	} {
		err := opts.Capabilities.Require(capability)
		if opts.Capabilities.Supports(capability) {
			if err != nil {
				t.Fatalf("declared capability %s was refused: %v", capability, err)
			}
			continue
		}
		if !errors.Is(err, storage.ErrUnsupported) {
			t.Fatalf("undeclared capability %s = %v, want ErrUnsupported", capability, err)
		}
	}
	for index, reporter := range opts.Reporters {
		if got := reporter.Capabilities(); got != *opts.Capabilities {
			t.Fatalf("reporter #%d declares %+v, want %+v", index, got, *opts.Capabilities)
		}
	}
}

func testStorageSync(t *testing.T, opts StorageOptions) {
	t.Helper()
	ctx := t.Context()
	ss := opts.SyncStore(t)
	tenant := storageTenant()
	other := storageTenant()
	key := boardsync.Key("slack")

	entry, err := ss.UpsertEntry(ctx, syncstore.Entry{Tenant: tenant, Key: key, Platform: "slack", Enabled: true})
	if err != nil || entry.Key != key || !entry.Enabled {
		t.Fatalf("upsert entry = %+v, %v", entry, err)
	}
	if _, ok, err := ss.Entry(ctx, other, key); err != nil || ok {
		t.Fatalf("another tenant reads the entry: %v, %v", ok, err)
	}

	record := boardsync.Record{Dedupe: "dedupe-1", Event: boardsync.Event{
		ID: "m1", Key: key, Platform: "slack", Kind: boardsync.KindMessage,
		Type: boardsync.TypeMessage, Channel: "C0123", Thread: "C0123:T1",
		Message: "m1", Text: "hello", Author: boardsync.Author{ID: "slack:user:U1", Handle: "ada", Name: "Ada"},
	}}
	pushed, err := ss.PushInbox(ctx, tenant, key, []boardsync.Record{record})
	if err != nil || pushed.Added != 1 {
		t.Fatalf("push inbox = %+v, %v", pushed, err)
	}
	items, err := ss.LeaseInbox(ctx, tenant, key, 1, "worker-a")
	if err != nil || len(items) != 1 || items[0].ID == "" || items[0].Lease == "" {
		t.Fatalf("lease inbox = %+v, %v", items, err)
	}
	if acked, err := ss.AckInbox(ctx, tenant, key, items[0].ID, items[0].Lease, nil); err != nil || !acked.Acked {
		t.Fatalf("ack inbox = %+v, %v", acked, err)
	}
	if depth, err := ss.InboxDepth(ctx, tenant, key); err != nil || depth.Pending != 0 || depth.Done != 1 {
		t.Fatalf("inbox depth after ack = %+v, %v", depth, err)
	}
	if depth, err := ss.InboxDepth(ctx, other, key); err != nil || depth.Pending+depth.Leased+depth.Done+depth.Dead != 0 {
		t.Fatalf("another tenant's inbox depth = %+v, %v", depth, err)
	}

	target := boardsync.ResolvedTarget{
		Key: key, Platform: "slack", Channel: "C0123", Thread: "C0123:T1", Kind: boardsync.TargetThread,
	}
	if _, err := ss.UpsertThread(ctx, syncstore.Thread{
		Tenant: tenant, BoardThread: 100, Key: key, Platform: "slack",
		Channel: "C0123", Thread: "C0123:T1", Kind: boardsync.TargetThread,
	}); err != nil {
		t.Fatalf("upsert thread: %v", err)
	}
	event := boardsync.BoardEvent{ID: 1, Type: boardsync.BoardMessagePosted, Thread: 100}
	message := boardsync.Message{
		ID: 100, Thread: 100, Channel: "support", Text: "root",
		Actor: "local:user:alice", ActorName: "Alice", Verified: true,
	}
	enqueued, err := ss.EnqueueOutbox(ctx, tenant, key, event, message, []boardsync.ResolvedTarget{target})
	if err != nil || enqueued.Added != 1 {
		t.Fatalf("enqueue outbox = %+v, %v", enqueued, err)
	}
	if lag, err := ss.OldestPending(ctx, tenant, key); err != nil || lag.Outbox == nil || lag.Inbox != nil {
		t.Fatalf("lag with one pending outbox = %+v, %v", lag, err)
	}
	jobs, err := ss.LeaseOutbox(ctx, tenant, []boardsync.Key{key}, 1, "worker-a")
	if err != nil || len(jobs) != 1 || jobs[0].Lease == "" {
		t.Fatalf("lease outbox = %+v, %v", jobs, err)
	}
	// Cross-process leases are an operation, not a label: a second handle must
	// not hand out the job the first one holds when the adapter declares the
	// capability. A bundle that does not declare it is refused by Require.
	if opts.Capabilities != nil {
		if opts.Capabilities.CrossProcessLeases {
			second := opts.SyncStore(t)
			if again, err := second.LeaseOutbox(ctx, tenant, []boardsync.Key{key}, 1, "worker-b"); err != nil || len(again) != 0 {
				t.Fatalf("second handle leased a live job = %+v, %v", again, err)
			}
		} else if err := opts.Capabilities.Require(storage.CrossProcessLeases); !errors.Is(err, storage.ErrUnsupported) {
			t.Fatalf("undeclared cross-process lease = %v, want ErrUnsupported", err)
		}
	}
	if acked, err := ss.AckOutbox(ctx, tenant, jobs[0].ID, jobs[0].Lease, nil, nil); err != nil || !acked.Acked {
		t.Fatalf("ack outbox = %+v, %v", acked, err)
	}
	// No outstanding rows is null on both queues, never a zero timestamp.
	if lag, err := ss.OldestPending(ctx, tenant, key); err != nil || lag.Inbox != nil || lag.Outbox != nil {
		t.Fatalf("empty lag = %+v, %v; want null members", lag, err)
	}
	if targets, err := ss.Targets(ctx, tenant, key, 10); err != nil || len(targets) != 1 {
		t.Fatalf("targets = %+v, %v", targets, err)
	} else if targets[0].Target == "" || targets[0].Undelivered != 0 {
		t.Fatalf("target health = %+v", targets[0])
	}
	if failures, err := ss.Failures24h(ctx, tenant, key, time.Now().Add(-24*time.Hour)); err != nil || failures != 0 {
		t.Fatalf("failures24h = %d, %v", failures, err)
	}
	if targets, err := ss.Targets(ctx, other, key, 10); err != nil || len(targets) != 0 {
		t.Fatalf("another tenant's targets = %+v, %v", targets, err)
	}

	if err := ss.SetCursor(ctx, tenant, key, "events", "cursor-1"); err != nil {
		t.Fatalf("set cursor: %v", err)
	}
	if cursor, ok, err := ss.Cursor(ctx, tenant, key, "events"); err != nil || !ok || cursor != "cursor-1" {
		t.Fatalf("cursor = %q, %v, %v", cursor, ok, err)
	}
	if claimed, err := ss.ClaimNonce(ctx, tenant, key, "nonce-1", time.Minute); err != nil || !claimed {
		t.Fatalf("claim nonce = %v, %v", claimed, err)
	} else if again, err := ss.ClaimNonce(ctx, tenant, key, "nonce-1", time.Minute); err != nil || again {
		t.Fatalf("claim nonce twice = %v, %v", again, err)
	}
}

func testStorageAutomation(t *testing.T, opts StorageOptions) {
	t.Helper()
	ctx := t.Context()
	as := opts.Automation(t)
	tenant := storageTenant()
	other := storageTenant()
	id, err := automation.NewWatcherID()
	if err != nil {
		t.Fatalf("mint watcher ID: %v", err)
	}
	watcher := automation.Watcher{
		Tenant: tenant, ID: id, Channel: "work", Enabled: true,
		Action: automation.ActionSpec{Name: "post", Params: json.RawMessage(`{"text":"hi"}`)},
	}
	created, err := as.CreateWatcher(ctx, watcher)
	if err != nil || created.ID != id {
		t.Fatalf("create watcher = %+v, %v", created, err)
	}
	if found, ok, err := as.Watcher(ctx, tenant, id); err != nil || !ok || found.ID != id {
		t.Fatalf("read watcher = %+v, %v, %v", found, ok, err)
	}
	if list, err := as.Watchers(ctx, other); err != nil || len(list) != 0 {
		t.Fatalf("another tenant's watchers = %+v, %v", list, err)
	}
	if queued, err := as.AdvanceWatcher(ctx, tenant, id, 1, []uint64{1}); err != nil || queued != 1 {
		t.Fatalf("advance watcher = %d, %v", queued, err)
	}
	deliveries, err := as.LeaseDeliveries(ctx, tenant, 1, "worker-a")
	if err != nil || len(deliveries) != 1 || deliveries[0].Lease == "" {
		t.Fatalf("lease deliveries = %+v, %v", deliveries, err)
	}
	if acked, err := as.AckDelivery(ctx, tenant, id, deliveries[0].EventSeq, deliveries[0].Lease, nil); err != nil || !acked.Acked {
		t.Fatalf("ack delivery = %+v, %v", acked, err)
	}
	if depth, err := as.DeliveryDepth(ctx, tenant, id); err != nil || depth.Done != 1 || depth.Pending != 0 {
		t.Fatalf("delivery depth = %+v, %v", depth, err)
	}
	if err := as.DeleteWatcher(ctx, tenant, id); err != nil {
		t.Fatalf("delete watcher: %v", err)
	}
}

func testStorageAudit(t *testing.T, opts StorageOptions) {
	t.Helper()
	ctx := t.Context()
	as := opts.Audit(t)
	tenant := storageTenant()
	other := storageTenant()
	stored, err := as.Append(ctx, audit.Event{
		Tenant: tenant, Kind: audit.KindWebhookAccepted,
		Key: "slack", Platform: "slack", Status: audit.StatusAccepted,
	})
	if err != nil || stored.ID == 0 || stored.At.IsZero() {
		t.Fatalf("append audit = %+v, %v", stored, err)
	}
	recent, err := as.Recent(ctx, tenant, 10)
	if err != nil || len(recent) != 1 || recent[0].ID != stored.ID {
		t.Fatalf("recent audit = %+v, %v", recent, err)
	}
	if list, err := as.Recent(ctx, other, 10); err != nil || len(list) != 0 {
		t.Fatalf("another tenant's audit = %+v, %v", list, err)
	}
	if _, err := as.Append(ctx, audit.Event{Tenant: tenant}); !errors.Is(err, audit.ErrInvalid) {
		t.Fatalf("invalid audit event error = %v, want ErrInvalid", err)
	}
}
