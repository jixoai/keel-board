package automation_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jixoai/keel-board/pgadapter"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jixoai/keel-board/automation"
	"github.com/jixoai/keel-board/protocol"
)

func testDSN(t *testing.T) string {
	t.Helper()
	for _, name := range []string{"KEEL_BOARD_TEST_DSN", "TEST_DSN"} {
		if dsn := strings.TrimSpace(os.Getenv(name)); dsn != "" {
			return dsn
		}
	}
	if strings.TrimSpace(os.Getenv("CI")) != "" {
		t.Fatal("CI requires KEEL_BOARD_TEST_DSN (or TEST_DSN); PostgreSQL tests must not be skipped")
	}
	t.Skip("PostgreSQL tests skipped: set KEEL_BOARD_TEST_DSN (or TEST_DSN)")
	return ""
}

var testSchemaSeq atomic.Uint64

func newTestSchema(t *testing.T, dsn string) string {
	t.Helper()
	name := fmt.Sprintf("keel_automation_%d_%d", time.Now().UnixNano(), testSchemaSeq.Add(1))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connecting to PostgreSQL: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{name}.Sanitize()); err != nil {
		admin.Close(ctx)
		t.Fatalf("creating test schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(cleanupCtx, "DROP SCHEMA "+pgx.Identifier{name}.Sanitize()+" CASCADE"); err != nil {
			t.Logf("dropping test schema %s: %v", name, err)
		}
		_ = admin.Close(cleanupCtx)
	})
	return name
}

func openPostgres(t *testing.T, dsn, schema string, cfg automation.Config) *pgadapter.Automation {
	t.Helper()
	postgres, err := pgadapter.OpenAutomation(t.Context(), pgadapter.AutomationOptions{
		DSN: dsn, SearchPath: schema, Config: cfg,
	})
	if err != nil {
		t.Fatalf("opening PostgreSQL automation store: %v", err)
	}
	t.Cleanup(postgres.Close)
	return postgres
}

// testConfig is the low-latency policy contract tests use: a failed attempt is
// due immediately unless a test asks for a backoff.
func testConfig() automation.Config {
	return automation.Config{
		Retry:    automation.RetryPolicy{MaxAttempts: 3, BaseDelay: 0, MaxDelay: 0, Jitter: 0},
		LeaseTTL: 5 * time.Minute,
	}
}

func withStores(t *testing.T, cfg automation.Config, fn func(t *testing.T, store automation.Store)) {
	t.Helper()
	t.Run("memory", func(t *testing.T) {
		memory, err := automation.NewMemory(cfg)
		if err != nil {
			t.Fatalf("new memory store: %v", err)
		}
		fn(t, memory)
	})
	t.Run("postgres", func(t *testing.T) {
		dsn := testDSN(t)
		schema := newTestSchema(t, dsn)
		fn(t, openPostgres(t, dsn, schema, cfg))
	})
}

func testWatcher(id string) automation.Watcher {
	return automation.Watcher{
		Tenant: "t1", ID: id, Channel: "work",
		Action:  automation.ActionSpec{Name: "post", Params: []byte(`{"text":"hello"}`)},
		Enabled: true,
	}
}

func testSchedule(id string, next time.Time) automation.Schedule {
	return automation.Schedule{
		Tenant: "t1", ID: id, Channel: "work", Text: "scheduled",
		Labels: map[string]string{"kind": "reminder"}, Creator: "local:user:alice",
		NextRunAt: next,
	}
}

func TestPostgresMigrationAndRLS(t *testing.T) {
	dsn := testDSN(t)
	schema := newTestSchema(t, dsn)
	first, err := pgadapter.OpenAutomation(t.Context(), pgadapter.AutomationOptions{
		DSN: dsn, SearchPath: schema, Config: testConfig(),
	})
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	first.Close()
	second, err := pgadapter.OpenAutomation(t.Context(), pgadapter.AutomationOptions{
		DSN: dsn, SearchPath: schema, Config: testConfig(),
	})
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	t.Cleanup(second.Close)
	ctx := t.Context()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect for schema check: %v", err)
	}
	defer admin.Close(ctx)
	for _, table := range []string{"board_watchers", "board_watch_deliveries", "board_schedules"} {
		var rls, forced bool
		err := admin.QueryRow(ctx, `
			SELECT c.relrowsecurity, c.relforcerowsecurity
			FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relname = $2`, schema, table).Scan(&rls, &forced)
		if err != nil {
			t.Fatalf("checking %s RLS: %v", table, err)
		}
		if !rls || !forced {
			t.Fatalf("%s RLS = %v, forced = %v; want true, true", table, rls, forced)
		}
	}
	if _, err := second.CreateWatcher(ctx, testWatcher("after-restart")); err != nil {
		t.Fatalf("write after the second migration: %v", err)
	}
}

func TestWatcherCRUDIsTenantScoped(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store automation.Store) {
		ctx := t.Context()
		watcher := testWatcher("alerts")
		watcher.Filter = automation.Filter{
			Types:  []string{"message.posted"},
			Labels: map[string]string{"kind": "alert"},
		}
		created, err := store.CreateWatcher(ctx, watcher)
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		if created.CreatedAt.IsZero() || created.UpdatedAt.IsZero() {
			t.Fatalf("timestamps not set: %+v", created)
		}
		if _, err := store.CreateWatcher(ctx, watcher); !errors.Is(err, automation.ErrExists) {
			t.Fatalf("duplicate create = %v, want ErrExists", err)
		}
		got, found, err := store.Watcher(ctx, "t1", "alerts")
		if err != nil || !found || got.Channel != "work" || len(got.Filter.Types) != 1 {
			t.Fatalf("get = %+v, %v, %v", got, found, err)
		}
		var params map[string]string
		if err := json.Unmarshal(got.Action.Params, &params); err != nil || params["text"] != "hello" {
			t.Fatalf("action params after a store round trip = %s, %v", got.Action.Params, err)
		}
		if _, found, err := store.Watcher(ctx, "t2", "alerts"); err != nil || found {
			t.Fatalf("cross-tenant get = %v, %v", found, err)
		}
		all, err := store.Watchers(ctx, "t1")
		if err != nil || len(all) != 1 {
			t.Fatalf("list = %+v, %v", all, err)
		}
		watcher.Channel = "work/"
		watcher.Action = automation.ActionSpec{Name: "notify"}
		watcher.Bot = "local:bot:alerts"
		updated, err := store.UpdateWatcher(ctx, watcher)
		if err != nil {
			t.Fatalf("update: %v", err)
		}
		if updated.Channel != "work/" || updated.Action.Name != "notify" || !updated.Enabled {
			t.Fatalf("updated = %+v", updated)
		}
		disabled, err := store.SetWatcherEnabled(ctx, "t1", "alerts", false, nil)
		if err != nil || disabled.Enabled {
			t.Fatalf("disable = %+v, %v", disabled, err)
		}
		head := uint64(42)
		enabled, err := store.SetWatcherEnabled(ctx, "t1", "alerts", true, &head)
		if err != nil || !enabled.Enabled || enabled.Cursor != 42 {
			t.Fatalf("enable = %+v, %v", enabled, err)
		}
		if err := store.DeleteWatcher(ctx, "t1", "alerts"); err != nil {
			t.Fatalf("delete: %v", err)
		}
		if _, found, err := store.Watcher(ctx, "t1", "alerts"); err != nil || found {
			t.Fatalf("after delete = %v, %v", found, err)
		}
		if err := store.DeleteWatcher(ctx, "t1", "alerts"); !errors.Is(err, automation.ErrNotFound) {
			t.Fatalf("delete missing = %v", err)
		}
	})
}

func TestWatcherPlanningAndDeliveryLifecycle(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store automation.Store) {
		ctx := t.Context()
		if _, err := store.CreateWatcher(ctx, testWatcher("alerts")); err != nil {
			t.Fatalf("create: %v", err)
		}
		added, err := store.AdvanceWatcher(ctx, "t1", "alerts", 10, []uint64{2, 4, 4, 9})
		if err != nil || added != 3 {
			t.Fatalf("advance = %d, %v; want 3", added, err)
		}
		// Planning the same page again is a no-op; a later cursor can't queue
		// beyond itself.
		if added, err := store.AdvanceWatcher(ctx, "t1", "alerts", 10, []uint64{2, 4, 9}); err != nil || added != 0 {
			t.Fatalf("repeat advance = %d, %v", added, err)
		}
		if added, err := store.AdvanceWatcher(ctx, "t1", "alerts", 12, []uint64{11, 13}); err != nil || added != 1 {
			t.Fatalf("second advance = %d, %v; want 1", added, err)
		}
		watcher, _, _ := store.Watcher(ctx, "t1", "alerts")
		if watcher.Cursor != 12 {
			t.Fatalf("cursor = %d, want 12", watcher.Cursor)
		}
		depth, err := store.DeliveryDepth(ctx, "t1", "alerts")
		if err != nil || depth.Pending != 4 {
			t.Fatalf("depth = %+v, %v", depth, err)
		}

		// One lane: only the oldest pending delivery of a watcher is offered,
		// even with a generous limit.
		leased, err := store.LeaseDeliveries(ctx, "t1", 10, "worker-a")
		if err != nil || len(leased) != 1 || leased[0].EventSeq != 2 {
			t.Fatalf("first lease = %+v, %v", leased, err)
		}
		if again, err := store.LeaseDeliveries(ctx, "t1", 10, "worker-b"); err != nil || len(again) != 0 {
			t.Fatalf("live lease re-offered = %+v, %v", again, err)
		}
		// Two failed attempts, then the third dead-letters and the lane moves
		// on to the next delivery.
		for attempt := 1; attempt <= 2; attempt++ {
			ack, err := store.AckDelivery(ctx, "t1", "alerts", leased[0].EventSeq, leased[0].Lease, errors.New("boom"))
			if err != nil || !ack.Retried || ack.Attempts != attempt {
				t.Fatalf("ack %d = %+v, %v", attempt, ack, err)
			}
			leased, err = store.LeaseDeliveries(ctx, "t1", 10, "worker-a")
			if err != nil || len(leased) != 1 || leased[0].EventSeq != 2 {
				t.Fatalf("retry lease %d = %+v, %v", attempt, leased, err)
			}
		}
		ack, err := store.AckDelivery(ctx, "t1", "alerts", leased[0].EventSeq, leased[0].Lease, errors.New("boom"))
		if err != nil || !ack.Dead || ack.Attempts != 3 {
			t.Fatalf("dead ack = %+v, %v", ack, err)
		}
		leased, err = store.LeaseDeliveries(ctx, "t1", 10, "worker-a")
		if err != nil || len(leased) != 1 || leased[0].EventSeq != 4 {
			t.Fatalf("lane after dead = %+v, %v", leased, err)
		}
		if _, err := store.AckDelivery(ctx, "t1", "alerts", leased[0].EventSeq, leased[0].Lease, nil); err != nil {
			t.Fatalf("ack success: %v", err)
		}
		leased, err = store.LeaseDeliveries(ctx, "t1", 10, "worker-a")
		if err != nil || len(leased) != 1 || leased[0].EventSeq != 9 {
			t.Fatalf("third lease = %+v, %v", leased, err)
		}
		if _, err := store.AckDelivery(ctx, "t1", "alerts", leased[0].EventSeq, leased[0].Lease, nil); err != nil {
			t.Fatalf("ack third: %v", err)
		}
		depth, err = store.DeliveryDepth(ctx, "t1", "alerts")
		if err != nil || depth.Done != 2 || depth.Dead != 1 || depth.Pending != 1 {
			t.Fatalf("depth after acks = %+v, %v", depth, err)
		}

		// A dead delivery can be retried; a permanent failure dead-letters it
		// again, and clear removes only dead letters.
		if err := store.RetryDelivery(ctx, "t1", "alerts", 2); err != nil {
			t.Fatalf("retry delivery: %v", err)
		}
		leased, err = store.LeaseDeliveries(ctx, "t1", 10, "worker-a")
		if err != nil || len(leased) != 1 || leased[0].EventSeq != 2 || leased[0].Attempts != 0 {
			t.Fatalf("retried lease = %+v, %v", leased, err)
		}
		if _, err := store.AckDelivery(ctx, "t1", "alerts", 2, leased[0].Lease, automation.Permanent(errors.New("no"))); err != nil {
			t.Fatalf("permanent ack: %v", err)
		}
		removed, err := store.ClearDeliveries(ctx, "t1", "alerts")
		if err != nil || removed != 1 {
			t.Fatalf("clear = %d, %v", removed, err)
		}
		if err := store.RetryDelivery(ctx, "t1", "alerts", 2); !errors.Is(err, automation.ErrNotDead) {
			t.Fatalf("retry cleared = %v", err)
		}
	})
}

func TestWatcherConcurrentPlanningAndLeasing(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store automation.Store) {
		ctx := t.Context()
		for index := 0; index < 8; index++ {
			id := fmt.Sprintf("watcher-%d", index)
			watcher := testWatcher(id)
			watcher.Cursor = 1
			if _, err := store.CreateWatcher(ctx, watcher); err != nil {
				t.Fatalf("create %s: %v", id, err)
			}
			if _, err := store.AdvanceWatcher(ctx, "t1", id, 2, []uint64{2}); err != nil {
				t.Fatalf("advance %s: %v", id, err)
			}
		}
		// Two planners racing over the same page insert each delivery once.
		var wg sync.WaitGroup
		for _, owner := range []string{"planner-a", "planner-b"} {
			wg.Add(1)
			go func(owner string) {
				defer wg.Done()
				if _, err := store.AdvanceWatcher(ctx, "t1", "watcher-0", 3, []uint64{3}); err != nil {
					t.Errorf("advance from %s: %v", owner, err)
				}
			}(owner)
		}
		wg.Wait()
		depth, err := store.DeliveryDepth(ctx, "t1", "watcher-0")
		if err != nil || depth.Pending != 2 {
			t.Fatalf("depth after concurrent planning = %+v, %v", depth, err)
		}

		// Two workers racing to lease see each delivery at most once.
		type leasedRow struct {
			watcher string
			seq     uint64
			lease   string
		}
		var (
			mu     sync.Mutex
			rows   = make(map[string]leasedRow)
			dupErr error
		)
		for _, owner := range []string{"worker-a", "worker-b"} {
			wg.Add(1)
			go func(owner string) {
				defer wg.Done()
				jobs, err := store.LeaseDeliveries(ctx, "t1", 100, owner)
				if err != nil {
					t.Errorf("lease from %s: %v", owner, err)
					return
				}
				mu.Lock()
				defer mu.Unlock()
				for _, job := range jobs {
					key := fmt.Sprintf("%s/%d", job.WatcherID, job.EventSeq)
					if _, exists := rows[key]; exists {
						dupErr = fmt.Errorf("delivery %s leased twice", key)
					}
					rows[key] = leasedRow{watcher: job.WatcherID, seq: job.EventSeq, lease: job.Lease}
				}
			}(owner)
		}
		wg.Wait()
		if dupErr != nil {
			t.Fatal(dupErr)
		}
		// There are 9 pending deliveries (8 original plus watcher-0's second),
		// and two workers racing can take at most one per watcher each.
		if len(rows) == 0 {
			t.Fatal("no deliveries were leased")
		}
		for _, row := range rows {
			if _, err := store.AckDelivery(ctx, "t1", row.watcher, row.seq, row.lease, nil); err != nil {
				t.Fatalf("ack %s/%d: %v", row.watcher, row.seq, err)
			}
		}
	})
}

func TestLeaseExpiryAndLostAck(t *testing.T) {
	cfg := testConfig()
	// Seconds, not milliseconds: under -count=10 the second lease itself must
	// stay live across the assertions. A 150ms lease expired mid-ack.
	cfg.LeaseTTL = 2 * time.Second
	withStores(t, cfg, func(t *testing.T, store automation.Store) {
		ctx := t.Context()
		if _, err := store.CreateWatcher(ctx, testWatcher("alerts")); err != nil {
			t.Fatalf("create: %v", err)
		}
		if _, err := store.AdvanceWatcher(ctx, "t1", "alerts", 2, []uint64{1}); err != nil {
			t.Fatalf("advance: %v", err)
		}
		first, err := store.LeaseDeliveries(ctx, "t1", 1, "worker-a")
		if err != nil || len(first) != 1 {
			t.Fatalf("first lease = %+v, %v", first, err)
		}
		time.Sleep(2500 * time.Millisecond)
		second, err := store.LeaseDeliveries(ctx, "t1", 1, "worker-b")
		if err != nil || len(second) != 1 || second[0].EventSeq != first[0].EventSeq {
			t.Fatalf("second lease = %+v, %v", second, err)
		}
		if first[0].Lease == second[0].Lease {
			t.Fatal("the crashed worker's lease was not replaced")
		}
		if _, err := store.AckDelivery(ctx, "t1", "alerts", first[0].EventSeq, first[0].Lease, nil); !errors.Is(err, automation.ErrLeaseLost) {
			t.Fatalf("old ack = %v, want ErrLeaseLost", err)
		}
		if _, err := store.AckDelivery(ctx, "t1", "alerts", second[0].EventSeq, second[0].Lease, nil); err != nil {
			t.Fatalf("new ack: %v", err)
		}
	})
}

func TestScheduleCRUDAndStates(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store automation.Store) {
		ctx := t.Context()
		id, err := automation.NewScheduleID()
		if err != nil {
			t.Fatal(err)
		}
		schedule := testSchedule(id, time.Now().Add(-time.Second))
		created, err := store.CreateSchedule(ctx, schedule)
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		if created.Status != automation.SchedulePending || created.Sent != 0 {
			t.Fatalf("created = %+v", created)
		}
		if _, err := store.CreateSchedule(ctx, schedule); !errors.Is(err, automation.ErrExists) {
			t.Fatalf("duplicate create = %v", err)
		}
		got, found, err := store.Schedule(ctx, "t1", id)
		if err != nil || !found || got.Text != "scheduled" {
			t.Fatalf("get = %+v, %v, %v", got, found, err)
		}
		if _, found, err := store.Schedule(ctx, "t2", id); err != nil || found {
			t.Fatalf("cross-tenant get = %v, %v", found, err)
		}
		list, err := store.Schedules(ctx, "t1")
		if err != nil || len(list) != 1 {
			t.Fatalf("list = %+v, %v", list, err)
		}

		// Update declarative fields while nothing is leased.
		got.Text = "updated"
		got.Labels = map[string]string{"kind": "reminder", "urgent": "yes"}
		got.NextRunAt = time.Now().Add(time.Hour)
		updated, err := store.UpdateSchedule(ctx, got)
		if err != nil || updated.Text != "updated" || updated.Labels["urgent"] != "yes" {
			t.Fatalf("update = %+v, %v", updated, err)
		}
		if _, err := store.SetScheduleEnabled(ctx, "t1", id, false, time.Now()); err != nil {
			t.Fatalf("disable: %v", err)
		}
		disabled, _, _ := store.Schedule(ctx, "t1", id)
		if disabled.Status != automation.ScheduleDisabled {
			t.Fatalf("disabled status = %s", disabled.Status)
		}
		enabled, err := store.SetScheduleEnabled(ctx, "t1", id, true, time.Now())
		if err != nil || enabled.Status != automation.SchedulePending {
			t.Fatalf("enable = %+v, %v", enabled, err)
		}
		if err := store.RetrySchedule(ctx, "t1", id); !errors.Is(err, automation.ErrNotDead) {
			t.Fatalf("retry pending = %v", err)
		}
		if err := store.ClearSchedule(ctx, "t1", id); !errors.Is(err, automation.ErrNotDead) {
			t.Fatalf("clear pending = %v", err)
		}
		latest, _, _ := store.Schedule(ctx, "t1", id)
		latest.NextRunAt = time.Now().Add(-time.Second)
		if _, err := store.UpdateSchedule(ctx, latest); err != nil {
			t.Fatalf("make due: %v", err)
		}

		// A lease blocks updates and enable/disable; a release frees them.
		leases, err := store.LeaseSchedules(ctx, "t1", 10, "worker-a")
		if err != nil || len(leases) != 1 {
			t.Fatalf("lease = %+v, %v", leases, err)
		}
		got.Text = "while running"
		if _, err := store.UpdateSchedule(ctx, got); !errors.Is(err, automation.ErrLeased) {
			t.Fatalf("update leased = %v", err)
		}
		if _, err := store.SetScheduleEnabled(ctx, "t1", id, false, time.Now()); !errors.Is(err, automation.ErrLeased) {
			t.Fatalf("disable leased = %v", err)
		}
		if err := store.ReleaseSchedule(ctx, "t1", id, leases[0].Lease, 0); err != nil {
			t.Fatalf("release: %v", err)
		}
		if err := store.DeleteSchedule(ctx, "t1", id); err != nil {
			t.Fatalf("delete: %v", err)
		}
		if _, found, err := store.Schedule(ctx, "t1", id); err != nil || found {
			t.Fatalf("after delete = %v, %v", found, err)
		}
	})
}

func TestScheduleRunAckAndDeadLetter(t *testing.T) {
	cfg := testConfig()
	cfg.Retry.MaxAttempts = 2
	withStores(t, cfg, func(t *testing.T, store automation.Store) {
		ctx := t.Context()
		id, err := automation.NewScheduleID()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.CreateSchedule(ctx, testSchedule(id, time.Now().Add(-time.Second))); err != nil {
			t.Fatalf("create: %v", err)
		}
		leases, err := store.LeaseSchedules(ctx, "t1", 10, "worker-a")
		if err != nil || len(leases) != 1 {
			t.Fatalf("lease = %+v, %v", leases, err)
		}
		sent, err := store.AckSchedule(ctx, "t1", id, leases[0].Lease, automation.ScheduleResult{
			Outcome: automation.ScheduleSent, At: time.Now(), MessageID: 7, Done: true,
		})
		if err != nil || sent.Status != automation.ScheduleDone || sent.Sent != 1 {
			t.Fatalf("sent ack = %+v, %v", sent, err)
		}
		if again, err := store.LeaseSchedules(ctx, "t1", 10, "worker-a"); err != nil || len(again) != 0 {
			t.Fatalf("done schedule leased = %+v, %v", again, err)
		}
		if _, err := store.SetScheduleEnabled(ctx, "t1", id, false, time.Now()); !errors.Is(err, automation.ErrScheduleNotPending) {
			t.Fatalf("disable done = %v", err)
		}

		// A retryable failure backs off; the second failure dead-letters; a
		// manual retry revives it and a success finishes it.
		retryID, _ := automation.NewScheduleID()
		if _, err := store.CreateSchedule(ctx, testSchedule(retryID, time.Now().Add(-time.Second))); err != nil {
			t.Fatalf("create retry: %v", err)
		}
		for attempt := 1; attempt <= 2; attempt++ {
			leases, err := store.LeaseSchedules(ctx, "t1", 10, "worker-a")
			if err != nil || len(leases) != 1 || leases[0].ID != retryID {
				t.Fatalf("retry lease %d = %+v, %v", attempt, leases, err)
			}
			acked, err := store.AckSchedule(ctx, "t1", retryID, leases[0].Lease, automation.ScheduleResult{
				Outcome: automation.ScheduleRetry, At: time.Now(), Err: errors.New("boom"),
			})
			if err != nil {
				t.Fatalf("retry ack %d: %v", attempt, err)
			}
			if attempt == 1 && (acked.Status != automation.SchedulePending || acked.Attempts != 1) {
				t.Fatalf("retry ack 1 = %+v", acked)
			}
			if attempt == 2 && (acked.Status != automation.ScheduleDead || acked.Attempts != 2) {
				t.Fatalf("dead ack = %+v", acked)
			}
		}
		if err := store.RetrySchedule(ctx, "t1", retryID); err != nil {
			t.Fatalf("manual retry: %v", err)
		}
		leases, err = store.LeaseSchedules(ctx, "t1", 10, "worker-a")
		if err != nil || len(leases) != 1 || leases[0].Attempts != 0 {
			t.Fatalf("revived lease = %+v, %v", leases, err)
		}
		if _, err := store.AckSchedule(ctx, "t1", retryID, leases[0].Lease, automation.ScheduleResult{
			Outcome: automation.ScheduleSent, At: time.Now(), MessageID: 9, Done: true,
		}); err != nil {
			t.Fatalf("final ack: %v", err)
		}
		if err := store.RetrySchedule(ctx, "t1", retryID); !errors.Is(err, automation.ErrNotDead) {
			t.Fatalf("retry finished = %v", err)
		}

		// Revocation cancels with its reason and is not retryable.
		revokedID, _ := automation.NewScheduleID()
		if _, err := store.CreateSchedule(ctx, testSchedule(revokedID, time.Now().Add(-time.Second))); err != nil {
			t.Fatalf("create revoked: %v", err)
		}
		leases, err = store.LeaseSchedules(ctx, "t1", 10, "worker-a")
		if err != nil || len(leases) != 1 || leases[0].ID != revokedID {
			t.Fatalf("revoked lease = %+v, %v", leases, err)
		}
		canceled, err := store.AckSchedule(ctx, "t1", revokedID, leases[0].Lease, automation.ScheduleResult{
			Outcome: automation.ScheduleRevoked, At: time.Now(), Reason: "revoked",
		})
		if err != nil || canceled.Status != automation.ScheduleCanceled || canceled.CancelReason != "revoked" {
			t.Fatalf("revoked ack = %+v, %v", canceled, err)
		}

		// Clear removes a dead plan only.
		deadID, _ := automation.NewScheduleID()
		if _, err := store.CreateSchedule(ctx, testSchedule(deadID, time.Now().Add(-time.Second))); err != nil {
			t.Fatalf("create dead: %v", err)
		}
		leases, _ = store.LeaseSchedules(ctx, "t1", 10, "worker-a")
		_, _ = store.AckSchedule(ctx, "t1", deadID, leases[0].Lease, automation.ScheduleResult{
			Outcome: automation.ScheduleRetry, At: time.Now(), Err: automation.Permanent(errors.New("bad")),
		})
		if err := store.ClearSchedule(ctx, "t1", deadID); err != nil {
			t.Fatalf("clear dead: %v", err)
		}
		if _, found, _ := store.Schedule(ctx, "t1", deadID); found {
			t.Fatal("dead schedule survived clear")
		}
	})
}

func TestScheduleLeaseExpiry(t *testing.T) {
	cfg := testConfig()
	cfg.LeaseTTL = 2 * time.Second
	withStores(t, cfg, func(t *testing.T, store automation.Store) {
		ctx := t.Context()
		id, _ := automation.NewScheduleID()
		if _, err := store.CreateSchedule(ctx, testSchedule(id, time.Now().Add(-time.Second))); err != nil {
			t.Fatalf("create: %v", err)
		}
		first, err := store.LeaseSchedules(ctx, "t1", 1, "worker-a")
		if err != nil || len(first) != 1 {
			t.Fatalf("first lease = %+v, %v", first, err)
		}
		time.Sleep(2500 * time.Millisecond)
		second, err := store.LeaseSchedules(ctx, "t1", 1, "worker-b")
		if err != nil || len(second) != 1 || second[0].ID != id {
			t.Fatalf("second lease = %+v, %v", second, err)
		}
		if first[0].Lease == second[0].Lease {
			t.Fatal("expired schedule lease was not replaced")
		}
		if _, err := store.AckSchedule(ctx, "t1", id, first[0].Lease, automation.ScheduleResult{
			Outcome: automation.ScheduleSent, Done: true,
		}); !errors.Is(err, automation.ErrLeaseLost) {
			t.Fatalf("old schedule ack = %v, want ErrLeaseLost", err)
		}
	})
}

func TestPostgresRestartRecoversCursorAndLease(t *testing.T) {
	dsn := testDSN(t)
	schema := newTestSchema(t, dsn)
	cfg := testConfig()
	// Seconds, not milliseconds: the post-reopen lease must survive the
	// assertions below while `go test -count=10` runs packages in parallel.
	// A 150ms lease made the recovered ack itself flaky.
	cfg.LeaseTTL = 2 * time.Second
	first, err := pgadapter.OpenAutomation(t.Context(), pgadapter.AutomationOptions{
		DSN: dsn, SearchPath: schema, Config: cfg,
	})
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	watcher := testWatcher("alerts")
	watcher.Cursor = 1
	if _, err := first.CreateWatcher(t.Context(), watcher); err != nil {
		first.Close()
		t.Fatalf("create: %v", err)
	}
	if added, err := first.AdvanceWatcher(t.Context(), "t1", "alerts", 5, []uint64{2, 4}); err != nil || added != 2 {
		first.Close()
		t.Fatalf("advance = %d, %v", added, err)
	}
	leased, err := first.LeaseDeliveries(t.Context(), "t1", 10, "crashed")
	if err != nil || len(leased) != 1 || leased[0].EventSeq != 2 {
		first.Close()
		t.Fatalf("lease before restart = %+v, %v", leased, err)
	}
	first.Close()
	time.Sleep(2500 * time.Millisecond)

	second, err := pgadapter.OpenAutomation(t.Context(), pgadapter.AutomationOptions{
		DSN: dsn, SearchPath: schema, Config: cfg,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(second.Close)
	stored, found, err := second.Watcher(t.Context(), "t1", "alerts")
	if err != nil || !found || stored.Cursor != 5 || !stored.CursorSet {
		t.Fatalf("watcher after restart = %+v, %v, %v", stored, found, err)
	}
	again, err := second.LeaseDeliveries(t.Context(), "t1", 10, "recovered")
	if err != nil || len(again) != 1 || again[0].EventSeq != 2 || again[0].Lease == leased[0].Lease {
		t.Fatalf("lease after restart = %+v, %v", again, err)
	}
	if _, err := second.AckDelivery(t.Context(), "t1", "alerts", 2, leased[0].Lease, nil); !errors.Is(err, automation.ErrLeaseLost) {
		t.Fatalf("crashed ack = %v, want ErrLeaseLost", err)
	}
	if _, err := second.AckDelivery(t.Context(), "t1", "alerts", 2, again[0].Lease, nil); err != nil {
		t.Fatalf("recovered ack: %v", err)
	}
}

func TestRenewDeliveryContract(t *testing.T) {
	cfg := testConfig()
	cfg.LeaseTTL = 2 * time.Second
	withStores(t, cfg, func(t *testing.T, store automation.Store) {
		ctx := t.Context()
		if _, err := store.CreateWatcher(ctx, testWatcher("renew")); err != nil {
			t.Fatalf("create: %v", err)
		}
		if _, err := store.AdvanceWatcher(ctx, "t1", "renew", 2, []uint64{1}); err != nil {
			t.Fatalf("advance: %v", err)
		}
		leased, err := store.LeaseDeliveries(ctx, "t1", 1, "worker")
		if err != nil || len(leased) != 1 {
			t.Fatalf("lease = %+v, %v", leased, err)
		}
		if err := store.RenewDelivery(ctx, "t1", "renew", 1, "wrong", time.Second); !errors.Is(err, automation.ErrLeaseLost) {
			t.Fatalf("wrong-token renew = %v", err)
		}
		if err := store.RenewDelivery(ctx, "t1", "renew", 1, leased[0].Lease, 5*time.Second); err != nil {
			t.Fatalf("renew: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
		again, err := store.LeaseDeliveries(ctx, "t1", 1, "other")
		if err != nil || len(again) != 0 {
			t.Fatalf("renewed lease was stolen = %+v, %v", again, err)
		}
		if _, err := store.AckDelivery(ctx, "t1", "renew", 1, leased[0].Lease, nil); err != nil {
			t.Fatalf("ack after renewal: %v", err)
		}
	})
}

func TestScheduleCreatorMetadataPersists(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store automation.Store) {
		ctx := t.Context()
		id, _ := automation.NewScheduleID()
		environment := "prod"
		schedule := testSchedule(id, time.Now().Add(time.Hour))
		schedule.CreatorKind = string(protocol.ActorAgent)
		schedule.CreatorVia = &protocol.Via{Name: "deploy", Environment: &environment}
		if _, err := store.CreateSchedule(ctx, schedule); err != nil {
			t.Fatalf("create: %v", err)
		}
		stored, found, err := store.Schedule(ctx, "t1", id)
		if err != nil || !found {
			t.Fatalf("get = %v, %v", found, err)
		}
		if stored.CreatorKind != string(protocol.ActorAgent) || stored.CreatorVia == nil ||
			stored.CreatorVia.Name != "deploy" || stored.CreatorVia.Environment == nil ||
			*stored.CreatorVia.Environment != "prod" {
			t.Fatalf("creator metadata = %+v, via = %+v", stored, stored.CreatorVia)
		}
	})
}

func TestWatcherFilterTypesAreTrimmed(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store automation.Store) {
		ctx := t.Context()
		watcher := testWatcher("trim")
		watcher.Filter = automation.Filter{Types: []string{"  message.posted\t", "labels.changed "}}
		stored, err := store.CreateWatcher(ctx, watcher)
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		if len(stored.Filter.Types) != 2 || stored.Filter.Types[0] != "message.posted" || stored.Filter.Types[1] != "labels.changed" {
			t.Fatalf("types = %q", stored.Filter.Types)
		}
		read, _, _ := store.Watcher(ctx, "t1", "trim")
		if read.Filter.Types[0] != "message.posted" {
			t.Fatalf("stored types = %q", read.Filter.Types)
		}
	})
}

func TestCrossTenantWritesAreNotFound(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store automation.Store) {
		ctx := t.Context()
		if _, err := store.CreateWatcher(ctx, testWatcher("alerts")); err != nil {
			t.Fatalf("create watcher: %v", err)
		}
		if _, err := store.AdvanceWatcher(ctx, "t1", "alerts", 2, []uint64{1}); err != nil {
			t.Fatalf("advance: %v", err)
		}
		leased, err := store.LeaseDeliveries(ctx, "t1", 1, "worker")
		if err != nil || len(leased) != 1 {
			t.Fatalf("lease = %+v, %v", leased, err)
		}
		foreign := testWatcher("alerts")
		foreign.Tenant = "t2"
		foreign.Channel = "other"
		if _, err := store.UpdateWatcher(ctx, foreign); !errors.Is(err, automation.ErrNotFound) {
			t.Fatalf("foreign watcher update = %v", err)
		}
		if _, err := store.SetWatcherEnabled(ctx, "t2", "alerts", false, nil); !errors.Is(err, automation.ErrNotFound) {
			t.Fatalf("foreign watcher enable = %v", err)
		}
		if err := store.DeleteWatcher(ctx, "t2", "alerts"); !errors.Is(err, automation.ErrNotFound) {
			t.Fatalf("foreign watcher delete = %v", err)
		}
		if _, err := store.ClearDeliveries(ctx, "t2", "alerts"); !errors.Is(err, automation.ErrNotFound) {
			t.Fatalf("foreign clear = %v", err)
		}
		if err := store.RetryDelivery(ctx, "t2", "alerts", 1); !errors.Is(err, automation.ErrNotDead) {
			t.Fatalf("foreign retry = %v", err)
		}
		if _, err := store.AckDelivery(ctx, "t2", "alerts", 1, leased[0].Lease, nil); !errors.Is(err, automation.ErrLeaseLost) {
			t.Fatalf("foreign ack = %v", err)
		}
		if jobs, err := store.LeaseDeliveries(ctx, "t2", 10, "worker"); err != nil || len(jobs) != 0 {
			t.Fatalf("foreign lease = %+v, %v", jobs, err)
		}

		scheduleID, _ := automation.NewScheduleID()
		if _, err := store.CreateSchedule(ctx, testSchedule(scheduleID, time.Now().Add(time.Hour))); err != nil {
			t.Fatalf("create schedule: %v", err)
		}
		foreignSchedule := testSchedule(scheduleID, time.Now().Add(time.Hour))
		foreignSchedule.Tenant = "t2"
		foreignSchedule.Text = "foreign"
		if _, err := store.UpdateSchedule(ctx, foreignSchedule); !errors.Is(err, automation.ErrNotFound) {
			t.Fatalf("foreign schedule update = %v", err)
		}
		if _, err := store.SetScheduleEnabled(ctx, "t2", scheduleID, false, time.Now()); !errors.Is(err, automation.ErrNotFound) {
			t.Fatalf("foreign schedule enable = %v", err)
		}
		if err := store.RetrySchedule(ctx, "t2", scheduleID); !errors.Is(err, automation.ErrNotFound) {
			t.Fatalf("foreign schedule retry = %v", err)
		}
		if err := store.ClearSchedule(ctx, "t2", scheduleID); !errors.Is(err, automation.ErrNotFound) {
			t.Fatalf("foreign schedule clear = %v", err)
		}
		if err := store.DeleteSchedule(ctx, "t2", scheduleID); !errors.Is(err, automation.ErrNotFound) {
			t.Fatalf("foreign schedule delete = %v", err)
		}
		if _, err := store.AckSchedule(ctx, "t2", scheduleID, "lease", automation.ScheduleResult{Outcome: automation.ScheduleSent}); !errors.Is(err, automation.ErrLeaseLost) {
			t.Fatalf("foreign schedule ack = %v", err)
		}
		if jobs, err := store.LeaseSchedules(ctx, "t2", 10, "worker"); err != nil || len(jobs) != 0 {
			t.Fatalf("foreign schedule lease = %+v, %v", jobs, err)
		}
	})
}

func TestPostgresRLSTenantIsolation(t *testing.T) {
	dsn := testDSN(t)
	schema := newTestSchema(t, dsn)
	store := openPostgres(t, dsn, schema, testConfig())
	ctx := t.Context()
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		watcher := testWatcher("rls-" + tenant)
		watcher.Tenant = tenant
		if _, err := store.CreateWatcher(ctx, watcher); err != nil {
			t.Fatalf("create %s: %v", tenant, err)
		}
	}
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = admin.Close(cleanup)
	})
	if _, err := admin.Exec(ctx, `SET search_path TO `+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatalf("set search_path: %v", err)
	}
	var currentUser string
	if err := admin.QueryRow(ctx, `SELECT current_user`).Scan(&currentUser); err != nil {
		t.Fatalf("current_user: %v", err)
	}
	role := fmt.Sprintf("keel_automation_rls_%d", time.Now().UnixNano())
	roleIdent := pgx.Identifier{role}.Sanitize()
	userIdent := pgx.Identifier{currentUser}.Sanitize()
	if _, err := admin.Exec(ctx, `CREATE ROLE `+roleIdent+` NOLOGIN`); err != nil {
		t.Fatalf("create role: %v", err)
	}
	if _, err := admin.Exec(ctx, `GRANT `+roleIdent+` TO `+userIdent); err != nil {
		admin.Exec(ctx, `DROP ROLE `+roleIdent)
		t.Fatalf("grant role: %v", err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = admin.Exec(cleanup, `DROP OWNED BY `+roleIdent)
		_, _ = admin.Exec(cleanup, `REVOKE `+roleIdent+` FROM `+userIdent)
		if _, err := admin.Exec(cleanup, `DROP ROLE `+roleIdent); err != nil {
			t.Logf("dropping role %s: %v", role, err)
		}
	})
	schemaIdent := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, `GRANT USAGE ON SCHEMA `+schemaIdent+` TO `+roleIdent); err != nil {
		t.Fatalf("grant schema: %v", err)
	}
	if _, err := admin.Exec(ctx, `GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA `+schemaIdent+` TO `+roleIdent); err != nil {
		t.Fatalf("grant tables: %v", err)
	}

	// A restricted role sees only the tenant it set.
	tx, err := admin.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE `+roleIdent); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("set role: %v", err)
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('keel.tenant_id', 'tenant-a', true)`); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("set tenant: %v", err)
	}
	var visible int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM board_watchers`).Scan(&visible); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("count: %v", err)
	}
	if visible != 1 {
		tx.Rollback(ctx)
		t.Fatalf("tenant-a visible watchers = %d, want 1", visible)
	}
	_, insertErr := tx.Exec(ctx, `
		INSERT INTO board_watchers (tenant_id, watcher_id, channel, action)
		VALUES ('tenant-b', 'forged', 'work', '{"name":"post"}'::jsonb)`)
	if insertErr == nil {
		tx.Rollback(ctx)
		t.Fatal("a cross-tenant insert succeeded under tenant-a's setting")
	}
	tx.Rollback(ctx)

	// With no tenant setting the policy matches nothing.
	tx, err = admin.Begin(ctx)
	if err != nil {
		t.Fatalf("begin unset: %v", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE `+roleIdent); err != nil {
		t.Fatalf("set role unset: %v", err)
	}
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM board_watchers`).Scan(&visible); err != nil {
		t.Fatalf("count unset: %v", err)
	}
	if visible != 0 {
		t.Fatalf("unset tenant visible watchers = %d, want 0", visible)
	}
}

func TestScheduleCreationKeyContract(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store automation.Store) {
		ctx := t.Context()
		id, _ := automation.NewScheduleID()
		schedule := testSchedule(id, time.Now().Add(time.Hour))
		schedule.CreationKey = "create-1"
		schedule.CreationDigest = "digest-1"
		first, err := store.CreateSchedule(ctx, schedule)
		if err != nil || first.Replayed {
			t.Fatalf("first create = %+v, %v", first, err)
		}
		replay, err := store.CreateSchedule(ctx, schedule)
		if err != nil || !replay.Replayed || replay.ID != first.ID {
			t.Fatalf("replay = %+v, %v", replay, err)
		}
		if list, err := store.Schedules(ctx, "t1"); err != nil || len(list) != 1 {
			t.Fatalf("schedules = %+v, %v; want one", list, err)
		}
		conflict := schedule
		conflict.CreationDigest = "digest-2"
		if _, err := store.CreateSchedule(ctx, conflict); !errors.Is(err, automation.ErrExists) {
			t.Fatalf("different digest = %v, want ErrExists", err)
		}
	})
}
