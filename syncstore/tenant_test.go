package syncstore_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/fake"
	"github.com/jixoai/keel-board/syncstore"
)

func testTarget(key boardsync.Key, platform boardsync.Platform, channel, thread string) boardsync.ResolvedTarget {
	return boardsync.ResolvedTarget{
		Key: key, Platform: platform, Channel: channel, Thread: thread,
		Kind: boardsync.TargetThread,
	}
}

func TestWebhookTenantIsolationSameKey(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		seedEntry(t, store, "tenant-a", "slack", true)
		seedEntry(t, store, "tenant-b", "slack", true)
		registry := boardsync.NewRegistry()
		providerA := fake.New("slack", boardsync.Capabilities{})
		providerB := fake.New("slack", boardsync.Capabilities{})
		routes := map[string]func(context.Context, string) (bool, error){
			"tenant-a": func(_ context.Context, channel string) (bool, error) { return channel == "CA", nil },
			"tenant-b": func(_ context.Context, channel string) (bool, error) { return channel == "CB", nil },
		}
		for tenant, provider := range map[string]*fake.Provider{"tenant-a": providerA, "tenant-b": providerB} {
			if err := registry.Add(boardsync.Entry{
				Tenant: tenant, Key: "slack", Platform: "slack", Provider: provider,
				Sync: boardsync.Mirror("support"), Routes: routes[tenant],
			}); err != nil {
				t.Fatalf("register %s: %v", tenant, err)
			}
		}
		channelA := testRecord("a1", "CA:T1")
		channelA.Event.Channel = "CA"
		channelACross := testRecord("a2", "CB:T1")
		channelACross.Event.Channel = "CB"
		channelB := testRecord("b1", "CB:T1")
		channelB.Event.Channel = "CB"
		channelBCross := testRecord("b2", "CA:T1")
		channelBCross.Event.Channel = "CA"
		providerA.ReceiveFunc = func(context.Context, boardsync.Key, *boardsync.WebhookRequest) (boardsync.Intake, error) {
			return boardsync.Intake{Response: boardsync.OK(), Records: []boardsync.Record{channelA, channelACross}}, nil
		}
		providerB.ReceiveFunc = func(context.Context, boardsync.Key, *boardsync.WebhookRequest) (boardsync.Intake, error) {
			return boardsync.Intake{Response: boardsync.OK(), Records: []boardsync.Record{channelB, channelBCross}}, nil
		}
		tenant := func(r *http.Request) (string, error) { return r.Header.Get("X-Test-Tenant"), nil }
		handler := newWebhookHandler(t, store, registry, tenant, 1<<20)
		for _, name := range []string{"tenant-a", "tenant-b"} {
			request := webhookRequest("slack", "{}")
			request.Header.Set("X-Test-Tenant", name)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusOK {
				t.Fatalf("%s webhook = %d", name, recorder.Code)
			}
		}
		if len(providerA.Receives()) != 1 || len(providerB.Receives()) != 1 {
			t.Fatalf("providers received %d/%d webhooks; same key crossed tenants",
				len(providerA.Receives()), len(providerB.Receives()))
		}
		a, err := store.InboxDepth(t.Context(), "tenant-a", "slack")
		if err != nil || a.Pending != 1 {
			t.Fatalf("tenant-a depth = %+v, %v", a, err)
		}
		b, err := store.InboxDepth(t.Context(), "tenant-b", "slack")
		if err != nil || b.Pending != 1 {
			t.Fatalf("tenant-b depth = %+v, %v", b, err)
		}
	})
}

func TestInboundWorkersCarryTenant(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		registry := boardsync.NewRegistry()
		hostA := fake.NewHost()
		hostB := fake.NewHost()
		for tenant, provider := range map[string]*fake.Provider{"tenant-a": fake.New("slack", boardsync.Capabilities{}), "tenant-b": fake.New("slack", boardsync.Capabilities{})} {
			if err := registry.Add(boardsync.Entry{Tenant: tenant, Key: "slack", Platform: "slack", Provider: provider, Sync: boardsync.Mirror("support")}); err != nil {
				t.Fatalf("register %s: %v", tenant, err)
			}
		}
		for tenant, id := range map[string]string{"tenant-a": "a1", "tenant-b": "b1"} {
			if _, err := store.PushInbox(ctx, tenant, "slack", []boardsync.Record{testRecord(id, "C:T1")}); err != nil {
				t.Fatalf("push %s: %v", tenant, err)
			}
		}
		for tenant, host := range map[string]*fake.Host{"tenant-a": hostA, "tenant-b": hostB} {
			worker, err := syncstore.NewInboundWorker(syncstore.InboundWorkerConfig{
				Store: store, Registry: registry, Runner: boardsync.NewRunner(host),
				Tenant: tenant, Key: "slack", Owner: "worker", Limit: 10,
			})
			if err != nil {
				t.Fatalf("worker %s: %v", tenant, err)
			}
			if result, err := worker.Process(ctx); err != nil || result.Acked != 1 {
				t.Fatalf("%s pass = %+v, %v", tenant, result, err)
			}
		}
		importsA := hostA.Imports()
		importsB := hostB.Imports()
		if len(importsA) != 1 || importsA[0].Tenant != "tenant-a" || importsA[0].EventID != "a1" {
			t.Fatalf("tenant-a imports = %+v", importsA)
		}
		if len(importsB) != 1 || importsB[0].Tenant != "tenant-b" || importsB[0].EventID != "b1" {
			t.Fatalf("tenant-b imports = %+v", importsB)
		}
	})
}

func outboundWorkerFor(t *testing.T, store syncstore.Store, registry *boardsync.Registry, tenant string, keys []boardsync.Key, host boardsync.Host) *syncstore.OutboundWorker {
	t.Helper()
	worker, err := syncstore.NewOutboundWorker(syncstore.OutboundWorkerConfig{
		Store: store, Registry: registry, Runner: boardsync.NewRunner(host), Host: host,
		Tenant: tenant, Keys: keys, Owner: tenant, Limit: 10, Defer: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new outbound worker: %v", err)
	}
	return worker
}

func TestOutboundWorkersOwnOnlyTheirKeys(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		registry := boardsync.NewRegistry()
		providerA := fake.New("alpha", boardsync.Capabilities{})
		providerB := fake.New("beta", boardsync.Capabilities{})
		for key, provider := range map[boardsync.Key]*fake.Provider{"alpha": providerA, "beta": providerB} {
			if err := registry.Add(boardsync.Entry{Tenant: "t1", Key: key, Platform: boardsync.Platform(key), Provider: provider, Sync: boardsync.Mirror("support")}); err != nil {
				t.Fatalf("register %s: %v", key, err)
			}
		}
		providerA.PostFunc = func(_ context.Context, _ boardsync.Key, target boardsync.ResolvedTarget, msg boardsync.OutboundMessage) (boardsync.Posted, error) {
			return boardsync.Posted{Thread: target.Thread, Message: "a-post"}, nil
		}
		providerB.PostFunc = func(_ context.Context, _ boardsync.Key, target boardsync.ResolvedTarget, msg boardsync.OutboundMessage) (boardsync.Posted, error) {
			return boardsync.Posted{Thread: target.Thread, Message: "b-post"}, nil
		}
		if _, err := store.EnqueueOutbox(ctx, "t1", "alpha", postedEvent(10, 100, 101),
			boardMessage(101, 100, "alpha first"), []boardsync.ResolvedTarget{testTarget("alpha", "alpha", "CA", "CA:T1")}); err != nil {
			t.Fatalf("enqueue alpha: %v", err)
		}
		if _, err := store.EnqueueOutbox(ctx, "t1", "beta", postedEvent(11, 100, 102),
			boardMessage(102, 100, "beta second"), []boardsync.ResolvedTarget{testTarget("beta", "beta", "CB", "CB:T1")}); err != nil {
			t.Fatalf("enqueue beta: %v", err)
		}
		hostA, hostB := fake.NewHost(), fake.NewHost()
		workerA := outboundWorkerFor(t, store, registry, "t1", []boardsync.Key{"alpha"}, hostA)
		workerB := outboundWorkerFor(t, store, registry, "t1", []boardsync.Key{"beta"}, hostB)
		// The lane head is alpha; beta's worker must not jump over it.
		if result, err := workerB.Process(ctx); err != nil || result.Leased != 0 {
			t.Fatalf("beta worker jumped the lane: %+v, %v", result, err)
		}
		if result, err := workerA.Process(ctx); err != nil || result.Acked != 1 {
			t.Fatalf("alpha pass = %+v, %v", result, err)
		}
		if len(providerB.Posts()) != 0 {
			t.Fatal("alpha's worker posted through beta's provider")
		}
		if result, err := workerB.Process(ctx); err != nil || result.Acked != 1 {
			t.Fatalf("beta pass = %+v, %v", result, err)
		}
		if len(providerA.Posts()) != 1 || len(providerB.Posts()) != 1 {
			t.Fatalf("posts = %d/%d", len(providerA.Posts()), len(providerB.Posts()))
		}
		for _, host := range []*fake.Host{hostA, hostB} {
			for _, record := range host.Records() {
				if record.Tenant != "t1" {
					t.Fatalf("receipt tenant = %q, want t1", record.Tenant)
				}
			}
		}
	})
}

func TestOutboundWorkerDefersUnregisteredOwnedKey(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		target := threadTarget("C0123", "C0123:T1")
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack", postedEvent(10, 100, 101),
			boardMessage(101, 100, "root"), []boardsync.ResolvedTarget{target}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		registry := boardsync.NewRegistry()
		host := fake.NewHost()
		worker := outboundWorkerFor(t, store, registry, "t1", []boardsync.Key{"slack"}, host)
		result, err := worker.Process(ctx)
		if err != nil || result.Deferred != 1 || result.Dead != 0 {
			t.Fatalf("unready pass = %+v, %v", result, err)
		}
		depth, err := store.OutboxDepth(ctx, "t1")
		if err != nil || depth.Pending != 1 || depth.Dead != 0 {
			t.Fatalf("depth after defer = %+v, %v", depth, err)
		}
		provider := fake.New("slack", boardsync.Capabilities{})
		if err := registry.Add(boardsync.Entry{Tenant: "t1", Key: "slack", Platform: "slack", Provider: provider, Sync: boardsync.Mirror("support")}); err != nil {
			t.Fatalf("register: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
		result, err = worker.Process(ctx)
		if err != nil || result.Acked != 1 {
			t.Fatalf("ready pass = %+v, %v", result, err)
		}
		if posts := provider.Posts(); len(posts) != 1 {
			t.Fatalf("posts = %d, want one", len(posts))
		}
	})
}

func TestOutboundWorkerTenantIsolationSameKey(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		registry := boardsync.NewRegistry()
		providerA := fake.New("slack", boardsync.Capabilities{})
		providerB := fake.New("slack", boardsync.Capabilities{})
		for tenant, provider := range map[string]*fake.Provider{"tenant-a": providerA, "tenant-b": providerB} {
			if err := registry.Add(boardsync.Entry{Tenant: tenant, Key: "slack", Platform: "slack", Provider: provider, Sync: boardsync.Mirror("support")}); err != nil {
				t.Fatalf("register %s: %v", tenant, err)
			}
		}
		providerA.PostFunc = func(_ context.Context, _ boardsync.Key, target boardsync.ResolvedTarget, msg boardsync.OutboundMessage) (boardsync.Posted, error) {
			return boardsync.Posted{Thread: target.Thread, Message: "a-post"}, nil
		}
		providerB.PostFunc = func(_ context.Context, _ boardsync.Key, target boardsync.ResolvedTarget, msg boardsync.OutboundMessage) (boardsync.Posted, error) {
			return boardsync.Posted{Thread: target.Thread, Message: "b-post"}, nil
		}
		target := threadTarget("C0123", "C0123:T1")
		for tenant, id := range map[string]uint64{"tenant-a": 101, "tenant-b": 102} {
			if _, err := store.EnqueueOutbox(ctx, tenant, "slack", postedEvent(id, 100, id),
				boardMessage(id, 100, "root"), []boardsync.ResolvedTarget{target}); err != nil {
				t.Fatalf("enqueue %s: %v", tenant, err)
			}
		}
		for tenant, provider := range map[string]*fake.Provider{"tenant-a": providerA, "tenant-b": providerB} {
			host := fake.NewHost()
			worker := outboundWorkerFor(t, store, registry, tenant, []boardsync.Key{"slack"}, host)
			if result, err := worker.Process(ctx); err != nil || result.Acked != 1 {
				t.Fatalf("%s pass = %+v, %v", tenant, result, err)
			}
			if posts := provider.Posts(); len(posts) != 1 {
				t.Fatalf("%s posts = %d", tenant, len(posts))
			}
			for _, record := range host.Records() {
				if record.Tenant != tenant {
					t.Fatalf("%s receipt tenant = %q", tenant, record.Tenant)
				}
			}
		}
		if len(providerA.Posts()) != 1 || len(providerB.Posts()) != 1 {
			t.Fatalf("tenant providers posted %d/%d", len(providerA.Posts()), len(providerB.Posts()))
		}
	})
}
