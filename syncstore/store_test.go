package syncstore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/syncstore"
)

func TestEntryContract(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		entry := syncstore.Entry{Tenant: "t1", Key: "slack", Platform: "slack", Enabled: true}
		stored, err := store.UpsertEntry(ctx, entry)
		if err != nil {
			t.Fatalf("upsert: %v", err)
		}
		if stored.Tenant != "t1" || stored.Key != "slack" || !stored.Enabled {
			t.Fatalf("stored entry = %+v", stored)
		}
		// An upsert refreshes the platform and keeps the operator's enabled flag.
		stored, err = store.UpsertEntry(ctx, syncstore.Entry{
			Tenant: "t1", Key: "slack", Platform: "slack-enterprise", Enabled: false,
		})
		if err != nil {
			t.Fatalf("upsert again: %v", err)
		}
		if stored.Platform != "slack-enterprise" || !stored.Enabled {
			t.Fatalf("second upsert = %+v", stored)
		}
		found, ok, err := store.Entry(ctx, "t1", "slack")
		if err != nil || !ok || found.Platform != "slack-enterprise" {
			t.Fatalf("entry = %+v, %v, %v", found, ok, err)
		}
		if _, ok, err := store.Entry(ctx, "t2", "slack"); err != nil || ok {
			t.Fatalf("another tenant sees the entry: %v, %v", ok, err)
		}
		changed, err := store.SetEntryEnabled(ctx, "t1", "slack", false)
		if err != nil || !changed {
			t.Fatalf("disable = %v, %v", changed, err)
		}
		found, _, err = store.Entry(ctx, "t1", "slack")
		if err != nil || found.Enabled {
			t.Fatalf("enabled after disable = %+v, %v", found, err)
		}
		if changed, err := store.SetEntryEnabled(ctx, "t2", "slack", true); err != nil || changed {
			t.Fatalf("disable another tenant's entry = %v, %v", changed, err)
		}
		entries, err := store.Entries(ctx, "t1")
		if err != nil || len(entries) != 1 || entries[0].Key != "slack" {
			t.Fatalf("entries = %+v, %v", entries, err)
		}
		if list, err := store.Entries(ctx, "t2"); err != nil || len(list) != 0 {
			t.Fatalf("another tenant's entries = %+v, %v", list, err)
		}
	})
}

func TestThreadContract(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		thread := syncstore.Thread{
			Tenant: "t1", BoardThread: 812, Key: "slack", Platform: "slack",
			Channel: "C0123", Thread: "C0123:1727.9", Kind: boardsync.TargetThread,
		}
		first, err := store.UpsertThread(ctx, thread)
		if err != nil {
			t.Fatalf("upsert thread: %v", err)
		}
		if first.ID == "" || first.CreatedAt.IsZero() {
			t.Fatalf("stored thread = %+v", first)
		}
		second, err := store.UpsertThread(ctx, thread)
		if err != nil || second.ID != first.ID {
			t.Fatalf("thread upsert isn't deterministic: %+v, %v", second, err)
		}
		other := syncstore.Thread{
			Tenant: "t1", BoardThread: 812, Key: "slack", Platform: "slack",
			Channel: "C0999", Kind: boardsync.TargetChannel,
		}
		if _, err := store.UpsertThread(ctx, other); err != nil {
			t.Fatalf("upsert second thread: %v", err)
		}
		threads, err := store.Threads(ctx, "t1", 812)
		if err != nil || len(threads) != 2 {
			t.Fatalf("threads = %+v, %v", threads, err)
		}
		if list, err := store.Threads(ctx, "t2", 812); err != nil || len(list) != 0 {
			t.Fatalf("another tenant's threads = %+v, %v", list, err)
		}
		if err := store.RemoveThread(ctx, "t1", first.ID); err != nil {
			t.Fatalf("remove thread: %v", err)
		}
		threads, err = store.Threads(ctx, "t1", 812)
		if err != nil || len(threads) != 1 {
			t.Fatalf("threads after remove = %+v, %v", threads, err)
		}
	})
}

func TestCursorContract(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		if cursor, ok, err := store.Cursor(ctx, "t1", "slack", "backfill"); err != nil || ok || cursor != "" {
			t.Fatalf("empty cursor = %q, %v, %v", cursor, ok, err)
		}
		if err := store.SetCursor(ctx, "t1", "slack", "backfill", "cursor-1"); err != nil {
			t.Fatalf("set cursor: %v", err)
		}
		if cursor, ok, err := store.Cursor(ctx, "t1", "slack", "backfill"); err != nil || !ok || cursor != "cursor-1" {
			t.Fatalf("cursor = %q, %v, %v", cursor, ok, err)
		}
		if err := store.SetCursor(ctx, "t1", "slack", "backfill", "cursor-2"); err != nil {
			t.Fatalf("advance cursor: %v", err)
		}
		if cursor, _, _ := store.Cursor(ctx, "t1", "slack", "backfill"); cursor != "cursor-2" {
			t.Fatalf("cursor after advance = %q", cursor)
		}
		if _, ok, err := store.Cursor(ctx, "t1", "github", "backfill"); err != nil || ok {
			t.Fatalf("another key's cursor = %v, %v", ok, err)
		}
		if _, ok, err := store.Cursor(ctx, "t2", "slack", "backfill"); err != nil || ok {
			t.Fatalf("another tenant's cursor = %v, %v", ok, err)
		}
		if err := store.SetCursor(ctx, "t1", "slack", "backfill", ""); !errors.Is(err, boardsync.ErrBadRequest) {
			t.Fatalf("empty cursor write = %v", err)
		}
	})
}

func TestNonceContract(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		claimed, err := store.ClaimNonce(ctx, "t1", "telegram", "nonce-1", 40*time.Millisecond)
		if err != nil || !claimed {
			t.Fatalf("first claim = %v, %v", claimed, err)
		}
		if claimed, err := store.ClaimNonce(ctx, "t1", "telegram", "nonce-1", time.Minute); err != nil || claimed {
			t.Fatalf("replay inside the window = %v, %v", claimed, err)
		}
		if claimed, err := store.ClaimNonce(ctx, "t2", "telegram", "nonce-1", time.Minute); err != nil || !claimed {
			t.Fatalf("another tenant's nonce = %v, %v", claimed, err)
		}
		time.Sleep(60 * time.Millisecond)
		if claimed, err := store.ClaimNonce(ctx, "t1", "telegram", "nonce-1", time.Minute); err != nil || !claimed {
			t.Fatalf("claim after the window = %v, %v", claimed, err)
		}
		removed, err := store.PruneNonces(ctx, time.Now().Add(2*time.Minute))
		if err != nil || removed < 2 {
			t.Fatalf("prune = %d, %v", removed, err)
		}
	})
}

func TestStoreRefusesUnscopedInput(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		if _, err := store.PushInbox(ctx, "", "slack", []boardsync.Record{testRecord("m1", "C1:T1")}); !errors.Is(err, boardsync.ErrBadRequest) {
			t.Fatalf("empty tenant push = %v", err)
		}
		if _, err := store.PushInbox(ctx, "t1", "Bad Key", []boardsync.Record{testRecord("m1", "C1:T1")}); !errors.Is(err, boardsync.ErrBadRequest) {
			t.Fatalf("bad key push = %v", err)
		}
		if _, err := store.LeaseInbox(ctx, "t1", "slack", 10, "w"); err != nil {
			t.Fatalf("empty lease should be an empty answer: %v", err)
		}
		if _, ok, err := store.Entry(ctx, "t1", "missing"); err != nil || ok {
			t.Fatalf("missing entry = %v, %v", ok, err)
		}
	})
}

func TestErrorCodeIsContentFree(t *testing.T) {
	err := errors.New("token=SECRET body=ship it")
	if code := syncstore.ErrorCode(err); code != "failed" {
		t.Fatalf("generic error code = %q", code)
	}
	if code := syncstore.ErrorCode(boardsync.Permanent(errors.New("secret"))); code != "permanent" {
		t.Fatalf("permanent error code = %q", code)
	}
	if code := syncstore.ErrorCode(context.Canceled); code != "canceled" {
		t.Fatalf("canceled code = %q", code)
	}
}
