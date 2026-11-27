package syncstore_test

import (
	"context"
	"testing"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/fake"
	"github.com/jixoai/keel-board/syncstore"
)

func workerWithFiles(t *testing.T, store syncstore.Store, provider *fake.Provider, loader boardsync.FileLoader) (*fake.Host, *syncstore.OutboundWorker) {
	t.Helper()
	registry := boardsync.NewRegistry()
	if err := registry.Add(boardsync.Entry{Tenant: "t1", Key: "slack", Platform: "slack", Provider: provider, Sync: boardsync.Mirror("support")}); err != nil {
		t.Fatalf("register entry: %v", err)
	}
	host := fake.NewHost()
	worker, err := syncstore.NewOutboundWorker(syncstore.OutboundWorkerConfig{
		Store: store, Registry: registry, Runner: boardsync.NewRunner(host), Host: host,
		Tenant: "t1", Keys: []boardsync.Key{"slack"}, Files: loader,
		Owner: "worker", Limit: 10,
	})
	if err != nil {
		t.Fatalf("new outbound worker: %v", err)
	}
	return host, worker
}

func TestOutboundWorkerLoadsAttachmentBytesAndPersistsTheReceipt(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		provider := fake.New("slack", boardsync.Capabilities{UploadsFiles: true})
		provider.PostFunc = func(_ context.Context, _ boardsync.Key, target boardsync.ResolvedTarget, msg boardsync.OutboundMessage) (boardsync.Posted, error) {
			if len(msg.Files) != 1 || string(msg.Files[0].Data) != "bytes" {
				t.Fatalf("post files = %+v", msg.Files)
			}
			return boardsync.Posted{Thread: target.Thread, Message: "m1", Files: []string{"F1"}}, nil
		}
		loader := func(_ context.Context, tenant string, message boardsync.Message) (boardsync.FileLoadResult, error) {
			if tenant != "t1" || len(message.Attachments) != 1 || message.Attachments[0].ID != "0123456789abcdefghijkl" {
				t.Fatalf("loader saw %q %+v", tenant, message.Attachments)
			}
			return boardsync.FileLoadResult{Files: []boardsync.OutboundFile{{Data: []byte("bytes"), Filename: "a.txt"}}}, nil
		}
		_, worker := workerWithFiles(t, store, provider, loader)
		target := threadTarget("C0123", "C0123:T1")
		message := boardMessage(101, 100, "with a file")
		message.Attachments = []boardsync.AttachmentRef{{ID: "0123456789abcdefghijkl", Name: "a.txt", Size: 5}}
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack", postedEvent(10, 100, 101), message, []boardsync.ResolvedTarget{target}); err != nil {
			t.Fatal(err)
		}
		result, err := worker.Process(ctx)
		if err != nil || result.Acked != 1 {
			t.Fatalf("process = %+v, %v", result, err)
		}
		if len(provider.Posts()) != 1 {
			t.Fatalf("posts = %+v", provider.Posts())
		}
		copied, found, err := store.Copy(ctx, "t1", "slack", target, 101)
		if err != nil || !found {
			t.Fatalf("copy = %+v, %v, %v", copied, found, err)
		}
		if !copied.FilesSet || len(copied.Files) != 1 || copied.Files[0] != "F1" {
			t.Fatalf("copy files = %+v", copied)
		}
		// A second pass finds nothing: the delivery is acknowledged.
		if result, err := worker.Process(ctx); err != nil || result.Leased != 0 {
			t.Fatalf("second pass = %+v, %v", result, err)
		}
		if len(provider.Posts()) != 1 {
			t.Fatalf("duplicate posts = %+v", provider.Posts())
		}
	})
}

func TestOutboundWorkerTypingIsBestEffort(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		provider := fake.New("slack", boardsync.Capabilities{Typing: true})
		provider.PostFunc = func(_ context.Context, _ boardsync.Key, target boardsync.ResolvedTarget, _ boardsync.OutboundMessage) (boardsync.Posted, error) {
			return boardsync.Posted{Thread: target.Thread, Message: "m1"}, nil
		}
		provider.TypingFunc = func(context.Context, boardsync.Key, boardsync.ThreadRef, string, string) error {
			return boardsync.ErrUnsupported
		}
		_, worker := workerWithFiles(t, store, provider, nil)
		target := threadTarget("C0123", "C0123:T1")
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack", postedEvent(10, 100, 101), boardMessage(101, 100, "one"), []boardsync.ResolvedTarget{target}); err != nil {
			t.Fatal(err)
		}
		result, err := worker.Process(ctx)
		if err != nil || result.Acked != 1 {
			t.Fatalf("process = %+v, %v", result, err)
		}
		if len(provider.Posts()) != 1 {
			t.Fatalf("post lost after typing failure: %+v", provider.Posts())
		}
		if len(provider.Typings()) != 1 {
			t.Fatalf("typing calls = %+v", provider.Typings())
		}
		// The unsupported answer is remembered: the next message skips typing.
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack", postedEvent(11, 100, 102), boardMessage(102, 100, "two"), []boardsync.ResolvedTarget{target}); err != nil {
			t.Fatal(err)
		}
		if _, err := worker.Process(ctx); err != nil {
			t.Fatal(err)
		}
		if len(provider.Posts()) != 2 {
			t.Fatalf("posts = %+v", provider.Posts())
		}
		if len(provider.Typings()) != 1 {
			t.Fatalf("unsupported typing asked again: %+v", provider.Typings())
		}
	})
}

func TestOutboundWorkerTypingSucceedsBeforeAPost(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		provider := fake.New("slack", boardsync.Capabilities{Typing: true})
		provider.PostFunc = func(_ context.Context, _ boardsync.Key, target boardsync.ResolvedTarget, _ boardsync.OutboundMessage) (boardsync.Posted, error) {
			return boardsync.Posted{Thread: target.Thread, Message: "m1"}, nil
		}
		_, worker := workerWithFiles(t, store, provider, nil)
		target := threadTarget("C0123", "C0123:T1")
		if _, err := store.EnqueueOutbox(ctx, "t1", "slack", postedEvent(10, 100, 101), boardMessage(101, 100, "one"), []boardsync.ResolvedTarget{target}); err != nil {
			t.Fatal(err)
		}
		if _, err := worker.Process(ctx); err != nil {
			t.Fatal(err)
		}
		calls := provider.Typings()
		if len(calls) != 1 || calls[0].Thread.Channel != "C0123" || calls[0].Thread.Thread != "C0123:T1" {
			t.Fatalf("typing calls = %+v", calls)
		}
	})
}
