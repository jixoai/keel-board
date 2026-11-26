package blob_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jixoai/keel-board/blob"
)

func TestMemoryStore(t *testing.T) {
	ctx := context.Background()
	store := blob.NewMemory()
	if err := store.Put(ctx, "attachments/a", []byte("hello"), "text/plain"); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, "attachments/a", []byte("again"), "text/plain"); err == nil {
		t.Fatal("a second put should not overwrite")
	}
	data, contentType, err := store.Get(ctx, "attachments/a")
	if err != nil || string(data) != "hello" || contentType != "text/plain" {
		t.Fatalf("get: %q %q %v", data, contentType, err)
	}
	// A returned slice is the caller's: changing it doesn't change the store.
	data[0] = 'j'
	if again, _, _ := store.Get(ctx, "attachments/a"); string(again) != "hello" {
		t.Fatalf("the store handed out its own bytes: %q", again)
	}
	size, err := store.Stat(ctx, "attachments/a")
	if err != nil || size != 5 {
		t.Fatalf("stat: %d %v", size, err)
	}
	if err := store.Delete(ctx, "attachments/a"); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, "attachments/a"); err != nil {
		t.Fatalf("deleting a missing key should be nothing: %v", err)
	}
	if _, _, err := store.Get(ctx, "attachments/a"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if _, err := store.Stat(ctx, "attachments/a"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestFSStore(t *testing.T) {
	ctx := context.Background()
	store, err := blob.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, "attachments/abc", []byte("bytes"), "image/png"); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, "attachments/abc", []byte("again"), "image/png"); err == nil {
		t.Fatal("a second put should not overwrite")
	}
	data, contentType, err := store.Get(ctx, "attachments/abc")
	if err != nil || string(data) != "bytes" || contentType != "image/png" {
		t.Fatalf("get: %q %q %v", data, contentType, err)
	}
	size, err := store.Stat(ctx, "attachments/abc")
	if err != nil || size != 5 {
		t.Fatalf("stat: %d %v", size, err)
	}
	if err := store.Delete(ctx, "attachments/abc"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Get(ctx, "attachments/abc"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	for _, key := range []string{"../escape", "attachments/../../escape", "/absolute", "attachments\\escape", "a//b", "."} {
		if err := store.Put(ctx, key, []byte("x"), "text/plain"); err == nil {
			t.Fatalf("%q should be an invalid key", key)
		}
	}
}
