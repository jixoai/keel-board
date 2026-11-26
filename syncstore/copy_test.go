package syncstore_test

import (
	"errors"
	"testing"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/syncstore"
)

func TestCopyByExternalAndFiles(t *testing.T) {
	withStores(t, testConfig(), func(t *testing.T, store syncstore.Store) {
		ctx := t.Context()
		target := threadTarget("C0123", "C0123:1727.9")
		copy := syncstore.Copy{
			BoardMessage: 12, BoardThread: 12, Key: "slack", Target: target,
			ExternalThread: "C0123:1727.9", ExternalMessage: "1700.1",
			Files: []string{"F1", "F2"}, FilesSet: true,
		}
		if _, err := store.SaveCopy(ctx, "t1", copy); err != nil {
			t.Fatalf("save copy: %v", err)
		}
		stored, found, err := store.Copy(ctx, "t1", "slack", target, 12)
		if err != nil || !found {
			t.Fatalf("copy = %+v, %v, %v", stored, found, err)
		}
		if !stored.FilesSet || len(stored.Files) != 2 || stored.Files[0] != "F1" {
			t.Fatalf("stored files = %+v", stored)
		}
		byExternal, found, err := store.CopyByExternal(ctx, "t1", "slack", target, "1700.1")
		if err != nil || !found || byExternal.BoardMessage != 12 {
			t.Fatalf("by external = %+v, %v, %v", byExternal, found, err)
		}
		anyTarget, found, err := store.CopyByExternalMessage(ctx, "t1", "slack", "1700.1")
		if err != nil || !found || anyTarget.BoardMessage != 12 {
			t.Fatalf("by external message = %+v, %v, %v", anyTarget, found, err)
		}
		if _, found, err := store.CopyByExternal(ctx, "t2", "slack", target, "1700.1"); err != nil || found {
			t.Fatalf("cross-tenant copy lookup = %v, %v", found, err)
		}
		if _, found, err := store.CopyByExternal(ctx, "t1", "slack", target, "missing"); err != nil || found {
			t.Fatalf("missing copy lookup = %v, %v", found, err)
		}
		// An update replaces the files; a receipt retry may carry the same
		// copy without files and must be able to keep what it stored.
		copy.Files, copy.FilesSet = []string{"F3"}, true
		if _, err := store.SaveCopy(ctx, "t1", copy); err != nil {
			t.Fatalf("update copy: %v", err)
		}
		stored, _, err = store.Copy(ctx, "t1", "slack", target, 12)
		if err != nil {
			t.Fatal(err)
		}
		if len(stored.Files) != 1 || stored.Files[0] != "F3" {
			t.Fatalf("updated files = %+v", stored)
		}
		// A second copy for another target of the same message keeps the
		// message's external identity unambiguous per target.
		channel := channelTarget("C0123")
		if _, err := store.SaveCopy(ctx, "t1", syncstore.Copy{
			BoardMessage: 12, BoardThread: 12, Key: "slack", Target: channel,
			ExternalThread: "C0123:1727.9", ExternalMessage: "1700.1",
		}); err != nil {
			t.Fatalf("save channel copy: %v", err)
		}
		if _, found, err := store.CopyByExternal(ctx, "t1", "slack", channel, "1700.1"); err != nil || !found {
			t.Fatalf("channel copy by external = %v, %v", found, err)
		}
		// Two targets now share one external message; the targetless lookup
		// refuses to guess.
		if _, _, err := store.CopyByExternalMessage(ctx, "t1", "slack", "1700.1"); err == nil {
			t.Fatal("ambiguous external message should be refused")
		}
	})
}

func TestCopyByExternalRejectsBadInput(t *testing.T) {
	memory, err := syncstore.NewMemory(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	if _, _, err := memory.CopyByExternal(ctx, "t1", "slack", threadTarget("C", "C:T"), ""); err == nil {
		t.Fatal("empty external message should be refused")
	}
	other := boardsync.ResolvedTarget{
		Key: "other", Platform: "slack", Channel: "C", Thread: "C:T", Kind: boardsync.TargetThread,
	}
	if _, _, err := memory.CopyByExternal(ctx, "t1", "slack", other, "1"); err == nil {
		t.Fatal("a target under another key should be refused")
	}
	if _, _, err := memory.CopyByExternal(ctx, "", "slack", threadTarget("C", "C:T"), "1"); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("empty tenant = %v", err)
	}
}
