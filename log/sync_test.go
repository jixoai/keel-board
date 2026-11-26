package log_test

import (
	"context"
	"testing"

	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/store"
)

func TestSyncProjectionFoldsAndReplays(t *testing.T) {
	events := store.NewMemory()
	board := log.New(log.Options{Store: events})
	ctx := context.Background()
	var rootID uint64
	if err := board.Update(ctx, "t1", log.UpdateOptions{}, func(tx *log.Tx) error {
		rootID = tx.NextID(0)
		from := "slack"
		tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
			ID: rootID, Channel: "support", Text: "imported", Thread: log.ThreadKey(rootID),
			Labels: map[string]string{}, By: protocol.Author{Actor: "slack:user:U1", Principal: "slack:user:U1"},
			From: &from, Origin: &log.MessageOrigin{Channel: "C1", Thread: "C1:1", Message: "m1"},
			Attachments: []protocol.Attachment{{ID: "att1", Name: "a.txt", Type: "file", Size: 3}},
			Links:       []protocol.LinkView{{URL: "https://example.test/x"}},
		})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	target := log.SyncTarget{Key: "slack", Platform: "slack", Channel: "C9"}
	if err := board.Update(ctx, "t1", log.UpdateOptions{}, func(tx *log.Tx) error {
		tx.Emit(log.EventThreadSynced, &log.ThreadSyncedPayload{
			ID: rootID, Thread: log.ThreadKey(rootID), Target: target, Cause: "attach",
		})
		tx.Emit(log.EventMessageSynced, &log.MessageSyncedPayload{
			ID: rootID, Thread: log.ThreadKey(rootID), Target: target,
			ExternalThread: "C9:1", ExternalMessage: "root-1", Files: []string{"F1"}, FilesSet: true,
		})
		tx.Emit(log.EventMessageSyncFailed, &log.MessageSyncFailedPayload{
			ID: rootID, Thread: log.ThreadKey(rootID), Target: target, Error: "not_in_channel",
		})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	check := func(board *log.Board) {
		t.Helper()
		if err := board.Read("t1", func(snapshot *log.Snapshot) error {
			message := snapshot.Message(rootID)
			if message == nil {
				t.Fatal("root missing")
			}
			if message.Origin == nil || message.Origin.Thread != "C1:1" {
				t.Fatalf("origin = %+v", message.Origin)
			}
			if len(message.Attachments) != 1 || message.Attachments[0].Name != "a.txt" {
				t.Fatalf("attachments = %+v", message.Attachments)
			}
			if len(message.Links) != 1 {
				t.Fatalf("links = %+v", message.Links)
			}
			if message.Sync == nil {
				t.Fatal("sync state missing")
			}
			if len(message.Sync.Threads) != 1 || message.Sync.Threads[0] != target {
				t.Fatalf("threads = %+v", message.Sync.Threads)
			}
			if len(message.Sync.Copies) != 1 || !message.Sync.Copies[0].FilesSet || message.Sync.Copies[0].ExternalMessage != "root-1" {
				t.Fatalf("copies = %+v", message.Sync.Copies)
			}
			// A failure for a target without a copy is visible; a later copy
			// clears it.
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	check(board)
	// A fresh board over the same store decodes every new payload type.
	check(log.New(log.Options{Store: events}))

	// A successful copy clears an earlier failure for that target.
	other := log.SyncTarget{Key: "slack", Platform: "slack", Channel: "C10", Thread: "C10:1"}
	if err := board.Update(ctx, "t1", log.UpdateOptions{}, func(tx *log.Tx) error {
		tx.Emit(log.EventMessageSyncFailed, &log.MessageSyncFailedPayload{
			ID: rootID, Thread: log.ThreadKey(rootID), Target: other, Error: "rate_limited",
		})
		tx.Emit(log.EventMessageSynced, &log.MessageSyncedPayload{
			ID: rootID, Thread: log.ThreadKey(rootID), Target: other,
			ExternalThread: "C10:1", ExternalMessage: "root-2",
		})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := board.Read("t1", func(snapshot *log.Snapshot) error {
		state := snapshot.Message(rootID).Sync
		if _, failed := state.Undelivered[other.External()]; failed {
			t.Fatalf("undelivered kept after success: %+v", state.Undelivered)
		}
		if len(state.Copies) != 2 {
			t.Fatalf("copies = %+v", state.Copies)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
