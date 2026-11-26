package log_test

import (
	"context"
	"testing"
	"time"

	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/store"
)

// requestOnlyStore hides request metadata from Load and History, as a cache
// that predates request results shows; only Request answers the key.
type requestOnlyStore struct {
	*store.Memory
}

func stripRequests(records []store.Record) []store.Record {
	for index := range records {
		records[index].RequestKey = ""
		records[index].Digest = ""
		records[index].RequestLast = 0
		records[index].RequestResult = nil
	}
	return records
}

func (s requestOnlyStore) Load(ctx context.Context, tenant string) ([]store.Record, error) {
	records, err := s.Memory.Load(ctx, tenant)
	return stripRequests(records), err
}

func (s requestOnlyStore) History(ctx context.Context, tenant string, after uint64, limit int) ([]store.Record, error) {
	records, err := s.Memory.History(ctx, tenant, after, limit)
	return stripRequests(records), err
}

func TestReplayKeySeesAStoreOnlyRequest(t *testing.T) {
	ctx := context.Background()
	memory := store.NewMemory()
	writer := log.New(log.Options{Store: memory, Freshness: -1})
	key := "post:local:user:alice:probe"
	var messageID uint64
	err := writer.Update(ctx, "default", log.UpdateOptions{Idempotency: &log.Idempotency{
		Key: key, Digest: "place:channel:work",
	}}, func(tx *log.Tx) error {
		messageID = tx.NextID(0)
		tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
			ID: messageID, Channel: "work", Text: "one", Thread: log.ThreadKey(messageID),
			Labels: map[string]string{}, By: protocol.Author{Actor: "local:user:alice", Principal: "local:user:alice"},
		})
		tx.RecordResult(messageID)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// The reader's cache has never been filled and its records don't carry the
	// request metadata; only the store's Request answers it.
	reader := log.New(log.Options{Store: requestOnlyStore{memory}, Freshness: -1})
	result, found, err := reader.ReplayKey(ctx, "default", key)
	if err != nil {
		t.Fatal(err)
	}
	if !found || !result.Replayed || result.Digest != "place:channel:work" {
		t.Fatalf("unexpected replay: %+v, found %v", result, found)
	}
	if len(result.MessageIDs) != 1 || result.MessageIDs[0] != messageID {
		t.Fatalf("message IDs: %+v, want %d", result.MessageIDs, messageID)
	}

	// A warming read before the write doesn't hide it either: ReplayKey
	// refreshes past the freshness window.
	warm := log.New(log.Options{Store: memory, Freshness: time.Hour})
	if err := warm.Read("default", func(*log.Snapshot) error { return nil }); err != nil {
		t.Fatal(err)
	}
	other := log.New(log.Options{Store: memory, Freshness: -1})
	if err := other.Update(ctx, "default", log.UpdateOptions{Idempotency: &log.Idempotency{
		Key: "post:local:user:bob:later", Digest: "place:channel:work",
	}}, func(tx *log.Tx) error {
		id := tx.NextID(0)
		tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
			ID: id, Channel: "work", Text: "two", Thread: log.ThreadKey(id),
			Labels: map[string]string{}, By: protocol.Author{Actor: "local:user:bob", Principal: "local:user:bob"},
		})
		tx.RecordResult(id)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	result, found, err = warm.ReplayKey(ctx, "default", "post:local:user:bob:later")
	if err != nil || !found || len(result.MessageIDs) != 1 {
		t.Fatalf("warm cache replay: %+v, %v, %v", result, found, err)
	}
}
