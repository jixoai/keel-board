package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jixoai/keel-board/store"
)

// storeContract runs the behavior every EventStore must have. Memory and
// PostgreSQL run the same subtests; a difference between them is a bug in one.
func storeContract(t *testing.T, open func(t *testing.T) store.EventStore) {
	t.Helper()

	t.Run("append and load", func(t *testing.T) {
		events := open(t)
		ctx := context.Background()
		at := time.Date(2026, 1, 2, 3, 4, 5, 6000, time.UTC)
		records := []store.Record{
			{Seq: 1, Type: "message.posted", Channel: "work", Payload: []byte(`{"id":1,"channel":"work","text":"one"}`), At: at},
			{Seq: 2, Type: "task.created", Channel: "work", Payload: []byte(`{"id":1,"thread":"1"}`), At: at.Add(time.Second)},
		}
		result, err := events.Append(ctx, "t", records, 0)
		if err != nil {
			t.Fatal(err)
		}
		if result.Replayed || result.FirstSeq != 1 || result.LastSeq != 2 {
			t.Fatalf("append result: %+v", result)
		}
		loaded, err := events.Load(ctx, "t")
		if err != nil {
			t.Fatal(err)
		}
		if len(loaded) != 2 {
			t.Fatalf("loaded %d records, want 2", len(loaded))
		}
		if loaded[0].Seq != 1 || loaded[1].Seq != 2 || loaded[0].Type != "message.posted" || loaded[1].Type != "task.created" {
			t.Fatalf("loaded records: %+v", loaded)
		}
		if loaded[0].Channel != "work" || loaded[1].Channel != "work" {
			t.Fatalf("channels: %q / %q", loaded[0].Channel, loaded[1].Channel)
		}
		if !loaded[0].At.Equal(at) || !loaded[1].At.Equal(at.Add(time.Second)) {
			t.Fatalf("times: %v / %v", loaded[0].At, loaded[1].At)
		}
		if !sameJSON(t, loaded[0].Payload, `{"id":1,"channel":"work","text":"one"}`) {
			t.Fatalf("payload: %s", loaded[0].Payload)
		}
		if loaded[0].RequestResult != nil || loaded[1].RequestResult != nil {
			t.Fatalf("a non-keyed record should carry no request result: %q / %q", loaded[0].RequestResult, loaded[1].RequestResult)
		}
		// Load copies: a caller can't rewrite the log.
		loaded[0].Payload[0] = 'X'
		again, err := events.Load(ctx, "t")
		if err != nil {
			t.Fatal(err)
		}
		if !sameJSON(t, again[0].Payload, `{"id":1,"channel":"work","text":"one"}`) {
			t.Fatalf("load should copy payloads: %s", again[0].Payload)
		}
		other, err := events.Load(ctx, "other")
		if err != nil || len(other) != 0 {
			t.Fatalf("another tenant: %+v, %v", other, err)
		}
	})

	t.Run("empty batch", func(t *testing.T) {
		events := open(t)
		result, err := events.Append(context.Background(), "t", nil, 0)
		if err != nil || result.Replayed || result.LastSeq != 0 {
			t.Fatalf("empty append: %+v, %v", result, err)
		}
		loaded, err := events.Load(context.Background(), "t")
		if err != nil || len(loaded) != 0 {
			t.Fatalf("empty append wrote: %+v, %v", loaded, err)
		}
	})

	t.Run("payload contract", func(t *testing.T) {
		events := open(t)
		ctx := context.Background()
		// Empty payload is JSON null and a zero time is filled in; the caller's
		// record is never changed.
		empty := seqRecord(1, "event", "")
		empty.Payload = nil
		empty.At = time.Time{}
		if _, err := events.Append(ctx, "t", []store.Record{empty}, 0); err != nil {
			t.Fatal(err)
		}
		if empty.Payload != nil || !empty.At.IsZero() {
			t.Fatalf("append mutated the caller's record: %+v", empty)
		}
		loaded, err := events.Load(ctx, "t")
		if err != nil {
			t.Fatal(err)
		}
		if len(loaded) != 1 || !sameJSON(t, loaded[0].Payload, `null`) {
			t.Fatalf("empty payload: %+v", loaded)
		}
		if loaded[0].At.IsZero() {
			t.Fatalf("a zero At should be filled at append")
		}
		if loaded[0].At.Nanosecond()%1000 != 0 {
			t.Fatalf("timestamps should keep microseconds, got %v", loaded[0].At)
		}

		// Nanoseconds are truncated to the store's microsecond precision.
		nanos := time.Date(2026, 2, 3, 4, 5, 6, 123456789, time.UTC)
		precise := seqRecord(2, "event", `{"n":2}`)
		precise.At = nanos
		mustAppend(t, events, "t", 1, precise)
		loaded, _ = events.Load(ctx, "t")
		if !loaded[1].At.Equal(nanos.Truncate(time.Microsecond)) {
			t.Fatalf("nanosecond timestamp: %v, want %v", loaded[1].At, nanos.Truncate(time.Microsecond))
		}

		// Malformed JSON and a NUL are refused, and nothing is written.
		for _, payload := range []string{`{not json`, `{"text":"a\u0000b"}`} {
			bad := seqRecord(3, "event", payload)
			if _, err := events.Append(ctx, "t", []store.Record{bad}, 2); !errors.Is(err, store.ErrInvalidPayload) {
				t.Fatalf("payload %q: %v", payload, err)
			}
			if string(bad.Payload) != payload {
				t.Fatalf("a refused append mutated the caller's payload: %q", bad.Payload)
			}
		}
		loaded, _ = events.Load(ctx, "t")
		if len(loaded) != 2 {
			t.Fatalf("a refused payload wrote: %+v", loaded)
		}
		mustAppend(t, events, "t", 2, seqRecord(3, "event", `{"n":3}`))
	})

	t.Run("conditional append", func(t *testing.T) {
		events := open(t)
		ctx := context.Background()
		mustAppend(t, events, "t", 0, seqRecord(1, "event", `{"n":1}`))
		if _, err := events.Append(ctx, "t", []store.Record{seqRecord(2, "event", `{"n":2}`)}, 0); !errors.Is(err, store.ErrHeadMoved) {
			t.Fatalf("stale head: %v", err)
		}
		mustAppend(t, events, "t", 1, seqRecord(2, "event", `{"n":2}`))
		// A batch whose own seqs aren't contiguous is refused.
		batch := []store.Record{seqRecord(3, "a", `{}`), seqRecord(5, "b", `{}`)}
		if _, err := events.Append(ctx, "t", batch, 2); !errors.Is(err, store.ErrHeadMoved) {
			t.Fatalf("non-contiguous batch: %v", err)
		}
		// A batch that starts past the expected head is refused too.
		if _, err := events.Append(ctx, "t", []store.Record{seqRecord(4, "event", `{}`)}, 2); !errors.Is(err, store.ErrHeadMoved) {
			t.Fatalf("skipped seq: %v", err)
		}
		loaded, _ := events.Load(ctx, "t")
		if len(loaded) != 2 {
			t.Fatalf("a refused batch wrote anyway: %+v", loaded)
		}
	})

	t.Run("concurrent appends keep one order", func(t *testing.T) {
		events := open(t)
		ctx := context.Background()
		const writers = 24
		var wait sync.WaitGroup
		for i := 0; i < writers; i++ {
			wait.Add(1)
			go func() {
				defer wait.Done()
				for {
					loaded, err := events.Load(ctx, "t")
					if err != nil {
						t.Errorf("load: %v", err)
						return
					}
					head := uint64(len(loaded))
					_, err = events.Append(ctx, "t", []store.Record{seqRecord(head+1, "event", fmt.Sprintf(`{"n":%d}`, head+1))}, head)
					if err == nil {
						return
					}
					if !errors.Is(err, store.ErrHeadMoved) {
						t.Errorf("append: %v", err)
						return
					}
				}
			}()
		}
		wait.Wait()
		loaded, err := events.Load(ctx, "t")
		if err != nil {
			t.Fatal(err)
		}
		if len(loaded) != writers {
			t.Fatalf("expected %d records, got %d", writers, len(loaded))
		}
		for index, record := range loaded {
			if record.Seq != uint64(index)+1 {
				t.Fatalf("record %d has seq %d", index, record.Seq)
			}
		}
	})

	t.Run("idempotent replay", func(t *testing.T) {
		events := open(t)
		ctx := context.Background()
		first := seqRecord(1, "message.posted", `{"id":1,"channel":"work","text":"one"}`)
		first.RequestKey = "post:alice:abc"
		first.Digest = "channel=work"
		first.RequestLast = 1
		first.RequestResult = resultJSON(t, 1, 1, []uint64{1})
		result, err := events.Append(ctx, "t", []store.Record{first}, 0)
		if err != nil {
			t.Fatal(err)
		}
		if result.Replayed || result.FirstSeq != 1 || result.LastSeq != 1 || !reflect.DeepEqual(result.MessageIDs, []uint64{1}) {
			t.Fatalf("first append: %+v", result)
		}

		// A retry names a fresh seq and stale head; the key answers the first.
		retry := seqRecord(2, "message.posted", `{"id":2,"channel":"work","text":"different"}`)
		retry.RequestKey = "post:alice:abc"
		retry.Digest = "channel=work"
		retry.RequestLast = 2
		retry.RequestResult = resultJSON(t, 2, 2, []uint64{2})
		replay, err := events.Append(ctx, "t", []store.Record{retry}, 0)
		if err != nil {
			t.Fatal(err)
		}
		if !replay.Replayed || replay.FirstSeq != 1 || replay.LastSeq != 1 || !reflect.DeepEqual(replay.MessageIDs, []uint64{1}) {
			t.Fatalf("replay: %+v", replay)
		}
		loaded, _ := events.Load(ctx, "t")
		if len(loaded) != 1 || !sameJSON(t, loaded[0].Payload, `{"id":1,"channel":"work","text":"one"}`) {
			t.Fatalf("a replay wrote: %+v", loaded)
		}
		if loaded[0].RequestKey != "post:alice:abc" || loaded[0].RequestLast != 1 || loaded[0].Digest != "channel=work" {
			t.Fatalf("request metadata: %+v", loaded[0])
		}
		if len(loaded[0].RequestResult) == 0 {
			t.Fatalf("Load dropped the request result")
		}
		var storedResult store.BatchResult
		if err := json.Unmarshal(loaded[0].RequestResult, &storedResult); err != nil {
			t.Fatalf("decoding the request result: %v", err)
		}
		if storedResult.FirstSeq != 1 || storedResult.LastSeq != 1 || !reflect.DeepEqual(storedResult.MessageIDs, []uint64{1}) {
			t.Fatalf("loaded request result: %+v", storedResult)
		}

		looker, ok := events.(store.RequestStore)
		if !ok {
			t.Fatalf("%T doesn't implement RequestStore", events)
		}
		stored, found, err := looker.Request(ctx, "t", "post:alice:abc")
		if err != nil || !found {
			t.Fatalf("request lookup: %+v, %v, %v", stored, found, err)
		}
		if stored.Digest != "channel=work" || stored.Result.FirstSeq != 1 || stored.Result.LastSeq != 1 || !reflect.DeepEqual(stored.Result.MessageIDs, []uint64{1}) {
			t.Fatalf("stored request: %+v", stored)
		}
		if _, found, err := looker.Request(ctx, "t", "unknown"); err != nil || found {
			t.Fatalf("unknown request: found=%v err=%v", found, err)
		}
	})

	t.Run("duplicate digest", func(t *testing.T) {
		events := open(t)
		ctx := context.Background()
		first := seqRecord(1, "message.posted", `{"id":1}`)
		first.RequestKey = "key"
		first.Digest = "one"
		first.RequestResult = resultJSON(t, 1, 1, []uint64{1})
		mustAppend(t, events, "t", 0, first)

		other := seqRecord(2, "message.posted", `{"id":2}`)
		other.RequestKey = "key"
		other.Digest = "two"
		other.RequestResult = resultJSON(t, 2, 2, []uint64{2})
		if _, err := events.Append(ctx, "t", []store.Record{other}, 1); !errors.Is(err, store.ErrDuplicateRequest) {
			t.Fatalf("different digest: %v", err)
		}
		loaded, _ := events.Load(ctx, "t")
		if len(loaded) != 1 {
			t.Fatalf("duplicate digest wrote: %+v", loaded)
		}
	})

	t.Run("tenant isolation", func(t *testing.T) {
		events := open(t)
		ctx := context.Background()
		a := seqRecord(1, "message.posted", `{"id":1,"text":"a"}`)
		a.RequestKey, a.Digest = "same", "digest"
		a.RequestResult = resultJSON(t, 1, 1, []uint64{1})
		b := seqRecord(1, "message.posted", `{"id":1,"text":"b"}`)
		b.RequestKey, b.Digest = "same", "digest"
		b.RequestResult = resultJSON(t, 1, 1, []uint64{1})
		mustAppend(t, events, "a", 0, a)
		mustAppend(t, events, "b", 0, b)
		for _, tenant := range []string{"a", "b"} {
			loaded, err := events.Load(ctx, tenant)
			if err != nil || len(loaded) != 1 {
				t.Fatalf("tenant %s: %+v, %v", tenant, loaded, err)
			}
			want := fmt.Sprintf(`{"id":1,"text":%q}`, tenant)
			if !sameJSON(t, loaded[0].Payload, want) {
				t.Fatalf("tenant %s payload: %s", tenant, loaded[0].Payload)
			}
		}
	})

	t.Run("history paging", func(t *testing.T) {
		events := open(t)
		ctx := context.Background()
		historian, ok := events.(store.HistoryStore)
		if !ok {
			t.Fatalf("%T doesn't implement HistoryStore", events)
		}
		mustAppend(t, events, "t", 0,
			seqRecord(1, "event", `{"n":1}`), seqRecord(2, "event", `{"n":2}`))
		mustAppend(t, events, "t", 2,
			seqRecord(3, "event", `{"n":3}`), seqRecord(4, "event", `{"n":4}`))
		mustAppend(t, events, "t", 4,
			seqRecord(5, "event", `{"n":5}`), seqRecord(6, "event", `{"n":6}`), seqRecord(7, "event", `{"n":7}`))

		cases := []struct {
			after uint64
			limit int
			want  []uint64
		}{
			{0, 3, []uint64{1, 2, 3}},
			{3, 3, []uint64{4, 5, 6}},
			{6, 3, []uint64{7}},
			{7, 3, nil},
			{0, 0, []uint64{1, 2, 3, 4, 5, 6, 7}},
			{100, 3, nil},
		}
		for _, test := range cases {
			records, err := historian.History(ctx, "t", test.after, test.limit)
			if err != nil {
				t.Fatal(err)
			}
			got := make([]uint64, len(records))
			for index, record := range records {
				got[index] = record.Seq
			}
			if len(got) != len(test.want) {
				t.Fatalf("history after %d limit %d: %v, want %v", test.after, test.limit, got, test.want)
			}
			for index := range got {
				if got[index] != test.want[index] {
					t.Fatalf("history after %d limit %d: %v, want %v", test.after, test.limit, got, test.want)
				}
			}
		}
	})

	t.Run("wait wakes", func(t *testing.T) {
		events := open(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		waiter, ok := events.(store.Waiter)
		if !ok {
			t.Fatalf("%T doesn't implement Waiter", events)
		}
		for round := uint64(1); round <= 2; round++ {
			after := round - 1
			done := make(chan error, 1)
			go func() { done <- waiter.Wait(ctx, "t", after) }()
			time.Sleep(25 * time.Millisecond)
			mustAppend(t, events, "t", after, seqRecord(round, "event", fmt.Sprintf(`{"n":%d}`, round)))
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("wait %d: %v", round, err)
				}
			case <-ctx.Done():
				t.Fatalf("wait %d didn't wake", round)
			}
		}
	})

	t.Run("wait honours cancellation", func(t *testing.T) {
		events := open(t)
		waiter, ok := events.(store.Waiter)
		if !ok {
			t.Fatalf("%T doesn't implement Waiter", events)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- waiter.Wait(ctx, "t", 0) }()
		time.Sleep(25 * time.Millisecond)
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("wait cancellation: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("wait ignored cancellation")
		}
	})

	t.Run("integrity and tenants", func(t *testing.T) {
		events := open(t)
		ctx := context.Background()
		integrity, ok := events.(store.IntegrityStore)
		if !ok {
			t.Fatalf("%T doesn't implement IntegrityStore", events)
		}
		tenants, ok := events.(store.TenantStore)
		if !ok {
			t.Fatalf("%T doesn't implement TenantStore", events)
		}
		empty, err := integrity.Integrity(ctx, "t1")
		if err != nil {
			t.Fatalf("empty integrity: %v", err)
		}
		if !empty.Consistent() || empty.Head != 0 || empty.MaxSeq != 0 {
			t.Fatalf("empty integrity: %+v", empty)
		}
		mustAppend(t, events, "t1", 0, seqRecord(1, "event", `{"n":1}`))
		mustAppend(t, events, "t1", 1, seqRecord(2, "event", `{"n":2}`))
		got, err := integrity.Integrity(ctx, "t1")
		if err != nil {
			t.Fatalf("integrity: %v", err)
		}
		if !got.Consistent() || got.Head != 2 || got.MaxSeq != 2 {
			t.Fatalf("integrity after appends: %+v", got)
		}
		names, err := tenants.Tenants(ctx)
		if err != nil {
			t.Fatalf("tenants: %v", err)
		}
		found := false
		for _, name := range names {
			if name == "t1" {
				found = true
			}
		}
		if !found {
			t.Fatalf("tenants %v don't name t1", names)
		}
	})
}

// TestMemoryContract runs the store contract against the in-memory store.
func TestMemoryContract(t *testing.T) {
	storeContract(t, func(*testing.T) store.EventStore { return store.NewMemory() })
}

func mustAppend(t *testing.T, events store.EventStore, tenant string, expectedHead uint64, records ...store.Record) store.AppendResult {
	t.Helper()
	result, err := events.Append(context.Background(), tenant, records, expectedHead)
	if err != nil {
		t.Fatalf("append %d records at head %d: %v", len(records), expectedHead, err)
	}
	return result
}

func seqRecord(seq uint64, kind, payload string) store.Record {
	return store.Record{
		Seq:     seq,
		Type:    kind,
		Channel: "work",
		Payload: []byte(payload),
		At:      time.Date(2026, 1, 2, 3, 4, 5, 6000, time.UTC),
	}
}

func resultJSON(t *testing.T, first, last uint64, messageIDs []uint64) []byte {
	t.Helper()
	raw, err := json.Marshal(store.BatchResult{FirstSeq: first, LastSeq: last, MessageIDs: messageIDs})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// sameJSON compares two JSON documents by value: PostgreSQL's jsonb may
// reorder keys and normalize spacing; the memory store keeps the bytes.
func sameJSON(t *testing.T, got []byte, want string) bool {
	t.Helper()
	var left, right any
	if err := json.Unmarshal(got, &left); err != nil {
		t.Fatalf("decoding %s: %v", got, err)
	}
	if err := json.Unmarshal([]byte(want), &right); err != nil {
		t.Fatalf("decoding %s: %v", want, err)
	}
	return reflect.DeepEqual(left, right)
}
