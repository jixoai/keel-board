package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/store"
)

func TestIdemKeyIsUnambiguous(t *testing.T) {
	cases := []struct{ namespace, actor, id string }{
		{"post", "local:user", "bob:k"},
		{"post", "local:user:bob", "k"},
	}
	seen := map[string]bool{}
	for _, test := range cases {
		key := idemKey(test.namespace, test.actor, test.id)
		if seen[key] {
			t.Fatalf("keys collided: %q", key)
		}
		seen[key] = true
	}
	if idemKey("post", "a", "b") == idemKey("task", "a", "b") {
		t.Fatal("namespaces share a key")
	}
	if idemKey("post", "a:b", "c") == idemKey("post", "a", "b:c") {
		t.Fatal("a colon in the actor and the id collided")
	}
}

// TestReplayRefusesAnotherActorsMessage proves the readback check itself: even
// a log entry that lands under one actor's key answers another actor 404.
func TestReplayRefusesAnotherActorsMessage(t *testing.T) {
	events := store.NewMemory()
	board := log.New(log.Options{Store: events})
	bob := protocol.Actor{ID: "local:user:bob", Principal: "local:user:bob", Kind: protocol.ActorPerson, Handle: "bob", Name: "bob"}

	seed := func(key, digest string) uint64 {
		t.Helper()
		var id uint64
		err := board.Update(context.Background(), "default", log.UpdateOptions{Idempotency: &log.Idempotency{
			Key: key, Digest: digest,
		}}, func(tx *log.Tx) error {
			id = tx.NextID(0)
			tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
				ID: id, Channel: "work", Text: "bob's secret", Thread: log.ThreadKey(id),
				Labels: map[string]string{"owner": "bob"}, By: bob.Actored(),
			})
			tx.RecordResult(id)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	batchBody := protocol.CreateTasksRequest{Channel: "work", Tasks: []protocol.BatchTask{{Text: "mine"}}}
	batchHash, perr := batchDigest(batchBody)
	if perr != nil {
		t.Fatal(perr)
	}
	seed(idemKey("post", "local:user:alice", "probe-post"), "place:channel:work")
	seed(batchKey("local:user:alice", "probe-batch"), batchHash)

	api := New(Options{Board: board, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	httpServer := httptest.NewServer(api)
	defer httpServer.Close()

	post := func(path string, body any) (int, []byte) {
		t.Helper()
		encoded, _ := json.Marshal(body)
		request, err := http.NewRequest(http.MethodPost, httpServer.URL+APIPrefix+path, bytes.NewReader(encoded))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("content-type", "application/json")
		request.Header.Set("X-Board-Actor", "local:user:alice")
		response, err := httpServer.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, _ := io.ReadAll(response.Body)
		return response.StatusCode, data
	}
	status, data := post("/messages", map[string]any{"channel": "work", "text": "mine", "id": "probe-post"})
	if status != http.StatusNotFound || !bytes.Contains(data, []byte("message_not_found")) {
		t.Fatalf("post replay of another actor: %d %s", status, data)
	}
	status, data = post("/tasks/batch", map[string]any{
		"channel": "work", "id": "probe-batch", "tasks": []any{map[string]any{"text": "mine"}},
	})
	if status != http.StatusNotFound || !bytes.Contains(data, []byte("batch_not_found")) {
		t.Fatalf("batch replay of another actor: %d %s", status, data)
	}
}

// missRequestOnceStore hides request metadata from replaying records and
// reports a key missing the first time it is asked. The early ReplayKey misses
// it; the write then finds it in the store, as another process's write racing
// the request does.
type missRequestOnceStore struct {
	*store.Memory
	seen map[string]int
}

func (s *missRequestOnceStore) Load(ctx context.Context, tenant string) ([]store.Record, error) {
	records, err := s.Memory.Load(ctx, tenant)
	for index := range records {
		records[index].RequestKey = ""
		records[index].Digest = ""
		records[index].RequestLast = 0
		records[index].RequestResult = nil
	}
	return records, err
}

func (s *missRequestOnceStore) History(ctx context.Context, tenant string, after uint64, limit int) ([]store.Record, error) {
	records, err := s.Memory.History(ctx, tenant, after, limit)
	for index := range records {
		records[index].RequestKey = ""
		records[index].Digest = ""
		records[index].RequestLast = 0
		records[index].RequestResult = nil
	}
	return records, err
}

func (s *missRequestOnceStore) Request(ctx context.Context, tenant, key string) (store.Request, bool, error) {
	if s.seen == nil {
		s.seen = make(map[string]int)
	}
	s.seen[key]++
	if s.seen[key] == 1 {
		return store.Request{}, false, nil
	}
	return s.Memory.Request(ctx, tenant, key)
}

// TestTxReplayChecksAuthor covers a race the early lookup can't: the key lands
// between ReplayKey and Update, so tx.Replay answers it. That path must still
// refuse another actor's message.
func TestTxReplayChecksAuthor(t *testing.T) {
	memory := store.NewMemory()
	writer := log.New(log.Options{Store: memory})
	bob := protocol.Actor{ID: "local:user:bob", Principal: "local:user:bob", Kind: protocol.ActorPerson, Handle: "bob", Name: "bob"}

	seed := func(key, digest string) uint64 {
		t.Helper()
		var id uint64
		err := writer.Update(context.Background(), "default", log.UpdateOptions{Idempotency: &log.Idempotency{
			Key: key, Digest: digest,
		}}, func(tx *log.Tx) error {
			id = tx.NextID(0)
			tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
				ID: id, Channel: "work", Text: "bob's secret", Thread: log.ThreadKey(id),
				Labels: map[string]string{"owner": "bob"}, By: bob.Actored(),
			})
			tx.RecordResult(id)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	postKey := idemKey("post", "local:user:alice", "tx-post")
	batchKey := batchKey("local:user:alice", "tx-batch")
	seed(postKey, "place:channel:work")
	batchBody := protocol.CreateTasksRequest{Channel: "work", Tasks: []protocol.BatchTask{{Text: "mine"}}}
	batchDigest, perr := batchDigest(batchBody)
	if perr != nil {
		t.Fatal(perr)
	}
	seed(batchKey, batchDigest)

	proxy := &missRequestOnceStore{Memory: memory}
	board := log.New(log.Options{Store: proxy})
	api := New(Options{Board: board, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	httpServer := httptest.NewServer(api)
	defer httpServer.Close()

	post := func(path string, body any) (int, []byte) {
		t.Helper()
		encoded, _ := json.Marshal(body)
		request, err := http.NewRequest(http.MethodPost, httpServer.URL+APIPrefix+path, bytes.NewReader(encoded))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("content-type", "application/json")
		request.Header.Set("X-Board-Actor", "local:user:alice")
		response, err := httpServer.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, _ := io.ReadAll(response.Body)
		return response.StatusCode, data
	}
	status, data := post("/messages", map[string]any{"channel": "work", "text": "mine", "id": "tx-post"})
	if status != http.StatusNotFound || !bytes.Contains(data, []byte("message_not_found")) {
		t.Fatalf("tx replay of another actor's post: %d %s", status, data)
	}
	status, data = post("/tasks/batch", map[string]any{
		"channel": "work", "id": "tx-batch", "tasks": []any{map[string]any{"text": "mine"}},
	})
	if status != http.StatusNotFound || !bytes.Contains(data, []byte("batch_not_found")) {
		t.Fatalf("tx replay of another actor's batch: %d %s", status, data)
	}
}

// TestTxReplayTaskAuthorAndRead covers the task path: the early ReplayKey
// misses, Update finds the key in the store and tx.Replay answers. Another
// actor's task is 404, and the caller's own task in a channel it can't read is
// 404 too (not a blind 201).
func TestTxReplayTaskAuthorAndRead(t *testing.T) {
	memory := store.NewMemory()
	writer := log.New(log.Options{Store: memory})
	bob := protocol.Actor{ID: "local:user:bob", Principal: "local:user:bob", Kind: protocol.ActorPerson, Handle: "bob", Name: "bob"}
	alice := protocol.Actor{ID: "local:user:alice", Principal: "local:user:alice", Kind: protocol.ActorPerson, Handle: "alice", Name: "alice"}

	seed := func(key, digest string, actor protocol.Actor, channel, text string) {
		t.Helper()
		err := writer.Update(context.Background(), "default", log.UpdateOptions{Idempotency: &log.Idempotency{
			Key: key, Digest: digest,
		}}, func(tx *log.Tx) error {
			id := tx.NextID(0)
			tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
				ID: id, Channel: channel, Text: text, Thread: log.ThreadKey(id),
				Labels: map[string]string{}, By: actor.Actored(),
			})
			tx.RecordResult(id)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	authorKey := idemKey("task", "local:user:alice", "tx-task-author")
	readKey := idemKey("task", "local:user:alice", "tx-task-read")
	seed(authorKey, writePlaceDigest("work", nil), bob, "work", "bob's task")
	seed(readKey, writePlaceDigest("secret", nil), alice, "secret", "alice's task")

	proxy := &missRequestOnceStore{Memory: memory}
	board := log.New(log.Options{Store: proxy})
	api := New(Options{
		Board:  board,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Authorize: AuthorizerFunc(func(_ context.Context, _ string, actor protocol.Actor, channel string, permission protocol.Permission) error {
			if actor.ID == alice.ID && channel == "secret" && permission == protocol.PermissionRead {
				return protocol.Forbidden("no secret")
			}
			return nil
		}),
	})
	httpServer := httptest.NewServer(api)
	defer httpServer.Close()

	post := func(path string, body any) (int, []byte) {
		t.Helper()
		encoded, _ := json.Marshal(body)
		request, err := http.NewRequest(http.MethodPost, httpServer.URL+APIPrefix+path, bytes.NewReader(encoded))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("content-type", "application/json")
		request.Header.Set("X-Board-Actor", alice.ID)
		response, err := httpServer.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, _ := io.ReadAll(response.Body)
		return response.StatusCode, data
	}

	status, data := post("/tasks", map[string]any{"channel": "work", "text": "mine", "id": "tx-task-author"})
	if status != http.StatusNotFound || !bytes.Contains(data, []byte("message_not_found")) {
		t.Fatalf("tx replay of another actor's task: %d %s", status, data)
	}
	status, data = post("/tasks", map[string]any{"channel": "secret", "text": "mine", "id": "tx-task-read"})
	if status != http.StatusNotFound || !bytes.Contains(data, []byte("message_not_found")) {
		t.Fatalf("tx replay of an unreadable task: %d %s", status, data)
	}
}

// TestTxReplayDigestConflict makes the early ReplayKey miss and the store report
// the key under another digest. The write must be 409, not a replay of the
// stored answer.
func TestTxReplayDigestConflict(t *testing.T) {
	memory := store.NewMemory()
	writer := log.New(log.Options{Store: memory})
	alice := protocol.Actor{ID: "local:user:alice", Principal: "local:user:alice", Kind: protocol.ActorPerson, Handle: "alice", Name: "alice"}
	key := idemKey("post", "local:user:alice", "tx-digest")
	err := writer.Update(context.Background(), "default", log.UpdateOptions{Idempotency: &log.Idempotency{
		Key: key, Digest: "place:channel:work",
	}}, func(tx *log.Tx) error {
		id := tx.NextID(0)
		tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
			ID: id, Channel: "work", Text: "first", Thread: log.ThreadKey(id),
			Labels: map[string]string{}, By: alice.Actored(),
		})
		tx.RecordResult(id)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	proxy := &missRequestOnceStore{Memory: memory}
	board := log.New(log.Options{Store: proxy})
	api := New(Options{Board: board, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	httpServer := httptest.NewServer(api)
	defer httpServer.Close()

	body, _ := json.Marshal(map[string]any{"channel": "other", "text": "second", "id": "tx-digest"})
	request, err := http.NewRequest(http.MethodPost, httpServer.URL+APIPrefix+"/messages", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("content-type", "application/json")
	request.Header.Set("X-Board-Actor", alice.ID)
	response, err := httpServer.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusConflict || !bytes.Contains(data, []byte("duplicate_id")) {
		t.Fatalf("digest conflict answered %d %s", response.StatusCode, data)
	}
}
