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
