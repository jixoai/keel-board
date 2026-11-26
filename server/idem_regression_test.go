package server_test

import (
	"context"
	"net/http"
	"sync"
	"testing"

	"github.com/jixoai/keel-board/blob"
	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/server"
)

// toggleAuthorizer flips its refusals while a test runs.
type toggleAuthorizer struct {
	mu        sync.Mutex
	denyRead  bool
	denyPost  bool
	denyLabel bool
}

func (p *toggleAuthorizer) Authorize(_ context.Context, _ string, _ protocol.Actor, _ string, permission protocol.Permission) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch permission {
	case protocol.PermissionRead:
		if p.denyRead {
			return protocol.Forbidden("no read")
		}
	case protocol.PermissionPost:
		if p.denyPost {
			return protocol.Forbidden("no post")
		}
	case protocol.PermissionLabel:
		if p.denyLabel {
			return protocol.Forbidden("no label")
		}
	}
	return nil
}

func (p *toggleAuthorizer) set(read, post, label bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.denyRead, p.denyPost, p.denyLabel = read, post, label
}

func TestIdempotencyKeysDoNotCollideAcrossActors(t *testing.T) {
	h := newHarness(t)
	// "local:user" + "bob:key" and "local:user:bob" + "key" used to join
	// into the same key. Both writes must land, and each retry must answer its
	// own.
	status, data := h.request(http.MethodPost, "/messages", map[string]any{
		"channel": "work", "text": "first actor", "id": "bob:post-key",
	}, "local:user", "", nil)
	h.expect(status, data, http.StatusCreated)
	first := decode[postBody](t, data).Message

	status, data = h.request(http.MethodPost, "/messages", map[string]any{
		"channel": "work", "text": "second actor", "id": "post-key",
	}, "local:user:bob", "", nil)
	h.expect(status, data, http.StatusCreated)
	second := decode[postBody](t, data).Message
	if first.ID == second.ID || first.Text == second.Text {
		t.Fatalf("the two actors shared a write: %+v vs %+v", first, second)
	}
	status, data = h.request(http.MethodPost, "/messages", map[string]any{
		"channel": "work", "text": "first actor", "id": "bob:post-key",
	}, "local:user", "", nil)
	h.expect(status, data, http.StatusCreated)
	if got := decode[postBody](t, data).Message; got.ID != first.ID {
		t.Fatalf("the first actor's retry answered %+v, want %s", got, first.ID)
	}

	// Batch keys.
	status, data = h.batch("local:user", map[string]any{
		"channel": "tasks", "id": "bob:batch-key",
		"tasks": []any{map[string]any{"text": "first batch"}},
	})
	h.expect(status, data, http.StatusCreated)
	firstBatch := decode[protocol.CreatedTasksPage](t, data)
	status, data = h.batch("local:user:bob", map[string]any{
		"channel": "tasks", "id": "batch-key",
		"tasks": []any{map[string]any{"text": "second batch"}},
	})
	h.expect(status, data, http.StatusCreated)
	secondBatch := decode[protocol.CreatedTasksPage](t, data)
	if firstBatch.Tasks[0].ID == secondBatch.Tasks[0].ID {
		t.Fatalf("the two actors shared a batch: %+v vs %+v", firstBatch, secondBatch)
	}
	status, data = h.request(http.MethodGet, "/tasks/batch/bob:batch-key", nil, "local:user", "", nil)
	h.expect(status, data, http.StatusOK)
	if got := decode[protocol.CreatedTasksPage](t, data); got.Tasks[0].ID != firstBatch.Tasks[0].ID {
		t.Fatalf("the first actor's batch read answered %+v", got)
	}
	status, data = h.request(http.MethodGet, "/tasks/batch/batch-key", nil, "local:user:bob", "", nil)
	h.expect(status, data, http.StatusOK)
	if got := decode[protocol.CreatedTasksPage](t, data); got.Tasks[0].ID != secondBatch.Tasks[0].ID {
		t.Fatalf("the second actor's batch read answered %+v", got)
	}

	// Task keys.
	status, data = h.request(http.MethodPost, "/tasks", map[string]any{
		"channel": "tasks", "text": "first task", "id": "bob:task-key",
	}, "local:user", "", nil)
	h.expect(status, data, http.StatusCreated)
	firstTask := decode[taskBody](t, data).Task
	status, data = h.request(http.MethodPost, "/tasks", map[string]any{
		"channel": "tasks", "text": "second task", "id": "task-key",
	}, "local:user:bob", "", nil)
	h.expect(status, data, http.StatusCreated)
	secondTask := decode[taskBody](t, data).Task
	if firstTask.ID == secondTask.ID {
		t.Fatalf("the two actors shared a task: %+v vs %+v", firstTask, secondTask)
	}
}

func TestReplayBeforePermissionAndValidation(t *testing.T) {
	policy := &toggleAuthorizer{}
	h := newHarness(t, server.Options{Authorize: policy})
	const alice = "local:user:alice"

	// A post answers its retry after `post` is revoked, even with text that
	// would no longer validate.
	status, data := h.request(http.MethodPost, "/messages", map[string]any{
		"channel": "work", "text": "already here", "id": "replay-post",
	}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	original := decode[postBody](t, data).Message
	// Make all three before revoking post.
	status, data = h.request(http.MethodPost, "/tasks", map[string]any{
		"channel": "work", "text": "task here", "id": "replay-task",
	}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	originalTask := decode[taskBody](t, data).Task
	status, data = h.batch(alice, map[string]any{
		"channel": "work", "id": "replay-batch",
		"tasks": []any{map[string]any{"text": "batch here"}},
	})
	h.expect(status, data, http.StatusCreated)
	originalBatch := decode[protocol.CreatedTasksPage](t, data)
	policy.set(false, true, false)

	status, data = h.request(http.MethodPost, "/messages", map[string]any{
		"channel": "work", "text": "", "id": "replay-post",
	}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	if got := decode[postBody](t, data).Message; got.ID != original.ID || got.Text != original.Text {
		t.Fatalf("a revoked post should still replay: %+v", got)
	}

	// A task answers its retry after `post` is revoked.
	status, data = h.request(http.MethodPost, "/tasks", map[string]any{
		"channel": "work", "text": "", "id": "replay-task",
	}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	if got := decode[taskBody](t, data).Task; got.ID != originalTask.ID || got.Text != originalTask.Text {
		t.Fatalf("a revoked task should still replay: %+v", got)
	}

	// A batch answers its retry after `post` is revoked.
	status, data = h.batch(alice, map[string]any{
		"channel": "work", "id": "replay-batch",
		"tasks": []any{map[string]any{"text": "batch here"}},
	})
	h.expect(status, data, http.StatusCreated)
	if got := decode[protocol.CreatedTasksPage](t, data); got.Tasks[0].ID != originalBatch.Tasks[0].ID {
		t.Fatalf("a revoked batch should still replay: %+v", got)
	}
}

func TestReplayAfterReadRevoked(t *testing.T) {
	policy := &toggleAuthorizer{}
	h := newHarness(t, server.Options{Authorize: policy})
	const alice = "local:user:alice"

	status, data := h.request(http.MethodPost, "/messages", map[string]any{
		"channel": "work", "text": "readable post", "id": "read-revoked-post",
	}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	status, data = h.batch(alice, map[string]any{
		"channel": "work", "id": "read-revoked",
		"tasks": []any{map[string]any{"text": "secret task", "labels": map[string]string{"status": "open"}}},
	})
	h.expect(status, data, http.StatusCreated)
	policy.set(true, false, false)

	// A message retry after read revocation is 404 too.
	status, data = h.request(http.MethodPost, "/messages", map[string]any{
		"channel": "work", "text": "readable post", "id": "read-revoked-post",
	}, alice, "", nil)
	h.expect(status, data, http.StatusNotFound)
	if code := errorCode(t, data); code != protocol.CodeMessageNotFound {
		t.Fatalf("expected message_not_found, got %s", code)
	}

	// A batch read after read revocation is 404, not its text or labels.
	status, data = h.request(http.MethodGet, "/tasks/batch/read-revoked", nil, alice, "", nil)
	h.expect(status, data, http.StatusNotFound)
	if code := errorCode(t, data); code != protocol.CodeBatchNotFound {
		t.Fatalf("expected batch_not_found, got %s", code)
	}
	// A retry is 404 too: a non-blind post gives nothing back without read.
	status, data = h.batch(alice, map[string]any{
		"channel": "work", "id": "read-revoked",
		"tasks": []any{map[string]any{"text": "secret task", "labels": map[string]string{"status": "open"}}},
	})
	h.expect(status, data, http.StatusNotFound)

	// A blind post still answers its retry with the text it sent, not labels or
	// files.
	policy.set(true, false, false)
	status, data = h.request(http.MethodPost, "/messages", map[string]any{
		"channel": "blind", "text": "blind text", "id": "blind-replay",
		"labels": map[string]string{"secret": "label"},
	}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	blind := decode[postBody](t, data).Message
	if !blind.Blind {
		t.Fatalf("expected a blind post: %+v", blind)
	}
	if len(blind.Labels) != 0 || len(blind.Attachments) != 0 || blind.By != nil {
		t.Fatalf("a blind write's first response must carry only its text: %+v", blind)
	}
	status, data = h.request(http.MethodPost, "/messages", map[string]any{
		"channel": "blind", "text": "blind text", "id": "blind-replay",
	}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	replayed := decode[postBody](t, data).Message
	if replayed.ID != blind.ID || replayed.Text != "blind text" {
		t.Fatalf("unexpected blind replay: %+v", replayed)
	}
	if len(replayed.Labels) != 0 || len(replayed.Attachments) != 0 || len(replayed.Reactions) != 0 || replayed.By != nil {
		t.Fatalf("a blind replay must carry only its text: %+v", replayed)
	}
}

func TestReplayAfterAttachmentDeleted(t *testing.T) {
	store := blob.NewMemory()
	h := newHarness(t, server.Options{Blobs: store})
	const alice = "local:user:alice"

	status, data := h.upload(alice, "gone.txt", "text/plain", []byte("bytes"))
	h.expect(status, data, http.StatusCreated)
	file := decodeAttachment(t, data)
	status, data = h.request(http.MethodPost, "/messages", map[string]any{
		"channel": "work", "text": "", "attachments": []string{file.ID}, "id": "post-with-file",
	}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	message := decode[postBody](t, data).Message
	status, data = h.deleteMessage(alice, message.ID, nil)
	h.expect(status, data, http.StatusOK)
	status, data = h.request(http.MethodDelete, "/attachments/"+file.ID, nil, alice, "", nil)
	h.expect(status, data, http.StatusNoContent)

	// The file is gone; the retry still answers the post that landed.
	status, data = h.request(http.MethodPost, "/messages", map[string]any{
		"channel": "work", "text": "", "attachments": []string{file.ID}, "id": "post-with-file",
	}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	if got := decode[postBody](t, data).Message; got.ID != message.ID || !got.Deleted {
		t.Fatalf("a replay should answer the original tombstone: %+v", got)
	}

	// The same for a batch.
	status, data = h.upload(alice, "batch.txt", "text/plain", []byte("bytes"))
	h.expect(status, data, http.StatusCreated)
	batchFile := decodeAttachment(t, data)
	status, data = h.batch(alice, map[string]any{
		"channel": "work", "id": "batch-with-file",
		"tasks": []any{map[string]any{"text": "", "attachments": []any{batchFile.ID}}},
	})
	h.expect(status, data, http.StatusCreated)
	batch := decode[protocol.CreatedTasksPage](t, data)
	status, data = h.deleteMessage(alice, batch.Tasks[0].ID, nil)
	h.expect(status, data, http.StatusOK)
	status, data = h.request(http.MethodDelete, "/attachments/"+batchFile.ID, nil, alice, "", nil)
	h.expect(status, data, http.StatusNoContent)
	status, data = h.batch(alice, map[string]any{
		"channel": "work", "id": "batch-with-file",
		"tasks": []any{map[string]any{"text": "", "attachments": []any{batchFile.ID}}},
	})
	h.expect(status, data, http.StatusCreated)
	if got := decode[protocol.CreatedTasksPage](t, data); got.Tasks[0].ID != batch.Tasks[0].ID {
		t.Fatalf("a batch replay should answer its original task: %+v", got)
	}
}

func TestIdempotencyIsPerTenant(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"

	status, data := h.request(http.MethodPost, "/messages", map[string]any{
		"channel": "work", "text": "tenant a", "id": "same-id",
	}, alice, "tenant-a", nil)
	h.expect(status, data, http.StatusCreated)
	foreign := decode[postBody](t, data).Message

	status, data = h.request(http.MethodPost, "/messages", map[string]any{
		"channel": "work", "text": "tenant b", "id": "same-id",
	}, alice, "tenant-b", nil)
	h.expect(status, data, http.StatusCreated)
	local := decode[postBody](t, data).Message
	if local.Text != "tenant b" || foreign.Text != "tenant a" {
		t.Fatalf("tenants shared a write: %+v vs %+v", foreign, local)
	}
	// Each tenant reads its own message, even at the same ref.
	status, data = h.request(http.MethodGet, "/messages/"+local.ID, nil, alice, "tenant-a", nil)
	h.expect(status, data, http.StatusOK)
	if got := decode[protocol.ThreadView](t, data).Message; got.Text != "tenant a" {
		t.Fatalf("tenant a read %+v", got)
	}
	status, data = h.request(http.MethodGet, "/messages/"+local.ID, nil, alice, "tenant-b", nil)
	h.expect(status, data, http.StatusOK)
	if got := decode[protocol.ThreadView](t, data).Message; got.Text != "tenant b" {
		t.Fatalf("tenant b read %+v", got)
	}

	// Batches are per tenant too: the same id on two boards is two batches.
	status, data = h.request(http.MethodPost, "/tasks/batch", map[string]any{
		"channel": "tasks", "id": "same-batch",
		"tasks": []any{map[string]any{"text": "tenant a batch"}},
	}, alice, "tenant-a", nil)
	h.expect(status, data, http.StatusCreated)
	foreignBatch := decode[protocol.CreatedTasksPage](t, data)
	status, data = h.request(http.MethodPost, "/tasks/batch", map[string]any{
		"channel": "tasks", "id": "same-batch",
		"tasks": []any{map[string]any{"text": "tenant b batch"}},
	}, alice, "tenant-b", nil)
	h.expect(status, data, http.StatusCreated)
	localBatch := decode[protocol.CreatedTasksPage](t, data)
	if foreignBatch.Tasks[0].Text != "tenant a batch" || localBatch.Tasks[0].Text != "tenant b batch" {
		t.Fatalf("tenant batches crossed: %+v vs %+v", foreignBatch, localBatch)
	}
	status, data = h.request(http.MethodGet, "/tasks/batch/same-batch", nil, alice, "tenant-a", nil)
	h.expect(status, data, http.StatusOK)
	if got := decode[protocol.CreatedTasksPage](t, data); got.Tasks[0].Text != "tenant a batch" {
		t.Fatalf("tenant a's batch read answered %+v", got)
	}
	status, data = h.request(http.MethodGet, "/tasks/batch/same-batch", nil, alice, "tenant-b", nil)
	h.expect(status, data, http.StatusOK)
	if got := decode[protocol.CreatedTasksPage](t, data); got.Tasks[0].Text != "tenant b batch" {
		t.Fatalf("tenant b's batch read answered %+v", got)
	}
}
