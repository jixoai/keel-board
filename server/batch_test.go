package server_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/jixoai/keel-board/blob"
	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/server"
)

func (h *harness) batch(actor string, body map[string]any) (int, []byte) {
	h.t.Helper()
	return h.request(http.MethodPost, "/tasks/batch", body, actor, "", nil)
}

func TestBatchTasks(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"
	const bob = "local:user:bob"

	body := map[string]any{
		"channel": "work",
		"id":      "batch-1",
		"tasks": []any{
			map[string]any{"key": "a", "text": "first"},
			map[string]any{"key": "b", "text": "second", "needs": []any{map[string]any{"key": "a"}}},
			map[string]any{"key": "c", "text": "third", "needs": []any{map[string]any{"key": "b"}}},
		},
	}
	status, data := h.batch(alice, body)
	h.expect(status, data, http.StatusCreated)
	created := decode[protocol.CreatedTasksPage](t, data)
	if len(created.Tasks) != 3 {
		t.Fatalf("unexpected batch: %+v", created)
	}
	first, second, third := created.Tasks[0], created.Tasks[1], created.Tasks[2]
	if first.Task == nil || first.Task.State != "ready" || first.Task.Owner != alice {
		t.Fatalf("unexpected first task: %+v", first.Task)
	}
	if second.Task == nil || second.Task.State != "waiting" || len(second.Task.Needs) != 1 || second.Task.Needs[0] != first.ID {
		t.Fatalf("unexpected second task: %+v", second.Task)
	}
	if third.Task == nil || third.Task.State != "waiting" || len(third.Task.Needs) != 1 || third.Task.Needs[0] != second.ID {
		t.Fatalf("unexpected third task: %+v", third.Task)
	}
	for _, task := range created.Tasks {
		if task.Thread != task.ID || task.Re != nil || task.Labels["board.task"] == "" {
			t.Fatalf("a batch root is a thread and a task: %+v", task)
		}
	}
	status, data = h.request(http.MethodGet, "/tasks?state=all&channel=work", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if page := decode[tasksBody](t, data); len(page.Tasks) != 3 {
		t.Fatalf("expected three tasks: %+v", page)
	}

	t.Run("a retry answers the same tasks", func(t *testing.T) {
		status, data := h.batch(alice, body)
		h.expect(status, data, http.StatusCreated)
		retry := decode[protocol.CreatedTasksPage](t, data)
		if len(retry.Tasks) != 3 {
			t.Fatalf("unexpected retry: %+v", retry)
		}
		for at := range retry.Tasks {
			if retry.Tasks[at].ID != created.Tasks[at].ID {
				t.Fatalf("retry task %d is %s, want %s", at, retry.Tasks[at].ID, created.Tasks[at].ID)
			}
		}
	})

	t.Run("the batch reads back by its id", func(t *testing.T) {
		status, data := h.request(http.MethodGet, "/tasks/batch/batch-1", nil, alice, "", nil)
		h.expect(status, data, http.StatusOK)
		found := decode[protocol.CreatedTasksPage](t, data)
		if len(found.Tasks) != 3 || found.Tasks[0].ID != created.Tasks[0].ID {
			t.Fatalf("unexpected batch read: %+v", found)
		}
		// Someone else's batch is none of bob's.
		status, data = h.request(http.MethodGet, "/tasks/batch/batch-1", nil, bob, "", nil)
		h.expect(status, data, http.StatusNotFound)
		if code := errorCode(t, data); code != protocol.CodeBatchNotFound {
			t.Fatalf("expected batch_not_found, got %s", code)
		}
		status, data = h.request(http.MethodGet, "/tasks/batch/nope", nil, alice, "", nil)
		h.expect(status, data, http.StatusNotFound)
	})

	t.Run("another request under the id conflicts", func(t *testing.T) {
		other := map[string]any{
			"channel": "work", "id": "batch-1",
			"tasks": []any{map[string]any{"text": "different"}},
		}
		status, data := h.batch(alice, other)
		h.expect(status, data, http.StatusConflict)
		if code := errorCode(t, data); code != protocol.CodeDuplicateID {
			t.Fatalf("expected duplicate_id, got %s", code)
		}
		status, data = h.request(http.MethodGet, "/tasks/batch/batch-1?request=other", nil, alice, "", nil)
		h.expect(status, data, http.StatusConflict)
		if code := errorCode(t, data); code != protocol.CodeDuplicateID {
			t.Fatalf("expected duplicate_id, got %s", code)
		}
	})

	t.Run("a fronting board's request names the batch", func(t *testing.T) {
		body := map[string]any{
			"channel": "work", "id": "batch-2", "request": "req-1",
			"tasks": []any{map[string]any{"text": "one"}},
		}
		status, data := h.batch(alice, body)
		h.expect(status, data, http.StatusCreated)
		first := decode[protocol.CreatedTasksPage](t, data)
		status, data = h.request(http.MethodGet, "/tasks/batch/batch-2?request=req-1", nil, alice, "", nil)
		h.expect(status, data, http.StatusOK)
		if got := decode[protocol.CreatedTasksPage](t, data); len(got.Tasks) != 1 || got.Tasks[0].ID != first.Tasks[0].ID {
			t.Fatalf("unexpected read: %+v", got)
		}
		status, data = h.request(http.MethodGet, "/tasks/batch/batch-2?request=req-2", nil, alice, "", nil)
		h.expect(status, data, http.StatusConflict)
		// The request digest is what a retry matches: what the tasks say can differ.
		body["tasks"] = []any{map[string]any{"text": "changed, same request"}}
		status, data = h.batch(alice, body)
		h.expect(status, data, http.StatusCreated)
		if got := decode[protocol.CreatedTasksPage](t, data); got.Tasks[0].ID != first.Tasks[0].ID {
			t.Fatalf("a request-keyed retry should answer the first batch: %+v", got)
		}
		// Even a body with a different number of tasks is the first batch under
		// the same request: it answers the tasks that landed.
		body["tasks"] = []any{map[string]any{"text": "one"}, map[string]any{"text": "two"}}
		status, data = h.batch(alice, body)
		h.expect(status, data, http.StatusCreated)
		if got := decode[protocol.CreatedTasksPage](t, data); len(got.Tasks) != 1 || got.Tasks[0].ID != first.Tasks[0].ID {
			t.Fatalf("a request-keyed retry should answer the original tasks: %+v", got)
		}
	})
}

func TestBatchTasksRefusals(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"
	const bob = "local:user:bob"

	countTasks := func() int {
		t.Helper()
		status, data := h.request(http.MethodGet, "/tasks?state=all&channel=work", nil, alice, "", nil)
		h.expect(status, data, http.StatusOK)
		return len(decode[tasksBody](t, data).Tasks)
	}
	if got := countTasks(); got != 0 {
		t.Fatalf("expected an empty board: %d", got)
	}

	tests := []struct {
		name string
		body map[string]any
		want int
		code protocol.Code
	}{
		{
			name: "an unknown key",
			body: map[string]any{"channel": "work", "tasks": []any{
				map[string]any{"key": "a", "text": "one", "needs": []any{map[string]any{"key": "ghost"}}},
			}},
			want: http.StatusBadRequest, code: protocol.CodeInvalidNeeds,
		},
		{
			name: "a cycle",
			body: map[string]any{"channel": "work", "tasks": []any{
				map[string]any{"key": "a", "text": "one", "needs": []any{map[string]any{"key": "b"}}},
				map[string]any{"key": "b", "text": "two", "needs": []any{map[string]any{"key": "a"}}},
			}},
			want: http.StatusBadRequest, code: protocol.CodeInvalidNeeds,
		},
		{
			name: "itself",
			body: map[string]any{"channel": "work", "tasks": []any{
				map[string]any{"key": "a", "text": "one", "needs": []any{map[string]any{"key": "a"}}},
			}},
			want: http.StatusBadRequest, code: protocol.CodeInvalidNeeds,
		},
		{
			name: "a duplicate key",
			body: map[string]any{"channel": "work", "tasks": []any{
				map[string]any{"key": "a", "text": "one"},
				map[string]any{"key": "a", "text": "two"},
			}},
			want: http.StatusBadRequest, code: protocol.CodeInvalidNeeds,
		},
		{
			name: "empty text",
			body: map[string]any{"channel": "work", "tasks": []any{map[string]any{"text": "  "}}},
			want: http.StatusBadRequest, code: protocol.CodeInvalidRequest,
		},
		{
			name: "an invalid block",
			body: map[string]any{"channel": "work", "tasks": []any{
				map[string]any{"text": "```json#git-commit@1\n{}\n```"},
			}},
			want: http.StatusBadRequest, code: protocol.CodeInvalidBlock,
		},
		{
			name: "no tasks",
			body: map[string]any{"channel": "work", "tasks": []any{}},
			want: http.StatusBadRequest, code: protocol.CodeBatchTooLarge,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status, data := h.batch(alice, test.body)
			h.expect(status, data, test.want)
			if code := errorCode(t, data); code != test.code {
				t.Fatalf("expected %s, got %s", test.code, code)
			}
		})
	}
	if got := countTasks(); got != 0 {
		t.Fatalf("a refused batch made tasks: %d", got)
	}

	// More tasks than one write holds.
	tasks := make([]any, protocol.TaskBatchMax+1)
	for at := range tasks {
		tasks[at] = map[string]any{"text": "task"}
	}
	status, data := h.batch(alice, map[string]any{"channel": "work", "tasks": tasks})
	h.expect(status, data, http.StatusBadRequest)
	if code := errorCode(t, data); code != protocol.CodeBatchTooLarge {
		t.Fatalf("expected batch_too_large, got %s", code)
	}

	t.Run("post and label permissions", func(t *testing.T) {
		policy := server.AuthorizerFunc(func(_ context.Context, _ string, actor protocol.Actor, channel string, permission protocol.Permission) error {
			switch {
			case actor.ID == bob && channel == "work" && permission == protocol.PermissionPost:
				return protocol.Forbidden("no post")
			case actor.ID == bob && channel == "work" && permission == protocol.PermissionLabel:
				return protocol.Forbidden("no label")
			}
			return nil
		})
		narrow := newHarness(t, server.Options{Authorize: policy})
		status, data := narrow.batch(bob, map[string]any{"channel": "work", "tasks": []any{map[string]any{"text": "x"}}})
		narrow.expect(status, data, http.StatusForbidden)
		status, data = narrow.batch(bob, map[string]any{"channel": "work", "tasks": []any{
			map[string]any{"text": "x", "labels": map[string]string{"step": "one"}},
		}})
		narrow.expect(status, data, http.StatusForbidden)
	})
}

func TestBatchTasksNeedsAssigneesAndAttachments(t *testing.T) {
	h := newHarness(t, server.Options{Blobs: blob.NewMemory()})
	const alice = "local:user:alice"

	// An external prerequisite that already completed readies its dependent at
	// once.
	status, data := h.request(http.MethodPost, "/tasks", map[string]any{"channel": "work", "text": "prereq", "claim": true}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	prereq := decode[taskBody](t, data)
	status, data = h.request(http.MethodPost, "/tasks/"+prereq.Task.ID+"/complete", map[string]any{
		"epoch": prereq.Claim.Epoch, "token": prereq.Claim.Token,
	}, alice, "", nil)
	h.expect(status, data, http.StatusOK)

	status, data = h.upload(alice, "spec.txt", "text/plain", []byte("spec"))
	h.expect(status, data, http.StatusCreated)
	file := decodeAttachment(t, data)

	status, data = h.batch(alice, map[string]any{
		"channel": "work",
		"tasks": []any{
			map[string]any{
				"text": "after the prereq", "needs": []any{prereq.Task.ID},
				"for": "local:user:alice+worker",
			},
			map[string]any{"text": "", "attachments": []any{file.ID}},
		},
	})
	h.expect(status, data, http.StatusCreated)
	created := decode[protocol.CreatedTasksPage](t, data)
	if created.Tasks[0].Task.State != "ready" || created.Tasks[0].Task.For == nil || *created.Tasks[0].Task.For != "local:user:alice+worker" {
		t.Fatalf("expected a ready, assigned task: %+v", created.Tasks[0].Task)
	}
	if len(created.Tasks[0].Task.Needs) != 1 || created.Tasks[0].Task.Needs[0] != prereq.Task.ID {
		t.Fatalf("unexpected needs: %+v", created.Tasks[0].Task)
	}
	if created.Tasks[1].Text != "" || len(created.Tasks[1].Attachments) != 1 || created.Tasks[1].Attachments[0].ID != file.ID {
		t.Fatalf("a file-only root: %+v", created.Tasks[1])
	}

	// A task assigned to another principal's profile is refused.
	status, data = h.batch(alice, map[string]any{
		"channel": "work",
		"tasks":   []any{map[string]any{"text": "x", "for": "local:user:bob+worker"}},
	})
	h.expect(status, data, http.StatusBadRequest)

	// An unknown or canceled external need is refused.
	status, data = h.batch(alice, map[string]any{
		"channel": "work",
		"tasks":   []any{map[string]any{"text": "x", "needs": []any{"999999"}}},
	})
	h.expect(status, data, http.StatusBadRequest)
	if code := errorCode(t, data); code != protocol.CodeInvalidNeeds {
		t.Fatalf("expected invalid_needs, got %s", code)
	}
	status, data = h.request(http.MethodPost, "/tasks", map[string]any{"channel": "work", "text": "canceled"}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	victim := decode[taskBody](t, data).Task
	status, data = h.request(http.MethodPost, "/tasks/"+victim.ID+"/cancel", map[string]any{"reason": "no"}, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	status, data = h.batch(alice, map[string]any{
		"channel": "work",
		"tasks":   []any{map[string]any{"text": "x", "needs": []any{victim.ID}}},
	})
	h.expect(status, data, http.StatusBadRequest)
	if code := errorCode(t, data); code != protocol.CodeInvalidNeeds {
		t.Fatalf("expected invalid_needs, got %s", code)
	}
}
