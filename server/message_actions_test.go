package server_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/server"
)

// edit asks for a message's text to change, optionally at a revision.
func (h *harness) edit(actor, id, text string, revision any) (int, []byte) {
	h.t.Helper()
	body := map[string]any{"text": text}
	if revision != nil {
		body["revision"] = revision
	}
	return h.request(http.MethodPost, "/messages/"+id+"/edit", body, actor, "", nil)
}

// deleteMessage asks for a message to become a tombstone.
func (h *harness) deleteMessage(actor, id string, revision any) (int, []byte) {
	h.t.Helper()
	body := map[string]any{}
	if revision != nil {
		body["revision"] = revision
	}
	return h.request(http.MethodPost, "/messages/"+id+"/delete", body, actor, "", nil)
}

// thread reads a message's thread.
func (h *harness) thread(actor, id string) protocol.ThreadView {
	h.t.Helper()
	status, data := h.request(http.MethodGet, "/messages/"+id, nil, actor, "", nil)
	h.expect(status, data, http.StatusOK)
	return decode[protocol.ThreadView](h.t, data)
}

// seedPost appends a message as actor without HTTP, for policy cases where the
// actor may write but no request would let it.
func (h *harness) seedPost(actor protocol.Actor, channel, text string) uint64 {
	h.t.Helper()
	var id uint64
	err := h.board.Update(context.Background(), "default", log.UpdateOptions{}, func(tx *log.Tx) error {
		tx.Emit(log.EventActorSeen, &log.ActorSeenPayload{
			Actor: actor.ID, Kind: string(actor.Kind), Handle: actor.Handle, Name: actor.Name,
		})
		id = tx.NextID(1)
		tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
			ID: id, Channel: channel, Text: text, Thread: log.ThreadKey(id),
			Labels: map[string]string{}, By: actor.Actored(),
		})
		return nil
	})
	if err != nil {
		h.t.Fatalf("seeding a post: %v", err)
	}
	return id
}

func TestMessageEdit(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"
	const bob = "local:user:bob"
	root := h.postMessage(alice, "work", "original")

	t.Run("success bumps revision and version", func(t *testing.T) {
		status, data := h.edit(alice, root.ID, "changed", root.Revision)
		h.expect(status, data, http.StatusOK)
		current := h.thread(alice, root.ID).Message
		if current.Text != "changed" || current.Revision <= root.Revision || current.Version <= root.Version {
			t.Fatalf("unexpected edited message: %+v", current)
		}
		if current.EditedAt == nil {
			t.Fatalf("an edit records its time: %+v", current)
		}
		if current.Author == nil || current.Author.Handle != "alice" {
			t.Fatalf("the author snapshot stands: %+v", current.Author)
		}
	})

	t.Run("the same text is a no-op", func(t *testing.T) {
		before := h.thread(alice, root.ID).Message
		status, data := h.edit(alice, root.ID, "changed", nil)
		h.expect(status, data, http.StatusOK)
		after := h.thread(alice, root.ID).Message
		if after.Revision != before.Revision || after.Version != before.Version {
			t.Fatalf("a retried edit changed the message: %+v then %+v", before, after)
		}
	})

	t.Run("a stale revision conflicts", func(t *testing.T) {
		status, data := h.edit(alice, root.ID, "lost update", root.Revision)
		h.expect(status, data, http.StatusConflict)
		if code := errorCode(t, data); code != protocol.CodeRevisionConflict {
			t.Fatalf("expected revision_conflict, got %s", code)
		}
	})

	t.Run("only the author edits", func(t *testing.T) {
		status, data := h.edit(bob, root.ID, "not mine", nil)
		h.expect(status, data, http.StatusForbidden)
		if code := errorCode(t, data); code != protocol.CodeForbidden {
			t.Fatalf("expected forbidden, got %s", code)
		}
	})

	t.Run("an unreadable message is not found", func(t *testing.T) {
		policy := server.AuthorizerFunc(func(_ context.Context, _ string, actor protocol.Actor, channel string, permission protocol.Permission) error {
			if actor.ID == bob && channel == "secret" && permission == protocol.PermissionRead {
				return protocol.Forbidden("no read")
			}
			return nil
		})
		private := newHarness(t, server.Options{Authorize: policy})
		hidden := private.postMessage(alice, "secret", "hidden")
		status, data := private.request(http.MethodPost, "/messages/"+hidden.ID+"/edit", map[string]any{"text": "x"}, bob, "", nil)
		private.expect(status, data, http.StatusNotFound)
	})

	t.Run("a write-only author edits its blind post", func(t *testing.T) {
		policy := server.AuthorizerFunc(func(_ context.Context, _ string, actor protocol.Actor, channel string, permission protocol.Permission) error {
			if actor.ID == alice && channel == "writeonly" && permission == protocol.PermissionRead {
				return protocol.Forbidden("no read")
			}
			return nil
		})
		blind := newHarness(t, server.Options{Authorize: policy})
		posted := blind.postMessage(alice, "writeonly", "blind")
		if !posted.Blind {
			t.Fatalf("expected a blind post: %+v", posted)
		}
		status, data := blind.request(http.MethodPost, "/messages/"+posted.ID+"/edit", map[string]any{"text": "fixed"}, alice, "", nil)
		blind.expect(status, data, http.StatusOK)
	})

	t.Run("a deleted message asks the delete's revision", func(t *testing.T) {
		victim := h.postMessage(alice, "work", "delete me")
		status, data := h.deleteMessage(alice, victim.ID, nil)
		h.expect(status, data, http.StatusOK)
		deleted := h.thread(alice, victim.ID).Message
		status, data = h.edit(alice, victim.ID, "too late", nil)
		h.expect(status, data, http.StatusBadRequest)
		if code := errorCode(t, data); code != protocol.CodeMessageDeleted {
			t.Fatalf("expected message_deleted, got %s", code)
		}
		status, data = h.edit(alice, victim.ID, "too late", victim.Revision)
		h.expect(status, data, http.StatusConflict)
		if code := errorCode(t, data); code != protocol.CodeRevisionConflict {
			t.Fatalf("expected revision_conflict, got %s", code)
		}
		status, data = h.edit(alice, victim.ID, "still too late", deleted.Revision)
		h.expect(status, data, http.StatusBadRequest)
		if code := errorCode(t, data); code != protocol.CodeMessageDeleted {
			t.Fatalf("expected message_deleted at the delete's revision, got %s", code)
		}
	})

	t.Run("the channel must allow post", func(t *testing.T) {
		policy := server.AuthorizerFunc(func(_ context.Context, _ string, actor protocol.Actor, channel string, permission protocol.Permission) error {
			if actor.ID == bob && channel == "locked" && permission == protocol.PermissionPost {
				return protocol.Forbidden("no post")
			}
			return nil
		})
		locked := newHarness(t, server.Options{Authorize: policy})
		id := locked.seedPost(protocol.Actor{ID: bob, Principal: bob, Kind: protocol.ActorPerson, Handle: "bob", Name: "bob"}, "locked", "mine")
		status, data := locked.edit(bob, protocol.FormatRef(id), "still mine", nil)
		locked.expect(status, data, http.StatusForbidden)
	})

	t.Run("blocks are checked", func(t *testing.T) {
		fresh := h.postMessage(alice, "work", "plain")
		status, data := h.edit(alice, fresh.ID, "```json#git-commit@1\n{\"title\": \"no sha\"}\n```", nil)
		h.expect(status, data, http.StatusBadRequest)
		if code := errorCode(t, data); code != protocol.CodeInvalidBlock {
			t.Fatalf("expected invalid_block, got %s", code)
		}
		if got := h.thread(alice, fresh.ID).Message.Text; got != "plain" {
			t.Fatalf("a refused edit changed the text: %q", got)
		}
	})

	t.Run("mentions are checked", func(t *testing.T) {
		status, data := h.edit(alice, root.ID, "hi @[not-an-actor]", nil)
		h.expect(status, data, http.StatusBadRequest)
	})
}

func TestMessageDelete(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"
	const bob = "local:user:bob"

	t.Run("a tombstone hides everything but its identity", func(t *testing.T) {
		root := h.postMessage(alice, "work", "root text")
		status, data := h.request(http.MethodPost, "/messages", map[string]any{"re": root.ID, "text": "reply"}, alice, "", nil)
		h.expect(status, data, http.StatusCreated)
		reply := decode[postBody](t, data).Message
		status, data = h.request(http.MethodPost, "/messages/"+root.ID+"/labels", map[string]any{"set": map[string]string{"keep": "me"}}, alice, "", nil)
		h.expect(status, data, http.StatusOK)
		before := h.thread(alice, root.ID).Message
		status, data = h.deleteMessage(alice, root.ID, before.Revision)
		h.expect(status, data, http.StatusOK)
		after := h.thread(alice, root.ID).Message
		if !after.Deleted || after.Text != "" || len(after.Labels) != 0 || len(after.Reactions) != 0 || len(after.Attachments) != 0 {
			t.Fatalf("unexpected tombstone: %+v", after)
		}
		if after.Revision <= before.Revision || after.Version <= before.Version {
			t.Fatalf("a delete moves revision and version: %+v then %+v", before, after)
		}
		if after.ID != root.ID || after.Channel != "work" || after.Thread != root.ID {
			t.Fatalf("a tombstone keeps its identity: %+v", after)
		}
		thread := h.thread(alice, root.ID)
		if len(thread.Replies) != 1 || thread.Replies[0].ID != reply.ID || thread.Replies[0].Deleted {
			t.Fatalf("deleting a root leaves its replies: %+v", thread)
		}
	})

	t.Run("deleting again is idempotent", func(t *testing.T) {
		victim := h.postMessage(alice, "work", "twice")
		status, data := h.deleteMessage(alice, victim.ID, nil)
		h.expect(status, data, http.StatusOK)
		deleted := h.thread(alice, victim.ID).Message
		status, data = h.deleteMessage(alice, victim.ID, nil)
		h.expect(status, data, http.StatusOK)
		status, data = h.deleteMessage(alice, victim.ID, deleted.Revision)
		h.expect(status, data, http.StatusOK)
		if again := h.thread(alice, victim.ID).Message; again.Revision != deleted.Revision {
			t.Fatalf("a retried delete moved the revision: %d then %d", deleted.Revision, again.Revision)
		}
	})

	t.Run("a stale revision conflicts", func(t *testing.T) {
		victim := h.postMessage(alice, "work", "stale")
		status, data := h.deleteMessage(alice, victim.ID, victim.Revision+100)
		h.expect(status, data, http.StatusConflict)
		if code := errorCode(t, data); code != protocol.CodeRevisionConflict {
			t.Fatalf("expected revision_conflict, got %s", code)
		}
		if got := h.thread(alice, victim.ID).Message; got.Deleted {
			t.Fatalf("a conflicted delete landed: %+v", got)
		}
	})

	t.Run("only the author deletes", func(t *testing.T) {
		victim := h.postMessage(alice, "work", "not yours")
		status, data := h.deleteMessage(bob, victim.ID, nil)
		h.expect(status, data, http.StatusForbidden)
		if code := errorCode(t, data); code != protocol.CodeForbidden {
			t.Fatalf("expected forbidden, got %s", code)
		}
	})

	t.Run("a deleted reply is a tombstone in its thread", func(t *testing.T) {
		root := h.postMessage(alice, "work", "thread root")
		status, data := h.request(http.MethodPost, "/messages", map[string]any{"re": root.ID, "text": "reply to delete"}, alice, "", nil)
		h.expect(status, data, http.StatusCreated)
		reply := decode[postBody](t, data).Message
		status, data = h.deleteMessage(alice, reply.ID, nil)
		h.expect(status, data, http.StatusOK)
		thread := h.thread(alice, root.ID)
		if len(thread.Replies) != 1 || !thread.Replies[0].Deleted || thread.Replies[0].Text != "" {
			t.Fatalf("expected a tombstone reply: %+v", thread.Replies)
		}
	})
}

func TestDeleteRootCancelsTask(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"
	root := h.postMessage(alice, "work", "ship it")
	status, data := h.request(http.MethodPost, "/tasks", map[string]any{"re": root.ID, "claim": true}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	created := decode[taskBody](t, data)
	if created.Claim == nil || created.Task.Task.State != "claimed" {
		t.Fatalf("expected a claimed task: %+v", created)
	}
	status, data = h.request(http.MethodPost, "/messages", map[string]any{"re": root.ID, "text": "still here"}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)

	status, data = h.deleteMessage(alice, root.ID, nil)
	h.expect(status, data, http.StatusOK)
	thread := h.thread(alice, root.ID)
	message := thread.Message
	if !message.Deleted || message.Task == nil || message.Task.State != "canceled" || message.Task.Claim != nil {
		t.Fatalf("deleting a task's root cancels it: %+v", message)
	}
	if message.Labels["board.task"] != "" {
		t.Fatalf("a tombstone wears no state label: %+v", message.Labels)
	}
	if len(thread.Replies) != 1 || thread.Replies[0].Text != "still here" {
		t.Fatalf("the thread's replies stay: %+v", thread)
	}
	// A canceled task can't be claimed.
	status, data = h.request(http.MethodPost, "/tasks/"+root.ID+"/claim", map[string]any{}, alice, "", nil)
	h.expect(status, data, http.StatusConflict)
}

func TestEditTaskRootByNonOwnerBlocks(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"
	const bob = "local:user:bob"
	root := h.postMessage(alice, "work", "someone else's words")
	// Bob owns the task; Alice wrote the root.
	status, data := h.request(http.MethodPost, "/tasks", map[string]any{"re": root.ID, "claim": true}, bob, "", nil)
	h.expect(status, data, http.StatusCreated)
	created := decode[taskBody](t, data)
	if created.Task.Task.Owner != bob || created.Task.Task.By != bob {
		t.Fatalf("bob should own the task: %+v", created.Task.Task)
	}

	status, data = h.edit(alice, root.ID, "changed by its author", nil)
	h.expect(status, data, http.StatusOK)
	blocked := h.thread(alice, root.ID).Message
	if blocked.Task == nil || blocked.Task.State != "blocked" || blocked.Task.Blocked == nil {
		t.Fatalf("a non-owner edit should hold the task: %+v", blocked.Task)
	}
	if blocked.Task.Blocked.Kind == nil || *blocked.Task.Blocked.Kind != protocol.EditedHoldKind {
		t.Fatalf("the hold kind is the board's edited: %+v", blocked.Task.Blocked)
	}
	if blocked.Task.Claim != nil {
		t.Fatalf("the hold ends the claim: %+v", blocked.Task.Claim)
	}
	// The owner's own edit leaves the hold alone; the owner can't edit another's message anyway.
	status, data = h.edit(bob, root.ID, "owner edit", nil)
	h.expect(status, data, http.StatusForbidden)
}

func TestReactions(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"
	const bob = "local:user:bob"
	root := h.postMessage(alice, "work", "react to me")

	t.Run("add and remove", func(t *testing.T) {
		status, data := h.request(http.MethodPost, "/messages/"+root.ID+"/reactions", map[string]any{"emoji": "+1"}, bob, "", nil)
		h.expect(status, data, http.StatusOK)
		message := h.thread(alice, root.ID).Message
		if got := message.Reactions["thumbs_up"]; len(got) != 1 || got[0] != bob {
			t.Fatalf("unexpected reactions: %+v", message.Reactions)
		}
		if message.Revision != root.Revision {
			t.Fatalf("a reaction leaves the revision: %d != %d", message.Revision, root.Revision)
		}
		if message.Version <= root.Version {
			t.Fatalf("a reaction moves the version: %d <= %d", message.Version, root.Version)
		}
		// A repeat is the same board.
		status, data = h.request(http.MethodPost, "/messages/"+root.ID+"/reactions", map[string]any{"emoji": "thumbs_up"}, bob, "", nil)
		h.expect(status, data, http.StatusOK)
		if again := h.thread(alice, root.ID).Message; again.Version != message.Version {
			t.Fatalf("a repeated reaction moved the version: %d then %d", message.Version, again.Version)
		}
		// Another actor adds it too.
		status, data = h.request(http.MethodPost, "/messages/"+root.ID+"/reactions", map[string]any{"emoji": "thumbs_up"}, alice, "", nil)
		h.expect(status, data, http.StatusOK)
		if got := h.thread(alice, root.ID).Message.Reactions["thumbs_up"]; len(got) != 2 {
			t.Fatalf("both actors should be there: %+v", got)
		}
		status, data = h.request(http.MethodPost, "/messages/"+root.ID+"/reactions", map[string]any{"emoji": "thumbs_up", "remove": true}, bob, "", nil)
		h.expect(status, data, http.StatusOK)
		if got := h.thread(alice, root.ID).Message.Reactions["thumbs_up"]; len(got) != 1 || got[0] != alice {
			t.Fatalf("removing bob should leave alice: %+v", got)
		}
		// Removing it again is a no-op.
		before := h.thread(alice, root.ID).Message.Version
		status, data = h.request(http.MethodPost, "/messages/"+root.ID+"/reactions", map[string]any{"emoji": "thumbs_up", "remove": true}, bob, "", nil)
		h.expect(status, data, http.StatusOK)
		if after := h.thread(alice, root.ID).Message.Version; after != before {
			t.Fatalf("a repeated removal moved the version: %d then %d", before, after)
		}
	})

	t.Run("invalid states", func(t *testing.T) {
		cases := []struct {
			name string
			id   string
			body map[string]any
			want int
			code protocol.Code
		}{
			{"no emoji", root.ID, map[string]any{}, http.StatusBadRequest, protocol.CodeInvalidRequest},
			{"bad emoji", root.ID, map[string]any{"emoji": "not an emoji"}, http.StatusBadRequest, protocol.CodeInvalidRequest},
			{"unknown message", "999999", map[string]any{"emoji": "eyes"}, http.StatusNotFound, protocol.CodeMessageNotFound},
		}
		for _, test := range cases {
			t.Run(test.name, func(t *testing.T) {
				status, data := h.request(http.MethodPost, "/messages/"+test.id+"/reactions", test.body, alice, "", nil)
				h.expect(status, data, test.want)
				if code := errorCode(t, data); code != test.code {
					t.Fatalf("expected %s, got %s", test.code, code)
				}
			})
		}
	})

	t.Run("a tombstone takes no reactions", func(t *testing.T) {
		victim := h.postMessage(alice, "work", "gone")
		status, data := h.deleteMessage(alice, victim.ID, nil)
		h.expect(status, data, http.StatusOK)
		status, data = h.request(http.MethodPost, "/messages/"+victim.ID+"/reactions", map[string]any{"emoji": "eyes"}, alice, "", nil)
		h.expect(status, data, http.StatusBadRequest)
		if code := errorCode(t, data); code != protocol.CodeMessageDeleted {
			t.Fatalf("expected message_deleted, got %s", code)
		}
	})

	t.Run("a reaction needs read", func(t *testing.T) {
		policy := server.AuthorizerFunc(func(_ context.Context, _ string, actor protocol.Actor, channel string, permission protocol.Permission) error {
			if actor.ID == bob && channel == "hidden" && permission == protocol.PermissionRead {
				return protocol.Forbidden("no read")
			}
			return nil
		})
		hidden := newHarness(t, server.Options{Authorize: policy})
		message := hidden.postMessage(alice, "hidden", "not for bob")
		status, data := hidden.request(http.MethodPost, "/messages/"+message.ID+"/reactions", map[string]any{"emoji": "eyes"}, bob, "", nil)
		hidden.expect(status, data, http.StatusNotFound)
		if code := errorCode(t, data); code != protocol.CodeMessageNotFound {
			t.Fatalf("expected message_not_found, got %s", code)
		}
	})

	t.Run("react is its own permission", func(t *testing.T) {
		policy := server.AuthorizerFunc(func(_ context.Context, _ string, actor protocol.Actor, channel string, permission protocol.Permission) error {
			if actor.ID == bob && channel == "quiet" && permission == protocol.PermissionReact {
				return protocol.Forbidden("no react")
			}
			return nil
		})
		quiet := newHarness(t, server.Options{Authorize: policy})
		message := quiet.postMessage(alice, "quiet", "no reactions for bob")
		status, data := quiet.request(http.MethodPost, "/messages/"+message.ID+"/reactions", map[string]any{"emoji": "eyes"}, bob, "", nil)
		quiet.expect(status, data, http.StatusForbidden)
	})
}

func TestLabelsListAndIdempotency(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"
	const bob = "local:user:bob"
	root := h.postMessage(alice, "work", "labelled")

	status, data := h.request(http.MethodPost, "/messages/"+root.ID+"/labels", map[string]any{
		"set": map[string]string{"status": "open"},
	}, alice, "", nil)
	h.expect(status, data, http.StatusOK)

	status, data = h.request(http.MethodGet, "/messages/"+root.ID+"/labels", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	labels := decode[protocol.LabelList](t, data)
	if labels.Labels["status"] != "open" || labels.Cursor == "" {
		t.Fatalf("unexpected labels: %+v", labels)
	}

	// Setting the same value again is the same board.
	before := h.thread(alice, root.ID).Message.Version
	status, data = h.request(http.MethodPost, "/messages/"+root.ID+"/labels", map[string]any{
		"set": map[string]string{"status": "open"},
	}, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	after := h.thread(alice, root.ID).Message.Version
	if after != before {
		t.Fatalf("a repeated label write moved the version: %d then %d", before, after)
	}
	// Unsetting a key that isn't set is too.
	status, data = h.request(http.MethodPost, "/messages/"+root.ID+"/labels", map[string]any{
		"unset": []string{"absent"},
	}, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if again := h.thread(alice, root.ID).Message.Version; again != after {
		t.Fatalf("an absent unset moved the version: %d then %d", after, again)
	}
	// Setting and unsetting the same key in one write has no net effect.
	status, data = h.request(http.MethodPost, "/messages/"+root.ID+"/labels", map[string]any{
		"set":   map[string]string{"temporary": "yes"},
		"unset": []string{"temporary"},
	}, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if again := h.thread(alice, root.ID).Message.Version; again != after {
		t.Fatalf("a net no-op label write moved the version: %d then %d", after, again)
	}

	// A label write needs `label`.
	policy := server.AuthorizerFunc(func(_ context.Context, _ string, actor protocol.Actor, channel string, permission protocol.Permission) error {
		if actor.ID == bob && channel == "work" && permission == protocol.PermissionLabel {
			return protocol.Forbidden("no label")
		}
		return nil
	})
	narrow := newHarness(t, server.Options{Authorize: policy})
	message := narrow.postMessage(alice, "work", "label me")
	status, data = narrow.request(http.MethodPost, "/messages/"+message.ID+"/labels", map[string]any{
		"set": map[string]string{"status": "blocked"},
	}, bob, "", nil)
	narrow.expect(status, data, http.StatusForbidden)

	// The board's namespace is refused on read-back too (it can't be written).
	status, data = h.request(http.MethodPost, "/messages/"+root.ID+"/labels", map[string]any{
		"set": map[string]string{"board.task": "completed"},
	}, alice, "", nil)
	h.expect(status, data, http.StatusBadRequest)
	if code := errorCode(t, data); code != protocol.CodeReservedLabel {
		t.Fatalf("expected reserved_label, got %s", code)
	}

	// A tombstone wears no labels and refuses writes.
	status, data = h.deleteMessage(alice, root.ID, nil)
	h.expect(status, data, http.StatusOK)
	status, data = h.request(http.MethodGet, "/messages/"+root.ID+"/labels", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if gone := decode[protocol.LabelList](t, data); len(gone.Labels) != 0 {
		t.Fatalf("a tombstone wears no labels: %+v", gone)
	}
	status, data = h.request(http.MethodPost, "/messages/"+root.ID+"/labels", map[string]any{
		"set": map[string]string{"later": "x"},
	}, alice, "", nil)
	h.expect(status, data, http.StatusBadRequest)
	if code := errorCode(t, data); code != protocol.CodeMessageDeleted {
		t.Fatalf("expected message_deleted, got %s", code)
	}
}

func TestMessageBlocksAndMentions(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"
	commit := "0123456789abcdef0123456789abcdef01234567"
	text := "Deployed\n\n```json#git-commit@1\n{\"sha\": \"" + commit + "\", \"title\": \"fix\"}\n```\n\ncc @[local:user:bob]"

	status, data := h.request(http.MethodPost, "/messages", map[string]any{"channel": "work", "text": text}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	message := decode[postBody](t, data).Message
	if len(message.Blocks) != 1 || message.Blocks[0].Schema != "git-commit" || message.Blocks[0].Version != 1 {
		t.Fatalf("unexpected blocks: %+v", message.Blocks)
	}
	if message.Blocks[0].Data["sha"] != commit {
		t.Fatalf("block data: %+v", message.Blocks[0].Data)
	}
	status, data = h.request(http.MethodGet, "/threads?mentions=bob", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if page := decode[protocol.ThreadsPage](t, data); len(page.Threads) != 0 {
		t.Fatalf("mentions=bob matches an actor ID only through the caller's handle: %+v", page)
	}
	status, data = h.request(http.MethodGet, "/threads?mentions="+urlQuery("local:user:bob"), nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if page := decode[protocol.ThreadsPage](t, data); len(page.Threads) != 1 || page.Threads[0].Message.ID != message.ID {
		t.Fatalf("mentions should find the thread: %+v", page)
	}

	// Invalid blocks and mentions are refused before anything lands.
	cases := []struct {
		name string
		text string
		code protocol.Code
	}{
		{"missing field", "```json#git-commit@1\n{\"title\": \"x\"}\n```", protocol.CodeInvalidBlock},
		{"unknown schema", "```json#mine@1\n{}\n```", protocol.CodeInvalidBlock},
		{"invalid mention", "hi @[not-an-actor]", protocol.CodeInvalidRequest},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			status, data := h.request(http.MethodPost, "/messages", map[string]any{"channel": "work", "text": test.text}, alice, "", nil)
			h.expect(status, data, http.StatusBadRequest)
			if code := errorCode(t, data); code != test.code {
				t.Fatalf("expected %s, got %s", test.code, code)
			}
		})
	}

	// An edit checks them too.
	root := h.postMessage(alice, "work", "plain")
	status, data = h.edit(alice, root.ID, "```json#mine@1\n{}\n```", nil)
	h.expect(status, data, http.StatusBadRequest)
	if code := errorCode(t, data); code != protocol.CodeInvalidBlock {
		t.Fatalf("expected invalid_block, got %s", code)
	}
}

func urlQuery(value string) string {
	return strings.ReplaceAll(value, ":", "%3A")
}

func TestMessageActionEvents(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"

	response, reader, cancel := h.openStream(t, "", nil)
	defer cancel()
	defer response.Body.Close()
	root := h.postMessage(alice, "work", "watch me")

	seen := map[string]protocol.EventView{}
	read := func(types ...string) {
		t.Helper()
		deadline := 12
		for deadline > 0 {
			missing := false
			for _, want := range types {
				if _, ok := seen[want]; !ok {
					missing = true
					break
				}
			}
			if !missing {
				return
			}
			event := readEvent(t, reader)
			seen[event.Type] = event
			deadline--
		}
		t.Fatalf("didn't see %v: %+v", types, seen)
	}

	status, data := h.edit(alice, root.ID, "edited", nil)
	h.expect(status, data, http.StatusOK)
	status, data = h.request(http.MethodPost, "/messages/"+root.ID+"/reactions", map[string]any{"emoji": "eyes"}, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	status, data = h.deleteMessage(alice, root.ID, nil)
	h.expect(status, data, http.StatusOK)
	read("message.edited", "reaction.added", "message.deleted")

	if edited := seen["message.edited"]; edited.Message == nil || edited.Message.Text != "edited" || edited.Message.Revision != root.Revision+1 {
		t.Fatalf("unexpected edit event: %+v", edited)
	}
	reaction := seen["reaction.added"]
	if reaction.Payload["emoji"] != "eyes" || reaction.Payload["id"] != root.ID || reaction.Actor == nil || reaction.Actor.ID != alice {
		t.Fatalf("unexpected reaction event: %+v", reaction)
	}
	if reaction.Message == nil || len(reaction.Message.Reactions["eyes"]) != 1 {
		t.Fatalf("a reaction event carries the message with it: %+v", reaction.Message)
	}
	if deleted := seen["message.deleted"]; deleted.Message == nil || !deleted.Message.Deleted || deleted.Message.Text != "" {
		t.Fatalf("unexpected delete event: %+v", deleted)
	}
}

func TestBlindTaskWriteResponses(t *testing.T) {
	policy := server.AuthorizerFunc(func(_ context.Context, _ string, actor protocol.Actor, channel string, permission protocol.Permission) error {
		if actor.ID == "local:user:alice" && channel == "blind" && permission == protocol.PermissionRead {
			return protocol.Forbidden("no read")
		}
		return nil
	})
	h := newHarness(t, server.Options{Authorize: policy})
	const alice = "local:user:alice"

	status, data := h.request(http.MethodPost, "/tasks", map[string]any{
		"channel": "blind", "text": "blind task", "labels": map[string]string{"status": "open"},
	}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	created := decode[taskBody](t, data).Task
	if created.ID == "" || !created.Blind || len(created.Labels) != 0 || created.Task != nil || created.By != nil {
		t.Fatalf("a blind task's create response carries only its text: %+v", created)
	}
	for _, call := range []struct {
		path string
		body any
	}{
		{"/tasks/" + created.ID + "/assign", map[string]any{"for": nil}},
		{"/tasks/" + created.ID + "/needs", map[string]any{"needs": []string{}}},
		{"/tasks/" + created.ID + "/cancel", map[string]any{"reason": "changed my mind"}},
	} {
		status, data := h.request(http.MethodPost, call.path, call.body, alice, "", nil)
		h.expect(status, data, http.StatusOK)
		view := decode[taskBody](t, data).Task
		if len(view.Labels) != 0 || len(view.Reactions) != 0 || view.Task != nil || view.By != nil {
			t.Fatalf("%s leaked a blind task: %+v", call.path, view)
		}
	}
}
