package server

import (
	"fmt"
	"net/http"

	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
)

// editedHoldReason is why a task waits for a person after its root was edited
// by someone other than its owner.
const editedHoldReason = "its text was edited by someone who does not own the task"

// authored reports whether actor wrote message: its principal did, as itself
// or any of its profiles, and a profile changes only what it wrote itself, as
// upstream's `authored` does.
func authored(message *log.Message, actor protocol.Actor) bool {
	if message.By.Principal != actor.Principal {
		return false
	}
	return actor.Profile == nil || message.By.Actor == actor.ID
}

// ownMessage looks up the message a caller edits or deletes as its author. A
// message the caller can't read answers 404 when it isn't the caller's blind
// write, and someone else's answers 403 where the caller can read it.
func (s *Server) ownMessage(req *request, snapshot *log.Snapshot, id uint64, verb string) (*log.Message, *protocol.Error) {
	message := snapshot.Message(id)
	if message == nil {
		return nil, protocol.NotFound(protocol.CodeMessageNotFound, fmt.Sprintf("message not found: #%d", id))
	}
	readable := req.can(s, message.Channel, protocol.PermissionRead)
	own := authored(message, req.actor)
	if !own || (!readable && !message.Blind) {
		if !readable {
			return nil, protocol.NotFound(protocol.CodeMessageNotFound, fmt.Sprintf("message not found: #%d", id))
		}
		return nil, protocol.Forbidden(fmt.Sprintf("only the author can %s a message", verb))
	}
	return message, nil
}

// revisionConflict builds the stable conflict an edit or a delete answers when
// the message moved since the caller read it.
func revisionConflict(message *log.Message, revision uint64) *protocol.Error {
	if message.Deleted {
		return protocol.NewError(protocol.CodeRevisionConflict,
			fmt.Sprintf("#%d was deleted since revision %d", message.ID, revision))
	}
	return protocol.NewError(protocol.CodeRevisionConflict,
		fmt.Sprintf("#%d was edited since revision %d: it's at %d", message.ID, revision, message.Revision))
}

// handleEdit answers POST /messages/{id}/edit: its author changes its text,
// optionally only while it is still at the revision the caller read.
func (s *Server) handleEdit(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	id, perr := pathRef(r)
	if perr != nil {
		writeError(w, perr)
		return
	}
	var body protocol.EditRequest
	if err := decodeBody(r, &body, false, false); err != nil {
		writeError(w, err)
		return
	}
	if err := protocol.ValidateRevision(body.Revision); err != nil {
		writeError(w, err)
		return
	}
	var message *log.Message
	err := s.board.Read(req.tenant, func(snapshot *log.Snapshot) error {
		found, messageErr := s.ownMessage(req, snapshot, id, "edit")
		if messageErr != nil {
			return messageErr
		}
		message = found
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if err := req.permit(s, message.Channel, protocol.PermissionPost); err != nil {
		writeError(w, err)
		return
	}
	if err := protocol.ValidateMessageText(body.Text, false); err != nil {
		writeError(w, err)
		return
	}
	if err := protocol.ValidateBlocks(body.Text); err != nil {
		writeError(w, err)
		return
	}
	if err := protocol.ValidateMentions(body.Text); err != nil {
		writeError(w, err)
		return
	}

	err = s.board.Update(req.ctx, req.tenant, log.UpdateOptions{}, func(tx *log.Tx) error {
		current := tx.Message(id)
		if current == nil {
			return protocol.NotFound(protocol.CodeMessageNotFound, fmt.Sprintf("message not found: #%d", id))
		}
		if current.Deleted {
			if body.Revision != nil && *body.Revision != current.Revision {
				return revisionConflict(current, *body.Revision)
			}
			return protocol.NewError(protocol.CodeMessageDeleted, "message was deleted")
		}
		if body.Revision != nil && *body.Revision != current.Revision {
			return revisionConflict(current, *body.Revision)
		}
		// The same text again is the same board: a retried edit lands nothing.
		if current.Text == body.Text {
			return nil
		}
		if needsActorSeen(tx.Read(), req.actor) {
			tx.Emit(log.EventActorSeen, actorSeenPayload(req.actor))
		}
		tx.Emit(log.EventMessageEdited, &log.MessageEditedPayload{
			ID: id, Thread: current.Thread, Text: body.Text, By: req.actor.Actored(),
		})
		emitTaskEditHold(tx, current, req.actor)
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// emitTaskEditHold makes a task's root wait for its owner after an edit by
// someone else. A hold or a finished task stays as it is.
func emitTaskEditHold(tx *log.Tx, current *log.Message, actor protocol.Actor) {
	if current == nil || current.Re != nil || current.Task == nil || ownedBy(current.Task, actor) {
		return
	}
	if current.Task.State != "waiting" && current.Task.State != "ready" && current.Task.State != "claimed" {
		return
	}
	kind := protocol.EditedHoldKind
	tx.Emit(log.EventTaskBlocked, &log.TaskBlockedPayload{
		ID: current.ID, Thread: current.Thread, Kind: &kind, Reason: editedHoldReason, By: actor.Actored(),
	})
}

// emitTaskDeleteCancel cancels a task before its root becomes a tombstone.
func emitTaskDeleteCancel(tx *log.Tx, current *log.Message, actor protocol.Actor) {
	if current == nil || current.Re != nil || current.Task == nil {
		return
	}
	if current.Task.State == "completed" || current.Task.State == "canceled" {
		return
	}
	tx.Emit(log.EventTaskCanceled, &log.TaskCanceledPayload{
		ID: current.ID, Thread: current.Thread, Reason: "deleted", By: actor.Actored(),
	})
}

// handleDelete answers POST /messages/{id}/delete: its author turns it into a
// tombstone. Deleting a task's root cancels the task; deleting a message
// already gone is nothing.
func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	id, perr := pathRef(r)
	if perr != nil {
		writeError(w, perr)
		return
	}
	var body protocol.DeleteRequest
	if err := decodeBody(r, &body, false, true); err != nil {
		writeError(w, err)
		return
	}
	if err := protocol.ValidateRevision(body.Revision); err != nil {
		writeError(w, err)
		return
	}
	var message *log.Message
	err := s.board.Read(req.tenant, func(snapshot *log.Snapshot) error {
		found, messageErr := s.ownMessage(req, snapshot, id, "delete")
		if messageErr != nil {
			return messageErr
		}
		message = found
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if err := req.permit(s, message.Channel, protocol.PermissionPost); err != nil {
		writeError(w, err)
		return
	}
	err = s.board.Update(req.ctx, req.tenant, log.UpdateOptions{}, func(tx *log.Tx) error {
		current := tx.Message(id)
		if current == nil {
			return protocol.NotFound(protocol.CodeMessageNotFound, fmt.Sprintf("message not found: #%d", id))
		}
		if current.Deleted {
			if body.Revision != nil && *body.Revision != current.Revision {
				return revisionConflict(current, *body.Revision)
			}
			return nil
		}
		if body.Revision != nil && *body.Revision != current.Revision {
			return revisionConflict(current, *body.Revision)
		}
		if needsActorSeen(tx.Read(), req.actor) {
			tx.Emit(log.EventActorSeen, actorSeenPayload(req.actor))
		}
		emitTaskDeleteCancel(tx, current, req.actor)
		tx.Emit(log.EventMessageDeleted, &log.MessageDeletedPayload{
			ID: id, Thread: current.Thread, By: req.actor.Actored(),
		})
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handleReact answers POST /messages/{id}/reactions: add or remove the
// caller's reaction. Adding one it already made and removing one it didn't
// change nothing.
func (s *Server) handleReact(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	id, perr := pathRef(r)
	if perr != nil {
		writeError(w, perr)
		return
	}
	var body protocol.ReactRequest
	if err := decodeBody(r, &body, false, false); err != nil {
		writeError(w, err)
		return
	}
	emoji, perr := protocol.NormalizeEmoji(body.Emoji)
	if perr != nil {
		writeError(w, perr)
		return
	}
	var channel string
	err := s.board.Read(req.tenant, func(snapshot *log.Snapshot) error {
		message := snapshot.Message(id)
		if message == nil {
			return protocol.NotFound(protocol.CodeMessageNotFound, fmt.Sprintf("message not found: #%d", id))
		}
		channel = message.Channel
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	// Reading is its own permission: a reaction on a message the caller can't
	// see is 404, not a 200/403 that tells it the message is there.
	if !req.can(s, channel, protocol.PermissionRead) {
		writeError(w, protocol.NotFound(protocol.CodeMessageNotFound, fmt.Sprintf("message not found: #%d", id)))
		return
	}
	if err := req.permit(s, channel, protocol.PermissionReact); err != nil {
		writeError(w, err)
		return
	}
	err = s.board.Update(req.ctx, req.tenant, log.UpdateOptions{}, func(tx *log.Tx) error {
		message := tx.Message(id)
		if message == nil {
			return protocol.NotFound(protocol.CodeMessageNotFound, fmt.Sprintf("message not found: #%d", id))
		}
		if message.Deleted {
			return protocol.NewError(protocol.CodeMessageDeleted, "message was deleted")
		}
		has := false
		for _, actor := range message.Reactions[emoji] {
			if actor == req.actor.ID {
				has = true
				break
			}
		}
		if has == !body.Remove {
			return nil
		}
		if needsActorSeen(tx.Read(), req.actor) {
			tx.Emit(log.EventActorSeen, actorSeenPayload(req.actor))
		}
		eventType := log.EventReactionAdded
		if body.Remove {
			eventType = log.EventReactionRemoved
		}
		tx.Emit(eventType, &log.ReactionPayload{
			ID: id, Thread: message.Thread, Emoji: emoji, By: req.actor.Actored(),
		})
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handleListLabels answers GET /messages/{id}/labels: the message's labels as
// they are. A tombstone wears none.
func (s *Server) handleListLabels(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	id, perr := pathRef(r)
	if perr != nil {
		writeError(w, perr)
		return
	}
	response := protocol.LabelList{Labels: map[string]string{}}
	err := s.board.Read(req.tenant, func(snapshot *log.Snapshot) error {
		message := snapshot.Message(id)
		if message == nil || !req.can(s, message.Channel, protocol.PermissionRead) {
			return protocol.NotFound(protocol.CodeMessageNotFound, fmt.Sprintf("message not found: #%d", id))
		}
		for key, value := range message.Labels {
			response.Labels[key] = value
		}
		response.Cursor = protocol.FormatRef(snapshot.Head())
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}
