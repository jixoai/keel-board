package server

import (
	"fmt"

	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
)

// idemKey names an idempotent write unambiguously. An actor ID and a client ID
// both contain colons, so joining them with ':' would let ("local:user",
// "bob:batch-1") and ("local:user:bob", "batch-1") share a key. The actor's
// length comes first, which ends the actor part exactly even when it holds any
// byte.
func idemKey(namespace, actorID, id string) string {
	return fmt.Sprintf("%s:%d:%s:%s", namespace, len(actorID), actorID, id)
}

// authorWroteAs reports whether a caller may have a write's message back: its
// principal wrote it, as itself or any of its profiles, and a profile gets
// only what it wrote itself.
func authorWroteAs(author protocol.Author, actor protocol.Actor) bool {
	if author.Principal != actor.Principal {
		return false
	}
	return actor.Profile == nil || author.Actor == actor.ID
}

// blindReplayView is what a caller that can't read a channel gets back for its
// own blind write: the text it sent and the identity a client uses to read or
// change it, never labels, files, reactions, or a task.
func blindReplayView(message *log.Message, text string) protocol.MessageView {
	return protocol.MessageView{
		ID:          protocol.FormatRef(message.ID),
		Channel:     message.Channel,
		Text:        text,
		Thread:      message.Thread,
		CreatedAt:   protocol.FormatTime(message.CreatedAt),
		Deleted:     message.Deleted,
		Blind:       true,
		Undelivered: map[string]string{},
		Labels:      map[string]string{},
		Reactions:   map[string][]protocol.Ref{},
		Attachments: []protocol.Attachment{},
		Links:       []protocol.LinkView{},
		Blocks:      []protocol.BlockView{},
		Version:     message.Version,
		Revision:    message.Revision,
	}
}

// viewForCaller renders a write's own message as its response: what it carries
// when the caller may read the channel, and only its sent text otherwise.
func (s *Server) viewForCaller(req *request, snapshot *log.Snapshot, message *log.Message, sentText string) protocol.MessageView {
	if req.can(s, message.Channel, protocol.PermissionRead) {
		return messageView(snapshot, message, &req.actor)
	}
	return blindReplayView(message, sentText)
}

// replayMessage renders a message an idempotent retry already landed, after
// checking that it is the caller's own and that the caller may read it. A
// caller that can't read gets only its blind write's sent text; anyone else's
// message is 404, as the message would be.
func (s *Server) replayMessage(req *request, id uint64, sentText string) (protocol.MessageView, *protocol.Error) {
	var view protocol.MessageView
	var failure *protocol.Error
	err := s.board.Read(req.tenant, func(snapshot *log.Snapshot) error {
		message := snapshot.Message(id)
		if message == nil || !authorWroteAs(message.By, req.actor) {
			failure = protocol.NotFound(protocol.CodeMessageNotFound, fmt.Sprintf("message not found: #%d", id))
			return nil
		}
		if req.can(s, message.Channel, protocol.PermissionRead) {
			view = messageView(snapshot, message, &req.actor)
			return nil
		}
		if message.Blind {
			view = blindReplayView(message, sentText)
			return nil
		}
		failure = protocol.NotFound(protocol.CodeMessageNotFound, fmt.Sprintf("message not found: #%d", id))
		return nil
	})
	if err != nil {
		return protocol.MessageView{}, protocol.NewError(protocol.CodeInternal, "could not read the board")
	}
	return view, failure
}

// replayBatch renders a batch an idempotent retry already landed. Every root
// must be the caller's own, and readable (or its own blind write, which gets
// its sent text alone); otherwise the batch is 404 and nothing of it leaks.
func (s *Server) replayBatch(req *request, ids []uint64) (protocol.CreatedTasksPage, *protocol.Error) {
	tasks := make([]protocol.MessageView, 0, len(ids))
	var failure *protocol.Error
	err := s.board.Read(req.tenant, func(snapshot *log.Snapshot) error {
		for _, id := range ids {
			message := snapshot.Message(id)
			if message == nil || !authorWroteAs(message.By, req.actor) {
				failure = protocol.NotFound(protocol.CodeBatchNotFound, "no batch of yours has that id")
				return nil
			}
			switch {
			case req.can(s, message.Channel, protocol.PermissionRead):
				tasks = append(tasks, messageView(snapshot, message, &req.actor))
			case message.Blind:
				tasks = append(tasks, blindReplayView(message, message.Text))
			default:
				failure = protocol.NotFound(protocol.CodeBatchNotFound, "no batch of yours has that id")
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return protocol.CreatedTasksPage{}, protocol.NewError(protocol.CodeInternal, "could not read the board")
	}
	return protocol.CreatedTasksPage{Tasks: tasks}, failure
}

// writePlaceDigest is what an idempotent post or task write asks for: the place
// it lands. Text, labels, and files can differ on a retry; where it lands
// can't.
func writePlaceDigest(channel string, re *string) string {
	if re != nil {
		return "place:re:" + *re
	}
	return "place:channel:" + channel
}
