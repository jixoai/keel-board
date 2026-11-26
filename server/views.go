package server

import (
	"encoding/json"
	"strconv"

	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
)

// actorView renders an actor ID, falling back to the request's own actor when
// the board hasn't recorded that actor yet (the write that first sees them).
func actorView(snapshot *log.Snapshot, actorID string, fallback *protocol.Actor) *protocol.ActorView {
	if record := snapshot.Actor(actorID); record != nil {
		view := &protocol.ActorView{
			Kind:     protocol.ActorKind(record.Kind),
			Handle:   record.Handle,
			Name:     record.Name,
			Avatar:   record.Avatar,
			Platform: record.Platform,
			Verified: record.Verified,
		}
		if record.Via != nil {
			view.Via = &protocol.Via{Name: record.Via.Name, Environment: record.Via.Environment}
		}
		return view
	}
	if fallback != nil && fallback.ID == actorID {
		return &protocol.ActorView{
			Kind:   fallback.Kind,
			Handle: fallback.Handle,
			Name:   fallback.Name,
			Avatar: fallback.Avatar,
			Via:    fallback.Via,
		}
	}
	return nil
}

// messageView renders a message as a client sees it. fallback covers the
// write's own actor before their actor.seen event is committed.
func messageView(snapshot *log.Snapshot, message *log.Message, fallback *protocol.Actor) protocol.MessageView {
	labels := make(map[string]string, len(message.Labels))
	for key, value := range message.Labels {
		labels[key] = value
	}
	view := protocol.MessageView{
		ID:          protocol.FormatRef(message.ID),
		Channel:     message.Channel,
		Text:        message.Text,
		Re:          optionalRef(message.Re),
		ReplyTo:     optionalRef(message.ReplyTo),
		Thread:      message.Thread,
		Labels:      labels,
		Reactions:   map[string][]protocol.Ref{},
		NoSync:      message.NoSync,
		From:        message.From,
		CreatedAt:   protocol.FormatTime(message.CreatedAt),
		EditedAt:    protocol.FormatTimePtr(message.EditedAt),
		Deleted:     message.Deleted,
		Undelivered: map[string]string{},
		Attachments: []protocol.Attachment{},
		Links:       []protocol.LinkView{},
		Version:     message.Version,
		Revision:    message.Revision,
		Task:        taskView(message.Task),
		Blind:       message.Blind,
		Blocks:      []protocol.BlockView{},
		By:          actorView(snapshot, message.By.Actor, fallback),
	}
	if len(message.Attachments) > 0 {
		view.Attachments = append([]protocol.Attachment(nil), message.Attachments...)
	}
	view.Author = view.By
	return view
}

// taskView renders a task, without its claim's token.
func taskView(task *log.Task) *protocol.TaskView {
	if task == nil {
		return nil
	}
	needs := make([]protocol.Ref, 0, len(task.Needs))
	for _, id := range task.Needs {
		needs = append(needs, protocol.FormatRef(id))
	}
	view := &protocol.TaskView{
		State: task.State,
		Owner: task.Owner,
		By:    task.By,
		For:   task.For,
		Needs: needs,
		Epoch: task.Epoch,
	}
	if task.Claim != nil {
		view.Claim = &protocol.ClaimView{Actor: task.Claim.Actor, Until: protocol.FormatTime(task.Claim.Until)}
	}
	if task.Blocked != nil {
		view.Blocked = &protocol.HoldView{Kind: task.Blocked.Kind, Reason: task.Blocked.Reason}
	}
	view.Result = task.Result
	view.Reason = task.Reason
	return view
}

// threadSummary renders a root, its live reply count, and its latest reply.
func threadSummary(snapshot *log.Snapshot, root *log.Message, fallback *protocol.Actor) protocol.ThreadSummary {
	replies := 0
	var last *log.Message
	for _, reply := range snapshot.AllReplies(root.ID) {
		if reply == nil || reply.Deleted {
			continue
		}
		replies++
		last = reply
	}
	summary := protocol.ThreadSummary{
		Message: messageView(snapshot, root, fallback),
		Replies: replies,
	}
	if last != nil {
		view := messageView(snapshot, last, fallback)
		summary.Last = &view
	}
	return summary
}

// eventView renders one log event as SSE and `--json` subscribers read it.
func eventView(snapshot *log.Snapshot, event log.Event, fallback *protocol.Actor) protocol.EventView {
	view := protocol.EventView{
		Cursor:    protocol.FormatRef(event.Seq),
		Type:      string(event.Type),
		CreatedAt: protocol.FormatTime(event.At),
		Payload:   payloadView(event.Payload),
	}
	if id := log.ConcernedMessageID(event); id != 0 {
		if message := snapshot.Message(id); message != nil {
			shown := messageView(snapshot, message, fallback)
			view.Message = &shown
		}
	}
	if actorID := payloadActor(event.Payload); actorID != "" {
		if actor := actorView(snapshot, actorID, fallback); actor != nil {
			view.Actor = &protocol.EventActor{ID: actorID, ActorView: *actor}
		}
	}
	return view
}

// payloadView makes an event's payload safe for clients: message IDs are
// refs, and a claim's token hash is left out.
func payloadView(payload any) map[string]any {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return map[string]any{}
	}
	decoded := make(map[string]any)
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		return map[string]any{}
	}
	delete(decoded, "token")
	delete(decoded, "mentions")
	delete(decoded, "along")
	for _, key := range []string{"id", "re", "replyTo"} {
		if number, ok := decoded[key].(float64); ok {
			decoded[key] = strconv.FormatUint(uint64(number), 10)
		}
	}
	if numbers, ok := decoded["needs"].([]any); ok {
		needs := make([]any, 0, len(numbers))
		for _, number := range numbers {
			if value, ok := number.(float64); ok {
				needs = append(needs, strconv.FormatUint(uint64(value), 10))
			} else {
				needs = append(needs, number)
			}
		}
		decoded["needs"] = needs
	}
	return decoded
}

// payloadActor returns the actor a payload's author names, if it has one.
func payloadActor(payload any) string {
	switch value := payload.(type) {
	case *log.MessagePostedPayload:
		return value.By.Actor
	case *log.ChannelCreatedPayload:
		return value.By.Actor
	case *log.MessageEditedPayload:
		return value.By.Actor
	case *log.MessageDeletedPayload:
		return value.By.Actor
	case *log.LabelsChangedPayload:
		return value.By.Actor
	case *log.TaskCreatedPayload:
		return value.By.Actor
	case *log.TaskAssignedPayload:
		return value.By.Actor
	case *log.TaskNeedsChangedPayload:
		return value.By.Actor
	case *log.TaskClaimedPayload:
		return value.By.Actor
	case *log.TaskRenewedPayload:
		return value.By.Actor
	case *log.TaskReleasedPayload:
		return value.By.Actor
	case *log.TaskCompletedPayload:
		return value.By.Actor
	case *log.TaskFailedPayload:
		return value.By.Actor
	case *log.TaskBlockedPayload:
		return value.By.Actor
	case *log.TaskUnblockedPayload:
		return value.By.Actor
	case *log.TaskCanceledPayload:
		return value.By.Actor
	case *log.TaskReopenedPayload:
		return value.By.Actor
	}
	return ""
}

func optionalRef(id *uint64) *protocol.Ref {
	if id == nil {
		return nil
	}
	return protocol.RefOf(*id)
}
