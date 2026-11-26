package log_test

import (
	"testing"
	"time"

	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
)

func event(seq uint64, eventType log.EventType, payload any) log.Event {
	return log.Event{Seq: seq, Type: eventType, Payload: payload, At: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func TestMessageFoldLifecycle(t *testing.T) {
	state := log.NewState()
	by := protocol.Author{Actor: "local:user:alice", Principal: "local:user:alice"}
	attachment := protocol.Attachment{ID: "abcdefghijklmnopqrstuv", Name: "shot.png", Type: "image/png", Size: 3}

	state.Apply(event(1, log.EventMessagePosted, &log.MessagePostedPayload{
		ID: 1, Channel: "work", Text: "original", Thread: "1",
		Labels: map[string]string{"status": "open"}, By: by, Attachments: []protocol.Attachment{attachment},
	}))
	message := state.Messages[1]
	if message == nil || message.Revision != 1 || message.Version != 1 {
		t.Fatalf("unexpected post: %+v", message)
	}
	if len(message.Attachments) != 1 || message.Attachments[0].Name != "shot.png" {
		t.Fatalf("attachments: %+v", message.Attachments)
	}
	if message.Reactions == nil || len(message.Reactions) != 0 {
		t.Fatalf("reactions should start empty: %+v", message.Reactions)
	}

	// An edit moves revision and version, and records when.
	state.Apply(event(2, log.EventMessageEdited, &log.MessageEditedPayload{ID: 1, Thread: "1", Text: "edited", By: by}))
	if message.Text != "edited" || message.Revision != 2 || message.Version != 2 || message.EditedAt == nil {
		t.Fatalf("unexpected edit: %+v", message)
	}

	// Reactions and labels move version only.
	state.Apply(event(3, log.EventReactionAdded, &log.ReactionPayload{ID: 1, Thread: "1", Emoji: "thumbs_up", By: by}))
	state.Apply(event(4, log.EventReactionAdded, &log.ReactionPayload{ID: 1, Thread: "1", Emoji: "thumbs_up", By: by}))
	state.Apply(event(5, log.EventReactionAdded, &log.ReactionPayload{ID: 1, Thread: "1", Emoji: "eyes", By: by}))
	if len(message.Reactions["thumbs_up"]) != 1 || len(message.Reactions["eyes"]) != 1 {
		t.Fatalf("unexpected reactions: %+v", message.Reactions)
	}
	if message.Version != 5 || message.Revision != 2 {
		t.Fatalf("a repeated reaction is no change: version %d revision %d", message.Version, message.Revision)
	}
	state.Apply(event(6, log.EventLabelsChanged, &log.LabelsChangedPayload{ID: 1, Thread: "1", Set: map[string]string{"owner": "bob"}, Unset: []string{"status"}, By: by}))
	if message.Labels["owner"] != "bob" || message.Labels["status"] != "" {
		t.Fatalf("unexpected labels: %+v", message.Labels)
	}
	if message.Revision != 2 || message.Version != 6 {
		t.Fatalf("labels leave revision and move version: %d %d", message.Revision, message.Version)
	}

	// A tombstone clears what a message carries; its identity stays.
	state.Apply(event(7, log.EventMessageDeleted, &log.MessageDeletedPayload{ID: 1, Thread: "1", By: by}))
	if !message.Deleted || message.Text != "" || len(message.Labels) != 0 || len(message.Reactions) != 0 || len(message.Attachments) != 0 {
		t.Fatalf("unexpected tombstone: %+v", message)
	}
	if message.Channel != "work" || message.Revision != 7 || message.Version != 7 {
		t.Fatalf("a tombstone keeps its identity and moves revision/version: %+v", message)
	}

	// An edit or a delete of a tombstone changes nothing.
	state.Apply(event(8, log.EventMessageEdited, &log.MessageEditedPayload{ID: 1, Thread: "1", Text: "zombie", By: by}))
	state.Apply(event(9, log.EventMessageDeleted, &log.MessageDeletedPayload{ID: 1, Thread: "1", By: by}))
	if message.Text != "" || message.Revision != 7 || message.Version != 7 {
		t.Fatalf("a tombstone doesn't change: %+v", message)
	}
}

func TestReactionRemovalAndReplyCounts(t *testing.T) {
	state := log.NewState()
	alice := protocol.Author{Actor: "local:user:alice", Principal: "local:user:alice"}
	bob := protocol.Author{Actor: "local:user:bob", Principal: "local:user:bob"}
	state.Apply(event(1, log.EventMessagePosted, &log.MessagePostedPayload{
		ID: 1, Channel: "work", Text: "root", Thread: "1", Labels: map[string]string{}, By: alice,
	}))
	state.Apply(event(2, log.EventMessagePosted, &log.MessagePostedPayload{
		ID: 2, Channel: "work", Text: "reply", Re: ref(1), Thread: "1", Labels: map[string]string{}, By: bob,
	}))
	state.Apply(event(3, log.EventReactionAdded, &log.ReactionPayload{ID: 1, Thread: "1", Emoji: "eyes", By: alice}))
	state.Apply(event(4, log.EventReactionAdded, &log.ReactionPayload{ID: 1, Thread: "1", Emoji: "eyes", By: bob}))
	state.Apply(event(5, log.EventReactionRemoved, &log.ReactionPayload{ID: 1, Thread: "1", Emoji: "eyes", By: alice}))
	// Removing one an actor never made is nothing.
	state.Apply(event(6, log.EventReactionRemoved, &log.ReactionPayload{ID: 1, Thread: "1", Emoji: "eyes", By: alice}))
	message := state.Messages[1]
	if got := message.Reactions["eyes"]; len(got) != 1 || got[0] != bob.Actor {
		t.Fatalf("unexpected reactions: %+v", message.Reactions)
	}
	if message.Version != 5 {
		t.Fatalf("a repeated removal shouldn't move version: %d", message.Version)
	}
	// Deleting a reply takes it out of the live replies but keeps its event.
	state.Apply(event(7, log.EventMessageDeleted, &log.MessageDeletedPayload{ID: 2, Thread: "1", By: bob}))
	snapshot := log.SnapshotOf(state, time.Now())
	if replies := snapshot.Replies(1); len(replies) != 0 {
		t.Fatalf("live replies: %+v", replies)
	}
	if replies := snapshot.AllReplies(1); len(replies) != 1 || !replies[0].Deleted {
		t.Fatalf("all replies: %+v", replies)
	}
}

func ref(id uint64) *uint64 { return &id }
