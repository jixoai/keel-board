package log

import (
	"strconv"
	"time"

	"github.com/jixoai/keel-board/protocol"
)

// Message is a message as the log's fold keeps it. It is the derived view of
// message.* and task.* events; a caller must not mutate one it read.
type Message struct {
	ID      uint64
	Channel string
	Text    string
	Re      *uint64
	ReplyTo *uint64
	// Thread is the thread key: the root's ID in decimal, as a ref.
	Thread    string
	Labels    map[string]string
	Reactions map[string][]string
	By        Author
	NoSync    bool
	From      *string
	CreatedAt time.Time
	EditedAt  *time.Time
	Deleted   bool
	// Version is the seq of the last event that changed the message.
	Version uint64
	// Revision is the seq of its post, or of its last edit or delete.
	Revision uint64
	// Task is set on a root that's a task.
	Task *Task
	// Blind marks a message its author couldn't read the channel of.
	Blind bool

	// Attachments are the files the message carries. A tombstone keeps none.
	Attachments []protocol.Attachment
	Links       []any
}

// Task is the state a task's root carries.
type Task struct {
	State   string
	Owner   string
	By      string
	For     *string
	Needs   []uint64
	Epoch   uint64
	Claim   *Claim
	Blocked *Hold
	Result  *string
	Reason  *string
}

// Claim is a live lease on a task.
type Claim struct {
	Actor     string
	Until     time.Time
	TokenHash string
}

// Hold is a task held for a person: why, and what kind of answer is needed.
type Hold struct {
	Kind   *string
	Reason string
}

// Channel is a channel as the fold counts it.
type Channel struct {
	Path      string
	LastIndex uint64
	Messages  int
	Threads   int
}

// ActorRecord is who an actor is, as actor.seen events recorded them.
type ActorRecord struct {
	ID       string
	Kind     string
	Handle   string
	Name     string
	Avatar   *string
	Via      *Via
	Platform string
	Host     bool
	Verified bool
}

// State is the folded board. All fields are derived from the log.
type State struct {
	Messages map[uint64]*Message
	// Replies maps a thread root to its reply IDs, in log order.
	Replies  map[uint64][]uint64
	Channels map[string]*Channel
	Actors   map[string]*ActorRecord
	ByHandle map[string][]string
	Head     uint64
}

// NewState returns an empty board.
func NewState() *State {
	return &State{
		Messages: make(map[uint64]*Message),
		Replies:  make(map[uint64][]uint64),
		Channels: make(map[string]*Channel),
		Actors:   make(map[string]*ActorRecord),
		ByHandle: make(map[string][]string),
	}
}

// Apply folds one event into the state. Events fold in seq order.
func (s *State) Apply(event Event) {
	switch payload := event.Payload.(type) {
	case *ActorSeenPayload:
		s.applyActorSeen(payload)
	case *ChannelCreatedPayload:
		s.channel(payload.Channel)
	case *MessagePostedPayload:
		s.applyPosted(event)
	case *MessageEditedPayload:
		if message := s.Messages[payload.ID]; message != nil && !message.Deleted {
			message.Text = payload.Text
			edited := event.At
			message.EditedAt = &edited
			message.Version = event.Seq
			message.Revision = event.Seq
		}
	case *MessageDeletedPayload:
		if message := s.Messages[payload.ID]; message != nil && !message.Deleted {
			message.Deleted = true
			message.Text = ""
			message.Labels = map[string]string{}
			message.Reactions = map[string][]string{}
			message.Attachments = nil
			message.Links = nil
			message.Version = event.Seq
			message.Revision = event.Seq
		}
	case *ReactionPayload:
		s.applyReaction(payload, event.Seq, event.Type == EventReactionRemoved)
	case *LabelsChangedPayload:
		if message := s.Messages[payload.ID]; message != nil && !message.Deleted {
			for key, value := range payload.Set {
				message.Labels[key] = value
			}
			for _, key := range payload.Unset {
				delete(message.Labels, key)
			}
			message.Version = event.Seq
		}
	case *TaskCreatedPayload:
		s.applyTaskCreated(payload, event.Seq)
	case *TaskAssignedPayload:
		s.applyTask(payload.ID, event.Seq, func(task *Task) bool {
			if task.For == nil && payload.For == nil {
				return false
			}
			if task.For != nil && payload.For != nil && *task.For == *payload.For {
				return false
			}
			task.For = copyString(payload.For)
			return true
		})
	case *TaskNeedsChangedPayload:
		s.applyTask(payload.ID, event.Seq, func(task *Task) bool {
			if task.State != "waiting" && task.State != "ready" && task.State != "blocked" {
				return false
			}
			task.Needs = append([]uint64(nil), payload.Needs...)
			if task.State != "blocked" {
				if len(task.Needs) > 0 {
					task.State = "waiting"
				} else {
					task.State = "ready"
				}
			}
			return true
		})
	case *TaskReadyPayload:
		s.applyTask(payload.ID, event.Seq, func(task *Task) bool {
			if task.State != "waiting" {
				return false
			}
			task.State = "ready"
			return true
		})
	case *TaskClaimedPayload:
		s.applyTask(payload.ID, event.Seq, func(task *Task) bool {
			if task.State != "ready" || payload.Epoch != task.Epoch+1 {
				return false
			}
			until, err := time.Parse(time.RFC3339, payload.Until)
			if err != nil {
				until, _ = time.Parse(time.RFC3339Nano, payload.Until)
			}
			task.State = "claimed"
			task.Epoch = payload.Epoch
			task.Claim = &Claim{Actor: payload.By.Actor, Until: until, TokenHash: payload.Token}
			return true
		})
	case *TaskRenewedPayload:
		s.applyTask(payload.ID, event.Seq, func(task *Task) bool {
			if !holds(task, payload.Epoch) {
				return false
			}
			until, err := time.Parse(time.RFC3339, payload.Until)
			if err != nil {
				until, _ = time.Parse(time.RFC3339Nano, payload.Until)
			}
			task.Claim.Until = until
			return true
		})
	case *TaskReleasedPayload:
		s.applyTask(payload.ID, event.Seq, func(task *Task) bool {
			if !holds(task, payload.Epoch) {
				return false
			}
			task.State = "ready"
			task.Claim = nil
			return true
		})
	case *TaskExpiredPayload:
		s.applyTask(payload.ID, event.Seq, func(task *Task) bool {
			if !holds(task, payload.Epoch) {
				return false
			}
			task.State = "ready"
			task.Claim = nil
			return true
		})
	case *TaskCompletedPayload:
		s.applyTask(payload.ID, event.Seq, func(task *Task) bool {
			if !holds(task, payload.Epoch) {
				return false
			}
			task.State = "completed"
			task.Claim = nil
			task.Result = copyString(payload.Result)
			return true
		})
	case *TaskFailedPayload:
		s.applyTask(payload.ID, event.Seq, func(task *Task) bool {
			if !holds(task, payload.Epoch) {
				return false
			}
			task.State = "failed"
			task.Claim = nil
			reason := payload.Reason
			task.Reason = &reason
			return true
		})
	case *TaskBlockedPayload:
		s.applyTask(payload.ID, event.Seq, func(task *Task) bool {
			if task.State != "waiting" && task.State != "ready" && task.State != "claimed" {
				return false
			}
			task.State = "blocked"
			task.Claim = nil
			task.Blocked = &Hold{Kind: copyString(payload.Kind), Reason: payload.Reason}
			return true
		})
	case *TaskUnblockedPayload:
		s.applyTask(payload.ID, event.Seq, func(task *Task) bool {
			if task.State != "blocked" {
				return false
			}
			if len(task.Needs) > 0 {
				task.State = "waiting"
			} else {
				task.State = "ready"
			}
			task.Blocked = nil
			return true
		})
	case *TaskCanceledPayload:
		s.applyTask(payload.ID, event.Seq, func(task *Task) bool {
			if task.State == "completed" || task.State == "canceled" {
				return false
			}
			task.State = "canceled"
			task.Claim = nil
			task.Blocked = nil
			reason := payload.Reason
			task.Reason = &reason
			return true
		})
	case *TaskReopenedPayload:
		s.applyTask(payload.ID, event.Seq, func(task *Task) bool {
			if task.State != "failed" {
				return false
			}
			task.State = "ready"
			task.Reason = nil
			return true
		})
	}
	if event.Seq > s.Head {
		s.Head = event.Seq
	}
}

func (s *State) applyActorSeen(payload *ActorSeenPayload) {
	if previous, ok := s.Actors[payload.Actor]; ok {
		key := lower(previous.Handle)
		s.ByHandle[key] = withoutString(s.ByHandle[key], payload.Actor)
		if len(s.ByHandle[key]) == 0 {
			delete(s.ByHandle, key)
		}
	}
	record := &ActorRecord{
		ID:       payload.Actor,
		Kind:     payload.Kind,
		Handle:   payload.Handle,
		Name:     payload.Name,
		Avatar:   copyString(payload.Avatar),
		Platform: payload.Platform,
		Host:     payload.Host,
		Verified: payload.Verified,
	}
	if payload.Via != nil {
		record.Via = &Via{Name: payload.Via.Name, Environment: copyString(payload.Via.Environment)}
	}
	if previous, ok := s.Actors[payload.Actor]; ok && previous.Host {
		record.Host = true
	}
	s.Actors[payload.Actor] = record
	key := lower(payload.Handle)
	if !containsString(s.ByHandle[key], payload.Actor) {
		s.ByHandle[key] = append(s.ByHandle[key], payload.Actor)
	}
}

func (s *State) applyPosted(event Event) {
	payload, ok := event.Payload.(*MessagePostedPayload)
	if !ok || s.Messages[event.Seq] != nil {
		return
	}
	labels := make(map[string]string, len(payload.Labels))
	for key, value := range payload.Labels {
		labels[key] = value
	}
	message := &Message{
		ID:          event.Seq,
		Channel:     payload.Channel,
		Text:        payload.Text,
		Re:          copyUint64(payload.Re),
		ReplyTo:     copyUint64(payload.ReplyTo),
		Thread:      payload.Thread,
		Labels:      labels,
		Reactions:   map[string][]string{},
		By:          payload.By,
		NoSync:      payload.NoSync,
		From:        copyString(payload.From),
		CreatedAt:   event.At,
		Version:     event.Seq,
		Revision:    event.Seq,
		Blind:       payload.Blind,
		Attachments: append([]protocol.Attachment(nil), payload.Attachments...),
	}
	s.Messages[event.Seq] = message
	if payload.Re != nil {
		s.Replies[*payload.Re] = append(s.Replies[*payload.Re], event.Seq)
	}
	channel := s.channel(payload.Channel)
	channel.LastIndex = event.Seq
	channel.Messages++
	if payload.Re == nil {
		channel.Threads++
	}
}

// applyReaction adds or removes one actor's reaction. An add an actor already
// made, or a remove of one it never made, changes nothing: a retried write is
// the same board.
func (s *State) applyReaction(payload *ReactionPayload, seq uint64, remove bool) {
	message := s.Messages[payload.ID]
	if message == nil || message.Deleted {
		return
	}
	if message.Reactions == nil {
		message.Reactions = map[string][]string{}
	}
	actors := message.Reactions[payload.Emoji]
	if remove {
		if !containsString(actors, payload.By.Actor) {
			return
		}
		kept := withoutString(append([]string(nil), actors...), payload.By.Actor)
		if len(kept) > 0 {
			message.Reactions[payload.Emoji] = kept
		} else {
			delete(message.Reactions, payload.Emoji)
		}
	} else {
		if containsString(actors, payload.By.Actor) {
			return
		}
		message.Reactions[payload.Emoji] = append(actors, payload.By.Actor)
	}
	message.Version = seq
}

func (s *State) applyTaskCreated(payload *TaskCreatedPayload, seq uint64) {
	message := s.Messages[payload.ID]
	if message == nil || message.Deleted || message.Re != nil || message.Task != nil {
		return
	}
	state := "ready"
	if len(payload.Needs) > 0 {
		state = "waiting"
	}
	message.Task = &Task{
		State: state,
		Owner: payload.By.Principal,
		By:    payload.By.Actor,
		For:   copyString(payload.For),
		Needs: append([]uint64(nil), payload.Needs...),
	}
	message.Version = seq
	s.labeled(message)
}

// applyTask applies change to the message's task, and relabels the root when
// it did something.
func (s *State) applyTask(id uint64, seq uint64, change func(*Task) bool) {
	message := s.Messages[id]
	if message == nil || message.Task == nil {
		return
	}
	if !change(message.Task) {
		return
	}
	message.Version = seq
	s.labeled(message)
}

// labeled makes a task's root wear its state as board.task. A tombstone wears
// no labels, this one included.
func (s *State) labeled(message *Message) {
	if message.Deleted || message.Task == nil {
		return
	}
	if message.Labels == nil {
		message.Labels = map[string]string{}
	}
	message.Labels[protocol.TaskLabel] = message.Task.State
}

func (s *State) channel(path string) *Channel {
	channel, ok := s.Channels[path]
	if !ok {
		channel = &Channel{Path: path}
		s.Channels[path] = channel
	}
	return channel
}

// holds reports whether a claim's write is by the claim that holds the task:
// the same epoch, while it's claimed.
func holds(task *Task, epoch uint64) bool {
	return task.State == "claimed" && task.Epoch == epoch && task.Claim != nil
}

// CloneMessage returns a deep copy, for a write's preview of what it left.
func CloneMessage(message *Message) *Message {
	if message == nil {
		return nil
	}
	copy := *message
	copy.Re = copyUint64(message.Re)
	copy.ReplyTo = copyUint64(message.ReplyTo)
	copy.From = copyString(message.From)
	if message.EditedAt != nil {
		edited := *message.EditedAt
		copy.EditedAt = &edited
	}
	copy.Labels = make(map[string]string, len(message.Labels))
	for key, value := range message.Labels {
		copy.Labels[key] = value
	}
	copy.Reactions = make(map[string][]string, len(message.Reactions))
	for emoji, actors := range message.Reactions {
		copy.Reactions[emoji] = append([]string(nil), actors...)
	}
	copy.Attachments = append([]protocol.Attachment(nil), message.Attachments...)
	copy.Links = append([]any(nil), message.Links...)
	if message.Task != nil {
		task := *message.Task
		task.For = copyString(message.Task.For)
		task.Needs = append([]uint64(nil), message.Task.Needs...)
		task.Result = copyString(message.Task.Result)
		task.Reason = copyString(message.Task.Reason)
		if message.Task.Claim != nil {
			claim := *message.Task.Claim
			task.Claim = &claim
		}
		if message.Task.Blocked != nil {
			hold := *message.Task.Blocked
			hold.Kind = copyString(message.Task.Blocked.Kind)
			task.Blocked = &hold
		}
		copy.Task = &task
	}
	return &copy
}

// ThreadKey returns the thread key for a root ID.
func ThreadKey(id uint64) string { return strconv.FormatUint(id, 10) }

func copyString(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func copyUint64(value *uint64) *uint64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func lower(value string) string {
	out := []byte(value)
	for i, c := range out {
		if c >= 'A' && c <= 'Z' {
			out[i] = c + ('a' - 'A')
		}
	}
	return string(out)
}

func withoutString(values []string, want string) []string {
	out := values[:0]
	for _, value := range values {
		if value != want {
			out = append(out, value)
		}
	}
	return out
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
