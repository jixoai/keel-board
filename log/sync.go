package log

import (
	"strings"

	"github.com/jixoai/keel-board/protocol"
)

// SyncTarget is the platform side of a sync attachment, as the board records
// it. It mirrors sync.ThreadRef without importing the sync package: the log is
// the board's own record and must not depend on an adapter layer.
//
// A channel target has no Thread; a thread target does. A sub-thread, when the
// platform groups one (a GitHub review thread), extends its thread and groups
// under the same board thread.
type SyncTarget struct {
	Key       string `json:"key"`
	Platform  string `json:"platform"`
	Channel   string `json:"channel"`
	Thread    string `json:"thread,omitempty"`
	Subthread string `json:"subthread,omitempty"`
}

// External returns the key-prefixed external thread ID, or the channel ID for
// a channel target.
func (t SyncTarget) External() string {
	native := t.Thread
	if native == "" {
		native = t.Channel
	}
	if t.Key == "" {
		return native
	}
	return t.Key + ":" + native
}

// ChannelID returns the key-prefixed channel ID.
func (t SyncTarget) ChannelID() string {
	if t.Key == "" {
		return t.Channel
	}
	return t.Key + ":" + t.Channel
}

// Valid reports whether the target names a key, a platform, and a channel.
func (t SyncTarget) Valid() bool {
	return t.Key != "" && t.Platform != "" && t.Channel != ""
}

// MessageOrigin is where a message came from: a platform's own IDs. It is
// carried on an imported post so a reader can find the platform copy.
type MessageOrigin struct {
	Channel   string `json:"channel,omitempty"`
	Thread    string `json:"thread,omitempty"`
	Subthread string `json:"subthread,omitempty"`
	Message   string `json:"message,omitempty"`
	Context   string `json:"context,omitempty"`
}

// SyncCopy records that a board message exists on a platform target.
type SyncCopy struct {
	Target          SyncTarget `json:"target"`
	ExternalThread  string     `json:"externalThread"`
	ExternalMessage string     `json:"externalMessage"`
	Files           []string   `json:"files,omitempty"`
	FilesSet        bool       `json:"filesSet,omitempty"`
}

// SyncState is a message's sync projection: the targets attached to its thread,
// the copies delivered, and the targets a delivery failed for.
type SyncState struct {
	Threads     []SyncTarget      `json:"threads,omitempty"`
	Copies      []SyncCopy        `json:"copies,omitempty"`
	Undelivered map[string]string `json:"undelivered,omitempty"`
}

// AddThread records a thread attachment. Re-attaching one target keeps one.
func (s *SyncState) AddThread(target SyncTarget) {
	for _, existing := range s.Threads {
		if existing == target {
			return
		}
	}
	s.Threads = append(s.Threads, target)
}

// AddCopy records a delivered copy. A repeated receipt replaces the earlier
// one for the same target and board message (files arrive in a later receipt).
func (s *SyncState) AddCopy(copy SyncCopy) {
	for i := range s.Copies {
		if s.Copies[i].Target == copy.Target {
			s.Copies[i] = copy
			if s.Undelivered != nil {
				delete(s.Undelivered, copy.Target.External())
			}
			return
		}
	}
	s.Copies = append(s.Copies, copy)
	if s.Undelivered != nil {
		delete(s.Undelivered, copy.Target.External())
	}
}

// AddFiles records the file IDs of a copy's files.
func (s *SyncState) AddFiles(target SyncTarget, files []string, filesSet bool) {
	if s.Undelivered != nil {
		delete(s.Undelivered, target.External())
	}
	for i := range s.Copies {
		if s.Copies[i].Target == target {
			s.Copies[i].Files = append([]string(nil), files...)
			s.Copies[i].FilesSet = filesSet
			return
		}
	}
	s.Copies = append(s.Copies, SyncCopy{
		Target: target, ExternalThread: target.Thread, Files: append([]string(nil), files...), FilesSet: filesSet,
	})
}

// Fail records that a delivery to target gave up. code is content-free.
func (s *SyncState) Fail(target SyncTarget, code string) {
	if s.Undelivered == nil {
		s.Undelivered = make(map[string]string)
	}
	if code == "" {
		code = "failed"
	}
	s.Undelivered[target.External()] = code
}

// CopyFor returns the copy delivered to target.
func (s *SyncState) CopyFor(target SyncTarget) (SyncCopy, bool) {
	for _, copy := range s.Copies {
		if copy.Target == target {
			return copy, true
		}
	}
	return SyncCopy{}, false
}

// Attached reports whether target is attached to the message's thread.
func (s *SyncState) Attached(target SyncTarget) bool {
	for _, existing := range s.Threads {
		if existing == target {
			return true
		}
	}
	return false
}

// ThreadSyncedPayload attaches a board thread to a platform target. A channel
// target starts a thread; a thread target joins an existing one.
type ThreadSyncedPayload struct {
	ID     uint64     `json:"id"`
	Thread string     `json:"thread"`
	Target SyncTarget `json:"target"`
	Cause  string     `json:"cause,omitempty"`
	By     Author     `json:"by,omitempty"`
}

func (p *ThreadSyncedPayload) messageID() uint64 { return p.ID }

// MessageSyncedPayload records that a board message exists on a platform
// target.
type MessageSyncedPayload struct {
	ID              uint64     `json:"id"`
	Thread          string     `json:"thread"`
	Target          SyncTarget `json:"target"`
	ExternalThread  string     `json:"externalThread"`
	ExternalMessage string     `json:"externalMessage"`
	Files           []string   `json:"files,omitempty"`
	FilesSet        bool       `json:"filesSet,omitempty"`
}

func (p *MessageSyncedPayload) messageID() uint64 { return p.ID }

// MessageFilesSyncedPayload records the file IDs of a delivered copy.
type MessageFilesSyncedPayload struct {
	ID       uint64     `json:"id"`
	Thread   string     `json:"thread"`
	Target   SyncTarget `json:"target"`
	Files    []string   `json:"files,omitempty"`
	FilesSet bool       `json:"filesSet,omitempty"`
}

func (p *MessageFilesSyncedPayload) messageID() uint64 { return p.ID }

// MessageSyncFailedPayload records that a delivery gave up. Error is a
// content-free class, never a platform body.
type MessageSyncFailedPayload struct {
	ID     uint64     `json:"id"`
	Thread string     `json:"thread"`
	Target SyncTarget `json:"target"`
	Error  string     `json:"error"`
}

func (p *MessageSyncFailedPayload) messageID() uint64 { return p.ID }

// applyThreadSynced folds a thread attachment onto the thread root.
func (s *State) applyThreadSynced(seq uint64, payload *ThreadSyncedPayload) {
	message := s.Messages[payload.ID]
	if message == nil || !payload.Target.Valid() {
		return
	}
	if message.Sync == nil {
		message.Sync = &SyncState{}
	}
	message.Sync.AddThread(payload.Target)
	message.Version = seq
}

// applyMessageSynced folds a delivered copy.
func (s *State) applyMessageSynced(seq uint64, payload *MessageSyncedPayload) {
	message := s.Messages[payload.ID]
	if message == nil || !payload.Target.Valid() {
		return
	}
	if message.Sync == nil {
		message.Sync = &SyncState{}
	}
	message.Sync.AddCopy(SyncCopy{
		Target:          payload.Target,
		ExternalThread:  payload.ExternalThread,
		ExternalMessage: payload.ExternalMessage,
		Files:           append([]string(nil), payload.Files...),
		FilesSet:        payload.FilesSet,
	})
	message.Version = seq
}

// applyMessageFilesSynced folds file IDs onto an existing copy.
func (s *State) applyMessageFilesSynced(seq uint64, payload *MessageFilesSyncedPayload) {
	message := s.Messages[payload.ID]
	if message == nil || !payload.Target.Valid() {
		return
	}
	if message.Sync == nil {
		message.Sync = &SyncState{}
	}
	message.Sync.AddFiles(payload.Target, payload.Files, payload.FilesSet)
	message.Version = seq
}

// applyMessageSyncFailed records a delivery failure.
func (s *State) applyMessageSyncFailed(seq uint64, payload *MessageSyncFailedPayload) {
	message := s.Messages[payload.ID]
	if message == nil || !payload.Target.Valid() {
		return
	}
	if message.Sync == nil {
		message.Sync = &SyncState{}
	}
	message.Sync.Fail(payload.Target, payload.Error)
	message.Version = seq
}

// CloneSync returns a deep copy of a sync projection.
func CloneSync(state *SyncState) *SyncState {
	if state == nil {
		return nil
	}
	copy := &SyncState{
		Threads: append([]SyncTarget(nil), state.Threads...),
		Copies:  make([]SyncCopy, len(state.Copies)),
	}
	for i, copyOf := range state.Copies {
		copy.Copies[i] = copyOf
		copy.Copies[i].Files = append([]string(nil), copyOf.Files...)
	}
	if state.Undelivered != nil {
		copy.Undelivered = make(map[string]string, len(state.Undelivered))
		for target, code := range state.Undelivered {
			copy.Undelivered[target] = code
		}
	}
	return copy
}

// TargetOf makes a SyncTarget out of its parts.
func TargetOf(key, platform, channel, thread, subthread string) SyncTarget {
	return SyncTarget{Key: key, Platform: platform, Channel: channel, Thread: thread, Subthread: subthread}
}

// TargetKey names a target for a map that keys failures by delivery.
func TargetKey(target SyncTarget) string {
	return strings.Join([]string{target.Key, target.Platform, target.Channel, target.Thread, target.Subthread}, "|")
}

// cloneOrigin returns a copy of a message origin.
func cloneOrigin(origin *MessageOrigin) *MessageOrigin {
	if origin == nil {
		return nil
	}
	copy := *origin
	return &copy
}

// linksAsAny turns typed link views into the board's wire-shaped list.
func linksAsAny(links []protocol.LinkView) []any {
	if len(links) == 0 {
		return nil
	}
	out := make([]any, len(links))
	for i, link := range links {
		out[i] = link
	}
	return out
}
