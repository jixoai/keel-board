// Package board is the pure domain model of the Keel task board.
//
// It transposes the task rules of vercel/board (Apache-2.0) into Go: a task
// is a unit of work with an owner, an optional assignee profile, a state, a
// lease-backed claim, prerequisites, and labels. There is no database, HTTP,
// or UI here. Every state change is a pure function over a Task: it returns a
// new Task and never mutates its input.
//
// Authorization (owner vs manage in a channel), token generation and storage,
// clock timers, and event folding belong to the layers above. This package
// keeps the invariants those layers must not bypass:
//
//   - A task's state moves only through the transitions in state.go.
//   - A claim is fenced by its epoch and its token hash, and a lapsed lease
//     makes the task ready without a timer.
//   - Prerequisites are tasks that completed; a failed or canceled task never
//     readies a dependent, and a change of needs cannot close a cycle.
//   - Labels under "board." are the board's own and cannot be written by a
//     caller.
package board

import "time"

// TaskState is what a task is doing. It is the value of the board.task label
// on a task's root.
type TaskState string

const (
	// StateWaiting means a prerequisite has not completed.
	StateWaiting TaskState = "waiting"
	// StateReady means the task can be claimed.
	StateReady TaskState = "ready"
	// StateClaimed means an agent holds the task until its lease ends.
	StateClaimed TaskState = "claimed"
	// StateBlocked means the task is held for a person.
	StateBlocked TaskState = "blocked"
	// StateCompleted means the task is done, with an optional result.
	StateCompleted TaskState = "completed"
	// StateFailed means the task was given up on, with a reason.
	StateFailed TaskState = "failed"
	// StateCanceled means the task was called off, with a reason.
	StateCanceled TaskState = "canceled"
)

// String returns the wire value of the state.
func (s TaskState) String() string { return string(s) }

// Valid reports whether s is one of the seven task states.
func (s TaskState) Valid() bool {
	switch s {
	case StateWaiting, StateReady, StateClaimed, StateBlocked,
		StateCompleted, StateFailed, StateCanceled:
		return true
	default:
		return false
	}
}

// Open reports whether the task can still be worked: waiting, ready, claimed,
// or blocked. It is the default set a list shows.
func (s TaskState) Open() bool {
	switch s {
	case StateWaiting, StateReady, StateClaimed, StateBlocked:
		return true
	default:
		return false
	}
}

// Finished reports whether the task is completed, failed, or canceled.
func (s TaskState) Finished() bool {
	switch s {
	case StateCompleted, StateFailed, StateCanceled:
		return true
	default:
		return false
	}
}

// ActorID identifies a principal (for example "local:user:julian") or one of
// its profiles ("local:user:julian+claude"). The part before the first colon
// names where the ID comes from; only a credential says whether the ID exists.
type ActorID string

// String returns the actor ID as a string.
func (a ActorID) String() string { return string(a) }

// Valid reports whether a is shaped like an actor ID: "<origin>:<who>", with
// no whitespace and no second colon in the origin.
func (a ActorID) Valid() bool { return ValidActorID(string(a)) }

// Claim is a live lease on a task. TokenHash is the SHA-256 of the claim's
// secret, hex encoded. The secret itself is answered once, to the claim, and
// is never stored here.
type Claim struct {
	// Actor is the actor that holds the claim.
	Actor ActorID `json:"actor"`
	// Epoch identifies this claim among the task's claims. It only grows.
	Epoch uint64 `json:"epoch"`
	// Until is when the lease ends. At Until the claim is already over.
	Until time.Time `json:"until"`
	// TokenHash is SHA-256(token), hex. It never leaves the domain layer.
	TokenHash string `json:"-"`
}

// Hold is a task held for a person: why, and what kind of answer is needed.
type Hold struct {
	Kind   string `json:"kind,omitempty"`
	Reason string `json:"reason"`
}

// ClaimProof is what a claim's own writes carry: the epoch and the raw token
// the claim answered. The token is compared with the stored hash in constant
// time.
type ClaimProof struct {
	Epoch uint64
	Token string
}

// Task is a task as the domain keeps it. Labels holds caller labels only; the
// board.task state label is derived with EffectiveLabels. For is empty when
// the task is in the pool; otherwise it is one of Owner's profiles, named
// "<owner>+<profile>".
type Task struct {
	ID        uint64
	Channel   string
	Owner     ActorID
	For       ActorID
	State     TaskState
	Labels    map[string]string
	Needs     []uint64
	Claim     *Claim
	Blocked   *Hold
	Result    string
	Reason    string
	CreatedAt time.Time
	UpdatedAt time.Time
	// Version is the task's write version. Every effective transition raises
	// it; Commit uses it as the compare-and-swap token, so of two writers
	// that started from the same version only one lands.
	Version uint64

	// epoch is the last claim epoch the task has seen. It stays after a
	// claim ends, so a released or settled task never reuses an epoch and an
	// old claim's writes stay lost. It is unexported: only the transitions in
	// state.go move it, and Epoch reads it.
	epoch uint64
}

// Epoch is the last claim epoch the task has seen, zero before the first
// claim. It stays raised after a claim is released, blocked, or settled.
func (t Task) Epoch() uint64 {
	if t.Claim != nil && t.Claim.Epoch > t.epoch {
		return t.Claim.Epoch
	}
	return t.epoch
}

// ClaimView is a claim as callers see it: no token, no hash.
type ClaimView struct {
	Actor ActorID   `json:"actor"`
	Epoch uint64    `json:"epoch"`
	Until time.Time `json:"until"`
}

// TaskView is a task as callers see it: the effective state at the time of
// the view, labels including board.task, and a claim without its token hash.
type TaskView struct {
	ID        uint64            `json:"id"`
	Channel   string            `json:"channel"`
	Owner     ActorID           `json:"owner"`
	For       ActorID           `json:"for,omitempty"`
	State     TaskState         `json:"state"`
	Labels    map[string]string `json:"labels,omitempty"`
	Needs     []uint64          `json:"needs,omitempty"`
	Claim     *ClaimView        `json:"claim,omitempty"`
	Blocked   *Hold             `json:"blocked,omitempty"`
	Result    string            `json:"result,omitempty"`
	Reason    string            `json:"reason,omitempty"`
	CreatedAt time.Time         `json:"createdAt"`
	UpdatedAt time.Time         `json:"updatedAt"`
}

// CreateInput is the shape a new task starts with. Channel and text are the
// caller's to validate; this layer validates labels, needs, and for.
type CreateInput struct {
	ID      uint64
	Channel string
	Owner   ActorID
	For     ActorID
	Labels  map[string]string
	Needs   []uint64
	Now     time.Time
}

// StateNow is the task's state as claims and lists take it: a claimed task
// whose lease has run out is ready at once, whether or not a timer fired.
func (t Task) StateNow(now time.Time) TaskState {
	if t.State == StateClaimed && !t.ClaimLive(now) {
		return StateReady
	}
	return t.State
}

// ClaimLive reports whether the task is claimed and its lease has not ended.
func (t Task) ClaimLive(now time.Time) bool {
	return t.State == StateClaimed && t.Claim != nil && now.Before(t.Claim.Until)
}

// EffectiveLabels returns the caller labels plus the board.task state label as
// of now. The state label always wins: callers cannot set it.
func (t Task) EffectiveLabels(now time.Time) map[string]string {
	out := make(map[string]string, len(t.Labels)+1)
	for key, value := range t.Labels {
		out[key] = value
	}
	out[StateLabel] = string(t.StateNow(now))
	return out
}

// View returns the task as callers see it at now. The claim's token hash is
// not part of a TaskView.
func (t Task) View(now time.Time) TaskView {
	view := TaskView{
		ID:        t.ID,
		Channel:   t.Channel,
		Owner:     t.Owner,
		For:       t.For,
		State:     t.StateNow(now),
		Labels:    t.EffectiveLabels(now),
		Needs:     append([]uint64(nil), t.Needs...),
		Result:    t.Result,
		Reason:    t.Reason,
		CreatedAt: t.CreatedAt,
		UpdatedAt: t.UpdatedAt,
	}
	if t.Claim != nil {
		view.Claim = &ClaimView{Actor: t.Claim.Actor, Epoch: t.Claim.Epoch, Until: t.Claim.Until}
	}
	if t.Blocked != nil {
		hold := *t.Blocked
		view.Blocked = &hold
	}
	return view
}
