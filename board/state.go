package board

import (
	"strconv"
	"strings"
	"time"
)

// Create makes a new task. Channel and text are the caller's to validate; this
// function validates the label, needs, and assignee shapes. A task with any
// prerequisite starts waiting; one with none starts ready.
func Create(in CreateInput) (Task, error) {
	if in.ID == 0 {
		return Task{}, taskErrorf(CodeNotATask, "#0 isn't a task")
	}
	if !in.Owner.Valid() {
		return Task{}, taskErrorf(CodeInvalidActor, "%q isn't an actor ID", in.Owner)
	}
	if err := validateFor(in.Owner, in.For); err != nil {
		return Task{}, err
	}
	labels, err := normalizeLabels(in.Labels)
	if err != nil {
		return Task{}, err
	}
	needs, err := normalizeNeeds(in.Needs, in.ID)
	if err != nil {
		return Task{}, err
	}
	state := StateReady
	if len(needs) > 0 {
		state = StateWaiting
	}
	return Task{
		ID:        in.ID,
		Channel:   in.Channel,
		Owner:     in.Owner,
		For:       in.For,
		State:     state,
		Labels:    labels,
		Needs:     needs,
		CreatedAt: in.Now,
		UpdatedAt: in.Now,
		Version:   1,
	}, nil
}

// ClaimTask takes a lease on a ready task as actor. It only succeeds from
// ready as of now: a claimed task whose lease ended is ready, and its old
// claim is replaced. The new claim raises the task's epoch, and one claim has
// one token. (Claim is the type; ClaimTask is the transition.)
func ClaimTask(t Task, actor ActorID, lease time.Duration, now time.Time, tokenHash string) (Task, error) {
	if err := checkTask(t); err != nil {
		return cloneTask(t), err
	}
	if !actor.Valid() {
		return cloneTask(t), taskErrorf(CodeInvalidActor, "%q isn't an actor ID", actor)
	}
	lease, err := normalizeLease(lease)
	if err != nil {
		return cloneTask(t), err
	}
	if !ValidTokenHash(tokenHash) {
		return cloneTask(t), taskErrorf(CodeInvalidToken, "a claim's token hash is 64 lowercase hex characters")
	}
	// Which state the task is in comes first, as on vercel/board: a task that
	// waits is waiting whoever it is assigned to. An assignee is checked
	// before a live claim, so a task kept for another profile answers that
	// rather than task_claimed.
	stateNow := t.StateNow(now)
	switch stateNow {
	case StateWaiting:
		return cloneTask(t), taskErrorf(CodeTaskWaiting, "#%d waits for %s", t.ID, formatIDs(t.Needs))
	case StateBlocked:
		reason := ""
		if t.Blocked != nil {
			reason = t.Blocked.Reason
		}
		return cloneTask(t), taskErrorf(CodeTaskBlocked, "#%d is held for a person: %s", t.ID, reason)
	case StateCompleted, StateFailed, StateCanceled:
		return cloneTask(t), taskErrorf(CodeTaskFinished, "#%d is %s", t.ID, t.State)
	case StateReady, StateClaimed:
		// Check the assignee and the live claim below.
	default:
		return cloneTask(t), taskErrorf(CodeInvalidState, "#%d has state %q", t.ID, t.State)
	}
	if t.For != "" && t.For != actor {
		return cloneTask(t), taskErrorf(CodeTaskAssigned, "#%d is assigned to %s", t.ID, t.For)
	}
	if stateNow == StateClaimed {
		return cloneTask(t), taskErrorf(CodeTaskClaimed, "#%d is claimed until %s", t.ID, t.Claim.Until.Format(time.RFC3339))
	}

	next := cloneTask(t)
	next.epoch = t.Epoch() + 1
	next.State = StateClaimed
	next.Blocked = nil
	next.Claim = &Claim{
		Actor:     actor,
		Epoch:     next.epoch,
		Until:     now.Add(lease),
		TokenHash: tokenHash,
	}
	return touch(next, now), nil
}

// Renew extends a claim's lease from now.
func Renew(t Task, proof ClaimProof, lease time.Duration, now time.Time) (Task, error) {
	if err := checkTask(t); err != nil {
		return cloneTask(t), err
	}
	lease, err := normalizeLease(lease)
	if err != nil {
		return cloneTask(t), err
	}
	if err := VerifyClaimAt(t, proof.Epoch, proof.Token, now); err != nil {
		return cloneTask(t), err
	}
	next := cloneTask(t)
	next.Claim.Until = now.Add(lease)
	return touch(next, now), nil
}

// Release gives a claimed task back: it becomes ready, and its claim is over.
func Release(t Task, proof ClaimProof, now time.Time) (Task, error) {
	if err := checkTask(t); err != nil {
		return cloneTask(t), err
	}
	if err := VerifyClaimAt(t, proof.Epoch, proof.Token, now); err != nil {
		return cloneTask(t), err
	}
	next := cloneTask(t)
	next.State = StateReady
	next.Claim = nil
	return touch(next, now), nil
}

// Complete settles a claimed task as done, with an optional result.
func Complete(t Task, proof ClaimProof, result string, now time.Time) (Task, error) {
	if err := checkTask(t); err != nil {
		return cloneTask(t), err
	}
	if result != "" {
		if err := validateNote("result", result); err != nil {
			return cloneTask(t), err
		}
	}
	if err := VerifyClaimAt(t, proof.Epoch, proof.Token, now); err != nil {
		return cloneTask(t), err
	}
	next := cloneTask(t)
	next.State = StateCompleted
	next.Claim = nil
	next.Blocked = nil
	next.Result = result
	next.Reason = ""
	return touch(next, now), nil
}

// Fail settles a claimed task as given up on, with a reason.
func Fail(t Task, proof ClaimProof, reason string, now time.Time) (Task, error) {
	if err := checkTask(t); err != nil {
		return cloneTask(t), err
	}
	if err := validateNote("reason", reason); err != nil {
		return cloneTask(t), err
	}
	if err := VerifyClaimAt(t, proof.Epoch, proof.Token, now); err != nil {
		return cloneTask(t), err
	}
	next := cloneTask(t)
	next.State = StateFailed
	next.Claim = nil
	next.Blocked = nil
	next.Result = ""
	next.Reason = reason
	return touch(next, now), nil
}

// Block holds a task for a person. With a proof, the hold comes from the claim
// that holds the task and ends it. Without one, it is its owner's to place,
// while no live claim holds the task; authorization for that is the caller's.
func Block(t Task, proof *ClaimProof, kind, reason string, now time.Time) (Task, error) {
	if err := checkTask(t); err != nil {
		return cloneTask(t), err
	}
	if err := validateHoldKind(kind); err != nil {
		return cloneTask(t), err
	}
	if err := validateNote("reason", reason); err != nil {
		return cloneTask(t), err
	}
	if proof != nil {
		if err := VerifyClaimAt(t, proof.Epoch, proof.Token, now); err != nil {
			return cloneTask(t), err
		}
	} else {
		switch state := t.StateNow(now); state {
		case StateClaimed:
			return cloneTask(t), taskErrorf(CodeTaskClaimed, "#%d is claimed until %s: only its claim holds it", t.ID, t.Claim.Until.Format(time.RFC3339))
		case StateBlocked:
			held := ""
			if t.Blocked != nil {
				held = t.Blocked.Reason
			}
			return cloneTask(t), taskErrorf(CodeTaskBlocked, "#%d is held already: %s", t.ID, held)
		case StateCompleted, StateFailed, StateCanceled:
			return cloneTask(t), taskErrorf(CodeTaskFinished, "#%d is %s", t.ID, t.State)
		case StateWaiting, StateReady:
			// Hold it.
		default:
			return cloneTask(t), taskErrorf(CodeInvalidState, "#%d has state %q", t.ID, t.State)
		}
	}
	next := cloneTask(t)
	next.State = StateBlocked
	next.Claim = nil
	next.Blocked = &Hold{Kind: kind, Reason: reason}
	return touch(next, now), nil
}

// Unblock lifts a hold: the task returns to ready when all its prerequisites
// have completed, and to waiting while any has not. A task that is not blocked
// is left as it is.
func Unblock(t Task, tasks map[uint64]Task, now time.Time) (Task, error) {
	if err := checkTask(t); err != nil {
		return cloneTask(t), err
	}
	if t.State.Finished() {
		return cloneTask(t), taskErrorf(CodeTaskFinished, "#%d is %s", t.ID, t.State)
	}
	if t.State != StateBlocked {
		return cloneTask(t), nil
	}
	next := cloneTask(t)
	next.Blocked = nil
	if NeedsSatisfied(tasks, t.Needs) {
		next.State = StateReady
	} else {
		next.State = StateWaiting
	}
	return touch(next, now), nil
}

// Reopen makes a failed task ready again, in place, so what waits for it still
// does. A task that is not failed is left as it is; a completed or canceled
// task cannot be reopened.
func Reopen(t Task, now time.Time) (Task, error) {
	if err := checkTask(t); err != nil {
		return cloneTask(t), err
	}
	if t.State == StateCompleted || t.State == StateCanceled {
		return cloneTask(t), taskErrorf(CodeTaskFinished, "#%d is %s", t.ID, t.State)
	}
	if t.State != StateFailed {
		return cloneTask(t), nil
	}
	next := cloneTask(t)
	next.State = StateReady
	next.Reason = ""
	return touch(next, now), nil
}

// Assign keeps a task for one of its owner's profiles, or with an empty
// forActor returns it to the pool. A live claim stays.
func Assign(t Task, forActor ActorID, now time.Time) (Task, error) {
	if err := checkTask(t); err != nil {
		return cloneTask(t), err
	}
	if t.State.Finished() {
		return cloneTask(t), taskErrorf(CodeTaskFinished, "#%d is %s", t.ID, t.State)
	}
	if err := validateFor(t.Owner, forActor); err != nil {
		return cloneTask(t), err
	}
	if t.For == forActor {
		return cloneTask(t), nil
	}
	next := cloneTask(t)
	next.For = forActor
	return touch(next, now), nil
}

// SetNeeds replaces what a task waits for. It applies while the task is not
// finished and no live claim holds it; a claim whose lease ran out is over,
// and is cleared in the same transition. The new prerequisites are checked as
// a new task's, and cannot make the task wait for itself through any chain.
//
// The task waits for the new set, or is ready at once when all of them have
// completed (or it waits for none). A held task stays held; its state follows
// what it needs when the hold is lifted.
func SetNeeds(t Task, needs []uint64, tasks map[uint64]Task, now time.Time) (Task, error) {
	if err := checkTask(t); err != nil {
		return cloneTask(t), err
	}
	if t.State.Finished() {
		return cloneTask(t), taskErrorf(CodeTaskFinished, "#%d is %s", t.ID, t.State)
	}
	if t.State == StateClaimed && t.ClaimLive(now) {
		return cloneTask(t), taskErrorf(CodeTaskClaimed, "#%d is claimed until %s: what it needs changes once no one holds it", t.ID, t.Claim.Until.Format(time.RFC3339))
	}
	normalized, err := ValidateNeeds(needs, tasks, t.ID)
	if err != nil {
		return cloneTask(t), err
	}
	if sameNeedSet(t.Needs, normalized) {
		return cloneTask(t), nil
	}
	if err := checkAcyclic(tasks, t.ID, addedNeeds(t.Needs, normalized)); err != nil {
		return cloneTask(t), err
	}

	next := cloneTask(t)
	if next.State == StateClaimed { // a lapsed claim is over
		next.Claim = nil
	}
	next.Needs = normalized
	if next.State != StateBlocked {
		if NeedsSatisfied(tasks, normalized) {
			next.State = StateReady
		} else {
			next.State = StateWaiting
		}
	}
	return touch(next, now), nil
}

// Cancel calls a task off and ends any claim. Completed and canceled tasks
// cannot be canceled again; a failed one can.
func Cancel(t Task, reason string, now time.Time) (Task, error) {
	if err := checkTask(t); err != nil {
		return cloneTask(t), err
	}
	if err := validateNote("reason", reason); err != nil {
		return cloneTask(t), err
	}
	if t.State == StateCompleted || t.State == StateCanceled {
		return cloneTask(t), taskErrorf(CodeTaskFinished, "#%d is %s", t.ID, t.State)
	}
	next := cloneTask(t)
	next.State = StateCanceled
	next.Claim = nil
	next.Blocked = nil
	next.Result = ""
	next.Reason = reason
	return touch(next, now), nil
}

// Commit applies next over current only while current is still the version
// base was read at. It is the in-memory form of the conditional append a store
// performs: of two writers that started from the same version, one commits and
// the other is refused as stale. The store can implement the same check in its
// own write.
func Commit(current, base, next Task) (Task, error) {
	if current.ID != base.ID || current.Version != base.Version {
		return cloneTask(current), taskErrorf(CodeVersionConflict, "#%d moved on from version %d", current.ID, base.Version)
	}
	if next.ID != current.ID || next.Version < current.Version {
		return cloneTask(current), taskErrorf(CodeVersionConflict, "#%d can't go back to version %d", current.ID, next.Version)
	}
	return cloneTask(next), nil
}

// NeedsSatisfied reports whether every prerequisite exists and has completed.
// A failed, canceled, waiting, or missing prerequisite means it has not: a
// failed task may be reopened, but until then it readies nothing.
func NeedsSatisfied(tasks map[uint64]Task, needs []uint64) bool {
	for _, id := range needs {
		need, ok := tasks[id]
		if !ok || need.State != StateCompleted {
			return false
		}
	}
	return true
}

// Lookup returns the task with id, or not_a_task.
func Lookup(tasks map[uint64]Task, id uint64) (Task, error) {
	t, ok := tasks[id]
	if !ok {
		return Task{}, taskErrorf(CodeNotATask, "#%d isn't a task", id)
	}
	return cloneTask(t), nil
}

// checkTask rejects a value that cannot be a task at all.
func checkTask(t Task) error {
	if t.ID == 0 {
		return taskErrorf(CodeNotATask, "#0 isn't a task")
	}
	if !t.State.Valid() {
		return taskErrorf(CodeInvalidState, "#%d has state %q", t.ID, t.State)
	}
	if !t.Owner.Valid() {
		return taskErrorf(CodeInvalidActor, "#%d has owner %q, which isn't an actor ID", t.ID, t.Owner)
	}
	return validateFor(t.Owner, t.For)
}

// touch raises a version and stamps the write time.
func touch(t Task, now time.Time) Task {
	t.UpdatedAt = now
	t.Version++
	return t
}

func cloneTask(t Task) Task {
	if t.Labels != nil {
		labels := make(map[string]string, len(t.Labels))
		for key, value := range t.Labels {
			labels[key] = value
		}
		t.Labels = labels
	}
	if t.Needs != nil {
		t.Needs = append([]uint64(nil), t.Needs...)
	}
	if t.Claim != nil {
		claim := *t.Claim
		t.Claim = &claim
	}
	if t.Blocked != nil {
		hold := *t.Blocked
		t.Blocked = &hold
	}
	return t
}

func sameNeedSet(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[uint64]struct{}, len(a))
	for _, id := range a {
		seen[id] = struct{}{}
	}
	for _, id := range b {
		if _, ok := seen[id]; !ok {
			return false
		}
	}
	return true
}

func addedNeeds(current, next []uint64) []uint64 {
	seen := make(map[uint64]struct{}, len(current))
	for _, id := range current {
		seen[id] = struct{}{}
	}
	var added []uint64
	for _, id := range next {
		if _, ok := seen[id]; !ok {
			added = append(added, id)
		}
	}
	return added
}

// checkAcyclic refuses added prerequisites that would make id wait for
// itself, directly or through any chain. It follows no completed task's needs:
// a completed task's prerequisites have completed, and id has not. It stops
// after MaxNeedsWalk tasks: a change it cannot check is refused.
func checkAcyclic(tasks map[uint64]Task, id uint64, added []uint64) error {
	if len(added) == 0 {
		return nil
	}
	seen := make(map[uint64]struct{}, len(added)+1)
	queue := make([]uint64, 0, len(added))
	for _, need := range added {
		if _, ok := seen[need]; ok {
			continue
		}
		seen[need] = struct{}{}
		queue = append(queue, need)
	}
	for len(queue) > 0 {
		at := queue[0]
		queue = queue[1:]
		if at == id {
			return taskErrorf(CodeInvalidNeeds, "#%d waits for #%d already: they would wait for each other", id, added[0])
		}
		task, ok := tasks[at]
		if !ok || task.State == StateCompleted {
			continue
		}
		for _, need := range task.Needs {
			if need == id {
				return taskErrorf(CodeInvalidNeeds, "#%d waits for #%d already: they would wait for each other", id, added[0])
			}
			if _, ok := seen[need]; ok {
				continue
			}
			seen[need] = struct{}{}
			if len(seen) > MaxNeedsWalk {
				return taskErrorf(CodeInvalidNeeds, "#%d has more than %d tasks behind it and the others: too many to check", id, MaxNeedsWalk)
			}
			queue = append(queue, need)
		}
	}
	return nil
}

func formatIDs(ids []uint64) string {
	if len(ids) == 0 {
		return "nothing"
	}
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = "#" + strconv.FormatUint(id, 10)
	}
	return strings.Join(parts, ", ")
}
