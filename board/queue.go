package board

import (
	"sort"
	"strings"
	"time"
)

// ListFilter narrows a list of tasks. The zero value lists every open task,
// oldest first. State names one effective state; All lists finished tasks too.
// Channel matches exactly, or a channel and its subtree when it ends in "/".
// Labels must all match, including the derived board.task state label. Needs,
// when nonzero, keeps tasks that wait for that task.
type ListFilter struct {
	Channel string
	State   TaskState
	All     bool
	Labels  map[string]string
	Owner   ActorID
	For     ActorID
	Needs   uint64
}

// List returns the tasks that match filter, oldest first: by CreatedAt, then
// by ID. A claimed task whose lease ended is listed as ready, and a state
// filter for ready matches it; one for claimed does not.
func List(tasks map[uint64]Task, filter ListFilter, now time.Time) []Task {
	out := make([]Task, 0, len(tasks))
	for _, task := range tasks {
		if !filter.match(task, now) {
			continue
		}
		out = append(out, cloneTask(task))
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

func (f ListFilter) match(task Task, now time.Time) bool {
	if !channelMatches(f.Channel, task.Channel) {
		return false
	}
	state := task.StateNow(now)
	switch {
	case f.All:
		// Every state.
	case f.State == "":
		if !state.Open() {
			return false
		}
	case state != f.State:
		return false
	}
	if f.Owner != "" && task.Owner != f.Owner {
		return false
	}
	if f.For != "" && task.For != f.For {
		return false
	}
	if f.Needs != 0 && !containsID(task.Needs, f.Needs) {
		return false
	}
	if len(f.Labels) > 0 {
		labels := task.EffectiveLabels(now)
		for key, value := range f.Labels {
			if got, ok := labels[key]; !ok || got != value {
				return false
			}
		}
	}
	return true
}

// ReadyFilter narrows the task a claim --ready takes: a channel (with subtree
// when it ends in "/"), labels, the claiming actor, and the owners whose pool
// it draws from. With no Owners, any owner's pool is eligible; a caller that
// takes only its own passes its own owner ID. Owners never narrow a task
// assigned to Actor: an assignment is taken first, whatever pool is named.
type ReadyFilter struct {
	Channel string
	Labels  map[string]string
	Actor   ActorID
	Owners  []ActorID
}

// ClaimReady selects the oldest ready task that satisfies all its
// prerequisites and matches filter. A task assigned to Actor is chosen before
// an unassigned one in the pool, as vercel/board's claim --ready does; each
// group is oldest first. It does not claim the task: the caller applies
// ClaimTask (and Commit) so the optimistic write is visible.
func ClaimReady(tasks map[uint64]Task, filter ReadyFilter, now time.Time) (Task, bool) {
	var assigned, pool *Task
	for _, candidate := range tasks {
		if !channelMatches(filter.Channel, candidate.Channel) {
			continue
		}
		if candidate.StateNow(now) != StateReady {
			continue
		}
		if !NeedsSatisfied(tasks, candidate.Needs) {
			continue
		}
		if !readyLabelsMatch(filter, candidate, now) {
			continue
		}
		if candidate.For != "" {
			if candidate.For != filter.Actor {
				continue
			}
			if assigned == nil || older(candidate, *assigned) {
				taken := candidate
				assigned = &taken
			}
			continue
		}
		if len(filter.Owners) > 0 && !containsActor(filter.Owners, candidate.Owner) {
			continue
		}
		if pool == nil || older(candidate, *pool) {
			taken := candidate
			pool = &taken
		}
	}
	chosen := pool
	if assigned != nil {
		chosen = assigned
	}
	if chosen == nil {
		return Task{}, false
	}
	return cloneTask(*chosen), true
}

func readyLabelsMatch(filter ReadyFilter, task Task, now time.Time) bool {
	if len(filter.Labels) == 0 {
		return true
	}
	labels := task.EffectiveLabels(now)
	for key, value := range filter.Labels {
		if got, ok := labels[key]; !ok || got != value {
			return false
		}
	}
	return true
}

// channelMatches reports whether got is want, or under want when want ends in
// "/". An empty want matches every channel.
func channelMatches(want, got string) bool {
	if want == "" {
		return true
	}
	if strings.HasSuffix(want, "/") {
		base := strings.TrimSuffix(want, "/")
		return got == base || strings.HasPrefix(got, base+"/")
	}
	return got == want
}

func older(a, b Task) bool {
	if !a.CreatedAt.Equal(b.CreatedAt) {
		return a.CreatedAt.Before(b.CreatedAt)
	}
	return a.ID < b.ID
}

func containsID(ids []uint64, want uint64) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

func containsActor(actors []ActorID, want ActorID) bool {
	for _, actor := range actors {
		if actor == want {
			return true
		}
	}
	return false
}
