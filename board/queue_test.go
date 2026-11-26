package board

import (
	"testing"
	"time"
)

func listTask(id uint64, channel string, created time.Time, state TaskState) Task {
	return Task{
		ID:        id,
		Channel:   channel,
		Owner:     testOwner,
		State:     state,
		CreatedAt: created,
		UpdatedAt: created,
		Version:   1,
	}
}

func listIDs(tasks []Task) []uint64 {
	out := make([]uint64, len(tasks))
	for i, task := range tasks {
		out[i] = task.ID
	}
	return out
}

func sameIDs(got []Task, want ...uint64) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i].ID != want[i] {
			return false
		}
	}
	return true
}

func TestListDefaultOpen(t *testing.T) {
	at := testNow
	tasks := map[uint64]Task{
		1: listTask(1, "work", at.Add(1*time.Second), StateWaiting),
		2: listTask(2, "work", at.Add(2*time.Second), StateReady),
		3: listTask(3, "work", at.Add(3*time.Second), StateClaimed),
		4: listTask(4, "work", at.Add(4*time.Second), StateBlocked),
		5: listTask(5, "work", at.Add(5*time.Second), StateCompleted),
		6: listTask(6, "work", at.Add(6*time.Second), StateFailed),
		7: listTask(7, "work", at.Add(7*time.Second), StateCanceled),
		8: listTask(8, "work", at.Add(8*time.Second), StateClaimed),
	}
	// 8's lease is over: it lists as ready, not claimed.
	tasks[8] = func() Task {
		task := tasks[8]
		task.Claim = &Claim{Actor: testAgent, Epoch: 1, Until: at.Add(-time.Second), TokenHash: HashToken("t")}
		return task
	}()
	tasks[3] = func() Task {
		task := tasks[3]
		task.Claim = &Claim{Actor: testAgent, Epoch: 1, Until: at.Add(time.Minute), TokenHash: HashToken("t")}
		return task
	}()

	got := List(tasks, ListFilter{}, at)
	if !sameIDs(got, 1, 2, 3, 4, 8) {
		t.Fatalf("open list = %v, want 1 2 3 4 8", listIDs(got))
	}
	all := List(tasks, ListFilter{All: true}, at)
	if !sameIDs(all, 1, 2, 3, 4, 5, 6, 7, 8) {
		t.Fatalf("all list = %v", listIDs(all))
	}
}

func TestListStateUsesEffectiveState(t *testing.T) {
	at := testNow
	live := listTask(1, "work", at, StateClaimed)
	live.Claim = &Claim{Actor: testAgent, Epoch: 1, Until: at.Add(time.Minute)}
	lapsed := listTask(2, "work", at.Add(time.Second), StateClaimed)
	lapsed.Claim = &Claim{Actor: testAgent, Epoch: 1, Until: at.Add(-time.Second)}
	ready := listTask(3, "work", at.Add(2*time.Second), StateReady)
	tasks := map[uint64]Task{1: live, 2: lapsed, 3: ready}

	if got := List(tasks, ListFilter{State: StateReady}, at); !sameIDs(got, 2, 3) {
		t.Fatalf("ready = %v", listIDs(got))
	}
	if got := List(tasks, ListFilter{State: StateClaimed}, at); !sameIDs(got, 1) {
		t.Fatalf("claimed = %v", listIDs(got))
	}
	if got := List(tasks, ListFilter{State: StateWaiting}, at); len(got) != 0 {
		t.Fatalf("waiting = %v", listIDs(got))
	}
}

func TestListOrdersByCreationThenID(t *testing.T) {
	at := testNow
	tasks := map[uint64]Task{
		3: listTask(3, "work", at, StateReady),
		1: listTask(1, "work", at, StateReady),
		2: listTask(2, "work", at.Add(-time.Second), StateReady),
		9: listTask(9, "work", at.Add(-2*time.Second), StateReady),
	}
	got := List(tasks, ListFilter{}, at)
	if !sameIDs(got, 9, 2, 1, 3) {
		t.Fatalf("order = %v, want 9 2 1 3", listIDs(got))
	}
}

func TestListFilters(t *testing.T) {
	at := testNow
	base := func(id uint64, channel string, created time.Time) Task {
		task := listTask(id, channel, created, StateReady)
		task.Labels = map[string]string{"step": "fix"}
		return task
	}
	fixer := ActorID(string(testOwner) + "+fixer")
	withFor := base(4, "work/a", at.Add(3*time.Second))
	withFor.For = fixer
	tasks := map[uint64]Task{
		1: base(1, "work", at),
		2: base(2, "work/a", at.Add(time.Second)),
		3: base(3, "work/a/deep", at.Add(2*time.Second)),
		4: withFor,
		5: func() Task {
			task := base(5, "work", at.Add(4*time.Second))
			task.Labels = map[string]string{"step": "review"}
			task.Needs = []uint64{1}
			return task
		}(),
		6: func() Task {
			task := base(6, "ops", at.Add(5*time.Second))
			task.Owner = testOther
			return task
		}(),
	}

	cases := []struct {
		name   string
		filter ListFilter
		want   []uint64
	}{
		{"channel", ListFilter{Channel: "work/a"}, []uint64{2, 4}},
		{"subtree", ListFilter{Channel: "work/"}, []uint64{1, 2, 3, 4, 5}},
		{"label", ListFilter{Labels: map[string]string{"step": "fix"}}, []uint64{1, 2, 3, 4, 6}},
		{"two labels", ListFilter{Labels: map[string]string{"step": "fix", "board.task": "ready"}}, []uint64{1, 2, 3, 4, 6}},
		{"board state", ListFilter{Labels: map[string]string{StateLabel: "waiting"}}, nil},
		{"absent empty value", ListFilter{Labels: map[string]string{"missing": ""}}, nil},
		{"owner", ListFilter{Owner: testOther}, []uint64{6}},
		{"for", ListFilter{For: fixer}, []uint64{4}},
		{"needs", ListFilter{Needs: 1}, []uint64{5}},
		{"no match", ListFilter{Channel: "nowhere"}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := List(tasks, tc.filter, at)
			if !sameIDs(got, tc.want...) {
				t.Fatalf("got %v, want %v", listIDs(got), tc.want)
			}
		})
	}

	// The derived board.task label follows a lapsed lease.
	lapsed := tasks[1]
	lapsed.State = StateClaimed
	lapsed.Claim = &Claim{Actor: testAgent, Epoch: 1, Until: at.Add(-time.Second)}
	tasks[1] = lapsed
	if got := List(tasks, ListFilter{Labels: map[string]string{StateLabel: "ready"}}, at); len(got) != len(tasks) {
		t.Fatalf("lapsed label list = %v", listIDs(got))
	}
	if got := List(tasks, ListFilter{Labels: map[string]string{StateLabel: "claimed"}}, at); len(got) != 0 {
		t.Fatalf("lapsed still claimed = %v", listIDs(got))
	}
}

func TestClaimReady(t *testing.T) {
	at := testNow
	completed := completedTask(t, 20, at)
	failed := failedTask(t, 21, at)
	fixer := ActorID(string(testOwner) + "+fixer")
	otherAssignee := ActorID(string(testOther) + "+fixer")

	poolOld := listTask(1, "work", at, StateReady)
	poolNew := listTask(2, "work", at.Add(time.Second), StateReady)
	assignedMine := listTask(3, "work", at.Add(2*time.Second), StateReady)
	assignedMine.For = fixer
	assignedOther := listTask(4, "work", at.Add(3*time.Second), StateReady)
	assignedOther.For = otherAssignee
	satisfied := listTask(5, "work", at.Add(4*time.Second), StateReady)
	satisfied.Needs = []uint64{20}
	failedNeed := listTask(6, "work", at.Add(5*time.Second), StateReady)
	failedNeed.Needs = []uint64{21}
	missingNeed := listTask(7, "work", at.Add(6*time.Second), StateReady)
	missingNeed.Needs = []uint64{99}
	lapsed := listTask(8, "work", at.Add(7*time.Second), StateClaimed)
	lapsed.Claim = &Claim{Actor: testOther, Epoch: 1, Until: at.Add(-time.Second)}
	blocked := listTask(9, "work", at.Add(8*time.Second), StateBlocked)
	waitingSatisfied := listTask(10, "work", at.Add(9*time.Second), StateWaiting)
	waitingSatisfied.Needs = []uint64{20}
	otherOwner := listTask(11, "ops", at.Add(10*time.Second), StateReady)
	otherOwner.Owner = testOther

	tasks := map[uint64]Task{
		1: poolOld, 2: poolNew, 3: assignedMine, 4: assignedOther,
		5: satisfied, 6: failedNeed, 7: missingNeed, 8: lapsed,
		9: blocked, 10: waitingSatisfied, 11: otherOwner,
		20: completed, 21: failed,
	}

	// Assigned to the actor first, then the oldest pool task whose needs hold.
	got, ok := ClaimReady(tasks, ReadyFilter{Channel: "work", Actor: fixer}, at)
	if !ok || got.ID != 3 {
		t.Fatalf("assigned first = %d, %v", got.ID, ok)
	}
	// Without an assignee match, the pool's oldest.
	got, ok = ClaimReady(tasks, ReadyFilter{Channel: "work", Actor: testAgent}, at)
	if !ok || got.ID != 1 {
		t.Fatalf("pool oldest = %d, %v", got.ID, ok)
	}
	// A lapsed claim is ready; 10 is still waiting and is not taken.
	got, ok = ClaimReady(tasks, ReadyFilter{Channel: "work"}, at)
	if !ok || got.ID != 1 {
		t.Fatalf("no actor = %d, %v", got.ID, ok)
	}
	// Owners narrows the pool.
	got, ok = ClaimReady(tasks, ReadyFilter{Channel: "ops", Owners: []ActorID{testOther}}, at)
	if !ok || got.ID != 11 {
		t.Fatalf("owner filter = %d, %v", got.ID, ok)
	}
	if _, ok := ClaimReady(tasks, ReadyFilter{Channel: "ops", Owners: []ActorID{testOwner}}, at); ok {
		t.Fatal("claimed another owner's task")
	}
	// Owners narrows only the pool: a task assigned to the actor is still
	// taken first.
	got, ok = ClaimReady(tasks, ReadyFilter{Channel: "work", Actor: fixer, Owners: []ActorID{testOther}}, at)
	if !ok || got.ID != 3 {
		t.Fatalf("assigned over owners = %d, %v", got.ID, ok)
	}
	// Labels and subtrees.
	tagged := listTask(12, "work/sub", at.Add(11*time.Second), StateReady)
	tagged.Labels = map[string]string{"step": "fix"}
	tasks[12] = tagged
	got, ok = ClaimReady(tasks, ReadyFilter{Channel: "work/", Labels: map[string]string{"step": "fix"}, Actor: testAgent}, at)
	if !ok || got.ID != 12 {
		t.Fatalf("label/subtree = %d, %v", got.ID, ok)
	}
	// A waiting task with completed prerequisites is still not ready until a
	// transition makes it so.
	if got, ok := ClaimReady(map[uint64]Task{10: waitingSatisfied, 20: completed}, ReadyFilter{}, at); ok {
		t.Fatalf("took a waiting task: %d", got.ID)
	}
	// None ready.
	if _, ok := ClaimReady(map[uint64]Task{}, ReadyFilter{}, at); ok {
		t.Fatal("took a task from an empty board")
	}
}

func TestClaimReadySkipsUnsatisfiedPrerequisites(t *testing.T) {
	at := testNow
	readyButBlocked := listTask(1, "work", at, StateReady)
	readyButBlocked.Needs = []uint64{2}
	tasks := map[uint64]Task{
		1: readyButBlocked,
		2: failedTask(t, 2, at),
	}
	if got, ok := ClaimReady(tasks, ReadyFilter{}, at); ok {
		t.Fatalf("took %d with a failed prerequisite", got.ID)
	}
	tasks[2] = completedTask(t, 2, at)
	got, ok := ClaimReady(tasks, ReadyFilter{}, at)
	if !ok || got.ID != 1 {
		t.Fatalf("satisfied prerequisite = %d, %v", got.ID, ok)
	}
}

func TestClaimReadyReturnsCopy(t *testing.T) {
	at := testNow
	task := listTask(1, "work", at, StateReady)
	tasks := map[uint64]Task{1: task}
	got, ok := ClaimReady(tasks, ReadyFilter{}, at)
	if !ok {
		t.Fatal("nothing ready")
	}
	got.Labels = map[string]string{"step": "mutated"}
	if tasks[1].Labels != nil {
		t.Fatal("ClaimReady returned an aliased task")
	}
}

func TestClaimReadyTieByID(t *testing.T) {
	at := testNow
	tasks := map[uint64]Task{
		5: listTask(5, "work", at, StateReady),
		2: listTask(2, "work", at, StateReady),
		9: listTask(9, "work", at, StateReady),
	}
	got, ok := ClaimReady(tasks, ReadyFilter{}, at)
	if !ok || got.ID != 2 {
		t.Fatalf("tie = %d, %v; want 2", got.ID, ok)
	}
}
