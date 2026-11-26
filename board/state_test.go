package board

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func assertTaskEqual(t *testing.T, got, want Task) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("task changed on a failed write:\n got %#v\nwant %#v", got, want)
	}
}

func TestCreate(t *testing.T) {
	task := mustCreate(t, CreateInput{
		ID: 1, Channel: "work", Owner: testOwner, Now: testNow,
		Labels: map[string]string{"step": "fix"}, Needs: []uint64{2, 2},
	})
	if task.State != StateWaiting || task.Version != 1 || task.Epoch() != 0 {
		t.Fatalf("task = %#v", task)
	}
	if len(task.Needs) != 1 || task.Needs[0] != 2 {
		t.Fatalf("needs = %#v", task.Needs)
	}
	if task.Labels["step"] != "fix" || task.CreatedAt != testNow || task.UpdatedAt != testNow {
		t.Fatalf("task = %#v", task)
	}
	if _, ok := task.Labels[StateLabel]; ok {
		t.Fatal("Create wrote the derived state label")
	}
	if task.Claim != nil || task.Blocked != nil {
		t.Fatalf("new task has an owner or a hold: %#v", task)
	}

	ready := readyTask(t, 2, testNow)
	if ready.State != StateReady {
		t.Fatalf("task with no needs is %s", ready.State)
	}

	cases := []struct {
		name string
		in   CreateInput
		code Code
	}{
		{"zero id", CreateInput{Owner: testOwner, Now: testNow}, CodeNotATask},
		{"no owner", CreateInput{ID: 3, Now: testNow}, CodeInvalidActor},
		{"bad owner", CreateInput{ID: 3, Owner: "alice", Now: testNow}, CodeInvalidActor},
		{"for another principal", CreateInput{ID: 3, Owner: testOwner, For: "local:user:alice+fixer", Now: testNow}, CodeInvalidProfile},
		{"for without a profile", CreateInput{ID: 3, Owner: testOwner, For: testOwner, Now: testNow}, CodeInvalidProfile},
		{"reserved label", CreateInput{ID: 3, Owner: testOwner, Labels: map[string]string{StateLabel: "completed"}, Now: testNow}, CodeReservedLabel},
		{"reserved prefix", CreateInput{ID: 3, Owner: testOwner, Labels: map[string]string{"board.custom": "x"}, Now: testNow}, CodeReservedLabel},
		{"uppercase key", CreateInput{ID: 3, Owner: testOwner, Labels: map[string]string{"Step": "fix"}, Now: testNow}, CodeInvalidLabel},
		{"key with space", CreateInput{ID: 3, Owner: testOwner, Labels: map[string]string{"ste p": "fix"}, Now: testNow}, CodeInvalidLabel},
		{"empty key", CreateInput{ID: 3, Owner: testOwner, Labels: map[string]string{"": "fix"}, Now: testNow}, CodeInvalidLabel},
		{"long key", CreateInput{ID: 3, Owner: testOwner, Labels: map[string]string{strings.Repeat("a", LabelKeyMax+1): "x"}, Now: testNow}, CodeInvalidLabel},
		{"long value", CreateInput{ID: 3, Owner: testOwner, Labels: map[string]string{"step": strings.Repeat("x", LabelValueMax+1)}, Now: testNow}, CodeInvalidLabel},
		{"control value", CreateInput{ID: 3, Owner: testOwner, Labels: map[string]string{"step": "a\nb"}, Now: testNow}, CodeInvalidLabel},
		{"zero need", CreateInput{ID: 3, Owner: testOwner, Needs: []uint64{0}, Now: testNow}, CodeInvalidNeeds},
		{"self need", CreateInput{ID: 3, Owner: testOwner, Needs: []uint64{3}, Now: testNow}, CodeInvalidNeeds},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Create(tc.in)
			wantCode(t, err, tc.code)
		})
	}

	tooMany := make([]uint64, 0, MaxNeeds+1)
	for i := uint64(2); i <= MaxNeeds+2; i++ {
		tooMany = append(tooMany, i)
	}
	_, err := Create(CreateInput{ID: 3, Owner: testOwner, Needs: tooMany, Now: testNow})
	wantCode(t, err, CodeInvalidNeeds)
}

func TestCreateCopiesInput(t *testing.T) {
	labels := map[string]string{"step": "fix"}
	needs := []uint64{2}
	task := mustCreate(t, CreateInput{ID: 1, Owner: testOwner, Labels: labels, Needs: needs, Now: testNow})
	labels["step"] = "review"
	needs[0] = 9
	if task.Labels["step"] != "fix" || task.Needs[0] != 2 {
		t.Fatalf("Create kept the caller's maps: %#v", task)
	}
}

func TestClaimMatrix(t *testing.T) {
	lease := time.Minute
	tokenHash := HashToken("token")
	valid := []struct {
		name string
		task Task
	}{
		{"ready", readyTask(t, 1, testNow)},
		{"assigned to me", func() Task {
			task := readyTask(t, 1, testNow)
			assigned, err := Assign(task, testAgent, testNow)
			mustNoError(t, err)
			return assigned
		}()},
	}
	for _, tc := range valid {
		t.Run(tc.name, func(t *testing.T) {
			next, err := ClaimTask(tc.task, testAgent, lease, testNow, tokenHash)
			mustNoError(t, err)
			if next.State != StateClaimed || next.Claim == nil {
				t.Fatalf("next = %#v", next)
			}
			if next.Claim.Actor != testAgent || next.Claim.Epoch != 1 || next.Claim.TokenHash != tokenHash {
				t.Fatalf("claim = %#v", next.Claim)
			}
			if !next.Claim.Until.Equal(testNow.Add(lease)) {
				t.Fatalf("until = %s", next.Claim.Until)
			}
			if next.Version != tc.task.Version+1 || !next.UpdatedAt.Equal(testNow) {
				t.Fatalf("version = %d, updatedAt = %s", next.Version, next.UpdatedAt)
			}
		})
	}

	assignedElsewhere := func() Task {
		task := readyTask(t, 1, testNow)
		assigned, err := Assign(task, ActorID(string(testOwner)+"+fixer"), testNow)
		mustNoError(t, err)
		return assigned
	}
	live := func() Task {
		task, _ := claimedTask(t, 1, testNow, lease)
		return task
	}
	lapsed := func() Task {
		task, _ := claimedTask(t, 1, testNow, LeaseMin)
		// Past the lease the raw state still says claimed.
		return task
	}
	invalid := []struct {
		name string
		task Task
		at   time.Time
		code Code
	}{
		{"waiting", waitingTask(t, 1, testNow, 2), testNow, CodeTaskWaiting},
		{"waiting assigned elsewhere", func() Task {
			task := waitingTask(t, 1, testNow, 2)
			assigned, err := Assign(task, ActorID(string(testOwner)+"+fixer"), testNow)
			mustNoError(t, err)
			return assigned
		}(), testNow, CodeTaskWaiting},
		{"blocked", blockedTask(t, 1, testNow), testNow, CodeTaskBlocked},
		{"completed", completedTask(t, 1, testNow), testNow, CodeTaskFinished},
		{"failed", failedTask(t, 1, testNow), testNow, CodeTaskFinished},
		{"canceled", canceledTask(t, 1, testNow), testNow, CodeTaskFinished},
		{"assigned elsewhere", assignedElsewhere(), testNow, CodeTaskAssigned},
		{"live claim", live(), testNow, CodeTaskClaimed},
		{"live claim assigned elsewhere", func() Task {
			assigned, err := Assign(live(), ActorID(string(testOwner)+"+fixer"), testNow)
			mustNoError(t, err)
			return assigned
		}(), testNow, CodeTaskAssigned},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			before := cloneTask(tc.task)
			_, err := ClaimTask(tc.task, testAgent, lease, tc.at, tokenHash)
			wantCode(t, err, tc.code)
			assertTaskEqual(t, tc.task, before)
		})
	}

	next, err := ClaimTask(lapsed(), testAgent, lease, testNow.Add(2*time.Minute), tokenHash)
	mustNoError(t, err)
	if next.Claim.Epoch != 2 {
		t.Fatalf("epoch after taking over a lapsed claim = %d, want 2", next.Claim.Epoch)
	}
	if next.Epoch() != 2 {
		t.Fatalf("task epoch = %d", next.Epoch())
	}

	_, err = ClaimTask(readyTask(t, 1, testNow), "not-an-actor", lease, testNow, tokenHash)
	wantCode(t, err, CodeInvalidActor)
	_, err = ClaimTask(readyTask(t, 1, testNow), testAgent, time.Second, testNow, tokenHash)
	wantCode(t, err, CodeInvalidLease)
	_, err = ClaimTask(readyTask(t, 1, testNow), testAgent, lease, testNow, "short")
	wantCode(t, err, CodeInvalidToken)
}

func TestClaimLeaseBoundsAndDefault(t *testing.T) {
	base := readyTask(t, 1, testNow)
	def, err := ClaimTask(base, testAgent, 0, testNow, HashToken("t"))
	mustNoError(t, err)
	if got := def.Claim.Until.Sub(testNow); got != LeaseDefault {
		t.Fatalf("default lease = %s, want %s", got, LeaseDefault)
	}
	for _, lease := range []time.Duration{LeaseMin, LeaseMax} {
		task := readyTask(t, 2, testNow)
		next, err := ClaimTask(task, testAgent, lease, testNow, HashToken("t"))
		mustNoError(t, err)
		if got := next.Claim.Until.Sub(testNow); got != lease {
			t.Fatalf("lease = %s, want %s", got, lease)
		}
	}
	for _, lease := range []time.Duration{LeaseMin - time.Nanosecond, LeaseMax + time.Nanosecond} {
		task := readyTask(t, 3, testNow)
		_, err := ClaimTask(task, testAgent, lease, testNow, HashToken("t"))
		wantCode(t, err, CodeInvalidLease)
	}
}

func TestClaimRaceOnOneVersion(t *testing.T) {
	base := readyTask(t, 1, testNow)
	first, err := ClaimTask(base, testAgent, time.Minute, testNow, HashToken("first"))
	mustNoError(t, err)
	second, err := ClaimTask(base, testOther, time.Minute, testNow, HashToken("second"))
	mustNoError(t, err)
	if first.Version != second.Version || first.Epoch() != second.Epoch() {
		t.Fatalf("both writers left the same version: %d/%d", first.Version, second.Version)
	}

	current, err := Commit(base, base, first)
	mustNoError(t, err)
	if !reflect.DeepEqual(current, first) {
		t.Fatalf("first commit = %#v", current)
	}
	if _, err := Commit(current, base, second); CodeOf(err) != CodeVersionConflict {
		t.Fatalf("second commit = %v, want version_conflict", err)
	}
	// The task a caller still sees is the winner's.
	if current.Claim.Actor != testAgent {
		t.Fatalf("winner = %s", current.Claim.Actor)
	}

	// A sequential claim from the winner's version sees a live claim.
	_, err = ClaimTask(current, testOther, time.Minute, testNow, HashToken("third"))
	wantCode(t, err, CodeTaskClaimed)
}

func TestClaimFencing(t *testing.T) {
	at := testNow
	task, proof := claimedTask(t, 1, at, time.Minute)
	tokenHash := task.Claim.TokenHash

	if err := VerifyClaimAt(task, proof.Epoch, "wrong", at); CodeOf(err) != CodeClaimLost {
		t.Fatalf("wrong token = %v", err)
	}
	if err := VerifyClaim(task, proof.Epoch, proof.Token); err != nil {
		t.Fatalf("VerifyClaim on a live claim: %v", err)
	}
	if err := VerifyClaim(task, proof.Epoch+1, proof.Token); CodeOf(err) != CodeClaimLost {
		t.Fatalf("wrong epoch = %v", err)
	}

	// A wrong token changes nothing.
	before := cloneTask(task)
	_, err := Complete(task, ClaimProof{Epoch: proof.Epoch, Token: "wrong"}, "done", at)
	wantCode(t, err, CodeClaimLost)
	assertTaskEqual(t, task, before)
	if strings.Contains(err.Error(), tokenHash) {
		t.Fatal("claim_lost leaked the token hash")
	}

	released, err := Release(task, proof, at)
	mustNoError(t, err)
	if released.State != StateReady || released.Claim != nil {
		t.Fatalf("released = %#v", released)
	}
	for name, call := range map[string]func() error{
		"renew": func() error {
			_, err := Renew(released, proof, time.Minute, at)
			return err
		},
		"release": func() error {
			_, err := Release(released, proof, at)
			return err
		},
		"complete": func() error {
			_, err := Complete(released, proof, "done", at)
			return err
		},
		"fail": func() error {
			_, err := Fail(released, proof, "nope", at)
			return err
		},
	} {
		if got := CodeOf(call()); got != CodeClaimLost {
			t.Fatalf("%s on a released claim = %s, want claim_lost", name, got)
		}
	}

	second, secondProof := claimTask(t, released, at, time.Minute)
	if second.Claim.Epoch != proof.Epoch+1 {
		t.Fatalf("second epoch = %d, want %d", second.Claim.Epoch, proof.Epoch+1)
	}
	if _, err := Complete(second, proof, "too late", at); CodeOf(err) != CodeClaimLost {
		t.Fatalf("old claim completed after takeover: %v", err)
	}
	if second.State != StateClaimed {
		t.Fatalf("lost write moved the task to %s", second.State)
	}

	// A claim that lapses is over even though nothing rewrote it.
	if _, err := Renew(second, secondProof, time.Minute, at.Add(2*time.Minute)); CodeOf(err) != CodeClaimLost {
		t.Fatalf("renew after the lease = %v", err)
	}
	if err := VerifyClaimAt(second, secondProof.Epoch, secondProof.Token, at.Add(2*time.Minute)); CodeOf(err) != CodeClaimLost {
		t.Fatalf("verify after the lease = %v", err)
	}
	if err := VerifyClaim(second, secondProof.Epoch, secondProof.Token); err != nil {
		t.Fatalf("VerifyClaim without the clock: %v", err)
	}
}

func TestRenewAndSettle(t *testing.T) {
	task, proof := claimedTask(t, 1, testNow, time.Minute)
	renewed, err := Renew(task, proof, 2*time.Minute, testNow.Add(30*time.Second))
	mustNoError(t, err)
	if got := renewed.Claim.Until.Sub(testNow.Add(30 * time.Second)); got != 2*time.Minute {
		t.Fatalf("renewed until = %s", renewed.Claim.Until)
	}
	if renewed.Claim.Epoch != proof.Epoch || renewed.Claim.TokenHash != task.Claim.TokenHash {
		t.Fatalf("renew changed the claim identity: %#v", renewed.Claim)
	}

	completed, err := Complete(renewed, proof, "fixed in a1b2c3", testNow.Add(time.Minute))
	mustNoError(t, err)
	if completed.State != StateCompleted || completed.Claim != nil || completed.Result != "fixed in a1b2c3" {
		t.Fatalf("completed = %#v", completed)
	}

	failed, err := Fail(task, proof, "exited 1", testNow)
	mustNoError(t, err)
	if failed.State != StateFailed || failed.Claim != nil || failed.Reason != "exited 1" {
		t.Fatalf("failed = %#v", failed)
	}

	tooLong := strings.Repeat("x", MaxNote+1)
	_, err = Complete(task, proof, tooLong, testNow)
	wantCode(t, err, CodeInvalidNote)
	_, err = Fail(task, proof, "   ", testNow)
	wantCode(t, err, CodeInvalidNote)
	_, err = Fail(task, proof, "", testNow)
	wantCode(t, err, CodeInvalidNote)
}

func TestReopen(t *testing.T) {
	failed := failedTask(t, 1, testNow)
	again, err := Reopen(failed, testNow)
	mustNoError(t, err)
	if again.State != StateReady || again.Reason != "" {
		t.Fatalf("reopened = %#v", again)
	}
	if again.Version != failed.Version+1 {
		t.Fatalf("version = %d", again.Version)
	}
	noop, err := Reopen(again, testNow)
	mustNoError(t, err)
	if noop.Version != again.Version {
		t.Fatalf("reopening a ready task changed it")
	}
	for _, task := range []Task{completedTask(t, 2, testNow), canceledTask(t, 3, testNow)} {
		_, err := Reopen(task, testNow)
		wantCode(t, err, CodeTaskFinished)
	}
}

func TestCancel(t *testing.T) {
	for _, task := range []Task{
		readyTask(t, 1, testNow),
		failedTask(t, 2, testNow),
		blockedTask(t, 3, testNow),
	} {
		canceled, err := Cancel(task, "superseded", testNow)
		mustNoError(t, err)
		if canceled.State != StateCanceled || canceled.Reason != "superseded" || canceled.Claim != nil || canceled.Blocked != nil {
			t.Fatalf("canceled = %#v", canceled)
		}
	}
	_, err := Cancel(canceledTask(t, 4, testNow), "again", testNow)
	wantCode(t, err, CodeTaskFinished)
	_, err = Cancel(completedTask(t, 5, testNow), "again", testNow)
	wantCode(t, err, CodeTaskFinished)
	_, err = Cancel(readyTask(t, 6, testNow), "", testNow)
	wantCode(t, err, CodeInvalidNote)
}

func TestAssign(t *testing.T) {
	fixer, err := ForActor(testOwner, "fixer")
	mustNoError(t, err)
	task := readyTask(t, 1, testNow)
	// A claim stays when the task is assigned.
	claimed, proof := claimTask(t, task, testNow, time.Minute)
	assigned, err := Assign(claimed, fixer, testNow)
	mustNoError(t, err)
	if assigned.For != fixer || assigned.Version != claimed.Version+1 {
		t.Fatalf("assigned = %#v", assigned)
	}
	if assigned.Claim == nil || assigned.Claim.Epoch != proof.Epoch {
		t.Fatalf("assign ended the claim: %#v", assigned.Claim)
	}
	moved, err := Assign(assigned, testAgent, testNow)
	mustNoError(t, err)
	if moved.For != testAgent || moved.Claim == nil || moved.Claim.Epoch != proof.Epoch {
		t.Fatalf("moved = %#v", moved)
	}
	same, err := Assign(moved, testAgent, testNow)
	mustNoError(t, err)
	if same.Version != moved.Version {
		t.Fatal("assigning the same actor changed the version")
	}
	pool, err := Assign(moved, "", testNow)
	mustNoError(t, err)
	if pool.For != "" {
		t.Fatalf("pool = %#v", pool)
	}

	for _, bad := range []ActorID{"local:user:alice+fixer", testOwner, "fixer"} {
		_, err := Assign(task, bad, testNow)
		if CodeOf(err) != CodeInvalidProfile {
			t.Fatalf("assign %q = %v, want invalid_profile", bad, err)
		}
	}
	_, err = Assign(completedTask(t, 2, testNow), fixer, testNow)
	wantCode(t, err, CodeTaskFinished)
}

func TestBlockAndUnblock(t *testing.T) {
	task, proof := claimedTask(t, 1, testNow, time.Minute)
	blocked, err := Block(task, &proof, "awaiting_approval", "approve the deploy?", testNow)
	mustNoError(t, err)
	if blocked.State != StateBlocked || blocked.Claim != nil || blocked.Blocked == nil {
		t.Fatalf("blocked = %#v", blocked)
	}
	if blocked.Blocked.Kind != "awaiting_approval" || blocked.Blocked.Reason != "approve the deploy?" {
		t.Fatalf("hold = %#v", blocked.Blocked)
	}
	// The claim that placed the hold is over.
	if _, err := Renew(blocked, proof, time.Minute, testNow); CodeOf(err) != CodeClaimLost {
		t.Fatalf("renew after block = %v", err)
	}
	unblocked, err := Unblock(blocked, nil, testNow)
	mustNoError(t, err)
	if unblocked.State != StateReady || unblocked.Blocked != nil {
		t.Fatalf("unblocked = %#v", unblocked)
	}

	prereqID := uint64(9)
	waiting := waitingTask(t, 2, testNow, prereqID)
	held, err := Block(waiting, nil, "needs_input", "which env?", testNow)
	mustNoError(t, err)
	stillWaiting, err := Unblock(held, map[uint64]Task{prereqID: waitingTask(t, prereqID, testNow)}, testNow)
	mustNoError(t, err)
	if stillWaiting.State != StateWaiting {
		t.Fatalf("unblocked before the prerequisite: %s", stillWaiting.State)
	}
	ready, err := Unblock(held, map[uint64]Task{prereqID: completedTask(t, prereqID, testNow)}, testNow)
	mustNoError(t, err)
	if ready.State != StateReady {
		t.Fatalf("unblocked after the prerequisite: %s", ready.State)
	}

	live, _ := claimedTask(t, 3, testNow, time.Minute)
	_, err = Block(live, nil, "", "held by its owner", testNow)
	wantCode(t, err, CodeTaskClaimed)
	_, err = Block(live, &ClaimProof{Epoch: 99, Token: "wrong"}, "", "x", testNow)
	wantCode(t, err, CodeClaimLost)
	_, err = Block(blocked, nil, "", "again", testNow)
	wantCode(t, err, CodeTaskBlocked)
	_, err = Block(completedTask(t, 4, testNow), nil, "", "x", testNow)
	wantCode(t, err, CodeTaskFinished)
	_, err = Block(readyTask(t, 5, testNow), nil, EditedHoldKind, "x", testNow)
	wantCode(t, err, CodeInvalidHoldKind)
	_, err = Block(readyTask(t, 6, testNow), nil, "Awaiting", "x", testNow)
	wantCode(t, err, CodeInvalidHoldKind)
	_, err = Block(readyTask(t, 7, testNow), nil, "", "", testNow)
	wantCode(t, err, CodeInvalidNote)

	// A lapsed claim is no one's, so its owner holds the task without a proof.
	lapsed, _ := claimedTask(t, 8, testNow, LeaseMin)
	heldLapsed, err := Block(lapsed, nil, "", "stale lease", testNow.Add(2*time.Minute))
	mustNoError(t, err)
	if heldLapsed.State != StateBlocked || heldLapsed.Claim != nil {
		t.Fatalf("held lapsed = %#v", heldLapsed)
	}
}

func TestSetNeeds(t *testing.T) {
	completed := completedTask(t, 2, testNow)
	failed := failedTask(t, 3, testNow)
	canceled := canceledTask(t, 4, testNow)
	waiting := waitingTask(t, 5, testNow, 99)
	tasks := map[uint64]Task{
		2: completed,
		3: failed,
		4: canceled,
		5: waiting,
	}
	target := readyTask(t, 1, testNow)

	// A completed prerequisite readies it at once.
	ready, err := SetNeeds(target, []uint64{2}, tasks, testNow)
	mustNoError(t, err)
	if ready.State != StateReady || len(ready.Needs) != 1 || ready.Needs[0] != 2 {
		t.Fatalf("ready = %#v", ready)
	}
	// A failed one never readies it; it may yet be reopened.
	waitingForFailed, err := SetNeeds(target, []uint64{3}, tasks, testNow)
	mustNoError(t, err)
	if waitingForFailed.State != StateWaiting {
		t.Fatalf("failed prerequisite readied the task: %s", waitingForFailed.State)
	}
	if !NeedsSatisfied(tasks, []uint64{2}) || NeedsSatisfied(tasks, []uint64{3}) ||
		NeedsSatisfied(tasks, []uint64{4}) || NeedsSatisfied(tasks, []uint64{99}) {
		t.Fatal("NeedsSatisfied disagrees about completed, failed, canceled, or missing")
	}

	invalid := []struct {
		name  string
		needs []uint64
	}{
		{"canceled", []uint64{4}},
		{"missing", []uint64{99}},
		{"self", []uint64{1}},
		{"zero", []uint64{0}},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			before := cloneTask(target)
			_, err := SetNeeds(target, tc.needs, tasks, testNow)
			wantCode(t, err, CodeInvalidNeeds)
			assertTaskEqual(t, target, before)
		})
	}

	tooMany := make([]uint64, 0, MaxNeeds+1)
	for i := uint64(100); i < 100+MaxNeeds+1; i++ {
		tooMany = append(tooMany, i)
	}
	_, err = SetNeeds(target, tooMany, tasks, testNow)
	wantCode(t, err, CodeInvalidNeeds)

	// Duplicates collapse; the same set is a no-op.
	dupes, err := SetNeeds(target, []uint64{2, 2}, tasks, testNow)
	mustNoError(t, err)
	if len(dupes.Needs) != 1 {
		t.Fatalf("duplicates = %#v", dupes.Needs)
	}
	same, err := SetNeeds(dupes, []uint64{2}, tasks, testNow)
	mustNoError(t, err)
	if same.Version != dupes.Version {
		t.Fatal("the same set changed the version")
	}

	// With no prerequisites it is ready.
	none, err := SetNeeds(waitingForFailed, nil, tasks, testNow)
	mustNoError(t, err)
	if none.State != StateReady || len(none.Needs) != 0 {
		t.Fatalf("none = %#v", none)
	}

	// A live claim blocks the change; a lapsed one is expired first.
	claimed, proof := claimTask(t, target, testNow, time.Minute)
	_, err = SetNeeds(claimed, []uint64{2}, tasks, testNow)
	wantCode(t, err, CodeTaskClaimed)
	lapsed, err := SetNeeds(claimed, []uint64{2}, tasks, testNow.Add(2*time.Minute))
	mustNoError(t, err)
	if lapsed.Claim != nil || lapsed.State != StateReady || lapsed.Epoch() != proof.Epoch {
		t.Fatalf("lapsed = %#v", lapsed)
	}

	// A held task stays held; what it needs says where it stands when lifted.
	held := blockedTask(t, 6, testNow)
	heldChanged, err := SetNeeds(held, []uint64{2}, tasks, testNow)
	mustNoError(t, err)
	if heldChanged.State != StateBlocked || heldChanged.Blocked == nil {
		t.Fatalf("held changed = %#v", heldChanged)
	}
	unblocked, err := Unblock(heldChanged, tasks, testNow)
	mustNoError(t, err)
	if unblocked.State != StateReady {
		t.Fatalf("unblocked = %s", unblocked.State)
	}

	for _, finished := range []Task{completed, failed, canceled} {
		_, err := SetNeeds(finished, []uint64{2}, tasks, testNow)
		wantCode(t, err, CodeTaskFinished)
	}
}

func TestSetNeedsCycles(t *testing.T) {
	target := readyTask(t, 1, testNow)
	second := waitingTask(t, 2, testNow, 1) // 2 waits for 1
	tasks := map[uint64]Task{1: target, 2: second}
	_, err := SetNeeds(target, []uint64{2}, tasks, testNow)
	wantCode(t, err, CodeInvalidNeeds)

	third := waitingTask(t, 3, testNow, 2)
	tasks[3] = third
	_, err = SetNeeds(target, []uint64{3}, tasks, testNow)
	wantCode(t, err, CodeInvalidNeeds)

	// Once the task between them waits for nothing, the same change is fine.
	secondFree, err := SetNeeds(second, nil, tasks, testNow)
	mustNoError(t, err)
	tasks[2] = secondFree
	moved, err := SetNeeds(target, []uint64{3}, tasks, testNow)
	mustNoError(t, err)
	if moved.State != StateWaiting {
		t.Fatalf("moved = %s", moved.State)
	}

	// A walk longer than MaxNeedsWalk is refused as uncheckable.
	chain := make(map[uint64]Task, MaxNeedsWalk+3)
	for i := uint64(100); i < 100+MaxNeedsWalk+2; i++ {
		chain[i] = waitingTask(t, i, testNow, i+1)
	}
	chain[uint64(100)+MaxNeedsWalk+2] = readyTask(t, 100+MaxNeedsWalk+2, testNow)
	target = readyTask(t, 9, testNow)
	chain[9] = target
	_, err = SetNeeds(target, []uint64{100}, chain, testNow)
	wantCode(t, err, CodeInvalidNeeds)

	// A completed prerequisite's needs are not walked: completed work cannot
	// lead back to a task that has not completed.
	done := completedTask(t, 20, testNow)
	done.Needs = []uint64{9} // a corrupt invariant, deliberately
	chain = map[uint64]Task{9: target, 20: done}
	_, err = SetNeeds(target, []uint64{20}, chain, testNow)
	mustNoError(t, err)
}

func TestSetNeedsPreservesEpochFloor(t *testing.T) {
	task, proof := claimedTask(t, 1, testNow, time.Minute)
	released, err := Release(task, proof, testNow)
	mustNoError(t, err)
	next, proof2 := claimTask(t, released, testNow, time.Minute)
	if next.Claim.Epoch != 2 {
		t.Fatalf("epoch after release = %d, want 2", next.Claim.Epoch)
	}
	if proof2.Epoch != 2 {
		t.Fatalf("proof epoch = %d", proof2.Epoch)
	}
}

func TestLookup(t *testing.T) {
	tasks := map[uint64]Task{1: readyTask(t, 1, testNow)}
	found, err := Lookup(tasks, 1)
	mustNoError(t, err)
	if found.ID != 1 {
		t.Fatalf("found = %#v", found)
	}
	found.Labels = map[string]string{"step": "mutated"}
	if tasks[1].Labels != nil {
		t.Fatal("Lookup returned an aliased task")
	}
	_, err = Lookup(tasks, 2)
	wantCode(t, err, CodeNotATask)
	_, err = ClaimTask(Task{}, testAgent, time.Minute, testNow, HashToken("t"))
	wantCode(t, err, CodeNotATask)
}

func TestCommit(t *testing.T) {
	base := readyTask(t, 1, testNow)
	next, err := ClaimTask(base, testAgent, time.Minute, testNow, HashToken("t"))
	mustNoError(t, err)
	current, err := Commit(base, base, next)
	mustNoError(t, err)
	if !reflect.DeepEqual(current, next) {
		t.Fatalf("commit = %#v", current)
	}
	// Committing the same no-op version is allowed.
	same, err := Commit(current, current, cloneTask(current))
	mustNoError(t, err)
	if same.Version != current.Version {
		t.Fatalf("same-version commit changed the version")
	}
	// Going back a version is stale, even from the right base.
	if _, err := Commit(current, current, base); CodeOf(err) != CodeVersionConflict {
		t.Fatalf("backwards commit = %v", err)
	}
	// A different task cannot be committed over this one.
	other := readyTask(t, 2, testNow)
	if _, err := Commit(current, current, other); CodeOf(err) != CodeVersionConflict {
		t.Fatalf("other task commit = %v", err)
	}
	if _, err := Commit(base, current, next); CodeOf(err) != CodeVersionConflict {
		t.Fatalf("stale base commit = %v", err)
	}
}

func TestCreateThirtyThreeDistinctNeeds(t *testing.T) {
	needs := make([]uint64, 0, MaxNeeds+1)
	for i := uint64(1); i <= MaxNeeds+1; i++ {
		needs = append(needs, i)
	}
	_, err := Create(CreateInput{ID: 100, Owner: testOwner, Needs: needs, Now: testNow})
	wantCode(t, err, CodeInvalidNeeds)
	if len(needs) != MaxNeeds+1 {
		t.Fatal("test setup")
	}
	// Exactly MaxNeeds distinct prerequisites is allowed.
	if _, err := Create(CreateInput{ID: 101, Owner: testOwner, Needs: needs[:MaxNeeds], Now: testNow}); err != nil {
		t.Fatalf("%d needs: %v", MaxNeeds, err)
	}
}

func TestInvalidStateError(t *testing.T) {
	task := Task{ID: 1, Channel: "work", Owner: testOwner, State: TaskState("bogus")}
	_, err := ClaimTask(task, testAgent, time.Minute, testNow, HashToken("t"))
	wantCode(t, err, CodeInvalidState)
	if task.State.Valid() {
		t.Fatal("bogus state is valid")
	}
}

func TestForActor(t *testing.T) {
	got, err := ForActor(testOwner, "fix-er")
	mustNoError(t, err)
	if got != ActorID("local:user:julian+fix-er") {
		t.Fatalf("ForActor = %q", got)
	}
	if _, err := ForActor("alice", "fixer"); CodeOf(err) != CodeInvalidActor {
		t.Fatalf("bad owner = %v", err)
	}
	for _, profile := range []string{"", "Fixer", "-fixer", "fix er", strings.Repeat("a", 33)} {
		if _, err := ForActor(testOwner, profile); CodeOf(err) != CodeInvalidProfile {
			t.Fatalf("profile %q = %v", profile, err)
		}
	}
}
