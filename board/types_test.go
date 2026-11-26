package board

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

var (
	testOwner = ActorID("local:user:julian")
	testAgent = ActorID("local:user:julian+claude")
	testOther = ActorID("local:user:alice")
)

var testNow = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func mustCreate(t *testing.T, in CreateInput) Task {
	t.Helper()
	task, err := Create(in)
	if err != nil {
		t.Fatalf("Create(%d): %v", in.ID, err)
	}
	return task
}

func readyTask(t *testing.T, id uint64, at time.Time) Task {
	t.Helper()
	return mustCreate(t, CreateInput{ID: id, Channel: "work", Owner: testOwner, Now: at})
}

func waitingTask(t *testing.T, id uint64, at time.Time, needs ...uint64) Task {
	t.Helper()
	return mustCreate(t, CreateInput{ID: id, Channel: "work", Owner: testOwner, Needs: needs, Now: at})
}

func claimTask(t *testing.T, task Task, at time.Time, lease time.Duration) (Task, ClaimProof) {
	t.Helper()
	token := fmt.Sprintf("token-for-%d", task.ID)
	next, err := ClaimTask(task, testAgent, lease, at, HashToken(token))
	if err != nil {
		t.Fatalf("ClaimTask(%d): %v", task.ID, err)
	}
	return next, ClaimProof{Epoch: next.Claim.Epoch, Token: token}
}

func claimedTask(t *testing.T, id uint64, at time.Time, lease time.Duration) (Task, ClaimProof) {
	t.Helper()
	return claimTask(t, readyTask(t, id, at), at, lease)
}

func completedTask(t *testing.T, id uint64, at time.Time) Task {
	t.Helper()
	task, proof := claimedTask(t, id, at, time.Minute)
	done, err := Complete(task, proof, "done", at)
	if err != nil {
		t.Fatalf("Complete(%d): %v", id, err)
	}
	return done
}

func failedTask(t *testing.T, id uint64, at time.Time) Task {
	t.Helper()
	task, proof := claimedTask(t, id, at, time.Minute)
	failed, err := Fail(task, proof, "could not", at)
	if err != nil {
		t.Fatalf("Fail(%d): %v", id, err)
	}
	return failed
}

func canceledTask(t *testing.T, id uint64, at time.Time) Task {
	t.Helper()
	canceled, err := Cancel(readyTask(t, id, at), "not wanted", at)
	if err != nil {
		t.Fatalf("Cancel(%d): %v", id, err)
	}
	return canceled
}

func blockedTask(t *testing.T, id uint64, at time.Time) Task {
	t.Helper()
	blocked, err := Block(readyTask(t, id, at), nil, "needs_input", "approve?", at)
	if err != nil {
		t.Fatalf("Block(%d): %v", id, err)
	}
	return blocked
}

func mustNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func wantCode(t *testing.T, err error, want Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("want %s, got nil", want)
	}
	if got := CodeOf(err); got != want {
		t.Fatalf("want %s, got %s (%v)", want, got, err)
	}
}

func TestStateNow(t *testing.T) {
	at := testNow
	cases := []struct {
		name string
		task Task
		want TaskState
	}{
		{"waiting", Task{ID: 1, State: StateWaiting}, StateWaiting},
		{"ready", Task{ID: 1, State: StateReady}, StateReady},
		{"blocked", Task{ID: 1, State: StateBlocked}, StateBlocked},
		{"completed", Task{ID: 1, State: StateCompleted}, StateCompleted},
		{"failed", Task{ID: 1, State: StateFailed}, StateFailed},
		{"canceled", Task{ID: 1, State: StateCanceled}, StateCanceled},
		{"claimed live", Task{ID: 1, State: StateClaimed, Claim: &Claim{Until: at.Add(time.Second)}}, StateClaimed},
		{"claimed at until", Task{ID: 1, State: StateClaimed, Claim: &Claim{Until: at}}, StateReady},
		{"claimed lapsed", Task{ID: 1, State: StateClaimed, Claim: &Claim{Until: at.Add(-time.Second)}}, StateReady},
		{"claimed without a claim", Task{ID: 1, State: StateClaimed}, StateReady},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.task.StateNow(at); got != tc.want {
				t.Fatalf("StateNow = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestEffectiveLabels(t *testing.T) {
	task := mustCreate(t, CreateInput{
		ID: 1, Channel: "work", Owner: testOwner, Now: testNow,
		Labels: map[string]string{"step": "fix"},
	})
	labels := task.EffectiveLabels(testNow)
	if labels["step"] != "fix" || labels[StateLabel] != string(StateReady) {
		t.Fatalf("labels = %#v", labels)
	}
	labels["step"] = "changed"
	if task.Labels["step"] != "fix" {
		t.Fatalf("EffectiveLabels aliased the task's labels")
	}

	task, proof := claimTask(t, task, testNow, time.Minute)
	if got := task.EffectiveLabels(testNow)[StateLabel]; got != string(StateClaimed) {
		t.Fatalf("claimed label = %q", got)
	}
	later := testNow.Add(2 * time.Minute)
	if got := task.StateNow(later); got != StateReady {
		t.Fatalf("lapsed state = %s", got)
	}
	if got := task.EffectiveLabels(later)[StateLabel]; got != string(StateReady) {
		t.Fatalf("lapsed label = %q", got)
	}
	if err := VerifyClaimAt(task, proof.Epoch, proof.Token, later); CodeOf(err) != CodeClaimLost {
		t.Fatalf("lapsed claim verified: %v", err)
	}
}

func TestViewDoesNotLeakTokenHash(t *testing.T) {
	task, _ := claimedTask(t, 7, testNow, time.Minute)
	tokenHash := task.Claim.TokenHash
	if tokenHash == "" {
		t.Fatal("claim has no token hash")
	}
	view := task.View(testNow)
	if view.Claim == nil || view.Claim.Epoch != 1 {
		t.Fatalf("view claim = %#v", view.Claim)
	}
	raw, err := json.Marshal(view)
	mustNoError(t, err)
	if strings.Contains(string(raw), tokenHash) {
		t.Fatalf("view leaked the token hash: %s", raw)
	}
	if strings.Contains(string(raw), "token") || strings.Contains(string(raw), "Token") {
		t.Fatalf("view mentions a token: %s", raw)
	}
	if strings.Contains(fmt.Sprintf("%+v", view), tokenHash) {
		t.Fatal("formatted view leaked the token hash")
	}
	// The task itself keeps the hash: fencing needs it.
	if task.Claim.TokenHash != tokenHash {
		t.Fatal("claim lost its token hash")
	}
}

func TestViewWithoutClaimOrBlocked(t *testing.T) {
	plain := readyTask(t, 1, testNow)
	if view := plain.View(testNow); view.Claim != nil || view.Blocked != nil {
		t.Fatalf("plain view = %#v", view)
	}
	held := blockedTask(t, 2, testNow)
	view := held.View(testNow)
	if view.Claim != nil || view.Blocked == nil || view.Blocked.Reason != "approve?" {
		t.Fatalf("held view = %#v", view)
	}
	view.Blocked.Reason = "changed"
	if held.Blocked.Reason != "approve?" {
		t.Fatal("View aliased the hold")
	}
	view.Needs = append(view.Needs, 9)
	if len(held.Needs) != 0 {
		t.Fatal("View aliased needs")
	}
}

func TestTokenHelpers(t *testing.T) {
	token, err := NewToken()
	mustNoError(t, err)
	if len(token) != 43 {
		t.Fatalf("token length = %d, want 43", len(token))
	}
	for _, r := range token {
		if !(r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' || r == '_') {
			t.Fatalf("token has %q, not base64url", r)
		}
	}
	hash := HashToken(token)
	if !ValidTokenHash(hash) || len(hash) != 64 {
		t.Fatalf("hash = %q", hash)
	}
	if HashToken(token) != hash {
		t.Fatal("HashToken is not deterministic")
	}
	if ValidTokenHash("not-a-hash") {
		t.Fatal("short token hash accepted")
	}
}

func TestErrorCodes(t *testing.T) {
	err := taskErrorf(CodeReservedLabel, "board.task")
	if CodeOf(err) != CodeReservedLabel {
		t.Fatalf("CodeOf = %q", CodeOf(err))
	}
	if CodeOf(fmt.Errorf("wrapped: %w", err)) != CodeReservedLabel {
		t.Fatal("CodeOf does not unwrap")
	}
	if CodeOf(nil) != "" {
		t.Fatal("CodeOf(nil) != empty")
	}
	if got := (&Error{Code: CodeClaimLost}).Error(); got != string(CodeClaimLost) {
		t.Fatalf("bare error = %q", got)
	}
	if !errors.Is(err, &Error{Code: CodeReservedLabel}) {
		t.Fatal("errors.Is did not match the code")
	}
	if errors.Is(err, &Error{Code: CodeClaimLost}) {
		t.Fatal("errors.Is matched another code")
	}
}
