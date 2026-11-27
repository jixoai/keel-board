package protocol_test

import (
	"strings"
	"testing"
	"time"

	"github.com/jixoai/keel-board/protocol"
)

func TestValidateChannel(t *testing.T) {
	valid := []string{"errors-www", "www/production", "a", "work/2026_q1", "x1/y2/z3"}
	for _, path := range valid {
		if err := protocol.ValidateChannel(path); err != nil {
			t.Errorf("%q should be a channel: %v", path, err)
		}
	}
	invalid := []string{"", "Errors", "www//production", "/www", "www/", "www production", "www/é"}
	for _, path := range invalid {
		if err := protocol.ValidateChannel(path); err == nil {
			t.Errorf("%q should be refused", path)
		}
	}
	if err := protocol.ValidateChannel(strings.Repeat("a", protocol.ChannelPathMax+1)); err == nil {
		t.Error("a too-long channel should be refused")
	}
	for _, value := range []string{"work", "work/", "a/b/"} {
		if err := protocol.ValidateChannelOrSubtree(value); err != nil {
			t.Errorf("%q should name a channel or subtree: %v", value, err)
		}
	}
	if err := protocol.ValidateChannelOrSubtree("Work/"); err == nil {
		t.Error("an uppercase subtree should be refused")
	}
}

func TestValidateLabels(t *testing.T) {
	set := map[string]string{"status": "open", "step": "fix-1", "a.b_c-d": "x"}
	if err := protocol.ValidateNewLabels(set); err != nil {
		t.Fatalf("valid labels refused: %v", err)
	}
	cases := []struct {
		labels map[string]string
		code   protocol.Code
	}{
		{map[string]string{"board.task": "ready"}, protocol.CodeReservedLabel},
		{map[string]string{"Verdict": "x"}, protocol.CodeInvalidRequest},
		{map[string]string{"-bad": "x"}, protocol.CodeInvalidRequest},
		{map[string]string{"ok": "line\nbreak"}, protocol.CodeInvalidRequest},
		{map[string]string{"ok": strings.Repeat("x", protocol.LabelValueMax+1)}, protocol.CodeInvalidRequest},
	}
	for _, test := range cases {
		err := protocol.ValidateNewLabels(test.labels)
		if err == nil {
			t.Fatalf("%v should be refused", test.labels)
		}
		if err.Code != test.code {
			t.Errorf("%v: code %s, want %s", test.labels, err.Code, test.code)
		}
	}
}

func TestValidateText(t *testing.T) {
	if err := protocol.ValidateText("a message"); err != nil {
		t.Fatal(err)
	}
	if err := protocol.ValidateText(" \n\t "); err == nil {
		t.Fatal("an empty text should be refused")
	}
	if err := protocol.ValidateText(strings.Repeat("x", protocol.MessageTextMax+1)); err == nil {
		t.Fatal("a too-long text should be refused")
	}
	if err := protocol.ValidateText("nul\x00"); err == nil {
		t.Fatal("a NUL should be refused")
	}
}

func TestValidateLease(t *testing.T) {
	if lease, err := protocol.ValidateLease(nil); err != nil || lease != protocol.LeaseDefaultSecs {
		t.Fatalf("default lease: %d, %v", lease, err)
	}
	thirty := float64(30)
	if lease, err := protocol.ValidateLease(&thirty); err != nil || lease != 30 {
		t.Fatalf("30s lease: %d, %v", lease, err)
	}
	short, long := float64(29), float64(3601)
	if _, err := protocol.ValidateLease(&short); err == nil {
		t.Fatal("29s should be refused")
	}
	if _, err := protocol.ValidateLease(&long); err == nil {
		t.Fatal("3601s should be refused")
	}
}

func TestValidateNeeds(t *testing.T) {
	if err := protocol.ValidateNeedsShape([]string{"1", "2"}, nil); err != nil {
		t.Fatal(err)
	}
	if err := protocol.ValidateNeedsShape([]string{"0"}, nil); err == nil {
		t.Fatal("#0 isn't a task")
	}
	if err := protocol.ValidateNeedsShape([]string{"nope"}, nil); err == nil {
		t.Fatal("a non-decimal ref isn't a task")
	}
	self := uint64(1)
	if err := protocol.ValidateNeedsShape([]string{"1"}, &self); err == nil {
		t.Fatal("a task can't wait for itself")
	}
	many := make([]string, protocol.TaskNeedsMax+1)
	for i := range many {
		many[i] = "1"
	}
	if err := protocol.ValidateNeedsShape(many, nil); err == nil {
		t.Fatal("too many needs should be refused")
	}
}

func TestResolveAssignee(t *testing.T) {
	owner := "local:user:alice"
	good := owner + "+worker"
	assigned, err := protocol.ResolveAssignee(owner, &good)
	if err != nil || assigned == nil || *assigned != good {
		t.Fatalf("resolving %q: %v, %v", good, assigned, err)
	}
	pool, err := protocol.ResolveAssignee(owner, nil)
	if err != nil || pool != nil {
		t.Fatalf("nil for is the pool: %v, %v", pool, err)
	}
	for _, bad := range []string{"local:user:bob+worker", owner + "+Worker", owner + "+", "worker"} {
		value := bad
		if _, err := protocol.ResolveAssignee(owner, &value); err == nil {
			t.Errorf("%q should be refused as an assignee", bad)
		}
	}
}

func TestRefsAndTime(t *testing.T) {
	if id, ok := protocol.ParseRef("812"); !ok || id != 812 {
		t.Fatalf("812: %d %v", id, ok)
	}
	for _, bad := range []string{"", "0x10", "812a", "01", "-1", "1.5"} {
		if _, ok := protocol.ParseRef(bad); ok {
			t.Errorf("%q should not parse", bad)
		}
	}
	at := time.Date(2026, 10, 8, 14, 2, 11, 0, time.UTC)
	if got := protocol.FormatTime(at); got != "2026-10-08T14:02:11.000Z" {
		t.Fatalf("wire time %q", got)
	}
	parsed, err := protocol.ParseTime("2026-10-08T14:02:11.000Z")
	if err != nil || !parsed.Equal(at) {
		t.Fatalf("parse: %v, %v", parsed, err)
	}
	if _, err := protocol.ParseTime("tomorrow"); err == nil {
		t.Fatal("a non-time should be refused")
	}
}

func TestErrorStatuses(t *testing.T) {
	cases := map[protocol.Code]int{
		protocol.CodeInvalidRequest:  400,
		protocol.CodeInvalidNeeds:    400,
		protocol.CodeUnauthorized:    401,
		protocol.CodeForbidden:       403,
		protocol.CodeTaskNotOwned:    403,
		protocol.CodeMessageNotFound: 404,
		protocol.CodeNotATask:        404,
		protocol.CodeDuplicateID:     409,
		protocol.CodeClaimLost:       409,
		protocol.CodeConflict:        409,
		protocol.CodeTaskClaimed:     409,
		protocol.CodeUnavailable:     503,
	}
	for code, status := range cases {
		err := protocol.NewError(code, "message")
		if err.Status != status {
			t.Errorf("%s: status %d, want %d", code, err.Status, status)
		}
		if err.Body().Error.Code != code {
			t.Errorf("%s: body code %s", code, err.Body().Error.Code)
		}
	}
	if protocol.StatusOf(protocol.Code("unknown")) != 500 {
		t.Error("an unknown code is a 500")
	}
	if body := protocol.NewError(protocol.CodeInvalidRequest, "bad").WithHint("do this").Body(); body.Error.Hint != "do this" {
		t.Error("hints ride the body")
	}
}

func TestValidateTaskShape(t *testing.T) {
	if !protocol.ValidTaskState("blocked") || protocol.ValidTaskState("doing") {
		t.Fatal("task state vocabulary")
	}
	if err := protocol.ValidateHoldKind(protocol.EditedHoldKind); err == nil {
		t.Fatal("edited is the board's own kind")
	}
	if err := protocol.ValidateHoldKind("awaiting_approval"); err != nil {
		t.Fatal(err)
	}
	if err := protocol.ValidateNote("reason", ""); err == nil {
		t.Fatal("an empty note is refused")
	}
	if err := protocol.ValidateNote("reason", strings.Repeat("x", protocol.TaskNoteMax+1)); err == nil {
		t.Fatal("a too-long note is refused")
	}
	if !protocol.ValidClaimToken("0123456789012345678901234567890123456789012") {
		t.Fatal("a 43-character token is valid")
	}
	if protocol.ValidClaimToken("short") {
		t.Fatal("a short token is not")
	}
	if err := protocol.ValidateOwnerFrom("julian"); err == nil {
		t.Fatal("a handle isn't an owner ID")
	}
	if err := protocol.ValidateOwnerFrom("local:user:julian"); err != nil {
		t.Fatal(err)
	}
}

func TestValidateClientIDReservesBoardNamespaces(t *testing.T) {
	for _, value := range []string{"client-1", "key:local:0123456789abcdef0123456789abcdef"} {
		if err := protocol.ValidateClientID(value); err != nil {
			t.Errorf("%q should be accepted: %v", value, err)
		}
	}
	for _, value := range []string{"sync:acme:1", "schedule:abc:1", "watch:alerts:1:0"} {
		if err := protocol.ValidateClientID(value); err == nil {
			t.Errorf("%q should be reserved to the board", value)
		}
	}
}
