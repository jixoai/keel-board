package board

import (
	"errors"
	"fmt"
)

// Code is a task error's stable code. HTTP and tool surfaces map it onto their
// own status or result code; the domain never depends on an HTTP status.
type Code string

const (
	// CodeTaskClaimed: another actor holds a live claim on the task.
	CodeTaskClaimed Code = "task_claimed"
	// CodeClaimLost: a claim's write came from an epoch or token that is no
	// longer its claim, or from a claim whose lease ended.
	CodeClaimLost Code = "claim_lost"
	// CodeNotATask: the ID does not name a task.
	CodeNotATask Code = "not_a_task"
	// CodeInvalidNeeds: prerequisites are misshapen, missing, canceled, or
	// would close a cycle.
	CodeInvalidNeeds Code = "invalid_needs"
	// CodeReservedLabel: a caller tried to write a label under board.
	CodeReservedLabel Code = "reserved_label"
	// CodeInvalidLabel: a label key or value is misshapen or too long.
	CodeInvalidLabel Code = "invalid_label"
	// CodeTaskWaiting: a prerequisite has not completed.
	CodeTaskWaiting Code = "task_waiting"
	// CodeTaskBlocked: the task is held for a person.
	CodeTaskBlocked Code = "task_blocked"
	// CodeTaskFinished: the task is completed, failed, or canceled.
	CodeTaskFinished Code = "task_finished"
	// CodeTaskAssigned: the task is assigned to another of its owner's
	// profiles.
	CodeTaskAssigned Code = "task_assigned"
	// CodeInvalidLease: a lease is outside LeaseMin..LeaseMax.
	CodeInvalidLease Code = "invalid_lease"
	// CodeInvalidNote: a result, reason, or hold reason is empty or too long.
	CodeInvalidNote Code = "invalid_note"
	// CodeInvalidActor: an actor or owner ID is misshapen.
	CodeInvalidActor Code = "invalid_actor"
	// CodeInvalidProfile: an assignee is not one of its owner's profiles.
	CodeInvalidProfile Code = "invalid_profile"
	// CodeInvalidToken: a claim's token hash is misshapen.
	CodeInvalidToken Code = "invalid_token"
	// CodeInvalidState: a task carries a state outside the vocabulary.
	CodeInvalidState Code = "invalid_state"
	// CodeInvalidHoldKind: a hold kind is misshapen or board-owned.
	CodeInvalidHoldKind Code = "invalid_hold_kind"
	// CodeVersionConflict: a Commit was based on a task version that has
	// since moved on.
	CodeVersionConflict Code = "version_conflict"
)

// Error is a typed task error. Code is stable; Message is for a person.
type Error struct {
	Code    Code
	Message string
}

// Error implements error.
func (e *Error) Error() string {
	if e.Message == "" {
		return string(e.Code)
	}
	return string(e.Code) + ": " + e.Message
}

// Is reports whether target is a task Error with the same code, so
// errors.Is(err, &Error{Code: CodeClaimLost}) matches any claim_lost error.
func (e *Error) Is(target error) bool {
	other, ok := target.(*Error)
	return ok && other != nil && other.Code == e.Code
}

// CodeOf returns the task error code of err, or "" when err is not a task
// error.
func CodeOf(err error) Code {
	var target *Error
	if errors.As(err, &target) {
		return target.Code
	}
	return ""
}

func taskErrorf(code Code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}
