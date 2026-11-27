package protocol

import (
	"errors"
	"fmt"
	"net/http"
)

// Code is a stable error code. Clients branch on it; the message is for a
// person.
type Code string

const (
	CodeInvalidRequest     Code = "invalid_request"
	CodeInvalidCursor      Code = "invalid_cursor"
	CodeInvalidAfter       Code = "invalid_after"
	CodeInvalidNeeds       Code = "invalid_needs"
	CodeInvalidFrom        Code = "invalid_from"
	CodeMessageDeleted     Code = "message_deleted"
	CodeRevisionConflict   Code = "revision_conflict"
	CodeReservedLabel      Code = "reserved_label"
	CodeTextTooLong        Code = "text_too_long"
	CodeBatchTooLarge      Code = "batch_too_large"
	CodeInvalidBlock       Code = "invalid_block"
	CodeAttachmentNotFound Code = "attachment_not_found"
	CodeAttachmentInUse    Code = "attachment_in_use"
	CodeInvalidAttachment  Code = "invalid_attachment"
	CodeTooLarge           Code = "too_large"
	CodeNoFiles            Code = "no_files"
	CodeNoDirectUploads    Code = "no_direct_uploads"
	CodeUnsupported        Code = "unsupported"
	CodeUploadNotFound     Code = "upload_not_found"
	CodeNotUploaded        Code = "not_uploaded"
	CodeSizeMismatch       Code = "size_mismatch"
	CodeBatchNotFound      Code = "batch_not_found"
	CodeUnauthorized       Code = "unauthorized"
	CodeForbidden          Code = "forbidden"
	CodeTaskNotOwned       Code = "task_not_owned"
	CodeNotFound           Code = "not_found"
	CodeMessageNotFound    Code = "message_not_found"
	CodeNotATask           Code = "not_a_task"
	CodeDuplicateID        Code = "duplicate_id"
	CodeLabelConflict      Code = "label_conflict"
	CodeNewerMessage       Code = "newer_message"
	CodeAlreadyATask       Code = "already_a_task"
	CodeTaskClaimed        Code = "task_claimed"
	CodeTaskAssigned       Code = "task_assigned"
	CodeTaskWaiting        Code = "task_waiting"
	CodeTaskBlocked        Code = "task_blocked"
	CodeTaskFinished       Code = "task_finished"
	CodeClaimLost          Code = "claim_lost"
	CodeConflict           Code = "conflict"
	CodeUnavailable        Code = "unavailable"
	CodeInternal           Code = "internal"
)

var statuses = map[Code]int{
	CodeInvalidRequest:     http.StatusBadRequest,
	CodeInvalidCursor:      http.StatusBadRequest,
	CodeInvalidAfter:       http.StatusBadRequest,
	CodeInvalidNeeds:       http.StatusBadRequest,
	CodeInvalidFrom:        http.StatusBadRequest,
	CodeMessageDeleted:     http.StatusBadRequest,
	CodeRevisionConflict:   http.StatusConflict,
	CodeReservedLabel:      http.StatusBadRequest,
	CodeTextTooLong:        http.StatusBadRequest,
	CodeBatchTooLarge:      http.StatusBadRequest,
	CodeInvalidBlock:       http.StatusBadRequest,
	CodeAttachmentNotFound: http.StatusNotFound,
	CodeAttachmentInUse:    http.StatusConflict,
	CodeInvalidAttachment:  http.StatusBadRequest,
	CodeTooLarge:           http.StatusRequestEntityTooLarge,
	CodeNoFiles:            http.StatusNotImplemented,
	CodeNoDirectUploads:    http.StatusNotImplemented,
	CodeUnsupported:        http.StatusNotImplemented,
	CodeUploadNotFound:     http.StatusNotFound,
	CodeNotUploaded:        http.StatusConflict,
	CodeSizeMismatch:       http.StatusBadRequest,
	CodeBatchNotFound:      http.StatusNotFound,
	CodeUnauthorized:       http.StatusUnauthorized,
	CodeForbidden:          http.StatusForbidden,
	CodeTaskNotOwned:       http.StatusForbidden,
	CodeNotFound:           http.StatusNotFound,
	CodeMessageNotFound:    http.StatusNotFound,
	CodeNotATask:           http.StatusNotFound,
	CodeDuplicateID:        http.StatusConflict,
	CodeLabelConflict:      http.StatusConflict,
	CodeNewerMessage:       http.StatusConflict,
	CodeAlreadyATask:       http.StatusConflict,
	CodeTaskClaimed:        http.StatusConflict,
	CodeTaskAssigned:       http.StatusConflict,
	CodeTaskWaiting:        http.StatusConflict,
	CodeTaskBlocked:        http.StatusConflict,
	CodeTaskFinished:       http.StatusConflict,
	CodeClaimLost:          http.StatusConflict,
	CodeConflict:           http.StatusConflict,
	CodeUnavailable:        http.StatusServiceUnavailable,
	CodeInternal:           http.StatusInternalServerError,
}

// StatusOf returns the HTTP status a code answers with. An unknown code is a
// 500, as an unknown failure is.
func StatusOf(code Code) int {
	if status, ok := statuses[code]; ok {
		return status
	}
	return http.StatusInternalServerError
}

// Error is a board failure: an HTTP status, a stable code, a message for a
// person, and optionally a hint for what fixes it.
type Error struct {
	Status  int
	Code    Code
	Message string
	Hint    string
}

// Error implements error.
func (e *Error) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

// NewError builds an error with a status derived from its code.
func NewError(code Code, message string) *Error {
	return &Error{Status: StatusOf(code), Code: code, Message: message}
}

// WithHint returns a copy carrying a hint.
func (e *Error) WithHint(hint string) *Error {
	copy := *e
	copy.Hint = hint
	return &copy
}

// Errorf builds an error with a formatted message.
func Errorf(code Code, format string, args ...any) *Error {
	return NewError(code, fmt.Sprintf(format, args...))
}

// Invalid reports a request that doesn't validate.
func Invalid(message string) *Error { return NewError(CodeInvalidRequest, message) }

// Invalidf is Invalid with a formatted message.
func Invalidf(format string, args ...any) *Error {
	return Errorf(CodeInvalidRequest, format, args...)
}

// NotFound reports a thing that's missing, or that the caller can't read.
func NotFound(code Code, message string) *Error { return NewError(code, message) }

// Forbidden reports a write the caller's access doesn't allow.
func Forbidden(message string) *Error { return NewError(CodeForbidden, message) }

// Conflict reports a write that lost against the board's current state.
func Conflict(code Code, message string) *Error { return NewError(code, message) }

// AsError extracts a board error from err.
func AsError(err error) (*Error, bool) {
	var target *Error
	if errors.As(err, &target) {
		return target, true
	}
	return nil, false
}

// ErrorBody is the JSON body every failure answers with.
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail is a failure's code, message, and optional hint.
type ErrorDetail struct {
	Code    Code   `json:"code"`
	Message string `json:"message"`
	Hint    string `json:"hint,omitempty"`
}

// Body returns the wire body of e.
func (e *Error) Body() ErrorBody {
	return ErrorBody{Error: ErrorDetail{Code: e.Code, Message: e.Message, Hint: e.Hint}}
}
