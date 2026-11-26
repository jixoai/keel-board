package access

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/jixoai/keel-board/protocol"
)

// Codes the access layer answers with. They extend the board's wire codes;
// protocol.StatusOf doesn't know them, so each error carries its status.
const (
	// CodeInvalidCan is a can, channel pattern, or permission that doesn't
	// parse. The board answers 400.
	CodeInvalidCan protocol.Code = "invalid_can"
	// CodeInvalidGrant is a grant whose principals don't make sense. The
	// board answers 400.
	CodeInvalidGrant protocol.Code = "invalid_grant"
	// CodeGrantLockout is a grant change that would take its caller's own
	// manage at a pattern and leave them unable to undo it. The board answers
	// 409.
	CodeGrantLockout protocol.Code = "grant_lockout"
	// CodeInvalidScope is a token narrowing that doesn't parse. The board
	// answers 400.
	CodeInvalidScope protocol.Code = "invalid_scope"
	// CodeNotInGrant is an update removing a principal the grant doesn't name.
	// The board answers 400.
	CodeNotInGrant protocol.Code = "not_in_grant"
	// CodeGrantEmpty is an update that would leave a grant naming no principal.
	// The board answers 400.
	CodeGrantEmpty protocol.Code = "grant_empty"
)

// InvalidCan reports a can, a channel pattern, or a permission that doesn't
// parse.
type InvalidCan struct {
	Message string
}

// Error implements error.
func (e *InvalidCan) Error() string { return e.Message }

// Code returns the board's code for the failure.
func (e *InvalidCan) Code() protocol.Code { return CodeInvalidCan }

// ProtocolError returns the failure as the board writes it.
func (e *InvalidCan) ProtocolError() *protocol.Error {
	return &protocol.Error{Status: http.StatusBadRequest, Code: CodeInvalidCan, Message: e.Message}
}

// InvalidGrant reports a grant whose principals don't name anything.
type InvalidGrant struct {
	Message string
}

// Error implements error.
func (e *InvalidGrant) Error() string { return e.Message }

// Code returns the board's code for the failure.
func (e *InvalidGrant) Code() protocol.Code { return CodeInvalidGrant }

// ProtocolError returns the failure as the board writes it.
func (e *InvalidGrant) ProtocolError() *protocol.Error {
	return &protocol.Error{Status: http.StatusBadRequest, Code: CodeInvalidGrant, Message: e.Message}
}

func invalidCanf(format string, args ...any) *InvalidCan {
	return &InvalidCan{Message: fmt.Sprintf(format, args...)}
}

func invalidGrantf(format string, args ...any) *InvalidGrant {
	return &InvalidGrant{Message: fmt.Sprintf(format, args...)}
}

// AsProtocolError renders an access failure as the board's error body. A
// failure it doesn't know is an internal error, never a guess.
func AsProtocolError(err error) *protocol.Error {
	if err == nil {
		return nil
	}
	if boardErr, ok := protocol.AsError(err); ok {
		return boardErr
	}
	var invalidCan *InvalidCan
	if errors.As(err, &invalidCan) {
		return invalidCan.ProtocolError()
	}
	var invalidGrant *InvalidGrant
	if errors.As(err, &invalidGrant) {
		return invalidGrant.ProtocolError()
	}
	var refusal *Refusal
	if errors.As(err, &refusal) {
		return refusal.ProtocolError()
	}
	switch {
	case errors.Is(err, ErrGrantNotFound):
		return protocol.NotFound(protocol.CodeNotFound, "grant not found")
	case errors.Is(err, ErrGrantExists):
		return protocol.Conflict(protocol.CodeDuplicateID, "a grant with that ID exists")
	case errors.Is(err, ErrTenantMismatch):
		return protocol.Invalid("a grant belongs to one tenant")
	default:
		return protocol.NewError(protocol.CodeInternal, "internal error")
	}
}
