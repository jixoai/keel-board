// Package server serves the board's HTTP API over a log.Board.
//
// It owns HTTP: routing, decoding and validating requests, resolving the
// caller and the tenant, authorizing each operation, and writing the board's
// errors. The semantics of a write live in messages.go and tasks.go, each a
// read-decide-append transaction on the log.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
)

// APIPrefix is where the board is mounted.
const APIPrefix = "/v1/board"

// Version is the discovery version this phase reports.
const Version = "0.1.0-phase1"

// Server is the board's HTTP handler.
type Server struct {
	board     *log.Board
	authorize Authorizer
	actorFn   ActorFunc
	tenantFn  TenantFunc
	version   string
	mux       *http.ServeMux
	logger    *slog.Logger
}

// Options configure a Server.
type Options struct {
	// Board is the log to serve. Required.
	Board *log.Board
	// Authorize decides what a caller may do. Default: allow everyone
	// everything, for development.
	Authorize Authorizer
	// Actor resolves who a request is. Default: a development actor, from
	// headers when they name one.
	Actor ActorFunc
	// Tenant resolves which board a request is on. Default: the
	// X-Board-Tenant header, or "default".
	Tenant TenantFunc
	// Version overrides the discovery version string.
	Version string
	// Logger receives unexpected failures. Default: slog.Default().
	Logger *slog.Logger
}

// New returns a server. It panics if no board is given: a server without a
// log has nothing to serve.
func New(opts Options) *Server {
	if opts.Board == nil {
		panic("server: a Board is required")
	}
	if opts.Authorize == nil {
		opts.Authorize = AllowAll{}
	}
	if opts.Actor == nil {
		opts.Actor = DevActor
	}
	if opts.Tenant == nil {
		opts.Tenant = DevTenant
	}
	if opts.Version == "" {
		opts.Version = Version
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	server := &Server{
		board:     opts.Board,
		authorize: opts.Authorize,
		actorFn:   opts.Actor,
		tenantFn:  opts.Tenant,
		version:   opts.Version,
		mux:       http.NewServeMux(),
		logger:    opts.Logger,
	}
	server.routes()
	return server
}

// routes registers every phase-1 route. Each path is mounted under APIPrefix.
func (s *Server) routes() {
	base := APIPrefix
	s.mux.HandleFunc("GET "+base, s.handleDiscovery)
	s.mux.HandleFunc("GET "+base+"/channels", s.handleListChannels)
	s.mux.HandleFunc("POST "+base+"/channels", s.handleCreateChannel)
	s.mux.HandleFunc("GET "+base+"/messages", s.handleListMessages)
	s.mux.HandleFunc("POST "+base+"/messages", s.handlePost)
	s.mux.HandleFunc("GET "+base+"/messages/{id}", s.handleThread)
	s.mux.HandleFunc("POST "+base+"/messages/{id}/labels", s.handleLabels)
	s.mux.HandleFunc("GET "+base+"/tasks", s.handleListTasks)
	s.mux.HandleFunc("POST "+base+"/tasks", s.handleCreateTask)
	s.mux.HandleFunc("POST "+base+"/tasks/claim", s.handleClaimReady)
	s.mux.HandleFunc("POST "+base+"/tasks/{id}/claim", s.handleClaimTask)
	s.mux.HandleFunc("POST "+base+"/tasks/{id}/renew", s.handleRenewTask)
	s.mux.HandleFunc("POST "+base+"/tasks/{id}/release", s.handleReleaseTask)
	s.mux.HandleFunc("POST "+base+"/tasks/{id}/complete", s.handleCompleteTask)
	s.mux.HandleFunc("POST "+base+"/tasks/{id}/fail", s.handleFailTask)
	s.mux.HandleFunc("POST "+base+"/tasks/{id}/block", s.handleBlockTask)
	s.mux.HandleFunc("POST "+base+"/tasks/{id}/unblock", s.handleUnblockTask)
	s.mux.HandleFunc("POST "+base+"/tasks/{id}/reopen", s.handleReopenTask)
	s.mux.HandleFunc("POST "+base+"/tasks/{id}/assign", s.handleAssignTask)
	s.mux.HandleFunc("POST "+base+"/tasks/{id}/needs", s.handleSetTaskNeeds)
	s.mux.HandleFunc("POST "+base+"/tasks/{id}/cancel", s.handleCancelTask)
	s.mux.HandleFunc("GET "+base+"/events", s.handleEvents)
	// Anything else answers the board's own error shape.
	s.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, protocol.NotFound(protocol.CodeNotFound, "no such route"))
	})
}

// Handler returns the server as an http.Handler, with panic recovery.
func (s *Server) Handler() http.Handler { return s }

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if recovered := recover(); recovered != nil {
			s.logger.Error("board request panicked", "method", r.Method, "path", r.URL.Path, "panic", recovered)
			// A stream that already wrote its headers can't be given a JSON
			// error; closing it is all that's left.
			if !headersWritten(w) {
				writeError(w, protocol.NewError(protocol.CodeInternal, "internal error"))
			}
		}
	}()
	s.mux.ServeHTTP(w, r)
}

// headersWritten can't be observed through the standard ResponseWriter; the
// recovery above writes an error only where the handler hasn't started, which
// net/http tolerates by ignoring the second WriteHeader.
func headersWritten(http.ResponseWriter) bool { return false }

// request is a request's resolved caller and tenant.
type request struct {
	tenant string
	actor  protocol.Actor
	http   *http.Request
	ctx    context.Context
}

// resolve resolves the caller and tenant of a request.
func (s *Server) resolve(w http.ResponseWriter, r *http.Request) (*request, bool) {
	tenant, err := s.tenantFn(r)
	if err != nil {
		writeError(w, asProtocol(err))
		return nil, false
	}
	actor, err := s.actorFn(r)
	if err != nil {
		writeError(w, asProtocol(err))
		return nil, false
	}
	if actor.ID == "" {
		actor.ID = "local:user:dev"
	}
	if actor.Principal == "" {
		actor.Principal = actor.ID
	}
	if actor.Kind == "" {
		actor.Kind = protocol.ActorPerson
	}
	req := &request{tenant: tenant, actor: actor, http: r, ctx: r.Context()}
	if validator, ok := s.authorize.(RequestValidator); ok {
		if err := validator.ValidateRequest(req.ctx, req.tenant, req.actor); err != nil {
			writeError(w, asProtocol(err))
			return nil, false
		}
	}
	return req, true
}

// can asks the authorizer whether the request may do permission in channel.
func (req *request) can(s *Server, channel string, permission protocol.Permission) bool {
	return s.authorize.Authorize(req.ctx, req.tenant, req.actor, channel, permission) == nil
}

// permit asks the authorizer and turns a refusal into an error body.
func (req *request) permit(s *Server, channel string, permission protocol.Permission) *protocol.Error {
	if err := s.authorize.Authorize(req.ctx, req.tenant, req.actor, channel, permission); err != nil {
		return asProtocol(err)
	}
	return nil
}

// asProtocol turns any error into the board's error body.
func asProtocol(err error) *protocol.Error {
	if boardErr, ok := protocol.AsError(err); ok {
		return boardErr
	}
	// An access refusal carries its own status and code, so a grant_lockout is
	// a 409 rather than a generic 403, without the server importing access.
	var provider interface{ ProtocolError() *protocol.Error }
	if errors.As(err, &provider) {
		if boardErr := provider.ProtocolError(); boardErr != nil {
			return boardErr
		}
	}
	if errors.Is(err, context.Canceled) {
		return &protocol.Error{Status: 499, Code: protocol.Code("canceled"), Message: "request canceled"}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &protocol.Error{Status: http.StatusServiceUnavailable, Code: protocol.CodeUnavailable, Message: "request timed out"}
	}
	return protocol.Forbidden(err.Error())
}

// writeJSON writes a JSON response.
func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("content-type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(body)
}

// writeError writes a board failure.
func writeError(w http.ResponseWriter, err *protocol.Error) {
	if err == nil {
		err = protocol.NewError(protocol.CodeInternal, "internal error")
	}
	writeJSON(w, err.Status, err.Body())
}

// fail writes any error a handler returned.
func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	if boardErr, ok := protocol.AsError(err); ok {
		writeError(w, boardErr)
		return
	}
	if errors.Is(err, log.ErrHeadMoved) {
		writeError(w, protocol.NewError(protocol.CodeUnavailable, "the board moved on while it checked: try again"))
		return
	}
	s.logger.Error("board request failed", "method", r.Method, "path", r.URL.Path, "error", err)
	writeError(w, protocol.NewError(protocol.CodeInternal, "internal error"))
}

// decodeBody decodes a JSON body. An empty body decodes into a zero value
// when optional is true. Strict refuses unknown fields, as the routes that
// promise it do.
func decodeBody(r *http.Request, into any, strict, optional bool) *protocol.Error {
	if r.Body == nil {
		if optional {
			return nil
		}
		return protocol.Invalid("invalid request: a JSON body is required")
	}
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	if strict {
		decoder.DisallowUnknownFields()
	}
	if err := decoder.Decode(into); err != nil {
		if errors.Is(err, io.EOF) && optional {
			return nil
		}
		return protocol.Invalid("invalid request: " + friendlyJSONError(err))
	}
	// Refuse trailing JSON values: a body is one object.
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return protocol.Invalid("invalid request: the body has more than one JSON value")
	}
	return nil
}

func friendlyJSONError(err error) string {
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		field := typeErr.Field
		if field == "" {
			field = "body"
		}
		return fmt.Sprintf("%s: expected %s", field, typeErr.Type)
	}
	var syntaxErr *json.SyntaxError
	if errors.As(err, &syntaxErr) {
		return fmt.Sprintf("the body isn't valid JSON (at byte %d)", syntaxErr.Offset)
	}
	return err.Error()
}

// queryString returns a query parameter, or "" when absent.
func queryString(r *http.Request, name string) (string, bool) {
	values, ok := r.URL.Query()[name]
	if !ok || len(values) == 0 {
		return "", false
	}
	return values[0], true
}

// queryRepeated returns every value of a repeatable query parameter.
func queryRepeated(r *http.Request, name string) []string {
	return r.URL.Query()[name]
}

// queryRef parses an optional ref query parameter.
func queryRef(r *http.Request, name string) (*uint64, bool, *protocol.Error) {
	value, ok := queryString(r, name)
	if !ok {
		return nil, false, nil
	}
	id, valid := protocol.ParseRef(value)
	if !valid || id == 0 {
		return nil, true, protocol.NewError(protocol.CodeInvalidCursor, fmt.Sprintf("%s isn't a cursor or a message ID on this board", value))
	}
	return &id, true, nil
}

// queryInt parses an optional integer query parameter within bounds.
func queryInt(r *http.Request, name string, def, min, max int) (int, *protocol.Error) {
	value, ok := queryString(r, name)
	if !ok {
		return def, nil
	}
	number, err := strconv.Atoi(value)
	if err != nil || number < min || number > max {
		return 0, protocol.Invalidf("%s: a number from %d to %d", name, min, max)
	}
	return number, nil
}

// queryFlag parses an optional boolean query parameter: true or 1, false or 0.
func queryFlag(r *http.Request, name string) (bool, bool, *protocol.Error) {
	value, ok := queryString(r, name)
	if !ok {
		return false, false, nil
	}
	switch value {
	case "true", "1":
		return true, true, nil
	case "false", "0":
		return false, true, nil
	default:
		return false, true, protocol.Invalidf("%s: use true, false, 1, or 0", name)
	}
}

// pathRef parses the {id} path segment.
func pathRef(r *http.Request) (uint64, *protocol.Error) {
	value := r.PathValue("id")
	id, ok := protocol.ParseRef(value)
	if !ok || id == 0 {
		return 0, protocol.NotFound(protocol.CodeMessageNotFound, fmt.Sprintf("message not found: #%s", value))
	}
	return id, nil
}

// seconds converts a seconds value to a duration.
func seconds(value int) time.Duration { return time.Duration(value) * time.Second }
