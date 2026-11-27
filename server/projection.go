package server

import (
	"net/http"

	"github.com/jixoai/keel-board/protocol"
)

// adminChannel is the channel an operations route authorizes against: manage
// on `*` is manage over every channel the board has or will have.
const adminChannel = "*"

// handleProjectionCheck answers POST /admin/projection/check: a report of the
// request tenant's cached projection against a full replay of its log. The
// body is empty; the tenant is the request's board so one tenant can't ask
// about another.
func (s *Server) handleProjectionCheck(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if err := req.permit(s, adminChannel, protocol.PermissionManage); err != nil {
		writeError(w, err)
		return
	}
	report, err := s.board.CheckProjection(req.ctx, req.tenant)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

// handleProjectionRebuild answers POST /admin/projection/rebuild: it replaces
// the request tenant's cached projection with a replay of its durable log.
// The body is empty; an idempotent rebuild is safe to retry.
func (s *Server) handleProjectionRebuild(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if err := req.permit(s, adminChannel, protocol.PermissionManage); err != nil {
		writeError(w, err)
		return
	}
	report, err := s.board.Rebuild(req.ctx, req.tenant)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}
