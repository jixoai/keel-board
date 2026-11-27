package server

import (
	"net/http"

	"github.com/jixoai/keel-board/protocol"
)

// auditEventView is one audit row as the admin route exposes it. The row's
// tenant is omitted: the caller already knows its board, and the answer can
// not name another tenant. Detail is re-filtered to the bounded scalar values
// the writers produce, so a row stored by an older build can't leak a nested
// payload through this route.
type auditEventView struct {
	ID         uint64         `json:"id"`
	At         string         `json:"at"`
	Kind       string         `json:"kind"`
	Key        string         `json:"key,omitempty"`
	Platform   string         `json:"platform,omitempty"`
	Target     string         `json:"target,omitempty"`
	DeliveryID string         `json:"deliveryId,omitempty"`
	Status     string         `json:"status,omitempty"`
	Attempts   int            `json:"attempts,omitempty"`
	Detail     map[string]any `json:"detail,omitempty"`
}

// handleAudit answers GET /admin/audit?limit=: the request tenant's newest
// audit rows. It needs manage at *, like the projection operations, because
// the rows name platform targets across the board.
func (s *Server) handleAudit(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if err := req.permit(s, adminChannel, protocol.PermissionManage); err != nil {
		writeError(w, err)
		return
	}
	if s.audit == nil {
		writeError(w, &protocol.Error{
			Status: http.StatusNotImplemented, Code: protocol.CodeUnavailable,
			Message: "audit is not configured",
		})
		return
	}
	limit, perr := queryInt(r, "limit", 100, 1, 500)
	if perr != nil {
		writeError(w, perr)
		return
	}
	events, err := s.audit.Recent(req.ctx, req.tenant, limit)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	views := make([]auditEventView, 0, len(events))
	for _, event := range events {
		views = append(views, auditEventView{
			ID: event.ID, At: protocol.FormatTime(event.At), Kind: event.Kind,
			Key: event.Key, Platform: event.Platform, Target: event.Target,
			DeliveryID: event.DeliveryID, Status: event.Status, Attempts: event.Attempts,
			Detail: boundedDetail(event.Detail),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": views, "count": len(views)})
}

// boundedDetail keeps only scalar detail values of a bounded shape. It is a
// read-side guard, not a replacement for the audit package's write validation.
func boundedDetail(detail map[string]any) map[string]any {
	if len(detail) == 0 {
		return nil
	}
	out := make(map[string]any, len(detail))
	for key, value := range detail {
		if len(key) == 0 || len(key) > 32 {
			continue
		}
		switch typed := value.(type) {
		case string:
			if len(typed) <= 256 {
				out[key] = typed
			}
		case bool:
			out[key] = typed
		case int:
			out[key] = typed
		case int64:
			out[key] = typed
		case float64:
			out[key] = typed
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
