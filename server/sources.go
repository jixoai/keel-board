package server

import (
	"net/http"
	"strings"

	"github.com/jixoai/keel-board/protocol"
	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/syncstore"
)

// sourceJoinRequest is the body of POST /sources/{key}/join. A join only
// names the platform target; the provider's configuration decides whether it
// may join at all.
type sourceJoinRequest struct {
	Target string `json:"target"`
}

// handleSourceRead answers GET /sources/{key}/read?target=&limit=&after=.
// It reads the platform on demand and returns a normalized result. Nothing is
// stored on the board and no board event is appended.
func (s *Server) handleSourceRead(w http.ResponseWriter, r *http.Request) {
	req, entry, key, ok := s.resolveSyncSource(w, r)
	if !ok {
		return
	}
	target := strings.TrimSpace(queryValue(r, "target"))
	if target == "" {
		writeError(w, protocol.Invalid("target: a platform channel, thread, or issue is required"))
		return
	}
	limit, perr := queryInt(r, "limit", boardsync.DefaultSourceLimit, 1, boardsync.MaxSourceLimit)
	if perr != nil {
		writeError(w, perr)
		return
	}
	after := strings.TrimSpace(queryValue(r, "after"))
	provider, found := s.sync.registry.Get(req.tenant, key)
	if !found {
		s.logSourceRead(req.tenant, key, entry.Platform, http.StatusNotImplemented, 0)
		writeError(w, protocol.NewError(protocol.CodeUnsupported,
			"this process doesn't host that sync connection"))
		return
	}
	reader, supported := provider.Provider.(boardsync.SourceReader)
	if !supported {
		s.logSourceRead(req.tenant, key, entry.Platform, http.StatusNotImplemented, 0)
		writeError(w, protocol.NewError(protocol.CodeUnsupported,
			"this sync connection can't read platform sources"))
		return
	}
	read, err := reader.ReadSource(req.ctx, key, target, boardsync.ReadOptions{Limit: limit, After: after})
	if err != nil {
		// A platform body never reaches the response or the log: only the
		// stable class the adapter returned.
		perr := syncFailureFor(err)
		s.logSourceRead(req.tenant, key, entry.Platform, perr.Status, 0)
		writeError(w, perr)
		return
	}
	if read.Messages == nil {
		// Keep one JSON shape: an empty read is [], never null.
		read.Messages = []boardsync.SourceMessage{}
	}
	s.logSourceRead(req.tenant, key, entry.Platform, http.StatusOK, len(read.Messages))
	writeJSON(w, http.StatusOK, read)
}

// handleSourceJoin answers POST /sources/{key}/join with {"target": "..."}.
// A provider that doesn't implement joining, or that hasn't opted in, answers
// a stable 501 rather than a server error.
func (s *Server) handleSourceJoin(w http.ResponseWriter, r *http.Request) {
	req, entry, key, ok := s.resolveSyncSource(w, r)
	if !ok {
		return
	}
	var body sourceJoinRequest
	if perr := decodeBody(r, &body, true, false); perr != nil {
		writeError(w, perr)
		return
	}
	target := strings.TrimSpace(body.Target)
	if target == "" {
		writeError(w, protocol.Invalid("target: a platform channel is required"))
		return
	}
	provider, found := s.sync.registry.Get(req.tenant, key)
	if !found {
		s.logSourceJoin(req.tenant, key, entry.Platform, http.StatusNotImplemented)
		writeError(w, protocol.NewError(protocol.CodeUnsupported,
			"this process doesn't host that sync connection"))
		return
	}
	joiner, supported := provider.Provider.(boardsync.SourceJoiner)
	if !supported {
		s.logSourceJoin(req.tenant, key, entry.Platform, http.StatusNotImplemented)
		writeError(w, protocol.NewError(protocol.CodeUnsupported,
			"this sync connection can't join platform channels"))
		return
	}
	join, err := joiner.JoinSource(req.ctx, key, target)
	if err != nil {
		perr := syncFailureFor(err)
		s.logSourceJoin(req.tenant, key, entry.Platform, perr.Status)
		writeError(w, perr)
		return
	}
	s.logSourceJoin(req.tenant, key, entry.Platform, http.StatusOK)
	writeJSON(w, http.StatusOK, join)
}

// handleSyncEntryStatus answers GET /sync/entries/{key}/status: the tenant
// status view filtered to one connection.
func (s *Server) handleSyncEntryStatus(w http.ResponseWriter, r *http.Request) {
	req, entry, _, ok := s.resolveSyncSource(w, r)
	if !ok {
		return
	}
	depth, err := s.sync.store.InboxDepth(req.ctx, req.tenant, entry.Key)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	status := syncStatusView{Keys: []syncKeyStatus{{
		syncEntryView: syncEntryView{
			Key: string(entry.Key), Platform: string(entry.Platform), Enabled: entry.Enabled,
			Owned:     s.sync.owns(req.tenant, entry.Key),
			CreatedAt: protocol.FormatTime(entry.CreatedAt), UpdatedAt: protocol.FormatTime(entry.UpdatedAt),
		},
		Inbox: depth,
	}}}
	status.Outbox, err = s.sync.store.OutboxDepth(req.ctx, req.tenant)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, status)
}

// resolveSyncSource resolves the caller, gates on board.manage, and reads the
// entry the path names. An unknown or malformed key is the same 404, so this
// surface never reveals which keys a tenant has.
func (s *Server) resolveSyncSource(w http.ResponseWriter, r *http.Request) (*request, syncstore.Entry, boardsync.Key, bool) {
	req, ok := s.resolve(w, r)
	if !ok {
		return nil, syncstore.Entry{}, "", false
	}
	if perr := req.permitSyncManage(s); perr != nil {
		writeError(w, perr)
		return nil, syncstore.Entry{}, "", false
	}
	key := boardsync.Key(strings.TrimSpace(r.PathValue("key")))
	if !key.Valid() {
		writeError(w, protocol.NotFound(protocol.CodeNotFound, "no such sync connection"))
		return nil, syncstore.Entry{}, "", false
	}
	entry, found, err := s.sync.store.Entry(req.ctx, req.tenant, key)
	if err != nil {
		s.fail(w, r, err)
		return nil, syncstore.Entry{}, "", false
	}
	if !found {
		writeError(w, protocol.NotFound(protocol.CodeNotFound, "no such sync connection"))
		return nil, syncstore.Entry{}, "", false
	}
	return req, entry, key, true
}

// logSourceRead records one bounded read audit line: connection, platform,
// action, status and how many messages came back. It carries no target and no
// message text.
func (s *Server) logSourceRead(tenant string, key boardsync.Key, platform boardsync.Platform, status, messages int) {
	s.sync.logger.Info("sync source read",
		"tenant", tenant, "key", key, "platform", platform,
		"action", "read", "status", status, "messages", messages)
}

// logSourceJoin records one bounded join audit line, with the same fields and
// no target.
func (s *Server) logSourceJoin(tenant string, key boardsync.Key, platform boardsync.Platform, status int) {
	s.sync.logger.Info("sync source join",
		"tenant", tenant, "key", key, "platform", platform,
		"action", "join", "status", status)
}

// queryValue returns a query parameter, trimmed. Empty and absent are the
// same for an optional cursor.
func queryValue(r *http.Request, name string) string {
	if value, ok := queryString(r, name); ok {
		return strings.TrimSpace(value)
	}
	return ""
}
