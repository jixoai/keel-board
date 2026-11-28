package server

import (
	"context"
	"net/http"
	"strings"

	"github.com/jixoai/keel-board/audit"
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
	provider, perr := s.sourceProvider(req, entry, key)
	if perr != nil {
		s.logSourceRead(req.ctx, req.tenant, key, entry.Platform, perr.Status, 0)
		writeError(w, perr)
		return
	}
	reader, supported := provider.Provider.(boardsync.SourceReader)
	if !supported {
		s.logSourceRead(req.ctx, req.tenant, key, entry.Platform, http.StatusNotImplemented, 0)
		writeError(w, protocol.NewError(protocol.CodeUnsupported,
			"this sync connection can't read platform sources"))
		return
	}
	read, err := reader.ReadSource(req.ctx, key, target, boardsync.ReadOptions{Limit: limit, After: after})
	if err != nil {
		// A platform body never reaches the response or the log: only the
		// stable class the adapter returned.
		perr := syncFailureFor(err)
		s.logSourceRead(req.ctx, req.tenant, key, entry.Platform, perr.Status, 0)
		writeError(w, perr)
		return
	}
	if read.Messages == nil {
		// Keep one JSON shape: an empty read is [], never null.
		read.Messages = []boardsync.SourceMessage{}
	}
	s.logSourceRead(req.ctx, req.tenant, key, entry.Platform, http.StatusOK, len(read.Messages))
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
	provider, perr := s.sourceProvider(req, entry, key)
	if perr != nil {
		s.logSourceJoin(req.ctx, req.tenant, key, entry.Platform, perr.Status)
		writeError(w, perr)
		return
	}
	joiner, supported := provider.Provider.(boardsync.SourceJoiner)
	if !supported {
		s.logSourceJoin(req.ctx, req.tenant, key, entry.Platform, http.StatusNotImplemented)
		writeError(w, protocol.NewError(protocol.CodeUnsupported,
			"this sync connection can't join platform channels"))
		return
	}
	join, err := joiner.JoinSource(req.ctx, key, target)
	if err != nil {
		perr := syncFailureFor(err)
		s.logSourceJoin(req.ctx, req.tenant, key, entry.Platform, perr.Status)
		writeError(w, perr)
		return
	}
	s.logSourceJoin(req.ctx, req.tenant, key, entry.Platform, http.StatusOK)
	writeJSON(w, http.StatusOK, join)
}

// syncEntryStatusLimit bounds the dead-letter counts one entry status reads.
// The counts saturate at this number; the list routes page the details.
const syncEntryStatusLimit = 100

// syncEntryStatusView is one connection's status. Outbox is this
// connection's depth, not the tenant total; Failures and DeadInbox count this
// connection's dead deliveries and dead inbound events, up to
// syncEntryStatusLimit. The embedded health view is the same shape the list
// and status routes return.
type syncEntryStatusView struct {
	syncEntryHealthView
	Failures  int `json:"failures"`
	DeadInbox int `json:"deadInbox"`
}

// handleSyncEntryStatus answers GET /sync/entries/{key}/status: one
// connection's status view, including its own outbox depth, targets, lag,
// 24h failures and bounded dead letter counts. A disabled entry stays readable
// here so an operator can see why source read/join answers 409.
func (s *Server) handleSyncEntryStatus(w http.ResponseWriter, r *http.Request) {
	req, entry, _, ok := s.resolveSyncSource(w, r)
	if !ok {
		return
	}
	health, err := s.syncEntryHealth(req.ctx, req.tenant, entry)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	failures, err := s.sync.store.TargetFailures(req.ctx, req.tenant, entry.Key, syncEntryStatusLimit)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	dead, err := s.sync.store.DeadInbox(req.ctx, req.tenant, entry.Key, syncEntryStatusLimit)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, syncEntryStatusView{
		syncEntryHealthView: health,
		Failures:            len(failures),
		DeadInbox:           len(dead),
	})
}

// handleSyncEntryTargets answers GET /sync/entries/{key}/targets: one
// connection's external targets and their outbound health. It is the by-key
// enumeration; GET /sync/threads?thread= remains the by-board-thread one.
func (s *Server) handleSyncEntryTargets(w http.ResponseWriter, r *http.Request) {
	req, entry, _, ok := s.resolveSyncSource(w, r)
	if !ok {
		return
	}
	targets, err := s.sync.store.Targets(req.ctx, req.tenant, entry.Key, syncEntryStatusLimit)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"targets": targetViews(targets)})
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

// sourceProvider returns this process's provider for a connection after the
// entry-level checks: the entry must be enabled, this process must host it,
// and the provider's platform must agree with the stored entry. A disabled
// entry is a stable 409; a missing provider or a config mismatch is a stable
// server-side refusal. No platform call is made on any of these paths.
func (s *Server) sourceProvider(req *request, entry syncstore.Entry, key boardsync.Key) (boardsync.Entry, *protocol.Error) {
	if !entry.Enabled {
		return boardsync.Entry{}, protocol.NewError(protocol.CodeSyncDisabled,
			"this sync connection is disabled")
	}
	provider, found := s.sync.registry.Get(req.tenant, key)
	if !found {
		return boardsync.Entry{}, protocol.NewError(protocol.CodeUnsupported,
			"this process doesn't host that sync connection")
	}
	if provider.Platform != entry.Platform {
		// A stored entry and a live provider that disagree about the platform
		// are a deployment error. Log the names for an operator, answer with
		// a stable message, and never call either side.
		s.sync.logger.Error("sync source configuration mismatch",
			"tenant", req.tenant, "key", key,
			"storedPlatform", entry.Platform, "providerPlatform", provider.Platform)
		return boardsync.Entry{}, protocol.NewError(protocol.CodeInternal,
			"the sync connection's configuration doesn't match its stored entry")
	}
	return provider, nil
}

// logSourceRead records one bounded read in the log and in the durable audit:
// connection, platform, action, status and how many messages came back. It
// carries no target and no message text.
func (s *Server) logSourceRead(ctx context.Context, tenant string, key boardsync.Key, platform boardsync.Platform, status, messages int) {
	s.sync.logger.Info("sync source read",
		"tenant", tenant, "key", key, "platform", platform,
		"action", "read", "status", status, "messages", messages)
	auditStatus := audit.StatusOK
	if status < 200 || status >= 300 {
		auditStatus = audit.StatusFailed
	}
	s.recordAudit(ctx, audit.Event{
		Tenant: tenant, Kind: audit.KindSourceRead, Key: string(key), Platform: string(platform),
		Status: auditStatus, Detail: map[string]any{"http": status, "messages": messages},
	})
}

// logSourceJoin records one bounded join in the log and in the durable audit,
// with the same fields and no target.
func (s *Server) logSourceJoin(ctx context.Context, tenant string, key boardsync.Key, platform boardsync.Platform, status int) {
	s.sync.logger.Info("sync source join",
		"tenant", tenant, "key", key, "platform", platform,
		"action", "join", "status", status)
	auditStatus := audit.StatusOK
	if status < 200 || status >= 300 {
		auditStatus = audit.StatusFailed
	}
	s.recordAudit(ctx, audit.Event{
		Tenant: tenant, Kind: audit.KindSourceJoin, Key: string(key), Platform: string(platform),
		Status: auditStatus, Detail: map[string]any{"http": status},
	})
}

// queryValue returns a query parameter, trimmed. Empty and absent are the
// same for an optional cursor.
func queryValue(r *http.Request, name string) string {
	if value, ok := queryString(r, name); ok {
		return strings.TrimSpace(value)
	}
	return ""
}
