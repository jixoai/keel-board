package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/syncstore"
)

// SyncKey names one connection a process owns and serves.
type SyncKey struct {
	Tenant string
	Key    boardsync.Key
}

// SyncOptions configure the sync surface of a Server.
type SyncOptions struct {
	// Store is the durable sync state. Required.
	Store syncstore.Store
	// Registry holds this process's sync entries. Required.
	Registry *boardsync.Registry
	// Tenant resolves the board a webhook belongs to. Required, and it must
	// come from the server's authentication (a deployment-fixed tenant or a
	// verified token), never from a header the platform can set.
	Tenant syncstore.TenantFunc
	// Owned lists the connections this process's workers and subscription
	// serve. Empty means the server serves webhooks and management only.
	Owned []SyncKey
	// StartChannels lists platform channels a new board root may start a
	// thread in, per key. The entry's own Sync function still decides; a
	// Mirror only starts in its board channel.
	StartChannels map[boardsync.Key][]string
	// FileHosts is the allowlist of hosts an inbound file or link URL may
	// name. Empty refuses every URL.
	FileHosts []string
	// MaxFileBytes bounds one inbound file's declared size.
	MaxFileBytes int64
	// MaxFiles and MaxLinks bound one inbound message.
	MaxFiles, MaxLinks int
	// MaxWebhookBody caps a platform webhook body. Zero means 1 MiB.
	MaxWebhookBody int64
	// Logger receives sync failures. Zero uses the server's logger.
	Logger *slog.Logger
}

// syncService is one server's sync wiring: the host, the runner, the webhook
// handler, and the board-event subscription.
type syncService struct {
	board         *log.Board
	store         syncstore.Store
	registry      *boardsync.Registry
	host          *SyncHost
	runner        *boardsync.Runner
	webhook       *syncstore.WebhookHandler
	startChannels map[boardsync.Key][]string
	owned         []SyncKey
	logger        *slog.Logger
}

// newSyncService validates opts and builds the sync seams.
func newSyncService(board *log.Board, opts SyncOptions) (*syncService, error) {
	if board == nil {
		return nil, errors.New("server: sync needs a board")
	}
	if opts.Store == nil {
		return nil, errors.New("server: sync needs a store")
	}
	if opts.Registry == nil {
		return nil, errors.New("server: sync needs a registry")
	}
	if opts.Tenant == nil {
		return nil, errors.New("server: sync needs a tenant resolver")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	for _, owned := range opts.Owned {
		if err := boardsync.ValidTenant(owned.Tenant); !err {
			return nil, fmt.Errorf("server: sync key %q has no usable tenant", owned.Key)
		}
		if !owned.Key.Valid() {
			return nil, fmt.Errorf("server: %q isn't a sync key", owned.Key)
		}
	}
	host := NewSyncHost(board, opts.Store, SyncHostOptions{
		FileHosts: opts.FileHosts, MaxFileBytes: opts.MaxFileBytes,
		MaxFiles: opts.MaxFiles, MaxLinks: opts.MaxLinks, Logger: opts.Logger,
	})
	handler, err := syncstore.NewWebhookHandler(syncstore.WebhookConfig{
		Registry: opts.Registry,
		Store:    opts.Store,
		Tenant:   opts.Tenant,
		GateFactory: func(string, boardsync.Key) (boardsync.Gate, error) {
			// The registry consults the entry's own Routes first; StoreGate
			// answers the thread half from the durable mappings. One gate per
			// request scope, never a shared static mapping.
			return syncstore.StoreGate{Store: opts.Store}, nil
		},
		MaxBodyBytes: opts.MaxWebhookBody,
		Logger:       opts.Logger,
	})
	if err != nil {
		return nil, err
	}
	return &syncService{
		board: board, store: opts.Store, registry: opts.Registry,
		host: host, runner: boardsync.NewRunner(host), webhook: handler,
		startChannels: opts.StartChannels, owned: append([]SyncKey(nil), opts.Owned...),
		logger: opts.Logger,
	}, nil
}

// SyncHost returns the server's board seam, for a deployment's workers.
func (s *Server) SyncHost() *SyncHost {
	if s.sync == nil {
		return nil
	}
	return s.sync.host
}

// SyncRunner returns the server's sync runner, for a deployment's workers.
func (s *Server) SyncRunner() *boardsync.Runner {
	if s.sync == nil {
		return nil
	}
	return s.sync.runner
}

// SyncRunnerFor returns the runner and whether sync is configured.
func (s *Server) SyncRunnerFor() (*boardsync.Runner, bool) {
	if s.sync == nil {
		return nil, false
	}
	return s.sync.runner, true
}

// RunSync follows the board log for each owned connection and queues outbound
// deliveries until ctx ends. It is the server's consumer; callers run one per
// process, and a restart resumes from the durable cursor.
func (s *Server) RunSync(ctx context.Context) error {
	if s.sync == nil {
		return nil
	}
	return s.sync.run(ctx)
}

// PrepareSync fixes each owned connection's starting cursor before the server
// accepts writes. Without it, an event posted between process start and the
// subscription's first read would be skipped as history.
func (s *Server) PrepareSync(ctx context.Context) error {
	if s.sync == nil {
		return nil
	}
	return s.sync.prepare(ctx)
}

// boardEventsSource is the syncstore cursor name for the board subscription.
const boardEventsSource = "board-events"

// prepare records each owned connection's cursor at the current head, unless
// one is already durable. It is safe to call more than once.
func (s *syncService) prepare(ctx context.Context) error {
	for _, owned := range s.owned {
		if _, err := s.loadCursor(ctx, owned.Tenant, owned.Key); err != nil {
			return err
		}
	}
	return nil
}

// run follows every owned connection's log. One connection's failure doesn't
// stop another; the first error is returned after all stop.
func (s *syncService) run(ctx context.Context) error {
	if len(s.owned) == 0 {
		<-ctx.Done()
		return nil
	}
	var wg sync.WaitGroup
	errs := make(chan error, len(s.owned))
	for _, owned := range s.owned {
		owned := owned
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.runKey(ctx, owned.Tenant, owned.Key); err != nil && ctx.Err() == nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		return err
	}
	return nil
}

// runKey follows one connection's board thread events.
func (s *syncService) runKey(ctx context.Context, tenant string, key boardsync.Key) error {
	after, err := s.loadCursor(ctx, tenant, key)
	if err != nil {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		if err := s.board.Wait(ctx, tenant, after); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		for {
			if err := ctx.Err(); err != nil {
				return nil
			}
			// Bound one batch so a long log doesn't need one huge slice.
			last := after + 256
			events, err := s.board.History(tenant, after+1, last)
			if err != nil {
				return err
			}
			if len(events) == 0 {
				break
			}
			for _, event := range events {
				if err := s.deliver(ctx, tenant, key, event); err != nil {
					if boardsync.IsPermanent(err) {
						s.logger.Error("sync subscription event is not deliverable",
							"tenant", tenant, "key", key, "event", event.Seq, "error", syncstore.ErrorCode(err))
						after = event.Seq
						if cursorErr := s.store.SetCursor(ctx, tenant, key, boardEventsSource, strconv.FormatUint(after, 10)); cursorErr != nil {
							return cursorErr
						}
						continue
					}
					// Retry the same event after a pause; the cursor hasn't
					// moved, and the outbox dedupes anything that did land.
					s.logger.Warn("sync subscription pass failed",
						"tenant", tenant, "key", key, "event", event.Seq, "error", syncstore.ErrorCode(err))
					select {
					case <-ctx.Done():
						return nil
					case <-time.After(time.Second):
					}
					break
				}
				after = event.Seq
				if err := s.store.SetCursor(ctx, tenant, key, boardEventsSource, strconv.FormatUint(after, 10)); err != nil {
					return err
				}
			}
			if len(events) < 256 {
				break
			}
		}
	}
}

// loadCursor reads the durable position. A new connection starts at the
// current head: a deployment doesn't dump a board's history into platforms.
func (s *syncService) loadCursor(ctx context.Context, tenant string, key boardsync.Key) (uint64, error) {
	raw, ok, err := s.store.Cursor(ctx, tenant, key, boardEventsSource)
	if err != nil {
		return 0, err
	}
	if ok {
		value, parseErr := strconv.ParseUint(raw, 10, 64)
		if parseErr != nil {
			return 0, fmt.Errorf("server: sync cursor for %q is not a sequence: %v", key, parseErr)
		}
		return value, nil
	}
	head, err := s.board.Head(tenant)
	if err != nil {
		return 0, err
	}
	// Persist even a zero cursor: it distinguishes "this connection started
	// on an empty log" from "no process has ever served it", so a restart
	// replays events that arrived while no worker was running.
	if err := s.store.SetCursor(ctx, tenant, key, boardEventsSource, strconv.FormatUint(head, 10)); err != nil {
		return 0, err
	}
	return head, nil
}

// deliver turns one board log event into outbox jobs and, for a new board
// root, offers it to the connections that can start threads.
func (s *syncService) deliver(ctx context.Context, tenant string, key boardsync.Key, event log.Event) error {
	eventType, ok := deliverableLogEvent(event.Type)
	if !ok {
		// Receipts (thread.synced, message.synced, files_synced, sync_failed),
		// task events and actor facts are not deliveries.
		return nil
	}
	var message *log.Message
	var root *log.Message
	if err := s.board.Read(tenant, func(snapshot *log.Snapshot) error {
		message = snapshot.Message(log.ConcernedMessageID(event))
		if message != nil {
			root = snapshot.RootOf(message.ID)
		}
		return nil
	}); err != nil {
		return err
	}
	if message == nil || root == nil {
		return nil
	}
	rendered, err := s.host.messageFor(tenant, message)
	if err != nil {
		return err
	}
	boardEvent := boardsync.BoardEvent{
		Tenant: tenant, ID: event.Seq, Type: eventType, Thread: root.ID, Actor: rendered.Actor,
	}
	if payload, ok := event.Payload.(*log.ReactionPayload); ok {
		boardEvent.Emoji = payload.Emoji
	}

	threads, err := s.store.Threads(ctx, tenant, root.ID)
	if err != nil {
		return err
	}
	targets := deliveryTargets(threads, key, message.ID == root.ID)
	if len(targets) == 0 {
		// A new root may still start a thread.
		return s.offerRoot(ctx, tenant, key, boardEvent, rendered, message, root)
	}
	if message.ID == root.ID {
		if err := s.offerRoot(ctx, tenant, key, boardEvent, rendered, message, root); err != nil {
			return err
		}
	}
	refs := make([]boardsync.ThreadRef, 0, len(targets))
	for _, target := range targets {
		refs = append(refs, target.Ref())
	}
	kept := boardsync.TargetsForDelivery(rendered, refs)
	if len(kept) == 0 {
		return nil
	}
	allowed := make(map[string]struct{}, len(kept))
	for _, ref := range kept {
		allowed[ref.External()] = struct{}{}
	}
	final := make([]boardsync.ResolvedTarget, 0, len(kept))
	for _, target := range targets {
		if _, ok := allowed[target.External()]; ok {
			final = append(final, target)
		}
	}
	if len(final) == 0 {
		return nil
	}
	_, err = s.store.EnqueueOutbox(ctx, tenant, key, boardEvent, rendered, final)
	return err
}

// offerRoot offers a new board root to each configured platform channel. A
// non-permanent failure is retried by the subscription; a permanent one (a
// channel the platform can't start, a mapping mismatch) is skipped.
func (s *syncService) offerRoot(ctx context.Context, tenant string, key boardsync.Key, event boardsync.BoardEvent, message boardsync.Message, logMessage, root *log.Message) error {
	if logMessage.ID != root.ID || message.From != "" || message.NoSync {
		return nil
	}
	channels := s.startChannels[key]
	if len(channels) == 0 {
		return nil
	}
	entry, ok := s.registry.Get(tenant, key)
	if !ok || !entry.Provider.Capabilities().StartThreads {
		return nil
	}
	for _, channel := range channels {
		if strings.TrimSpace(channel) == "" {
			continue
		}
		if err := s.runner.OfferRoot(ctx, entry, event, message, channel); err != nil {
			if boardsync.IsPermanent(err) {
				s.logger.Warn("sync root didn't start a thread",
					"tenant", tenant, "key", key, "message", message.ID,
					"channel", channel, "error", syncstore.ErrorCode(err))
				continue
			}
			return err
		}
	}
	return nil
}

// deliveryTargets turns a board thread's mappings into targets for one key.
// A reply never starts a new platform thread, and a concrete thread target
// replaces its channel target for the same platform channel.
func deliveryTargets(threads []syncstore.Thread, key boardsync.Key, isRoot bool) []boardsync.ResolvedTarget {
	mine := make([]boardsync.ResolvedTarget, 0, len(threads))
	hasThread := make(map[string]struct{})
	for _, thread := range threads {
		if thread.Key != key {
			continue
		}
		target := boardsync.ResolvedTarget{
			Key: thread.Key, Platform: thread.Platform, Channel: thread.Channel,
			Thread: thread.Thread, Subthread: thread.Subthread, Kind: thread.Kind,
		}
		if err := target.Validate(); err != nil {
			continue
		}
		if target.Kind == boardsync.TargetThread {
			hasThread[string(target.Platform)+"|"+target.Channel] = struct{}{}
		}
		mine = append(mine, target)
	}
	out := make([]boardsync.ResolvedTarget, 0, len(mine))
	seen := make(map[string]struct{}, len(mine))
	for _, target := range mine {
		if !isRoot && target.Kind == boardsync.TargetChannel {
			continue
		}
		if target.Kind == boardsync.TargetChannel {
			if _, concrete := hasThread[string(target.Platform)+"|"+target.Channel]; concrete {
				continue
			}
		}
		identity := string(target.Key) + "\x00" + string(target.Platform) + "\x00" +
			target.Channel + "\x00" + target.Thread + "\x00" + target.Subthread + "\x00" + string(target.Kind)
		if _, duplicate := seen[identity]; duplicate {
			continue
		}
		seen[identity] = struct{}{}
		out = append(out, target)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind == boardsync.TargetThread
		}
		return out[i].Thread+out[i].Channel < out[j].Thread+out[j].Channel
	})
	return out
}

// deliverableLogEvent maps a board log event to the sync fact a platform
// receives. Receipts and task bookkeeping are not deliverable.
func deliverableLogEvent(eventType log.EventType) (boardsync.BoardEventType, bool) {
	switch eventType {
	case log.EventMessagePosted:
		return boardsync.BoardMessagePosted, true
	case log.EventMessageEdited:
		return boardsync.BoardMessageEdited, true
	case log.EventMessageDeleted:
		return boardsync.BoardMessageDeleted, true
	case log.EventLabelsChanged:
		return boardsync.BoardLabelsChanged, true
	case log.EventReactionAdded:
		return boardsync.BoardReactionAdded, true
	case log.EventReactionRemoved:
		return boardsync.BoardReactionRemove, true
	default:
		return "", false
	}
}

// syncKeyFromRequest reads and validates a `key` query parameter.
func syncKeyFromRequest(r *http.Request) (boardsync.Key, *protocol.Error) {
	value, ok := queryString(r, "key")
	if !ok || value == "" {
		return "", protocol.Invalid("key: a sync connection key is required")
	}
	key := boardsync.Key(value)
	if !key.Valid() {
		return "", protocol.Invalidf("key: %q isn't a sync key", value)
	}
	return key, nil
}

// Sync management views. None carries a platform credential (there are none in
// the store), a message body, or another tenant's rows.

type syncEntryView struct {
	Key       string `json:"key"`
	Platform  string `json:"platform"`
	Enabled   bool   `json:"enabled"`
	Owned     bool   `json:"owned"`
	CreatedAt string `json:"createdAt"`
	UpdatedAt string `json:"updatedAt"`
}

type syncKeyStatus struct {
	syncEntryView
	Inbox syncstore.InboxDepth `json:"inbox"`
}

type syncStatusView struct {
	Keys   []syncKeyStatus       `json:"keys"`
	Outbox syncstore.OutboxDepth `json:"outbox"`
}

type syncInboxView struct {
	ID       string `json:"id"`
	Key      string `json:"key"`
	Thread   string `json:"thread"`
	Attempts int    `json:"attempts"`
}

type syncFailureView struct {
	Delivery string `json:"delivery"`
	Message  string `json:"message"`
	Target   string `json:"target"`
	Attempts int    `json:"attempts"`
	Error    string `json:"error"`
}

type syncThreadView struct {
	BoardThread string `json:"boardThread"`
	Key         string `json:"key"`
	Platform    string `json:"platform"`
	Channel     string `json:"channel"`
	Thread      string `json:"thread,omitempty"`
	Subthread   string `json:"subthread,omitempty"`
	Kind        string `json:"kind"`
	Target      string `json:"target"`
}

type syncAttachRequest struct {
	Key    string `json:"key"`
	Thread string `json:"thread"`
	Target string `json:"target"`
}

type syncAttachView struct {
	syncThreadView
	ID string `json:"id"`
}

// permitSyncManage gates the sync management surface on manage at *.
func (req *request) permitSyncManage(s *Server) *protocol.Error {
	return req.permit(s, "*", protocol.PermissionManage)
}

// handleSyncEntries answers GET /sync/entries.
func (s *Server) handleSyncEntries(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if perr := req.permitSyncManage(s); perr != nil {
		writeError(w, perr)
		return
	}
	entries, err := s.sync.store.Entries(req.ctx, req.tenant)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	views := make([]syncEntryView, 0, len(entries))
	for _, entry := range entries {
		views = append(views, syncEntryView{
			Key: string(entry.Key), Platform: string(entry.Platform), Enabled: entry.Enabled,
			Owned:     s.sync.owns(req.tenant, entry.Key),
			CreatedAt: protocol.FormatTime(entry.CreatedAt), UpdatedAt: protocol.FormatTime(entry.UpdatedAt),
		})
	}
	sort.Slice(views, func(i, j int) bool { return views[i].Key < views[j].Key })
	writeJSON(w, http.StatusOK, map[string]any{"entries": views})
}

// handleSyncStatus answers GET /sync/status: entries with their inbox depth
// and the tenant's outbox depth.
func (s *Server) handleSyncStatus(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if perr := req.permitSyncManage(s); perr != nil {
		writeError(w, perr)
		return
	}
	entries, err := s.sync.store.Entries(req.ctx, req.tenant)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	status := syncStatusView{Keys: make([]syncKeyStatus, 0, len(entries))}
	for _, entry := range entries {
		depth, err := s.sync.store.InboxDepth(req.ctx, req.tenant, entry.Key)
		if err != nil {
			s.fail(w, r, err)
			return
		}
		status.Keys = append(status.Keys, syncKeyStatus{
			syncEntryView: syncEntryView{
				Key: string(entry.Key), Platform: string(entry.Platform), Enabled: entry.Enabled,
				Owned:     s.sync.owns(req.tenant, entry.Key),
				CreatedAt: protocol.FormatTime(entry.CreatedAt), UpdatedAt: protocol.FormatTime(entry.UpdatedAt),
			},
			Inbox: depth,
		})
	}
	sort.Slice(status.Keys, func(i, j int) bool { return status.Keys[i].Key < status.Keys[j].Key })
	status.Outbox, err = s.sync.store.OutboxDepth(req.ctx, req.tenant)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, status)
}

// handleSyncInbox answers GET /sync/inbox?key=&state=dead: dead-lettered
// inbound events, without their payloads or text.
func (s *Server) handleSyncInbox(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if perr := req.permitSyncManage(s); perr != nil {
		writeError(w, perr)
		return
	}
	key, perr := syncKeyFromRequest(r)
	if perr != nil {
		writeError(w, perr)
		return
	}
	state, hasState := queryString(r, "state")
	if hasState && state != "dead" {
		writeError(w, protocol.Invalid("state: only dead letters are listed"))
		return
	}
	limit, perr := queryInt(r, "limit", 100, 1, 500)
	if perr != nil {
		writeError(w, perr)
		return
	}
	items, err := s.sync.store.DeadInbox(req.ctx, req.tenant, key, limit)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	views := make([]syncInboxView, 0, len(items))
	for _, item := range items {
		views = append(views, syncInboxView{
			ID: item.ID, Key: string(item.Key), Thread: item.Thread, Attempts: item.Attempts,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"dead": views})
}

// handleSyncFailures answers GET /sync/outbox/failures?key=: deliveries that
// gave up, by message and target, without the message text.
func (s *Server) handleSyncFailures(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if perr := req.permitSyncManage(s); perr != nil {
		writeError(w, perr)
		return
	}
	key, perr := syncKeyFromRequest(r)
	if perr != nil {
		writeError(w, perr)
		return
	}
	limit, perr := queryInt(r, "limit", 100, 1, 500)
	if perr != nil {
		writeError(w, perr)
		return
	}
	failures, err := s.sync.store.TargetFailures(req.ctx, req.tenant, key, limit)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	views := make([]syncFailureView, 0, len(failures))
	for _, failure := range failures {
		views = append(views, syncFailureView{
			Delivery: failure.ID, Message: protocol.FormatRef(failure.Message.ID),
			Target: failure.Target.External(), Attempts: failure.Attempts, Error: failure.LastError,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"failures": views})
}

// handleSyncRetryInbox answers POST /sync/inbox/{id}/retry?key=.
func (s *Server) handleSyncRetryInbox(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if perr := req.permitSyncManage(s); perr != nil {
		writeError(w, perr)
		return
	}
	key, perr := syncKeyFromRequest(r)
	if perr != nil {
		writeError(w, perr)
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeError(w, protocol.Invalid("id: an inbox event ID is required"))
		return
	}
	if err := s.sync.store.RetryInbox(req.ctx, req.tenant, key, id); err != nil {
		if errors.Is(err, syncstore.ErrNotDead) {
			writeError(w, protocol.NotFound(protocol.CodeNotFound, "no dead inbox event with that id"))
			return
		}
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"retried": true})
}

// handleSyncRetryOutbox answers POST /sync/outbox/{id}/retry.
func (s *Server) handleSyncRetryOutbox(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if perr := req.permitSyncManage(s); perr != nil {
		writeError(w, perr)
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeError(w, protocol.Invalid("id: a delivery ID is required"))
		return
	}
	if err := s.sync.store.RetryOutbox(req.ctx, req.tenant, id); err != nil {
		if errors.Is(err, syncstore.ErrNotDead) {
			writeError(w, protocol.NotFound(protocol.CodeNotFound, "no dead delivery with that id"))
			return
		}
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"retried": true})
}

// handleSyncThreads answers GET /sync/threads?thread=: one board thread's
// attachments.
func (s *Server) handleSyncThreads(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if perr := req.permitSyncManage(s); perr != nil {
		writeError(w, perr)
		return
	}
	value, has := queryString(r, "thread")
	if !has {
		writeError(w, protocol.Invalid("thread: a board thread or message ref is required"))
		return
	}
	id, valid := protocol.ParseRef(value)
	if !valid || id == 0 {
		writeError(w, protocol.NotFound(protocol.CodeMessageNotFound, fmt.Sprintf("message not found: #%s", value)))
		return
	}
	var root *log.Message
	if err := s.board.Read(req.tenant, func(snapshot *log.Snapshot) error {
		root = snapshot.RootOf(id)
		return nil
	}); err != nil {
		s.fail(w, r, err)
		return
	}
	if root == nil || !req.can(s, root.Channel, protocol.PermissionRead) {
		writeError(w, protocol.NotFound(protocol.CodeMessageNotFound, fmt.Sprintf("message not found: #%s", value)))
		return
	}
	threads, err := s.sync.store.Threads(req.ctx, req.tenant, root.ID)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	views := make([]syncThreadView, 0, len(threads))
	for _, thread := range threads {
		views = append(views, threadView(thread))
	}
	writeJSON(w, http.StatusOK, map[string]any{"threads": views})
}

// handleSyncAttach answers POST /sync/threads: attach a board thread to a
// platform target. A channel target starts a thread and backfills; a thread
// target receives only what's written after it.
func (s *Server) handleSyncAttach(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if perr := req.permitSyncManage(s); perr != nil {
		writeError(w, perr)
		return
	}
	var body syncAttachRequest
	if perr := decodeBody(r, &body, true, false); perr != nil {
		writeError(w, perr)
		return
	}
	key := boardsync.Key(strings.TrimSpace(body.Key))
	if !key.Valid() {
		writeError(w, protocol.Invalid("key: a sync connection key is required"))
		return
	}
	entry, found := s.sync.registry.Get(req.tenant, key)
	if !found {
		// A key this tenant doesn't run is indistinguishable from a missing
		// target.
		writeError(w, protocol.NotFound(protocol.CodeNotFound, "no such sync connection"))
		return
	}
	if strings.TrimSpace(body.Target) == "" {
		writeError(w, protocol.Invalid("target: a platform channel or thread is required"))
		return
	}
	target, err := entry.Provider.ResolveTarget(req.ctx, key, strings.TrimSpace(body.Target))
	if err != nil {
		writeError(w, syncFailureFor(err))
		return
	}
	if target.Key != key || target.Platform != entry.Platform {
		writeError(w, protocol.Invalid("target: resolves outside this sync connection"))
		return
	}
	if err := target.Validate(); err != nil {
		writeError(w, protocol.Invalid("target: "+err.Error()))
		return
	}
	id, valid := protocol.ParseRef(strings.TrimSpace(body.Thread))
	if !valid || id == 0 {
		writeError(w, protocol.NotFound(protocol.CodeMessageNotFound, fmt.Sprintf("message not found: #%s", body.Thread)))
		return
	}
	var root *log.Message
	if err := s.board.Read(req.tenant, func(snapshot *log.Snapshot) error {
		root = snapshot.RootOf(id)
		return nil
	}); err != nil {
		s.fail(w, r, err)
		return
	}
	if root == nil || root.Deleted || !req.can(s, root.Channel, protocol.PermissionRead) {
		// Another tenant's thread, a missing one, and one the caller can't
		// read are the same 404.
		writeError(w, protocol.NotFound(protocol.CodeMessageNotFound, fmt.Sprintf("message not found: #%s", body.Thread)))
		return
	}
	if target.Kind == boardsync.TargetChannel && !entry.Provider.Capabilities().StartThreads {
		writeError(w, protocol.Invalidf("%s can't start a thread in a channel", entry.Platform))
		return
	}
	if target.Kind == boardsync.TargetThread {
		existing, found, err := s.sync.store.ThreadByTarget(req.ctx, req.tenant, key, target)
		if err != nil {
			s.fail(w, r, err)
			return
		}
		if found && existing.BoardThread != root.ID {
			writeError(w, protocol.Conflict(protocol.CodeDuplicateID, "that platform thread is already attached to another board thread"))
			return
		}
	}
	ref := target.Ref()
	if err := s.sync.host.Record(req.ctx, boardsync.BoardEvent{
		Tenant: req.tenant, Type: boardsync.BoardThreadSynced,
		Thread: root.ID, Target: &ref, Cause: "attach",
	}); err != nil {
		s.fail(w, r, err)
		return
	}
	thread, found, err := s.sync.store.ThreadByTarget(req.ctx, req.tenant, key, target)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if !found {
		thread = syncstore.Thread{
			Tenant: req.tenant, BoardThread: root.ID, Key: key, Platform: target.Platform,
			Channel: target.Channel, Thread: target.Thread, Subthread: target.Subthread, Kind: target.Kind,
		}
	}
	view := syncAttachView{syncThreadView: threadView(thread), ID: thread.ID}
	writeJSON(w, http.StatusOK, view)
}

// syncFailureFor maps a provider failure to a board error without copying a
// platform's body or a redaction-only message into the response.
func syncFailureFor(err error) *protocol.Error {
	switch boardsync.StatusCode(err) {
	case http.StatusBadRequest:
		return protocol.Invalid("target: the platform target isn't usable")
	case http.StatusNotFound:
		return protocol.NotFound(protocol.CodeNotFound, "no such platform target")
	case http.StatusUnauthorized:
		return protocol.NewError(protocol.CodeUnauthorized, "the sync connection can't read that platform target")
	case http.StatusTooManyRequests:
		return protocol.NewError(protocol.CodeUnavailable, "the platform is rate limiting; try again")
	default:
		return protocol.NewError(protocol.CodeUnavailable, "the platform didn't answer")
	}
}

// threadView renders one mapping, with the key-prefixed external target.
func threadView(thread syncstore.Thread) syncThreadView {
	target := boardsync.ResolvedTarget{
		Key: thread.Key, Platform: thread.Platform, Channel: thread.Channel,
		Thread: thread.Thread, Subthread: thread.Subthread, Kind: thread.Kind,
	}
	return syncThreadView{
		BoardThread: protocol.FormatRef(thread.BoardThread), Key: string(thread.Key),
		Platform: string(thread.Platform), Channel: thread.Channel,
		Thread: thread.Thread, Subthread: thread.Subthread, Kind: string(thread.Kind),
		Target: target.External(),
	}
}

// owns reports whether this process serves one connection's workers.
func (s *syncService) owns(tenant string, key boardsync.Key) bool {
	for _, owned := range s.owned {
		if owned.Tenant == tenant && owned.Key == key {
			return true
		}
	}
	return false
}
