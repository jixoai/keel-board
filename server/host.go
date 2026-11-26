package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"

	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/syncstore"
)

// SyncHostOptions bound what an import may put on the board. The board never
// fetches a platform URL; these limits decide what metadata is kept.
type SyncHostOptions struct {
	// FileHosts is the allowlist of hosts a file or link URL may name. Empty
	// refuses every URL (the file or link is kept without one).
	FileHosts []string
	// MaxFileBytes bounds one file's declared size. Zero means 50 MiB.
	MaxFileBytes int64
	// MaxFiles bounds one message's files. Zero means 32.
	MaxFiles int
	// MaxLinks bounds one message's links. Zero means 32.
	MaxLinks int
	// Logger receives dropped-input summaries. Zero uses slog.Default.
	Logger *slog.Logger
}

// SyncHost implements boardsync.Host over the board log. It is the only place
// an inbound platform event becomes board facts, so it owns:
//
//   - idempotent imports, keyed by the inbox event ID;
//   - echo suppression (own markers are fail-closed, other markers stripped);
//   - safe file and link placeholders, with no network fetch;
//   - receipts projected onto the message they concern.
type SyncHost struct {
	board        *log.Board
	store        syncstore.Store
	fileHosts    map[string]struct{}
	maxFileBytes int64
	maxFiles     int
	maxLinks     int
	logger       *slog.Logger
}

// NewSyncHost returns a host over board and store.
func NewSyncHost(board *log.Board, store syncstore.Store, opts SyncHostOptions) *SyncHost {
	if opts.MaxFileBytes <= 0 {
		opts.MaxFileBytes = 50 << 20
	}
	if opts.MaxFiles <= 0 {
		opts.MaxFiles = 32
	}
	if opts.MaxLinks <= 0 {
		opts.MaxLinks = 32
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	hosts := make(map[string]struct{}, len(opts.FileHosts))
	for _, host := range opts.FileHosts {
		host = strings.ToLower(strings.TrimSpace(host))
		if host != "" {
			hosts[host] = struct{}{}
		}
	}
	return &SyncHost{
		board: board, store: store, fileHosts: hosts,
		maxFileBytes: opts.MaxFileBytes, maxFiles: opts.MaxFiles, maxLinks: opts.MaxLinks,
		logger: opts.Logger,
	}
}

var _ boardsync.Host = (*SyncHost)(nil)

// Import applies one inbound platform event. It is idempotent by the inbox
// event ID: a retry finds the first write's messages and returns their IDs.
func (h *SyncHost) Import(ctx context.Context, req boardsync.ImportRequest) (boardsync.ImportResult, error) {
	if h == nil || h.board == nil || h.store == nil {
		return boardsync.ImportResult{}, errors.New("server: sync host is not configured")
	}
	if !boardsync.ValidTenant(req.Tenant) {
		return boardsync.ImportResult{}, fmt.Errorf("%w: import has no usable tenant", boardsync.ErrBadRequest)
	}
	if !req.Key.Valid() {
		return boardsync.ImportResult{}, fmt.Errorf("%w: %q isn't a sync key", boardsync.ErrBadRequest, req.Key)
	}
	event := req.Event
	if event.Key == "" {
		event.Key = req.Key
	}
	if event.Key != req.Key {
		return boardsync.ImportResult{}, fmt.Errorf("%w: event key %q isn't the import key %q", boardsync.ErrBadRequest, event.Key, req.Key)
	}
	if err := event.Validate(); err != nil {
		return boardsync.ImportResult{}, err
	}
	event, dropped := h.sanitize(event)
	if dropped {
		return boardsync.ImportResult{}, nil
	}
	switch event.Kind {
	case boardsync.KindMessage:
		return h.importMessage(ctx, req, event)
	case boardsync.KindEdited:
		return h.importEdit(ctx, req, event)
	case boardsync.KindDeleted:
		return h.importDelete(ctx, req, event)
	case boardsync.KindReaction:
		return h.importReaction(ctx, req, event)
	case boardsync.KindLifecycle:
		return h.importLifecycle(ctx, req, event)
	default:
		return boardsync.ImportResult{}, fmt.Errorf("%w: sync can't import %q", boardsync.ErrUnsupported, event.Kind)
	}
}

// sanitize enforces echo suppression and safe input. A dropped event is our
// own copy (a matching marker) or has nothing left to import.
func (h *SyncHost) sanitize(event boardsync.Event) (boardsync.Event, bool) {
	text := event.Text
	if boardsync.IsOwnMarker(text, event.Key) {
		// Fail closed: losing a copied comment is better than echoing our own
		// post back onto the board.
		return event, true
	}
	text = boardsync.StripMarker(text)
	event.Text = boardsync.StorableText(text, boardsync.MaxTextBytes)
	event.Files = h.safeFiles(event.Files)
	if event.Kind == boardsync.KindMessage && event.Text == "" && len(event.Files) == 0 {
		return event, true
	}
	return event, false
}

// safeFiles keeps at most MaxFiles files, with bounded sizes and text. A URL
// that isn't an allowed https host is dropped from the file, not fetched.
func (h *SyncHost) safeFiles(files []boardsync.File) []boardsync.File {
	if len(files) == 0 {
		return nil
	}
	out := make([]boardsync.File, 0, len(files))
	for _, file := range files {
		if len(out) >= h.maxFiles {
			break
		}
		if file.Size < 0 || file.Size > h.maxFileBytes {
			continue
		}
		file.Type = boardsync.StorableText(file.Type, 128)
		file.Name = boardsync.StorableText(file.Name, 200)
		file.MIMEType = boardsync.StorableText(file.MIMEType, 200)
		file.FetchMetadata = nil
		originalURL := file.URL
		file.URL = safeURL(originalURL, h.fileHosts)
		if strings.TrimSpace(originalURL) != "" && file.URL == "" {
			// A URL that isn't an allowed https host makes the file
			// unimportable: keeping it without a handle would only mislead.
			continue
		}
		out = append(out, file)
	}
	return out
}

// safeLinks keeps at most MaxLinks links. A link whose URL isn't https on an
// allowed host is kept as an unavailable placeholder with no URL.
func (h *SyncHost) safeLinks(links []boardsync.Link) []protocol.LinkView {
	if len(links) == 0 {
		return nil
	}
	out := make([]protocol.LinkView, 0, len(links))
	seen := make(map[string]struct{}, len(links))
	for _, link := range links {
		if len(out) >= h.maxLinks {
			break
		}
		safe := safeURL(link.URL, h.fileHosts)
		if safe == "" {
			out = append(out, protocol.LinkView{Unavailable: "external link"})
			continue
		}
		if _, duplicate := seen[safe]; duplicate {
			continue
		}
		seen[safe] = struct{}{}
		view := protocol.LinkView{URL: safe}
		if link.Message != "" {
			if len(link.Message) > 300 {
				link.Message = link.Message[:300]
			}
			view.Message = &link.Message
		}
		out = append(out, view)
	}
	return out
}

// safeURL returns url if it is an absolute https URL on an allowed host
// without credentials. An empty allowlist refuses every URL.
func safeURL(raw string, hosts map[string]struct{}) string {
	raw = strings.TrimSpace(raw)
	if raw == "" || len(raw) > 2048 || len(hosts) == 0 {
		return ""
	}
	parsed, err := url.Parse(raw)
	if err != nil || !strings.EqualFold(parsed.Scheme, "https") || parsed.Host == "" {
		return ""
	}
	if parsed.User != nil {
		return ""
	}
	host := strings.ToLower(parsed.Hostname())
	if _, allowed := hosts[host]; !allowed {
		return ""
	}
	return raw
}

// importMessage creates a thread root or a reply. Root creation is serialized
// per external thread and keyed by the platform thread, so two concurrent
// events cannot create two roots; the loser imports as a reply.
func (h *SyncHost) importMessage(ctx context.Context, req boardsync.ImportRequest, event boardsync.Event) (boardsync.ImportResult, error) {
	mapping, mapped, err := h.lookupThread(ctx, req.Tenant, req.Key, event)
	if err != nil {
		return boardsync.ImportResult{}, err
	}
	if mapped {
		return h.importReply(ctx, req, event, mapping)
	}
	if req.Channel == "" {
		// A reply on a thread the board doesn't sync has nowhere to land.
		return boardsync.ImportResult{}, nil
	}
	// Serialize root creation across processes for this external thread.
	lockTarget := boardsync.ResolvedTarget{
		Key: req.Key, Platform: event.Platform, Channel: event.Channel,
		Thread: event.Thread, Kind: boardsync.TargetThread,
	}
	release, err := h.store.LockTarget(ctx, req.Tenant, req.Key, lockTarget)
	if err != nil {
		return boardsync.ImportResult{}, err
	}
	defer release()
	// Another process may have created the root while this one waited.
	mapping, mapped, err = h.lookupThread(ctx, req.Tenant, req.Key, event)
	if err != nil {
		return boardsync.ImportResult{}, err
	}
	if mapped {
		return h.importReply(ctx, req, event, mapping)
	}
	return h.importRoot(ctx, req, event)
}

// importRoot creates the board root for an external thread. Its idempotency
// key is the thread, not the event, so a duplicate root delivery replays the
// same board message instead of creating a second one.
func (h *SyncHost) importRoot(ctx context.Context, req boardsync.ImportRequest, event boardsync.Event) (boardsync.ImportResult, error) {
	text, labels, err := overrideRoot(req, event, event.Text)
	if err != nil {
		return boardsync.ImportResult{}, err
	}
	attachments := attachmentRefs(event.Files)
	links := h.safeLinks(event.Links)
	origin := originOf(event)
	from := string(req.Key)
	rootKey := syncRootKey(req, event)
	rootDigest := rootDigest(req, event)
	var result boardsync.ImportResult
	needsReply := false
	err = h.board.Update(ctx, req.Tenant, log.UpdateOptions{
		Idempotency: &log.Idempotency{Key: rootKey, Digest: rootDigest},
	}, func(tx *log.Tx) error {
		if replay, ok := tx.Replay(); ok {
			rootID := lastMessageID(replay.MessageIDs)
			if rootID == 0 {
				return fmt.Errorf("%w: root replay has no message", boardsync.ErrBadRequest)
			}
			result.Root, result.Message = rootID, rootID
			if rootIsEvent(tx.Message(rootID), event, req.Key) {
				result.CreatedRoot = true
				return nil
			}
			// A different event lost the root race; it becomes a reply.
			needsReply = true
			return nil
		}
		offset := h.emitActorSeen(tx, event)
		id := tx.NextID(offset)
		tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
			ID: id, Channel: req.Channel, Text: text,
			Thread: log.ThreadKey(id), Labels: nonNilLabels(labels), NoSync: false,
			By: boardsyncAuthor(event, req.Key), From: &from, Origin: origin,
			Attachments: attachments, Links: links,
		})
		tx.RecordResult(id)
		result = boardsync.ImportResult{Root: id, Message: id, CreatedRoot: true}
		return nil
	})
	if err != nil {
		return boardsync.ImportResult{}, err
	}
	if needsReply {
		mapping, found, lookupErr := h.lookupThread(ctx, req.Tenant, req.Key, event)
		if lookupErr != nil {
			return boardsync.ImportResult{}, lookupErr
		}
		if !found {
			// The winner may not have written its mapping yet; this side can
			// record the same mapping, keyed by the root it replayed.
			targets, targetErr := h.threadTargetsFor(req, event)
			if targetErr != nil {
				return boardsync.ImportResult{}, targetErr
			}
			for _, target := range targets {
				mapping, err = h.store.UpsertThread(ctx, syncstore.Thread{
					Tenant: req.Tenant, BoardThread: result.Root, Key: req.Key,
					Platform: target.Platform, Channel: target.Channel,
					Thread: target.Thread, Subthread: target.Subthread, Kind: target.Kind,
					Since: result.Root,
				})
				if err != nil {
					return boardsync.ImportResult{}, err
				}
			}
		}
		return h.importReply(ctx, req, event, mapping)
	}
	targets, err := h.threadTargetsFor(req, event)
	if err != nil {
		return result, err
	}
	for _, target := range targets {
		if _, err := h.store.UpsertThread(ctx, syncstore.Thread{
			Tenant: req.Tenant, BoardThread: result.Root, Key: req.Key,
			Platform: target.Platform, Channel: target.Channel,
			Thread: target.Thread, Subthread: target.Subthread, Kind: target.Kind,
			Since: result.Root,
		}); err != nil {
			return result, err
		}
	}
	if err := h.storeCopy(ctx, req, event, result); err != nil {
		return result, err
	}
	return result, nil
}

// importReply appends one imported reply to an existing board thread.
func (h *SyncHost) importReply(ctx context.Context, req boardsync.ImportRequest, event boardsync.Event, mapping syncstore.Thread) (boardsync.ImportResult, error) {
	root, err := h.rootMessage(ctx, req.Tenant, mapping.BoardThread)
	if err != nil {
		return boardsync.ImportResult{}, err
	}
	if rootIsEvent(root, event, req.Key) {
		// A retried root delivery finds its own root and its mapping; replay
		// it instead of appending the root text as a reply.
		return boardsync.ImportResult{Root: root.ID, Message: root.ID, CreatedRoot: true}, nil
	}
	text := overrideText(req, event.Text)
	attachments := attachmentRefs(event.Files)
	links := h.safeLinks(event.Links)
	origin := originOf(event)
	from := string(req.Key)
	noSync := req.Overrides.NoSync
	idempotency := &log.Idempotency{Key: syncImportKey(req), Digest: importDigest(req)}
	var result boardsync.ImportResult
	err = h.board.Update(ctx, req.Tenant, log.UpdateOptions{Idempotency: idempotency}, func(tx *log.Tx) error {
		if replay, ok := tx.Replay(); ok {
			result.Message = lastMessageID(replay.MessageIDs)
			result.Root = mapping.BoardThread
			return nil
		}
		current := tx.Message(root.ID)
		if current == nil {
			return fmt.Errorf("%w: board thread %d is missing", boardsync.ErrNotFound, root.ID)
		}
		offset := h.emitActorSeen(tx, event)
		offset += emitLabelChanges(tx, event, current, req.Overrides.Labels, req.Overrides.Unset)
		id := tx.NextID(offset)
		tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
			ID: id, Channel: current.Channel, Text: text,
			Re: uint64Ptr(current.ID), Thread: current.Thread,
			Labels: map[string]string{}, NoSync: noSync,
			By: boardsyncAuthor(event, req.Key), From: &from, Origin: origin,
			Attachments: attachments, Links: links,
		})
		tx.RecordResult(id)
		result = boardsync.ImportResult{Root: current.ID, Message: id}
		return nil
	})
	if err != nil {
		return boardsync.ImportResult{}, err
	}
	if result.Message != 0 {
		if err := h.storeCopy(ctx, req, event, result); err != nil {
			return result, err
		}
	}
	return result, nil
}

// rootIsEvent reports whether the root was created by this platform event.
func rootIsEvent(root *log.Message, event boardsync.Event, key boardsync.Key) bool {
	if root == nil || root.From == nil || *root.From != string(key) || root.Origin == nil {
		return false
	}
	return root.Origin.Message == event.Message && root.Origin.Thread == keyedThread(key, event)
}

// overrideRoot shapes the text and labels a root is created with.
func overrideRoot(req boardsync.ImportRequest, event boardsync.Event, text string) (string, map[string]string, error) {
	labels := sanitizeLabels(req.Overrides.Labels)
	draft := boardsync.RootDraft{Text: text, Labels: labels, Author: event.Author, Raw: event.Raw}
	if req.Overrides.Root != nil {
		patch, err := req.Overrides.Root(draft)
		if err != nil {
			return "", nil, fmt.Errorf("%w: root override: %v", boardsync.ErrBadRequest, err)
		}
		if patch.Text != nil {
			text = *patch.Text
		}
		for key, value := range patch.Labels {
			if clean, ok := cleanLabel(key, value); ok {
				labels[key] = clean
			}
		}
	}
	for _, key := range req.Overrides.Unset {
		delete(labels, key)
	}
	if req.Overrides.Text != nil {
		text = *req.Overrides.Text
	}
	text = boardsync.StorableText(text, boardsync.MaxTextBytes)
	return text, labels, nil
}

// overrideText returns the text an imported message carries.
func overrideText(req boardsync.ImportRequest, text string) string {
	if req.Overrides.Text != nil {
		text = *req.Overrides.Text
	}
	return boardsync.StorableText(text, boardsync.MaxTextBytes)
}

// emitLabelChanges appends a labels.changed for a root, and returns how many
// events it used.
func emitLabelChanges(tx *log.Tx, event boardsync.Event, root *log.Message, set map[string]string, unset []string) int {
	if len(set) == 0 && len(unset) == 0 {
		return 0
	}
	clean := sanitizeLabels(set)
	if len(clean) == 0 && len(unset) == 0 {
		return 0
	}
	tx.Emit(log.EventLabelsChanged, &log.LabelsChangedPayload{
		ID: root.ID, Thread: root.Thread, Set: clean, Unset: append([]string(nil), unset...),
		By: boardsyncAuthor(event, event.Key),
	})
	return 1
}

// importEdit applies a platform edit to the board copy it names.
func (h *SyncHost) importEdit(ctx context.Context, req boardsync.ImportRequest, event boardsync.Event) (boardsync.ImportResult, error) {
	copy, found, err := h.lookupCopy(ctx, req, event)
	if err != nil || !found {
		return boardsync.ImportResult{}, err
	}
	text := overrideText(req, event.Text)
	key, digest := syncEventKey("edit", req)
	var result boardsync.ImportResult
	err = h.board.Update(ctx, req.Tenant, log.UpdateOptions{Idempotency: &log.Idempotency{Key: key, Digest: digest}}, func(tx *log.Tx) error {
		if replay, ok := tx.Replay(); ok {
			result.Message = lastMessageID(replay.MessageIDs)
			result.Root = copy.BoardThread
			return nil
		}
		message := tx.Message(copy.BoardMessage)
		if message == nil || message.Deleted {
			return nil
		}
		if message.Text == text {
			// The same text again is the same board.
			result = boardsync.ImportResult{Root: copy.BoardThread, Message: message.ID}
			return nil
		}
		root := tx.RootOf(message.ID)
		actor := syncActor(event)
		h.emitActorSeen(tx, event)
		tx.Emit(log.EventMessageEdited, &log.MessageEditedPayload{
			ID: message.ID, Thread: message.Thread, Text: text,
			By: actor.Actored(), From: stringPtr(string(req.Key)),
		})
		emitTaskEditHold(tx, message, actor)
		tx.RecordResult(message.ID)
		result = boardsync.ImportResult{Root: root.ID, Message: message.ID}
		return nil
	})
	return result, err
}

// importDelete applies a platform delete to the board copy it names.
func (h *SyncHost) importDelete(ctx context.Context, req boardsync.ImportRequest, event boardsync.Event) (boardsync.ImportResult, error) {
	copy, found, err := h.lookupCopy(ctx, req, event)
	if err != nil || !found {
		return boardsync.ImportResult{}, err
	}
	key, digest := syncEventKey("delete", req)
	var result boardsync.ImportResult
	err = h.board.Update(ctx, req.Tenant, log.UpdateOptions{Idempotency: &log.Idempotency{Key: key, Digest: digest}}, func(tx *log.Tx) error {
		if replay, ok := tx.Replay(); ok {
			result.Message = lastMessageID(replay.MessageIDs)
			result.Root = copy.BoardThread
			return nil
		}
		message := tx.Message(copy.BoardMessage)
		if message == nil || message.Deleted {
			return nil
		}
		root := tx.RootOf(message.ID)
		actor := syncActor(event)
		h.emitActorSeen(tx, event)
		emitTaskDeleteCancel(tx, message, actor)
		tx.Emit(log.EventMessageDeleted, &log.MessageDeletedPayload{
			ID: message.ID, Thread: message.Thread,
			By: actor.Actored(), From: stringPtr(string(req.Key)),
		})
		tx.RecordResult(message.ID)
		result = boardsync.ImportResult{Root: root.ID, Message: message.ID}
		return nil
	})
	return result, err
}

// errSyncCopyPending reports a reaction whose message copy isn't recorded yet.
// It is retryable: the inbound worker tries again on its lane.
var errSyncCopyPending = errors.New("sync: the message copy is not recorded yet")

// importReaction applies a reaction to the board copy it names.
func (h *SyncHost) importReaction(ctx context.Context, req boardsync.ImportRequest, event boardsync.Event) (boardsync.ImportResult, error) {
	copy, found, err := h.lookupCopy(ctx, req, event)
	if err != nil {
		return boardsync.ImportResult{}, err
	}
	if !found {
		return boardsync.ImportResult{}, errSyncCopyPending
	}
	key, digest := syncEventKey("reaction", req)
	var result boardsync.ImportResult
	err = h.board.Update(ctx, req.Tenant, log.UpdateOptions{Idempotency: &log.Idempotency{Key: key, Digest: digest}}, func(tx *log.Tx) error {
		if replay, ok := tx.Replay(); ok {
			result.Message = lastMessageID(replay.MessageIDs)
			result.Root = copy.BoardThread
			return nil
		}
		message := tx.Message(copy.BoardMessage)
		if message == nil || message.Deleted {
			return nil
		}
		root := tx.RootOf(message.ID)
		h.emitActorSeen(tx, event)
		eventType := log.EventReactionRemoved
		if event.Added {
			eventType = log.EventReactionAdded
		}
		tx.Emit(eventType, &log.ReactionPayload{
			ID: message.ID, Thread: message.Thread, Emoji: boardsync.StorableText(event.Emoji, 64),
			By: boardsyncAuthor(event, req.Key), From: stringPtr(string(req.Key)),
		})
		tx.RecordResult(message.ID)
		result = boardsync.ImportResult{Root: root.ID, Message: message.ID}
		return nil
	})
	return result, err
}

// importLifecycle applies a platform lifecycle event to a thread the board
// syncs, or creates its root when the event says to.
func (h *SyncHost) importLifecycle(ctx context.Context, req boardsync.ImportRequest, event boardsync.Event) (boardsync.ImportResult, error) {
	mapping, mapped, err := h.lookupThread(ctx, req.Tenant, req.Key, event)
	if err != nil {
		return boardsync.ImportResult{}, err
	}
	if !mapped {
		if !event.Lifecycle.CreateRoot || req.Channel == "" {
			return boardsync.ImportResult{}, nil
		}
		text := event.Lifecycle.Title
		if event.Lifecycle.Body != "" {
			text += "\n\n" + event.Lifecycle.Body
		}
		labels := sanitizeLabels(event.Lifecycle.Labels)
		for key, value := range sanitizeLabels(req.Overrides.Labels) {
			labels[key] = value
		}
		text, labels, err = overrideRoot(req, event, text)
		if err != nil {
			return boardsync.ImportResult{}, err
		}
		key, digest := syncEventKey("lifecycle", req)
		var result boardsync.ImportResult
		err = h.board.Update(ctx, req.Tenant, log.UpdateOptions{Idempotency: &log.Idempotency{Key: key, Digest: digest}}, func(tx *log.Tx) error {
			if replay, ok := tx.Replay(); ok {
				result.Message = lastMessageID(replay.MessageIDs)
				result.Root = result.Message
				result.CreatedRoot = true
				return nil
			}
			offset := h.emitActorSeen(tx, event)
			id := tx.NextID(offset)
			tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
				ID: id, Channel: req.Channel, Text: text,
				Thread: log.ThreadKey(id), Labels: nonNilLabels(labels),
				By: boardsyncAuthor(event, req.Key), From: stringPtr(string(req.Key)),
				Origin: originOf(event), Links: h.lifecycleLinks(event.Lifecycle),
			})
			tx.RecordResult(id)
			result = boardsync.ImportResult{Root: id, Message: id, CreatedRoot: true}
			return nil
		})
		if err != nil {
			return result, err
		}
		if result.CreatedRoot {
			threads, threadErr := h.threadTargetsFor(req, event)
			if threadErr != nil {
				return result, threadErr
			}
			for _, target := range threads {
				if _, err := h.store.UpsertThread(ctx, syncstore.Thread{
					Tenant: req.Tenant, BoardThread: result.Root, Key: target.Key,
					Platform: target.Platform, Channel: target.Channel,
					Thread: target.Thread, Subthread: target.Subthread, Kind: target.Kind,
					Since: result.Root,
				}); err != nil {
					return result, err
				}
			}
		}
		return result, nil
	}

	key, digest := syncEventKey("lifecycle", req)
	var result boardsync.ImportResult
	err = h.board.Update(ctx, req.Tenant, log.UpdateOptions{Idempotency: &log.Idempotency{Key: key, Digest: digest}}, func(tx *log.Tx) error {
		if replay, ok := tx.Replay(); ok {
			result.Message = lastMessageID(replay.MessageIDs)
			result.Root = mapping.BoardThread
			return nil
		}
		root := tx.Message(mapping.BoardThread)
		if root == nil {
			return nil
		}
		offset := h.emitActorSeen(tx, event)
		if lifecycle := event.Lifecycle; lifecycle != nil {
			set := sanitizeLabels(lifecycle.Labels)
			unset := append(append([]string(nil), lifecycle.Unset...), req.Overrides.Unset...)
			for key, value := range sanitizeLabels(req.Overrides.Labels) {
				set[key] = value
			}
			if len(set) > 0 || len(unset) > 0 {
				offset += emitLabelChanges(tx, event, root, set, unset)
			}
			if lifecycle.Retitled && (lifecycle.Title != "" || lifecycle.Body != "") {
				tx.Emit(log.EventMessageEdited, &log.MessageEditedPayload{
					ID: root.ID, Thread: root.Thread,
					Text: boardsync.StorableText(lifecycleText(lifecycle), boardsync.MaxTextBytes),
					By:   boardsyncAuthor(event, req.Key), From: stringPtr(string(req.Key)),
					Links: h.lifecycleLinks(lifecycle),
				})
				offset++
			}
			if lifecycle.Reply != "" {
				id := tx.NextID(offset)
				tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
					ID: id, Channel: root.Channel,
					Text: boardsync.StorableText(lifecycle.Reply, boardsync.MaxTextBytes),
					Re:   uint64Ptr(root.ID), Thread: root.Thread, Labels: map[string]string{},
					By: boardsyncAuthor(event, req.Key), From: stringPtr(string(req.Key)),
				})
				tx.RecordResult(id)
				result = boardsync.ImportResult{Root: root.ID, Message: id}
			} else {
				result = boardsync.ImportResult{Root: root.ID, Message: root.ID}
			}
		}
		return nil
	})
	return result, err
}

// lifecycleText is a lifecycle subject's title and body; an edit keeps both,
// so a body-only change is not lost.
func lifecycleText(lifecycle *boardsync.Lifecycle) string {
	if lifecycle == nil {
		return ""
	}
	text := lifecycle.Title
	if lifecycle.Body != "" {
		if text != "" {
			text += "\n\n"
		}
		text += lifecycle.Body
	}
	return text
}

// lifecycleLinks keeps a lifecycle subject's URL as a safe board link.
func (h *SyncHost) lifecycleLinks(lifecycle *boardsync.Lifecycle) []protocol.LinkView {
	if lifecycle == nil || strings.TrimSpace(lifecycle.URL) == "" {
		return nil
	}
	safe := safeURL(lifecycle.URL, h.fileHosts)
	if safe == "" {
		return []protocol.LinkView{{Unavailable: "external link"}}
	}
	return []protocol.LinkView{{URL: safe}}
}

// lookupThread finds the board thread a platform thread maps to. A precise
// sub-thread mapping wins over its parent thread's.
func (h *SyncHost) lookupThread(ctx context.Context, tenant string, key boardsync.Key, event boardsync.Event) (syncstore.Thread, bool, error) {
	if event.Subthread != "" {
		target := boardsync.ResolvedTarget{
			Key: key, Platform: event.Platform, Channel: event.Channel,
			Thread: event.Thread, Subthread: event.Subthread, Kind: boardsync.TargetThread,
		}
		if thread, found, err := h.store.ThreadByTarget(ctx, tenant, key, target); err != nil || found {
			return thread, found, err
		}
	}
	target := boardsync.ResolvedTarget{
		Key: key, Platform: event.Platform, Channel: event.Channel,
		Thread: event.Thread, Kind: boardsync.TargetThread,
	}
	return h.store.ThreadByTarget(ctx, tenant, key, target)
}

// lookupCopy finds the board message a platform message copies. An exact
// target wins; a single copy elsewhere for the same external message is the
// fallback (Slack reactions don't carry a reply's parent thread).
func (h *SyncHost) lookupCopy(ctx context.Context, req boardsync.ImportRequest, event boardsync.Event) (syncstore.Copy, bool, error) {
	target := boardsync.ResolvedTarget{
		Key: req.Key, Platform: event.Platform, Channel: event.Channel,
		Thread: event.Thread, Subthread: event.Subthread, Kind: boardsync.TargetThread,
	}
	if copy, found, err := h.store.CopyByExternal(ctx, req.Tenant, req.Key, target, event.Message); err != nil || found {
		return copy, found, err
	}
	return h.store.CopyByExternalMessage(ctx, req.Tenant, req.Key, event.Message)
}

// threadTargetsFor returns the target rows a newly created root maps to.
func (h *SyncHost) threadTargetsFor(req boardsync.ImportRequest, event boardsync.Event) ([]boardsync.ResolvedTarget, error) {
	target := boardsync.ResolvedTarget{
		Key: req.Key, Platform: event.Platform, Channel: event.Channel,
		Thread: event.Thread, Kind: boardsync.TargetThread,
	}
	if err := target.Validate(); err != nil {
		return nil, err
	}
	return []boardsync.ResolvedTarget{target}, nil
}

// storeCopy records the platform message ID of an imported board message.
func (h *SyncHost) storeCopy(ctx context.Context, req boardsync.ImportRequest, event boardsync.Event, result boardsync.ImportResult) error {
	if event.Message == "" || result.Message == 0 {
		return nil
	}
	target := boardsync.ResolvedTarget{
		Key: req.Key, Platform: event.Platform, Channel: event.Channel,
		Thread: event.Thread, Subthread: event.Subthread, Kind: boardsync.TargetThread,
	}
	if err := target.Validate(); err != nil {
		return err
	}
	_, err := h.store.SaveCopy(ctx, req.Tenant, syncstore.Copy{
		BoardMessage: result.Message, BoardThread: result.Root, Key: req.Key,
		Target: target, ExternalThread: event.Thread, ExternalMessage: event.Message,
	})
	return err
}

// emitActorSeen records a platform author the board hasn't seen, or whose
// display changed. It returns 1 when it emitted, so a caller's NextID offset
// stays right.
func (h *SyncHost) emitActorSeen(tx *log.Tx, event boardsync.Event) int {
	actor := syncActor(event)
	if !needsActorSeen(tx.Read(), actor) {
		return 0
	}
	tx.Emit(log.EventActorSeen, &log.ActorSeenPayload{
		Actor: actor.ID, Kind: string(actor.Kind), Handle: actor.Handle,
		Name: actor.Name, Avatar: actor.Avatar, Platform: actor.Platform,
		Verified: false,
	})
	return 1
}

// syncActor names a platform author under the entry's key.
func syncActor(event boardsync.Event) protocol.Actor {
	id := boardsync.KeyedID(event.Key, event.Platform, event.Author.ID)
	actor := protocol.Actor{
		ID: id, Principal: id, Kind: protocol.ActorExternal,
		Handle:   boardsync.StorableText(event.Author.Handle, 200),
		Name:     boardsync.StorableText(event.Author.Name, 200),
		Platform: string(event.Platform),
	}
	if actor.Handle == "" && actor.Name == "" {
		actor.Handle = "platform"
	}
	if event.Author.Avatar != "" {
		avatar := boardsync.StorableText(event.Author.Avatar, 1000)
		actor.Avatar = &avatar
	}
	return actor
}

// boardsyncAuthor is the write provenance an import carries.
func boardsyncAuthor(event boardsync.Event, key boardsync.Key) protocol.Author {
	id := boardsync.KeyedID(key, event.Platform, event.Author.ID)
	return protocol.Author{Actor: id, Principal: id}
}

// originOf is the platform's identity of an imported message, key-prefixed so
// a reader can tell two connections' native IDs apart.
func originOf(event boardsync.Event) *log.MessageOrigin {
	origin := &log.MessageOrigin{
		Channel: keyedChannel(event.Key, event), Thread: keyedThread(event.Key, event),
		Subthread: keyedSubthread(event.Key, event), Message: event.Message, Context: event.Context,
	}
	if origin.Channel == "" && origin.Thread == "" && origin.Message == "" {
		return nil
	}
	return origin
}

// keyedChannel returns the key-prefixed channel ID, or "".
func keyedChannel(key boardsync.Key, event boardsync.Event) string {
	if event.Channel == "" {
		return ""
	}
	return string(key) + ":" + event.Channel
}

// keyedThread returns the key-prefixed thread ID.
func keyedThread(key boardsync.Key, event boardsync.Event) string {
	if event.Thread == "" {
		return ""
	}
	return string(key) + ":" + event.Thread
}

// keyedSubthread returns the key-prefixed sub-thread ID, or "".
func keyedSubthread(key boardsync.Key, event boardsync.Event) string {
	if event.Subthread == "" {
		return ""
	}
	return string(key) + ":" + event.Subthread
}

// attachmentRefs turns safe platform files into board attachment metadata. No
// bytes and no URL are stored.
func attachmentRefs(files []boardsync.File) []protocol.Attachment {
	if len(files) == 0 {
		return nil
	}
	out := make([]protocol.Attachment, 0, len(files))
	for _, file := range files {
		name := file.Name
		if name == "" {
			name = "file"
		}
		kind := file.Type
		if kind == "" {
			kind = file.MIMEType
		}
		if kind == "" {
			kind = "file"
		}
		out = append(out, protocol.Attachment{
			ID:   "sync:" + boardsync.Hash(kind+"\x00"+name+"\x00"+file.URL),
			Name: name, Type: kind, Size: file.Size,
		})
	}
	return out
}

// sanitizeLabels keeps caller labels the board may store: reserved board.*
// keys are dropped, as are empty or control-character ones.
func sanitizeLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	out := make(map[string]string, len(labels))
	for key, value := range labels {
		clean, ok := cleanLabel(key, value)
		if !ok {
			continue
		}
		out[key] = clean
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func cleanLabel(key, value string) (string, bool) {
	if key == "" || len(key) > 128 || len(value) > 1024 {
		return "", false
	}
	if strings.HasPrefix(key, "board.") {
		return "", false
	}
	for _, r := range key + value {
		if r == 0 || r < 0x20 || r == 0x7f {
			return "", false
		}
	}
	return value, true
}

// syncImportKey is the log idempotency key of one import.
func syncImportKey(req boardsync.ImportRequest) string {
	return "sync:import:" + string(req.Key) + ":" + req.EventID
}

// syncRootKey names the root of one external thread. Duplicate root events,
// and two events racing to create the thread, share it.
func syncRootKey(req boardsync.ImportRequest, event boardsync.Event) string {
	return "sync:root:" + string(req.Key) + ":" + event.Thread
}

// rootDigest is fixed for a thread: the first event to create its root wins,
// and a different event replays instead of conflicting.
func rootDigest(req boardsync.ImportRequest, event boardsync.Event) string {
	return digestOf(struct {
		Tenant   string
		Key      boardsync.Key
		Platform boardsync.Platform
		Thread   string
	}{
		Tenant: req.Tenant, Key: req.Key, Platform: event.Platform,
		Thread: event.Thread,
	})
}

// syncEventKey is the log idempotency key of an inbound change.
func syncEventKey(kind string, req boardsync.ImportRequest) (string, string) {
	key := "sync:" + kind + ":" + string(req.Key) + ":" + req.EventID
	return key, digestOf(req)
}

// importDigest is the stable digest of an import's decision inputs.
func importDigest(req boardsync.ImportRequest) string {
	return digestOf(struct {
		Tenant  string
		Key     boardsync.Key
		EventID string
		Channel string
		Event   boardsync.Event
		Text    *string
		Labels  map[string]string
		Unset   []string
		NoSync  bool
	}{
		Tenant: req.Tenant, Key: req.Key, EventID: req.EventID, Channel: req.Channel,
		Event: req.Event, Text: req.Overrides.Text, Labels: req.Overrides.Labels,
		Unset: req.Overrides.Unset, NoSync: req.Overrides.NoSync,
	})
}

// digestOf hashes a value's JSON form.
func digestOf(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

// lastMessageID returns the last recorded message an import wrote.
func lastMessageID(ids []uint64) uint64 {
	if len(ids) == 0 {
		return 0
	}
	return ids[len(ids)-1]
}

func uint64Ptr(value uint64) *uint64 { return &value }

// messageThread is the board thread a receipt's message belongs to: its
// recorded thread, or its own ID when it is a root.
func messageThread(message *boardsync.Message) uint64 {
	if message.Thread != 0 {
		return message.Thread
	}
	return message.ID
}

func stringPtr(value string) *string { return &value }

// Record appends a receipt sync produces. thread.synced is idempotent per
// (board thread, target); message receipts per (message, target).
func (h *SyncHost) Record(ctx context.Context, event boardsync.BoardEvent) error {
	if h == nil || h.board == nil || h.store == nil {
		return errors.New("server: sync host is not configured")
	}
	if !boardsync.ValidTenant(event.Tenant) {
		return fmt.Errorf("%w: receipt has no usable tenant", boardsync.ErrBadRequest)
	}
	switch event.Type {
	case boardsync.BoardThreadSynced:
		return h.recordThreadSynced(ctx, event)
	case boardsync.BoardMessageSynced:
		return h.recordMessageSynced(ctx, event)
	case boardsync.BoardMessageFilesSynced:
		return h.recordFilesSynced(ctx, event)
	case boardsync.BoardMessageSyncFailed:
		return h.recordSyncFailed(ctx, event)
	default:
		return fmt.Errorf("%w: sync doesn't record %q", boardsync.ErrBadRequest, event.Type)
	}
}

// recordThreadSynced attaches a board thread to a target and, for a channel
// target, queues the root that starts the platform thread.
func (h *SyncHost) recordThreadSynced(ctx context.Context, event boardsync.BoardEvent) error {
	if event.Target == nil {
		return fmt.Errorf("%w: thread.synced has no target", boardsync.ErrBadRequest)
	}
	target := logTarget(*event.Target)
	if !target.Valid() {
		return fmt.Errorf("%w: thread.synced has an invalid target", boardsync.ErrBadRequest)
	}
	id := event.Thread
	if id == 0 && event.Message != nil {
		id = event.Message.ID
	}
	if id == 0 {
		return fmt.Errorf("%w: thread.synced names no board thread", boardsync.ErrBadRequest)
	}
	root, err := h.rootMessage(ctx, event.Tenant, id)
	if err != nil {
		return err
	}
	kind := boardsync.TargetThread
	if target.Thread == "" {
		kind = boardsync.TargetChannel
	}
	since := uint64(0)
	if kind == boardsync.TargetThread {
		// A receipt without an attach API's sequence still makes its thread
		// future-only from now.
		head, err := h.board.Head(event.Tenant)
		if err != nil {
			return err
		}
		since = head
	}
	// The mapping is written before the receipt: an attach that loses a race
	// is refused without leaving a thread.synced fact behind. Since stays as
	// it is for a re-attach.
	if _, err := h.store.UpsertThread(ctx, syncstore.Thread{
		Tenant: event.Tenant, BoardThread: root.ID, Key: boardsync.Key(target.Key),
		Platform: boardsync.Platform(target.Platform), Channel: target.Channel,
		Thread: target.Thread, Subthread: target.Subthread, Kind: kind, Since: since,
	}); err != nil {
		if errors.Is(err, syncstore.ErrThreadAttached) {
			return boardsync.Permanent(err)
		}
		return err
	}
	payload := &log.ThreadSyncedPayload{
		ID: root.ID, Thread: root.Thread, Target: target, Cause: event.Cause,
	}
	key := "sync:thread:" + target.Key + ":" + target.External() + ":" + strconv.FormatUint(root.ID, 10)
	digest := struct {
		ID     uint64
		Thread string
		Target log.SyncTarget
	}{ID: payload.ID, Thread: payload.Thread, Target: payload.Target}
	result, err := h.appendSync(ctx, event.Tenant, key, digest, payload, func(tx *log.Tx) {
		tx.Emit(log.EventThreadSynced, payload)
	})
	if err != nil {
		return err
	}
	if kind != boardsync.TargetChannel {
		return nil
	}
	eventID := result.FirstSeq
	if eventID == 0 {
		// A cache that predates request results can replay without a first
		// sequence; the root's own ID names the same fact stably.
		eventID = root.ID
	}
	return h.enqueueRoot(ctx, event.Tenant, boardsync.Key(target.Key), eventID, root, target)
}

// enqueueRoot queues the root post that starts a channel target's thread.
func (h *SyncHost) enqueueRoot(ctx context.Context, tenant string, key boardsync.Key, eventID uint64, root *log.Message, target log.SyncTarget) error {
	message, err := h.messageFor(tenant, root)
	if err != nil {
		return err
	}
	ref := boardsync.ThreadRef{
		Key: key, Platform: boardsync.Platform(target.Platform),
		Channel: target.Channel, Thread: target.Thread, Subthread: target.Subthread,
	}
	resolved := boardsync.ResolvedTarget{
		Key: key, Platform: boardsync.Platform(target.Platform), Channel: target.Channel,
		Thread: target.Thread, Subthread: target.Subthread, Kind: boardsync.TargetChannel,
	}
	// Cause stays out of the outbox fact: the same root and target are one
	// delivery whether the attachment came from configuration or an explicit
	// attach. The log receipt keeps the cause for readers.
	_, err = h.store.EnqueueOutbox(ctx, tenant, key, boardsync.BoardEvent{
		Tenant: tenant, ID: eventID, Type: boardsync.BoardThreadSynced,
		Thread: root.ID, Target: &ref,
	}, message, []boardsync.ResolvedTarget{resolved})
	return err
}

// recordMessageSynced projects a delivered copy and starts a channel
// target's reply backfill once its root exists on the platform.
func (h *SyncHost) recordMessageSynced(ctx context.Context, event boardsync.BoardEvent) error {
	if event.Message == nil || event.External == nil {
		return fmt.Errorf("%w: message.synced needs a message and an external thread", boardsync.ErrBadRequest)
	}
	target := logTarget(*event.External)
	if !target.Valid() {
		return fmt.Errorf("%w: message.synced has an invalid external thread", boardsync.ErrBadRequest)
	}
	message := event.Message
	root, err := h.rootMessage(ctx, event.Tenant, messageThread(message))
	if err != nil {
		return err
	}
	key := boardsync.Key(target.Key)
	copied, found, err := h.store.Copy(ctx, event.Tenant, key, resolvedTarget(target, targetKind(target)), message.ID)
	if err != nil {
		return err
	}
	if !found {
		// A channel target's copy is stored under the channel target, while
		// the receipt names the platform thread the post created.
		copies, listErr := h.store.CopyForMessage(ctx, event.Tenant, key, message.ID)
		if listErr != nil {
			return listErr
		}
		if len(copies) == 1 {
			copied, found = copies[0], true
		}
	}
	externalMessage := event.ExternalMessage
	files := event.Files
	filesSet := event.FilesSet
	if found {
		if externalMessage == "" {
			externalMessage = copied.ExternalMessage
		}
		if !filesSet {
			files = copied.Files
			filesSet = copied.FilesSet
		}
	}
	if externalMessage == "" {
		externalMessage = event.ExternalMessage
	}
	payload := &log.MessageSyncedPayload{
		ID: message.ID, Thread: root.Thread, Target: target,
		ExternalThread: target.Thread, ExternalMessage: externalMessage,
		Files: append([]string(nil), files...), FilesSet: filesSet,
	}
	receiptKey := "sync:synced:" + target.Key + ":" + target.External() + ":" + strconv.FormatUint(message.ID, 10)
	digest := struct {
		ID              uint64
		Thread          string
		Target          log.SyncTarget
		ExternalThread  string
		ExternalMessage string
	}{payload.ID, payload.Thread, payload.Target, payload.ExternalThread, payload.ExternalMessage}
	if _, err := h.appendSync(ctx, event.Tenant, receiptKey, digest, payload, func(tx *log.Tx) {
		tx.Emit(log.EventMessageSynced, payload)
	}); err != nil {
		return err
	}
	if target.Thread != "" {
		if _, err := h.store.UpsertThread(ctx, syncstore.Thread{
			Tenant: event.Tenant, BoardThread: root.ID, Key: key,
			Platform: boardsync.Platform(target.Platform), Channel: target.Channel,
			Thread: target.Thread, Kind: boardsync.TargetThread,
			Since: message.ID,
		}); err != nil {
			return err
		}
	}
	// Backfill only a root that a channel target started. A message delivered
	// to an existing thread target is future traffic and has no queue to fill.
	if message.ID != root.ID || target.Thread == "" || !found || copied.Target.Kind != boardsync.TargetChannel {
		return nil
	}
	return h.backfill(ctx, event.Tenant, key, root, target)
}

// backfill queues every existing reply of a newly started thread, in order.
// The outbox lane keeps them behind the root post; replies already copied are
// skipped by the worker.
func (h *SyncHost) backfill(ctx context.Context, tenant string, key boardsync.Key, root *log.Message, target log.SyncTarget) error {
	var replies []*log.Message
	err := h.board.Read(tenant, func(snapshot *log.Snapshot) error {
		replies = snapshot.Replies(root.ID)
		return nil
	})
	if err != nil {
		return err
	}
	resolved := boardsync.ResolvedTarget{
		Key: key, Platform: boardsync.Platform(target.Platform), Channel: target.Channel,
		Thread: target.Thread, Kind: boardsync.TargetThread,
	}
	for _, reply := range replies {
		if reply == nil || reply.Deleted || reply.NoSync {
			continue
		}
		message, err := h.messageFor(tenant, reply)
		if err != nil {
			return err
		}
		if _, err := h.store.EnqueueOutbox(ctx, tenant, key, boardsync.BoardEvent{
			Tenant: tenant, ID: reply.ID, Type: boardsync.BoardMessagePosted,
			Thread: root.ID,
		}, message, []boardsync.ResolvedTarget{resolved}); err != nil {
			return err
		}
	}
	return nil
}

// recordFilesSynced projects a copy's file IDs.
func (h *SyncHost) recordFilesSynced(ctx context.Context, event boardsync.BoardEvent) error {
	if event.Message == nil || event.External == nil {
		return fmt.Errorf("%w: message.files_synced needs a message and an external thread", boardsync.ErrBadRequest)
	}
	target := logTarget(*event.External)
	if !target.Valid() {
		return fmt.Errorf("%w: message.files_synced has an invalid external thread", boardsync.ErrBadRequest)
	}
	root, err := h.rootMessage(ctx, event.Tenant, messageThread(event.Message))
	if err != nil {
		return err
	}
	payload := &log.MessageFilesSyncedPayload{
		ID: event.Message.ID, Thread: root.Thread, Target: target,
		Files: append([]string(nil), event.Files...), FilesSet: event.FilesSet,
	}
	key := "sync:files:" + target.Key + ":" + target.External() + ":" + strconv.FormatUint(event.Message.ID, 10)
	_, err = h.appendSync(ctx, event.Tenant, key, payload, payload, func(tx *log.Tx) {
		tx.Emit(log.EventMessageFilesSynced, payload)
	})
	return err
}

// recordSyncFailed projects a delivery failure. Error is a content-free class.
func (h *SyncHost) recordSyncFailed(ctx context.Context, event boardsync.BoardEvent) error {
	if event.Message == nil || event.External == nil {
		return fmt.Errorf("%w: message.sync_failed needs a message and an external thread", boardsync.ErrBadRequest)
	}
	target := logTarget(*event.External)
	if !target.Valid() {
		return fmt.Errorf("%w: message.sync_failed has an invalid external thread", boardsync.ErrBadRequest)
	}
	root, err := h.rootMessage(ctx, event.Tenant, messageThread(event.Message))
	if err != nil {
		return err
	}
	code := event.Error
	if code == "" {
		code = "failed"
	}
	if len(code) > 64 {
		code = code[:64]
	}
	payload := &log.MessageSyncFailedPayload{
		ID: event.Message.ID, Thread: root.Thread, Target: target, Error: code,
	}
	key := "sync:failed:" + target.Key + ":" + target.External() + ":" + strconv.FormatUint(event.Message.ID, 10)
	digest := struct {
		ID     uint64
		Thread string
		Target log.SyncTarget
		Error  string
	}{payload.ID, payload.Thread, payload.Target, payload.Error}
	_, err = h.appendSync(ctx, event.Tenant, key, digest, payload, func(tx *log.Tx) {
		tx.Emit(log.EventMessageSyncFailed, payload)
	})
	return err
}

// appendSync appends one sync event under a stable idempotency key. The digest
// is the payload's, so a retry with the same fact replays and a different fact
// under one key is refused.
func (h *SyncHost) appendSync(ctx context.Context, tenant, key string, digestValue, payload any, emit func(*log.Tx)) (log.AppendResult, error) {
	var result log.AppendResult
	err := h.board.Update(ctx, tenant, log.UpdateOptions{
		Idempotency: &log.Idempotency{Key: key, Digest: digestOf(digestValue)},
	}, func(tx *log.Tx) error {
		if replay, ok := tx.Replay(); ok {
			result = replay
			return nil
		}
		emit(tx)
		result = tx.Result()
		return nil
	})
	return result, err
}

// rootMessage reads a board thread's root.
func (h *SyncHost) rootMessage(ctx context.Context, tenant string, id uint64) (*log.Message, error) {
	var root *log.Message
	err := h.board.Read(tenant, func(snapshot *log.Snapshot) error {
		root = snapshot.RootOf(id)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if root == nil {
		return nil, fmt.Errorf("%w: board thread %d is missing", boardsync.ErrNotFound, id)
	}
	return root, nil
}

// messageFor renders one board message as sync sees it.
func (h *SyncHost) messageFor(tenant string, message *log.Message) (boardsync.Message, error) {
	out := boardsync.Message{
		ID: message.ID, Thread: message.ID, Channel: message.Channel,
		Text: message.Text, Labels: map[string]string{}, Deleted: message.Deleted,
		NoSync: message.NoSync, Actor: message.By.Actor,
	}
	if message.Re != nil {
		out.Thread = *message.Re
	}
	for key, value := range message.Labels {
		out.Labels[key] = value
	}
	if message.From != nil {
		out.From = boardsync.Key(*message.From)
	}
	if message.Origin != nil {
		out.Origin = &boardsync.Origin{
			Channel: message.Origin.Channel, Thread: message.Origin.Thread,
			Subthread: message.Origin.Subthread, Message: message.Origin.Message,
			Context: message.Origin.Context,
		}
	}
	for _, attachment := range message.Attachments {
		out.Attachments = append(out.Attachments, boardsync.AttachmentRef{
			Name: attachment.Name, Type: attachment.Type, Size: attachment.Size,
		})
	}
	var actorName string
	var verified bool
	if err := h.board.Read(tenant, func(snapshot *log.Snapshot) error {
		if record := snapshot.Actor(message.By.Actor); record != nil {
			actorName = record.Name
			verified = record.Verified
		}
		return nil
	}); err != nil {
		return boardsync.Message{}, err
	}
	out.ActorName = actorName
	out.Verified = verified
	return out, nil
}

// logTarget turns a platform thread ref into the log's target shape.
func logTarget(ref boardsync.ThreadRef) log.SyncTarget {
	return log.SyncTarget{
		Key: string(ref.Key), Platform: string(ref.Platform), Channel: ref.Channel,
		Thread: ref.Thread, Subthread: ref.Subthread,
	}
}

// targetKind says whether a log target starts a thread or joins one.
func targetKind(target log.SyncTarget) boardsync.TargetKind {
	if target.Thread == "" {
		return boardsync.TargetChannel
	}
	return boardsync.TargetThread
}

// resolvedTarget turns a log target into a sync target with its kind.
func resolvedTarget(target log.SyncTarget, kind boardsync.TargetKind) boardsync.ResolvedTarget {
	return boardsync.ResolvedTarget{
		Key: boardsync.Key(target.Key), Platform: boardsync.Platform(target.Platform),
		Channel: target.Channel, Thread: target.Thread, Subthread: target.Subthread, Kind: kind,
	}
}
