package server

import (
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
)

// handleDiscovery answers GET /v1/board: where the board is and what it
// syncs. Phase 1 has no sync entries.
func (s *Server) handleDiscovery(w http.ResponseWriter, r *http.Request) {
	base := strings.TrimSuffix(r.URL.Path, "/")
	if base == "" {
		base = APIPrefix
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"version":    s.version,
		"url":        base,
		"audience":   base,
		"team_id":    nil,
		"project_id": nil,
		"sync":       []any{},
	})
}

// handleListChannels answers GET /channels: a page of the board's channels,
// by path.
func (s *Server) handleListChannels(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	prefix, hasPrefix := queryString(r, "prefix")
	if hasPrefix && len(prefix) > protocol.ChannelPatternMax {
		writeError(w, protocol.Invalid("prefix: at most 501 characters"))
		return
	}
	after, _, perr := queryRefString(r, "after")
	if perr != nil {
		writeError(w, perr)
		return
	}
	limit, perr := queryInt(r, "limit", 100, 1, 500)
	if perr != nil {
		writeError(w, perr)
		return
	}
	var page []protocol.ChannelView
	var cursor string
	var next *string
	err := s.board.Read(req.tenant, func(snapshot *log.Snapshot) error {
		cursor = protocol.FormatRef(snapshot.Head())
		for _, channel := range snapshot.Channels() {
			if hasPrefix && !strings.HasPrefix(channel.Path, prefix) {
				continue
			}
			if after != nil && channel.Path <= *after {
				continue
			}
			if !req.can(s, channel.Path, protocol.PermissionRead) {
				continue
			}
			page = append(page, channelView(channel))
			if len(page) == limit+1 {
				break
			}
		}
		if len(page) > limit {
			page = page[:limit]
			more := page[len(page)-1].Path
			next = &more
		}
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if page == nil {
		page = []protocol.ChannelView{}
	}
	writeJSON(w, http.StatusOK, protocol.ChannelsPage{Channels: page, Cursor: cursor, Next: next})
}

// queryRefString parses an optional cursor made of arbitrary characters (a
// channel path for /channels), which has no decimal shape.
func queryRefString(r *http.Request, name string) (*string, bool, *protocol.Error) {
	value, ok := queryString(r, name)
	if !ok {
		return nil, false, nil
	}
	if len(value) == 0 || len(value) > 1024 {
		return nil, true, protocol.NewError(protocol.CodeInvalidCursor, fmt.Sprintf("%s isn't a cursor on this board", value))
	}
	return &value, true, nil
}

// handleCreateChannel answers POST /channels.
func (s *Server) handleCreateChannel(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	var body protocol.CreateChannelRequest
	if err := decodeBody(r, &body, true, false); err != nil {
		writeError(w, err)
		return
	}
	if err := protocol.ValidateChannel(body.Path); err != nil {
		writeError(w, err)
		return
	}
	if err := req.permit(s, body.Path, protocol.PermissionRead); err != nil {
		writeError(w, err)
		return
	}
	if err := req.permit(s, body.Path, protocol.PermissionPost); err != nil {
		writeError(w, err)
		return
	}
	var view protocol.ChannelView
	created := false
	err := s.board.Update(req.ctx, req.tenant, log.UpdateOptions{}, func(tx *log.Tx) error {
		if channel := tx.Channel(body.Path); channel != nil {
			view = channelView(channel)
			return nil
		}
		created = true
		if needsActorSeen(tx.Read(), req.actor) {
			tx.Emit(log.EventActorSeen, actorSeenPayload(req.actor))
		}
		tx.Emit(log.EventChannelCreated, &log.ChannelCreatedPayload{Channel: body.Path, By: req.actor.Actored()})
		view = protocol.ChannelView{Path: body.Path}
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, map[string]any{"channel": view, "created": created})
}

// channelView renders a channel.
func channelView(channel *log.Channel) protocol.ChannelView {
	return protocol.ChannelView{
		Path:      channel.Path,
		LastIndex: channel.LastIndex,
		Messages:  channel.Messages,
		Threads:   channel.Threads,
	}
}

// handleListMessages answers GET /messages: one channel's messages across its
// threads, oldest first.
func (s *Server) handleListMessages(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	channel, hasChannel := queryString(r, "channel")
	if !hasChannel {
		writeError(w, protocol.Invalid("channel: a read names one channel"))
		return
	}
	if err := protocol.ValidateChannel(channel); err != nil {
		writeError(w, err)
		return
	}
	after, hasAfter, perr := queryRef(r, "after")
	if perr != nil {
		writeError(w, perr)
		return
	}
	limit, perr := queryInt(r, "limit", 50, 1, 500)
	if perr != nil {
		writeError(w, perr)
		return
	}
	if err := req.permit(s, channel, protocol.PermissionRead); err != nil {
		writeError(w, err)
		return
	}
	var page []protocol.MessageView
	cursor := ""
	err := s.board.Read(req.tenant, func(snapshot *log.Snapshot) error {
		messages := snapshot.MessagesInChannel(channel)
		selected := make([]*log.Message, 0, limit)
		if hasAfter {
			for _, message := range messages {
				if message.ID <= *after {
					continue
				}
				selected = append(selected, message)
				if len(selected) == limit {
					break
				}
			}
		} else if len(messages) > limit {
			selected = messages[len(messages)-limit:]
		} else {
			selected = messages
		}
		for _, message := range selected {
			page = append(page, messageView(snapshot, message, nil))
		}
		if hasAfter && len(selected) == limit {
			cursor = protocol.FormatRef(selected[len(selected)-1].ID)
		} else {
			cursor = protocol.FormatRef(snapshot.Head())
		}
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if page == nil {
		page = []protocol.MessageView{}
	}
	writeJSON(w, http.StatusOK, protocol.MessagesPage{Messages: page, Cursor: cursor})
}

// postResponse is what a post answers.
type postResponse struct {
	Message protocol.MessageView `json:"message"`
}

// handlePost answers POST /messages: a new thread's root, or a reply.
func (s *Server) handlePost(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	var body protocol.PostRequest
	if err := decodeBody(r, &body, false, false); err != nil {
		writeError(w, err)
		return
	}
	if (body.Channel == nil) == (body.Re == nil) {
		writeError(w, protocol.Invalid("a post needs `channel`; a reply needs `re`"))
		return
	}
	if len(body.Attachments) > 0 {
		writeError(w, protocol.Invalid("attachments: phase 1 keeps no files"))
		return
	}
	if len(body.Sync) > 0 {
		writeError(w, protocol.Invalid("sync: phase 1 syncs nothing"))
		return
	}
	if body.After != nil && body.Re == nil {
		writeError(w, protocol.NewError(protocol.CodeInvalidAfter, "`after` is for replies: it names the newest message in `re`'s thread"))
		return
	}
	if err := protocol.ValidateText(body.Text); err != nil {
		writeError(w, err)
		return
	}
	if err := protocol.ValidateNewLabels(body.Labels); err != nil {
		writeError(w, err)
		return
	}

	clientID := body.ID
	if clientID == "" {
		clientID = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	if clientID != "" {
		if err := protocol.ValidateClientID(clientID); err != nil {
			writeError(w, err)
			return
		}
	}

	var after *uint64
	if body.After != nil {
		id, valid := protocol.ParseRef(*body.After)
		if !valid || id == 0 {
			writeError(w, protocol.NewError(protocol.CodeInvalidAfter, fmt.Sprintf("after names no message in the thread")))
			return
		}
		after = &id
	}

	var replyTo *uint64
	var rootID uint64
	channel := ""
	hasLabels := len(body.Labels) > 0
	if body.Re != nil {
		id, valid := protocol.ParseRef(*body.Re)
		if !valid || id == 0 {
			writeError(w, protocol.NotFound(protocol.CodeMessageNotFound, fmt.Sprintf("message not found: #%s", *body.Re)))
			return
		}
		var target *log.Message
		var targetChannel string
		var targetRe *uint64
		var targetID uint64
		err := s.board.Read(req.tenant, func(snapshot *log.Snapshot) error {
			target = snapshot.Message(id)
			if target != nil {
				targetChannel = target.Channel
				targetRe = target.Re
				targetID = target.ID
			}
			return nil
		})
		if err != nil {
			s.fail(w, r, err)
			return
		}
		if target == nil {
			writeError(w, protocol.NotFound(protocol.CodeMessageNotFound, fmt.Sprintf("message not found: #%d", id)))
			return
		}
		if !req.can(s, targetChannel, protocol.PermissionRead) {
			writeError(w, protocol.NotFound(protocol.CodeMessageNotFound, fmt.Sprintf("message not found: #%d", id)))
			return
		}
		rootID = targetID
		if targetRe != nil {
			rootID = *targetRe
			replied := targetID
			replyTo = &replied
		}
		channel = targetChannel
	} else {
		if err := protocol.ValidateChannel(*body.Channel); err != nil {
			writeError(w, err)
			return
		}
		channel = *body.Channel
	}
	if err := req.permit(s, channel, protocol.PermissionPost); err != nil {
		writeError(w, err)
		return
	}
	if hasLabels {
		if err := req.permit(s, channel, protocol.PermissionLabel); err != nil {
			writeError(w, err)
			return
		}
	}

	reValue := ""
	if body.Re != nil {
		reValue = *body.Re
	}
	digest := fmt.Sprintf("channel=%s;re=%s", channel, reValue)
	var opts log.UpdateOptions
	if clientID != "" {
		opts.Idempotency = &log.Idempotency{Key: "post:" + req.actor.ID + ":" + clientID, Digest: digest}
	}
	postID := uint64(0)
	err := s.board.Update(req.ctx, req.tenant, opts, func(tx *log.Tx) error {
		if replay, ok := tx.Replay(); ok {
			if len(replay.MessageIDs) == 0 {
				return protocol.NewError(protocol.CodeDuplicateID, "that id was already used for a different write")
			}
			postID = replay.MessageIDs[0]
			return nil
		}
		thread := log.ThreadKey(rootID)
		if body.Re != nil {
			target := tx.Message(rootID)
			if target == nil {
				return protocol.NotFound(protocol.CodeMessageNotFound, fmt.Sprintf("message not found: #%d", rootID))
			}
			if after != nil {
				if err := checkNewest(tx, rootID, *after); err != nil {
					return err
				}
			}
		}
		offset := 0
		if needsActorSeen(tx.Read(), req.actor) {
			tx.Emit(log.EventActorSeen, actorSeenPayload(req.actor))
			offset++
		}
		postID = tx.NextID(offset)
		if body.Re == nil {
			thread = log.ThreadKey(postID)
		}
		var re *uint64
		if body.Re != nil {
			re = &rootID
		}
		tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
			ID:      postID,
			Channel: channel,
			Text:    body.Text,
			Re:      re,
			ReplyTo: replyTo,
			Thread:  thread,
			Labels:  nonNilLabels(body.Labels),
			NoSync:  body.NoSync,
			By:      req.actor.Actored(),
			// Where its author can't read the channel, the write still lands,
			// but the board marks it: only its author changes such a post
			// without reading it.
			Blind: !req.can(s, channel, protocol.PermissionRead),
		})
		tx.RecordResult(postID)
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	var view protocol.MessageView
	err = s.board.Read(req.tenant, func(snapshot *log.Snapshot) error {
		message := snapshot.Message(postID)
		if message == nil {
			return protocol.NotFound(protocol.CodeMessageNotFound, fmt.Sprintf("message not found: #%d", postID))
		}
		view = messageView(snapshot, message, &req.actor)
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, postResponse{Message: view})
}

// checkNewest refuses an `after` that isn't the thread's newest message: a
// deleted message came after it all the same.
func checkNewest(tx *log.Tx, root uint64, after uint64) error {
	found := after == root
	newest := uint64(0)
	if root > after {
		newest = root
	}
	for _, reply := range tx.Read().AllReplies(root) {
		if reply.ID == after {
			found = true
		}
		if reply.ID > after && reply.ID > newest {
			newest = reply.ID
		}
	}
	if !found {
		return protocol.NewError(protocol.CodeInvalidAfter, fmt.Sprintf("after names no message in #%d's thread", root))
	}
	if newest != 0 {
		return protocol.NewError(protocol.CodeNewerMessage, fmt.Sprintf("#%d came after #%d", newest, after))
	}
	return nil
}

// handleThread answers GET /messages/{id}: the thread around any message.
func (s *Server) handleThread(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	id, perr := pathRef(r)
	if perr != nil {
		writeError(w, perr)
		return
	}
	after, hasAfter, perr := queryRef(r, "after")
	if perr != nil {
		writeError(w, perr)
		return
	}
	limit, perr := queryInt(r, "limit", 500, 1, 500)
	if perr != nil {
		writeError(w, perr)
		return
	}
	var response protocol.ThreadView
	err := s.board.Read(req.tenant, func(snapshot *log.Snapshot) error {
		root := snapshot.RootOf(id)
		if root == nil || !req.can(s, root.Channel, protocol.PermissionRead) {
			return protocol.NotFound(protocol.CodeMessageNotFound, fmt.Sprintf("message not found: #%d", id))
		}
		all := snapshot.AllReplies(root.ID)
		if hasAfter {
			found := false
			for _, reply := range all {
				if reply.ID == *after {
					found = true
					break
				}
			}
			if *after != root.ID && !found {
				return protocol.NewError(protocol.CodeInvalidAfter, fmt.Sprintf("#%d isn't a message in #%d's thread", *after, root.ID))
			}
		}
		selected := make([]*log.Message, 0, limit)
		if hasAfter {
			for _, reply := range all {
				if reply.ID <= *after {
					continue
				}
				selected = append(selected, reply)
				if len(selected) == limit {
					break
				}
			}
		} else if len(all) > limit {
			selected = all[len(all)-limit:]
		} else {
			selected = all
		}
		replies := make([]protocol.MessageView, 0, len(selected))
		for _, reply := range selected {
			replies = append(replies, messageView(snapshot, reply, nil))
		}
		response = protocol.ThreadView{
			Message: messageView(snapshot, root, nil),
			Replies: replies,
			Cursor:  protocol.FormatRef(snapshot.Head()),
		}
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

// handleLabels answers POST /messages/{id}/labels: set and unset labels, and
// only while an `if` condition holds, in one write.
func (s *Server) handleLabels(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	id, perr := pathRef(r)
	if perr != nil {
		writeError(w, perr)
		return
	}
	var body protocol.LabelRequest
	if err := decodeBody(r, &body, false, false); err != nil {
		writeError(w, err)
		return
	}
	if body.Set == nil && body.Unset == nil {
		writeError(w, protocol.Invalid("nothing to change: pass `set` or `unset`"))
		return
	}
	if err := protocol.ValidateNewLabels(body.Set); err != nil {
		writeError(w, err)
		return
	}
	for _, key := range body.Unset {
		if strings.HasPrefix(key, protocol.ReservedLabelPrefix) {
			writeError(w, protocol.NewError(protocol.CodeReservedLabel,
				fmt.Sprintf("labels under %s are the board's own: only its task writes set them", protocol.ReservedLabelPrefix)))
			return
		}
		if err := protocol.ValidateLabelKey(key); err != nil {
			writeError(w, err)
			return
		}
	}
	if body.If != nil {
		if len(body.If) > protocol.MaxLabelConditions {
			writeError(w, protocol.Invalidf("if: at most %d labels", protocol.MaxLabelConditions))
			return
		}
		for key, wanted := range body.If {
			if err := protocol.ValidateLabelKey(key); err != nil {
				writeError(w, err)
				return
			}
			if wanted != nil {
				if err := protocol.ValidateLabelValue(*wanted); err != nil {
					writeError(w, err)
					return
				}
			}
		}
	}

	var channel string
	var deleted bool
	err := s.board.Read(req.tenant, func(snapshot *log.Snapshot) error {
		message := snapshot.Message(id)
		if message == nil || !req.can(s, message.Channel, protocol.PermissionRead) {
			return protocol.NotFound(protocol.CodeMessageNotFound, fmt.Sprintf("message not found: #%d", id))
		}
		channel, deleted = message.Channel, message.Deleted
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if err := req.permit(s, channel, protocol.PermissionLabel); err != nil {
		writeError(w, err)
		return
	}
	if deleted {
		writeError(w, protocol.NewError(protocol.CodeMessageDeleted, "message was deleted"))
		return
	}
	err = s.board.Update(req.ctx, req.tenant, log.UpdateOptions{}, func(tx *log.Tx) error {
		message := tx.Message(id)
		if message == nil {
			return protocol.NotFound(protocol.CodeMessageNotFound, fmt.Sprintf("message not found: #%d", id))
		}
		if message.Deleted {
			return protocol.NewError(protocol.CodeMessageDeleted, "message was deleted")
		}
		if body.If != nil {
			keys := make([]string, 0, len(body.If))
			for key := range body.If {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				wanted := body.If[key]
				now, set := message.Labels[key]
				if wanted == nil {
					if set {
						return protocol.NewError(protocol.CodeLabelConflict, fmt.Sprintf("%s is set", key))
					}
					continue
				}
				if !set {
					return protocol.NewError(protocol.CodeLabelConflict, fmt.Sprintf("%s isn't set", key))
				}
				if now != *wanted {
					return protocol.NewError(protocol.CodeLabelConflict, fmt.Sprintf("%s isn't %s", key, *wanted))
				}
			}
		}
		if needsActorSeen(tx.Read(), req.actor) {
			tx.Emit(log.EventActorSeen, actorSeenPayload(req.actor))
		}
		tx.Emit(log.EventLabelsChanged, &log.LabelsChangedPayload{
			ID: id, Thread: message.Thread, Set: nonNilLabels(body.Set), Unset: nonNilStrings(body.Unset), By: req.actor.Actored(),
		})
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// actorSeenPayload records an actor as the board knows them from a write.
func actorSeenPayload(actor protocol.Actor) *log.ActorSeenPayload {
	payload := &log.ActorSeenPayload{
		Actor:    actor.ID,
		Kind:     string(actor.Kind),
		Handle:   actor.Handle,
		Name:     actor.Name,
		Avatar:   actor.Avatar,
		Platform: actor.Platform,
		Verified: true,
	}
	if actor.Via != nil {
		payload.Via = &log.Via{Name: actor.Via.Name, Environment: actor.Via.Environment}
	}
	return payload
}

// needsActorSeen reports whether the board's record of actor differs from who
// the request says they are.
func needsActorSeen(snapshot *log.Snapshot, actor protocol.Actor) bool {
	record := snapshot.Actor(actor.ID)
	if record == nil {
		return true
	}
	if record.Kind != string(actor.Kind) || record.Handle != actor.Handle || record.Name != actor.Name || record.Platform != actor.Platform {
		return true
	}
	if !sameOptional(record.Avatar, actor.Avatar) {
		return true
	}
	if (record.Via == nil) != (actor.Via == nil) {
		return true
	}
	if record.Via != nil && actor.Via != nil {
		if record.Via.Name != actor.Via.Name || !sameOptional(record.Via.Environment, actor.Via.Environment) {
			return true
		}
	}
	return false
}

// nonNilLabels returns labels as an event writes them: {} rather than null.
func nonNilLabels(labels map[string]string) map[string]string {
	if labels == nil {
		return map[string]string{}
	}
	return labels
}

// nonNilStrings returns a list as an event writes it: [] rather than null.
func nonNilStrings(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}

func sameOptional(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
