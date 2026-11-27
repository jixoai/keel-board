package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
)

// hiddenEvents are bookkeeping events the feed leaves out unless `all` is
// true.
var hiddenEvents = map[log.EventType]bool{
	log.EventActorSeen:   true,
	log.EventTaskRenewed: true,
}

// eventFilter is a feed subscription's filters.
type eventFilter struct {
	channel    string
	hasChannel bool
	re         *uint64
	by         string
	hasBy      bool
	mentions   string
	hasMention bool
	from       string
	hasFrom    bool
	text       string
	hasText    bool
	labels     []labelFilter
	all        bool
}

// criteria reports whether any filter narrows the feed. A feed without one
// carries every message event.
func (f eventFilter) criteria() bool {
	return f.hasChannel || f.re != nil || f.hasBy || f.hasMention || f.hasFrom || f.hasText || len(f.labels) > 0
}

// matches reports whether a message matches the filters, now. The feed also
// checks the message as it was before an event, so a subscriber sees a
// message enter and leave a view; this port folds the event first and checks
// the result, which covers what messages say now.
func (f eventFilter) matches(req *request, snapshot *log.Snapshot, message *log.Message) bool {
	if message == nil {
		return false
	}
	if f.hasChannel && !channelMatches(message.Channel, f.channel) {
		return false
	}
	if f.re != nil && message.ID != *f.re && (message.Re == nil || *message.Re != *f.re) {
		return false
	}
	if f.hasBy && !matchesWho(snapshot, req, f.by, message.By.Actor, message.By.Principal) {
		return false
	}
	if f.hasFrom {
		source := "board"
		if message.From != nil {
			source = *message.From
		}
		if source != f.from {
			return false
		}
	}
	if f.hasText && !strings.Contains(strings.ToLower(message.Text), strings.ToLower(f.text)) {
		return false
	}
	if f.hasMention && !mentionsMatch(message.Text, f.mentions, req.actor) {
		return false
	}
	if len(f.labels) > 0 {
		root := message
		if message.Re != nil {
			if found := snapshot.Message(*message.Re); found != nil {
				root = found
			}
		}
		if !labelsMatch(message.Labels, f.labels) && !labelsMatch(root.Labels, f.labels) {
			return false
		}
	}
	return true
}

// mentionsMatch is the substring check a mention filter reduces to here: the
// text names @value, or @[value] for an exact actor.
func mentionsMatch(text, value string, actor protocol.Actor) bool {
	terms := []string{value}
	if value == "me" {
		terms = []string{actor.Handle, actor.ID}
	}
	lowered := strings.ToLower(text)
	for _, term := range terms {
		if term == "" {
			continue
		}
		if strings.Contains(lowered, "@"+strings.ToLower(term)) ||
			strings.Contains(lowered, "@["+strings.ToLower(term)+"]") {
			return true
		}
	}
	return false
}

// visible reports whether an event reaches a subscriber. The message is
// checked before and after the event, so a subscriber sees messages enter and
// leave a filtered view. A nil before is a current read (the `last` feed).
func (f eventFilter) visible(s *Server, req *request, snapshot *log.Snapshot, event log.Event, before *log.Message) bool {
	if hiddenEvents[event.Type] && !f.all {
		return false
	}
	id := log.ConcernedMessageID(event)
	if id == 0 {
		// An event about no message is shown only with `all`, and never to a
		// filtered feed: it can't match a message filter.
		return f.all && !f.criteria()
	}
	after := snapshot.Message(id)
	channel := ""
	if after != nil {
		channel = after.Channel
	} else if before != nil {
		channel = before.Channel
	}
	if channel == "" || !req.can(s, channel, protocol.PermissionRead) {
		return false
	}
	if f.criteria() {
		return (before != nil && f.matches(req, snapshot, before)) || (after != nil && f.matches(req, snapshot, after))
	}
	return after != nil || before != nil
}

// handleEvents answers GET /events with Server-Sent Events. It starts at
// `after`, at the Last-Event-ID header, or at the head; `last` replays the
// feed's newest matching events first.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	filter := eventFilter{}
	if channel, has := queryString(r, "channel"); has {
		if err := protocol.ValidateChannelOrSubtree(channel); err != nil {
			writeError(w, err)
			return
		}
		filter.channel = channel
		filter.hasChannel = true
	}
	if value, has := queryString(r, "re"); has {
		id, valid := protocol.ParseRef(value)
		if !valid {
			id = 0 // names nothing, so matches nothing
		}
		filter.re = &id
	}
	if value, has := queryString(r, "by"); has {
		filter.by, filter.hasBy = value, true
	}
	if value, has := queryString(r, "mentions"); has {
		filter.mentions, filter.hasMention = value, true
	}
	if value, has := queryString(r, "from"); has {
		if len(value) == 0 || len(value) > 165 {
			writeError(w, protocol.Invalid("from: 1 to 165 characters"))
			return
		}
		filter.from, filter.hasFrom = value, true
	}
	if value, has := queryString(r, "text"); has {
		if len(value) == 0 || len(value) > 200 {
			writeError(w, protocol.Invalid("text: 1 to 200 characters"))
			return
		}
		filter.text, filter.hasText = value, true
	}
	labels, perr := parseLabels(queryRepeated(r, "label"))
	if perr != nil {
		writeError(w, perr)
		return
	}
	filter.labels = labels
	all, _, perr := queryFlag(r, "all")
	if perr != nil {
		writeError(w, perr)
		return
	}
	filter.all = all
	_, hasWait := queryString(r, "wait")
	if hasWait {
		if value, _ := queryString(r, "wait"); value != "" {
			wait, err := strconv.ParseFloat(value, 64)
			if err != nil || wait < 0 || wait > 300 {
				writeError(w, protocol.Invalid("wait: 0 to 300 seconds"))
				return
			}
		}
	}
	last := 0
	if value, has := queryString(r, "last"); has {
		number, err := strconv.Atoi(value)
		if err != nil || number < 1 || number > 100 {
			writeError(w, protocol.Invalid("last: 1 to 100 events"))
			return
		}
		last = number
	}
	if last > 0 && (hasWait || hasQuery(r, "after")) {
		writeError(w, protocol.Invalid("`last` reads the newest events at once: it takes no `after` or `wait`"))
		return
	}
	var after *uint64
	if value, has := queryString(r, "after"); has {
		id, valid := protocol.ParseRef(value)
		if !valid || id == 0 {
			writeError(w, protocol.NewError(protocol.CodeInvalidCursor, fmt.Sprintf("%s isn't a cursor or a message ID on this board", value)))
			return
		}
		after = &id
	}
	if after == nil {
		if value := strings.TrimSpace(r.Header.Get("Last-Event-ID")); value != "" {
			id, valid := protocol.ParseRef(value)
			if !valid {
				writeError(w, protocol.NewError(protocol.CodeInvalidCursor, fmt.Sprintf("Last-Event-ID %s isn't a cursor on this board", value)))
				return
			}
			after = &id
		}
	}

	flusher, canFlush := w.(http.Flusher)
	if !canFlush {
		writeError(w, protocol.NewError(protocol.CodeInternal, "this server can't stream"))
		return
	}

	// Catch the cache up before reading the start. A stream cursor may name
	// events another process wrote; never clamp it below what the store already
	// holds just because this process's cache is behind.
	if err := s.board.Refresh(req.ctx, req.tenant); err != nil {
		s.fail(w, r, err)
		return
	}
	// Read the start before announcing the stream: a write that lands after
	// this head is delivered, never skipped because it raced the opening.
	head := uint64(0)
	if err := s.board.Read(req.tenant, func(snapshot *log.Snapshot) error {
		head = snapshot.Head()
		return nil
	}); err != nil {
		s.fail(w, r, err)
		return
	}
	cursor := head
	if after != nil {
		cursor = *after
	}
	if cursor > head {
		// The cursor is ahead of the log even after a forced catch-up: it
		// isn't this board's, so refuse it rather than silently replay.
		writeError(w, protocol.NewError(protocol.CodeInvalidCursor,
			fmt.Sprintf("%s is ahead of this board's log", protocol.FormatRef(cursor))))
		return
	}

	// Read the replay and the feed's starting state before announcing the
	// stream: a store failure must be a 503, not an empty 200 stream.
	var lastViews []protocol.EventView
	if last > 0 {
		events, err := s.board.History(req.tenant, 1, head)
		if err != nil {
			s.fail(w, r, err)
			return
		}
		views, err := s.visibleViews(req, filter, events)
		if err != nil {
			s.fail(w, r, err)
			return
		}
		if len(views) > last {
			views = views[len(views)-last:]
		}
		lastViews = views
		cursor = head
	}
	feed := log.NewState()
	prefix, err := s.board.History(req.tenant, 1, cursor)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	for _, event := range prefix {
		feed.Apply(event)
	}

	w.Header().Set("content-type", "text/event-stream; charset=utf-8")
	w.Header().Set("cache-control", "no-cache")
	w.Header().Set("connection", "keep-alive")
	w.Header().Set("x-accel-buffering", "no")
	w.WriteHeader(http.StatusOK)
	// The stream is open: count it until this handler returns.
	releaseStream := s.observeSSEOpen()
	defer releaseStream()
	s.observeSSELag(head, cursor)
	// A comment says the stream is open, so a client needn't post and hope
	// the subscription was ready.
	if _, err := fmt.Fprint(w, ": connected\n\n"); err != nil {
		return
	}
	flusher.Flush()
	for _, view := range lastViews {
		if !writeSSE(w, flusher, view) {
			return
		}
	}
	for {
		if req.ctx.Err() != nil {
			return
		}
		currentHead := head
		if err := s.board.Read(req.tenant, func(snapshot *log.Snapshot) error {
			currentHead = snapshot.Head()
			return nil
		}); err != nil {
			return
		}
		if currentHead > cursor {
			events, err := s.board.History(req.tenant, cursor+1, currentHead)
			if err != nil {
				return
			}
			if len(events) > 0 {
				cursor = events[len(events)-1].Seq
			}
			s.observeSSELag(currentHead, cursor)
			for _, event := range events {
				before := (*log.Message)(nil)
				if id := log.ConcernedMessageID(event); id != 0 {
					before = log.CloneMessage(feed.Messages[id])
				}
				feed.Apply(event)
				snapshot := log.SnapshotOf(feed, time.Now().UTC())
				if !filter.visible(s, req, snapshot, event, before) {
					continue
				}
				if !writeSSE(w, flusher, eventView(snapshot, event, nil)) {
					return
				}
			}
			continue
		}
		if err := s.board.Wait(req.ctx, req.tenant, cursor); err != nil {
			return
		}
	}
}

func hasQuery(r *http.Request, name string) bool {
	_, ok := r.URL.Query()[name]
	return ok
}

// visibleViews filters events and renders them as subscribers see them.
func (s *Server) visibleViews(req *request, filter eventFilter, events []log.Event) ([]protocol.EventView, error) {
	if len(events) == 0 {
		return nil, nil
	}
	views := make([]protocol.EventView, 0, len(events))
	err := s.board.Read(req.tenant, func(snapshot *log.Snapshot) error {
		for _, event := range events {
			if !filter.visible(s, req, snapshot, event, nil) {
				continue
			}
			views = append(views, eventView(snapshot, event, &req.actor))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return views, nil
}

// writeSSE writes one event and flushes it.
func writeSSE(w http.ResponseWriter, flusher http.Flusher, view protocol.EventView) bool {
	data, err := json.Marshal(view)
	if err != nil {
		return false
	}
	if _, err := fmt.Fprintf(w, "id: %s\ndata: %s\n\n", view.Cursor, data); err != nil {
		return false
	}
	flusher.Flush()
	return true
}
