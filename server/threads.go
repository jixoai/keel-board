package server

import (
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
)

// threadFilter is what a `/threads` read narrows its threads by. A thread
// matches when any of its messages does, as upstream's table query does.
type threadFilter struct {
	channel    string
	hasChannel bool
	re         *uint64
	by         string
	hasBy      bool
	mentions   []string
	hasMention bool
	labels     []labelFilter
	from       *string
	text       string
}

// threadMatches reports whether the thread satisfies every filter predicate.
// Each predicate may be met by the root or by any reply: a root label and a
// reply's text both count, as a single message needn't carry everything a
// filter names.
func (s *Server) threadMatches(req *request, snapshot *log.Snapshot, root *log.Message, filter threadFilter) bool {
	messages := make([]*log.Message, 0, 1+len(snapshot.AllReplies(root.ID)))
	messages = append(messages, root)
	messages = append(messages, snapshot.AllReplies(root.ID)...)
	any := func(matches func(*log.Message) bool) bool {
		for _, message := range messages {
			if matches(message) {
				return true
			}
		}
		return false
	}
	if filter.hasChannel && !any(func(message *log.Message) bool {
		return channelMatches(message.Channel, filter.channel)
	}) {
		return false
	}
	if filter.hasBy && !any(func(message *log.Message) bool {
		return matchesWho(snapshot, req, filter.by, message.By.Actor, message.By.Principal)
	}) {
		return false
	}
	if filter.hasMention && !any(func(message *log.Message) bool {
		return protocol.MentionsMatch(message.Text, filter.mentions)
	}) {
		return false
	}
	if len(filter.labels) > 0 && !any(func(message *log.Message) bool {
		return labelsMatch(message.Labels, filter.labels)
	}) {
		return false
	}
	if filter.from != nil && !any(func(message *log.Message) bool {
		if *filter.from == "board" {
			return message.From == nil
		}
		return message.From != nil && *message.From == *filter.from
	}) {
		return false
	}
	if filter.text != "" && !any(func(message *log.Message) bool {
		return strings.Contains(strings.ToLower(message.Text), strings.ToLower(filter.text))
	}) {
		return false
	}
	return true
}

// threadEntry is a thread as a page orders it: by its latest live message.
type threadEntry struct {
	root     *log.Message
	activity uint64
	last     *log.Message
	replies  int
}

// summarizeThread returns a thread's ordering position and summary.
func summarizeThread(snapshot *log.Snapshot, root *log.Message) threadEntry {
	entry := threadEntry{root: root, activity: root.ID}
	for _, reply := range snapshot.Replies(root.ID) {
		entry.replies++
		entry.last = reply
	}
	if entry.last != nil {
		entry.activity = entry.last.ID
	}
	return entry
}

// handleThreads answers GET /threads: the threads with a matching message,
// newest activity first, a page at a time. `before` takes a page's `older`
// and pages backward; `after` reads forward and takes no `before`.
func (s *Server) handleThreads(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	var filter threadFilter
	if channel, hasChannel := queryString(r, "channel"); hasChannel {
		if err := protocol.ValidateChannelOrSubtree(channel); err != nil {
			writeError(w, err)
			return
		}
		filter.channel, filter.hasChannel = channel, true
	}
	re, hasRe, perr := queryRef(r, "re")
	if perr != nil {
		writeError(w, perr)
		return
	}
	filter.re = re
	if by, hasBy := queryString(r, "by"); hasBy {
		filter.by, filter.hasBy = by, true
	}
	if mentions, hasMentions := queryString(r, "mentions"); hasMentions {
		filter.hasMention = true
		filter.mentions = mentionTermsFor(req, mentions)
	}
	filter.labels, perr = parseLabels(queryRepeated(r, "label"))
	if perr != nil {
		writeError(w, perr)
		return
	}
	if from, hasFrom := queryString(r, "from"); hasFrom {
		if len(from) == 0 || len(from) > 165 {
			writeError(w, protocol.Invalid("from: name a sync key, or board for the board itself"))
			return
		}
		filter.from = &from
	}
	if text, hasText := queryString(r, "text"); hasText {
		if len(text) == 0 || len(text) > 200 {
			writeError(w, protocol.Invalid("text: a search is 1 to 200 characters"))
			return
		}
		filter.text = text
	}
	after, hasAfter, perr := queryRef(r, "after")
	if perr != nil {
		writeError(w, perr)
		return
	}
	var beforeAt, beforeRoot uint64
	hasBefore := false
	if before, ok := queryString(r, "before"); ok {
		hasBefore = true
		at, root, valid := protocol.ParseThreadPosition(before)
		if !valid {
			writeError(w, protocol.NewError(protocol.CodeInvalidCursor,
				fmt.Sprintf("%s isn't a thread page's cursor on this board", before)))
			return
		}
		beforeAt, beforeRoot = at, root
	}
	if hasBefore && hasAfter {
		writeError(w, protocol.Invalid("`before` pages back from a page's `older`, and `after` reads forward: pass one"))
		return
	}
	limit, perr := queryInt(r, "limit", protocol.ThreadsLimitDefault, 1, protocol.ThreadsLimitMax)
	if perr != nil {
		writeError(w, perr)
		return
	}

	entries := make([]threadEntry, 0)
	cursor := ""
	err := s.board.Read(req.tenant, func(snapshot *log.Snapshot) error {
		cursor = protocol.FormatRef(snapshot.Head())
		var onlyRoot *uint64
		if hasRe {
			if root := snapshot.RootOf(*re); root != nil {
				onlyRoot = &root.ID
			} else {
				// A ref that names nothing matches no thread.
				empty := uint64(0)
				onlyRoot = &empty
			}
		}
		for _, root := range snapshot.AllMessages() {
			if root.Re != nil {
				continue
			}
			if onlyRoot != nil && root.ID != *onlyRoot {
				continue
			}
			if !req.can(s, root.Channel, protocol.PermissionRead) {
				continue
			}
			if !s.threadMatches(req, snapshot, root, filter) {
				continue
			}
			entries = append(entries, summarizeThread(snapshot, root))
		}
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].activity != entries[j].activity {
			return entries[i].activity < entries[j].activity
		}
		return entries[i].root.ID < entries[j].root.ID
	})

	var page []threadEntry
	var older *string
	switch {
	case hasAfter:
		for _, entry := range entries {
			if entry.activity <= *after {
				continue
			}
			page = append(page, entry)
			if len(page) == limit {
				break
			}
		}
	case hasBefore:
		before := make([]threadEntry, 0, len(entries))
		for _, entry := range entries {
			if entry.activity < beforeAt || (entry.activity == beforeAt && entry.root.ID < beforeRoot) {
				before = append(before, entry)
			}
		}
		if len(before) > limit {
			page = before[len(before)-limit:]
			olderRef := protocol.FormatThreadPosition(page[0].activity, page[0].root.ID)
			older = &olderRef
		} else {
			page = before
		}
	default:
		if len(entries) > limit {
			page = entries[len(entries)-limit:]
			olderRef := protocol.FormatThreadPosition(page[0].activity, page[0].root.ID)
			older = &olderRef
		} else {
			page = entries
		}
	}

	threads := make([]protocol.ThreadSummary, 0, len(page))
	err = s.board.Read(req.tenant, func(snapshot *log.Snapshot) error {
		for _, entry := range page {
			threads = append(threads, threadSummary(snapshot, entry.root, &req.actor))
		}
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, protocol.ThreadsPage{Threads: threads, Cursor: cursor, Older: older})
}

// mentionTermsFor turns a `mentions` value into what it matches: `me` is the
// caller and its handle, an actor ID is itself, and a handle also names the
// actors that go by it.
func mentionTermsFor(req *request, value string) []string {
	if value == "me" {
		terms := []string{req.actor.ID}
		if req.actor.Handle != "" {
			terms = append(terms, strings.ToLower(req.actor.Handle))
		}
		return terms
	}
	if strings.Contains(value, ":") {
		return []string{value}
	}
	return []string{strings.ToLower(value)}
}

// handleActors answers GET /actors: who wrote, was mentioned, or reacted in
// the channels the caller reads. `id` names at most 100 of them.
func (s *Server) handleActors(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	wantedValues := queryRepeated(r, "id")
	if len(wantedValues) > protocol.ActorsByIdMax {
		writeError(w, protocol.Invalidf("id: name at most %d actors", protocol.ActorsByIdMax))
		return
	}
	for _, value := range wantedValues {
		if len(value) == 0 || len(value) > 300 {
			writeError(w, protocol.Invalid("id: an actor ID is 1 to 300 characters"))
			return
		}
	}
	var wanted map[string]bool
	if len(wantedValues) > 0 {
		wanted = make(map[string]bool, len(wantedValues))
		for _, value := range wantedValues {
			wanted[value] = true
		}
	}
	visible := make(map[string]bool)
	response := protocol.ActorsPage{Actors: []protocol.ActorWithID{}}
	err := s.board.Read(req.tenant, func(snapshot *log.Snapshot) error {
		response.Cursor = protocol.FormatRef(snapshot.Head())
		for _, message := range snapshot.AllMessages() {
			if !req.can(s, message.Channel, protocol.PermissionRead) {
				continue
			}
			visible[message.By.Actor] = true
			for _, ids := range message.Reactions {
				for _, id := range ids {
					visible[id] = true
				}
			}
			for _, id := range protocol.MentionIDs(message.Text) {
				visible[id] = true
			}
			if message.Task != nil {
				visible[message.Task.Owner] = true
				visible[message.Task.By] = true
				if message.Task.For != nil {
					visible[*message.Task.For] = true
				}
			}
		}
		ids := make([]string, 0, len(visible))
		for id := range visible {
			if wanted != nil && !wanted[id] {
				continue
			}
			if snapshot.Actor(id) == nil {
				continue
			}
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool {
			a, b := snapshot.Actor(ids[i]), snapshot.Actor(ids[j])
			left, right := strings.ToLower(a.Handle), strings.ToLower(b.Handle)
			if left != right {
				return left < right
			}
			return ids[i] < ids[j]
		})
		for _, id := range ids {
			if view := actorView(snapshot, id, &req.actor); view != nil {
				response.Actors = append(response.Actors, protocol.ActorWithID{ID: id, ActorView: *view})
			}
		}
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}
