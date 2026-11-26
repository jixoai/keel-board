package server

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jixoai/keel-board/board"
	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
)

const (
	// taskListPage is the default and maximum task page size.
	taskListDefault = 50
	taskListMax     = 500
	// needsWalkMax is how many tasks a cycle check may visit before it
	// refuses a change it cannot check.
	needsWalkMax = 500
)

// taskResponse is what a task write answers.
type taskResponse struct {
	Task protocol.MessageView `json:"task"`
}

// claimResponse is what a claim answers.
type claimResponse struct {
	Task  protocol.MessageView `json:"task"`
	Claim protocol.TaskClaim   `json:"claim"`
}

// claimReadyResponse answers `{ task: null }` when nothing was ready.
type claimReadyResponse struct {
	Task  *protocol.MessageView `json:"task"`
	Claim *protocol.TaskClaim   `json:"claim,omitempty"`
}

// renewResponse is what a renewal answers.
type renewResponse struct {
	Task  protocol.MessageView `json:"task"`
	Until string               `json:"until"`
}

// stateNow is a task's state as claims and lists take it: a claim whose lease
// ran out is ready at once, whether or not an expiry has been appended.
func stateNow(task *log.Task, now time.Time) string {
	if task.State == "claimed" && task.Claim != nil && !now.Before(task.Claim.Until) {
		return "ready"
	}
	return task.State
}

func lapsed(task *log.Task, now time.Time) bool {
	return task.State == "claimed" && task.Claim != nil && !now.Before(task.Claim.Until)
}

func finished(task *log.Task) bool {
	return task.State == "completed" || task.State == "failed" || task.State == "canceled"
}

// assignedTo reports whether the task is kept for this caller: one of its
// owner's profiles, writing as that profile.
func assignedTo(task *log.Task, actor protocol.Actor) bool {
	return task.For != nil && *task.For == actor.ID && task.Owner == actor.Principal
}

// ownedBy reports whether identity changes a task as its owner: the principal
// that made it, as itself, or the profile it made it as.
func ownedBy(task *log.Task, actor protocol.Actor) bool {
	if task.Owner != actor.Principal {
		return false
	}
	return actor.Profile == nil || task.By == actor.ID
}

// owns refuses a task change for anyone but its owner or manage in its
// channel.
func (s *Server) owns(req *request, root *log.Message) *protocol.Error {
	if req.can(s, root.Channel, protocol.PermissionManage) {
		return nil
	}
	if ownedBy(root.Task, req.actor) &&
		(req.can(s, root.Channel, protocol.PermissionPost) || req.can(s, root.Channel, protocol.PermissionLabel)) {
		return nil
	}
	return protocol.NewError(protocol.CodeTaskNotOwned,
		fmt.Sprintf("#%d is someone else's task: its owner as itself, the profile that made it, or manage in %s, changes it", root.ID, root.Channel))
}

// readTask returns a task's root, or the 404 a caller who can't read it gets.
func (s *Server) readTask(req *request, snapshot *log.Snapshot, id uint64) (*log.Message, *protocol.Error) {
	message := snapshot.Message(id)
	if message == nil || !req.can(s, message.Channel, protocol.PermissionRead) {
		return nil, protocol.NotFound(protocol.CodeMessageNotFound, fmt.Sprintf("message not found: #%d", id))
	}
	if message.Task == nil {
		return nil, protocol.NotFound(protocol.CodeNotATask, fmt.Sprintf("#%d isn't a task", id))
	}
	return message, nil
}

// fenced refuses a write whose claim is over: released, settled, held, taken
// over, or past its lease.
func fenced(root *log.Message, epoch uint64, token string, now time.Time) *protocol.Error {
	task := root.Task
	if task == nil || task.State != "claimed" || task.Claim == nil || task.Epoch != epoch || !now.Before(task.Claim.Until) {
		return claimLost(root.ID)
	}
	want := board.HashToken(token)
	if subtle.ConstantTimeCompare([]byte(want), []byte(task.Claim.TokenHash)) != 1 {
		return claimLost(root.ID)
	}
	return nil
}

func claimLost(id uint64) *protocol.Error {
	return protocol.NewError(protocol.CodeClaimLost, fmt.Sprintf("this claim on #%d is over: claim it again", id))
}

// claimable refuses a claim of a task that can't be claimed now.
func claimable(root *log.Message, actor protocol.Actor, now time.Time) *protocol.Error {
	task := root.Task
	switch {
	case task.State == "waiting":
		return protocol.NewError(protocol.CodeTaskWaiting, fmt.Sprintf("#%d waits for %s", root.ID, formatNeeds(task.Needs)))
	case task.State == "blocked":
		reason := ""
		if task.Blocked != nil {
			reason = task.Blocked.Reason
		}
		return protocol.NewError(protocol.CodeTaskBlocked, fmt.Sprintf("#%d is held for a person: %s", root.ID, reason))
	case finished(task):
		return protocol.NewError(protocol.CodeTaskFinished, fmt.Sprintf("#%d is %s", root.ID, task.State))
	case task.For != nil && !assignedTo(task, actor):
		return protocol.NewError(protocol.CodeTaskAssigned, fmt.Sprintf("#%d is assigned to %s", root.ID, *task.For))
	case task.State == "claimed" && !lapsed(task, now):
		return protocol.NewError(protocol.CodeTaskClaimed, fmt.Sprintf("#%d is claimed until %s", root.ID, protocol.FormatTime(task.Claim.Until)))
	}
	return nil
}

// claimEvents builds the events a claim appends: an expiry first when a
// stale claim still wears claimed, then the new claim.
func claimEvents(root *log.Message, actor protocol.Actor, leaseSeconds int, token string, now time.Time) ([]log.Event, protocol.TaskClaim) {
	epoch := root.Task.Epoch + 1
	until := now.Add(seconds(leaseSeconds))
	untilText := protocol.FormatTime(until)
	events := make([]log.Event, 0, 2)
	if root.Task.State == "claimed" {
		events = append(events, log.Event{Type: log.EventTaskExpired, Payload: &log.TaskExpiredPayload{
			ID: root.ID, Thread: root.Thread, Epoch: root.Task.Epoch,
		}})
	}
	events = append(events, log.Event{Type: log.EventTaskClaimed, Payload: &log.TaskClaimedPayload{
		ID: root.ID, Thread: root.Thread, Epoch: epoch, Until: untilText,
		Token: board.HashToken(token), By: actor.Actored(),
	}})
	return events, protocol.TaskClaim{Epoch: epoch, Token: token, Until: untilText}
}

// emit appends events a helper built; it assigns seqs and fixes post IDs.
func emit(tx *log.Tx, events []log.Event) {
	for _, event := range events {
		tx.Emit(event.Type, event.Payload)
	}
}

// handleListTasks answers GET /tasks.
func (s *Server) handleListTasks(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	channel, hasChannel := queryString(r, "channel")
	if hasChannel {
		if err := protocol.ValidateChannelOrSubtree(channel); err != nil {
			writeError(w, err)
			return
		}
	}
	state, hasState := queryString(r, "state")
	if hasState && state != "open" && state != "all" && !protocol.ValidTaskState(state) {
		writeError(w, protocol.Invalid("state: use open, all, or one task state"))
		return
	}
	if !hasState {
		state = "open"
	}
	labels, perr := parseLabels(queryRepeated(r, "label"))
	if perr != nil {
		writeError(w, perr)
		return
	}
	owner, hasOwner := queryString(r, "owner")
	forName, hasFor := queryString(r, "for")
	by, hasBy := queryString(r, "by")
	var needed *uint64
	if value, ok := queryString(r, "needs"); ok {
		// A ref that names nothing matches no task, as an index lookup with
		// an unknown ID does.
		id, valid := protocol.ParseRef(value)
		if !valid {
			id = 0
		}
		needed = &id
	}
	after, hasAfter, perr := queryRef(r, "after")
	if perr != nil {
		writeError(w, perr)
		return
	}
	latest, _, perr := queryFlag(r, "latest")
	if perr != nil {
		writeError(w, perr)
		return
	}
	if latest && hasAfter {
		writeError(w, protocol.Invalid("latest: `latest` reads every task to find the last active: it takes no `after`"))
		return
	}
	limit, perr := queryInt(r, "limit", taskListDefault, 1, taskListMax)
	if perr != nil {
		writeError(w, perr)
		return
	}

	var page []protocol.ThreadSummary
	cursor := ""
	err := s.board.Read(req.tenant, func(snapshot *log.Snapshot) error {
		cursor = protocol.FormatRef(snapshot.Head())
		now := snapshot.Now()
		selected := make([]*log.Message, 0)
		for _, root := range snapshot.Tasks() {
			if !req.can(s, root.Channel, protocol.PermissionRead) {
				continue
			}
			if hasChannel && !channelMatches(root.Channel, channel) {
				continue
			}
			if !stateMatches(root.Task, state, now) {
				continue
			}
			if !labelsMatch(root.Labels, labels) {
				continue
			}
			if hasBy && !matchesWho(snapshot, req, by, root.By.Actor, root.By.Principal) {
				continue
			}
			if hasOwner && !matchesWho(snapshot, req, owner, root.Task.Owner, root.Task.Owner) {
				continue
			}
			if hasFor {
				if root.Task.For == nil || !matchesWho(snapshot, req, forName, *root.Task.For, *root.Task.For) {
					continue
				}
			}
			if needed != nil && !containsID(root.Task.Needs, *needed) {
				continue
			}
			if hasAfter && root.ID <= *after {
				continue
			}
			selected = append(selected, root)
		}
		if latest {
			sort.SliceStable(selected, func(i, j int) bool {
				return activityAt(snapshot, selected[i]).Before(activityAt(snapshot, selected[j]))
			})
			if len(selected) > limit {
				selected = selected[len(selected)-limit:]
			}
		} else {
			if len(selected) > limit {
				selected = selected[:limit]
			}
			// A full page's cursor picks up after its last task; a short one
			// has read to the head.
			if len(selected) == limit {
				cursor = protocol.FormatRef(selected[len(selected)-1].ID)
			}
		}
		for _, root := range selected {
			page = append(page, threadSummary(snapshot, root, &req.actor))
		}
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if page == nil {
		page = []protocol.ThreadSummary{}
	}
	writeJSON(w, http.StatusOK, protocol.TasksPage{Tasks: page, Cursor: cursor})
}

// activityAt is when a thread was last active: its latest live reply, or its
// root.
func activityAt(snapshot *log.Snapshot, root *log.Message) time.Time {
	latest := root.CreatedAt
	for _, reply := range snapshot.Replies(root.ID) {
		if reply.CreatedAt.After(latest) {
			latest = reply.CreatedAt
		}
	}
	return latest
}

// stateMatches applies a task list's state filter.
func stateMatches(task *log.Task, state string, now time.Time) bool {
	switch state {
	case "all":
		return true
	case "open":
		return task.State == "waiting" || task.State == "ready" || task.State == "claimed" || task.State == "blocked"
	default:
		return stateNow(task, now) == state
	}
}

// handleCreateTask answers POST /tasks: a new thread that's a task, or a
// thread that's there made one.
func (s *Server) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	var body protocol.CreateTaskRequest
	if err := decodeBody(r, &body, false, false); err != nil {
		writeError(w, err)
		return
	}
	if (body.Channel == "") == (body.Re == nil) {
		writeError(w, protocol.Invalid("a task needs `channel` and `text` for a new thread, or `re` for a thread that's there"))
		return
	}
	if len(body.Attachments) > 0 && body.Re != nil {
		writeError(w, protocol.Invalid("`attachments` are a new thread's: `re` makes a task of a thread as it is"))
		return
	}
	if err := protocol.ValidateAttachmentIDs(body.Attachments); err != nil {
		writeError(w, err)
		return
	}
	if body.Lease != nil && !body.Claim {
		writeError(w, protocol.Invalid("lease: `lease` goes with `claim`"))
		return
	}
	if body.Token != "" && !body.Claim {
		writeError(w, protocol.Invalid("token: `token` goes with `claim`"))
		return
	}
	lease, perr := protocol.ValidateLease(body.Lease)
	if perr != nil {
		writeError(w, perr)
		return
	}
	token := body.Token
	if token != "" {
		if err := protocol.ValidateClaimToken(token); err != nil {
			writeError(w, err)
			return
		}
	}
	assignee, perr := protocol.ResolveAssignee(req.actor.Principal, body.For)
	if perr != nil {
		writeError(w, perr)
		return
	}
	var self *uint64
	if body.Re != nil {
		if body.Text != nil || body.Labels != nil || body.NoSync || body.ID != "" || len(body.Attachments) > 0 {
			writeError(w, protocol.Invalid("`text`, `labels`, `noSync`, `attachments`, and `id` are a new thread's: `re` makes a task of a thread as it is"))
			return
		}
		id, valid := protocol.ParseRef(*body.Re)
		if !valid || id == 0 {
			writeError(w, protocol.NotFound(protocol.CodeMessageNotFound, fmt.Sprintf("message not found: #%s", *body.Re)))
			return
		}
		self = &id
	} else {
		if err := protocol.ValidateChannel(body.Channel); err != nil {
			writeError(w, err)
			return
		}
		text := ""
		if body.Text != nil {
			text = *body.Text
		}
		if err := protocol.ValidateMessageText(text, len(body.Attachments) > 0); err != nil {
			writeError(w, err)
			return
		}
		if err := protocol.ValidateBlocks(text); err != nil {
			writeError(w, err)
			return
		}
		if err := protocol.ValidateMentions(text); err != nil {
			writeError(w, err)
			return
		}
		if err := protocol.ValidateNewLabels(body.Labels); err != nil {
			writeError(w, err)
			return
		}
	}
	if err := protocol.ValidateNeedsShape(body.Needs, self); err != nil {
		writeError(w, err)
		return
	}

	clientID := body.ID
	if clientID == "" && body.Re == nil {
		clientID = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	if clientID != "" {
		if err := protocol.ValidateClientID(clientID); err != nil {
			writeError(w, err)
			return
		}
	}

	channel := body.Channel
	rootID := uint64(0)
	if body.Re != nil {
		id, _ := protocol.ParseRef(*body.Re)
		rootID = id
		var rootChannel string
		var rootReply *uint64
		var exists bool
		err := s.board.Read(req.tenant, func(snapshot *log.Snapshot) error {
			root := snapshot.Message(id)
			if root == nil {
				return nil
			}
			exists = true
			rootChannel = root.Channel
			rootReply = root.Re
			return nil
		})
		if err != nil {
			s.fail(w, r, err)
			return
		}
		if !exists || !req.can(s, rootChannel, protocol.PermissionRead) {
			writeError(w, protocol.NotFound(protocol.CodeMessageNotFound, fmt.Sprintf("message not found: #%d", id)))
			return
		}
		if rootReply != nil {
			writeError(w, protocol.Invalidf("#%d is a reply: a task is a thread, so name its root, #%d", id, *rootReply))
			return
		}
		if err := req.permit(s, rootChannel, protocol.PermissionLabel); err != nil {
			writeError(w, err)
			return
		}
		channel = rootChannel
	} else {
		if err := req.permit(s, channel, protocol.PermissionPost); err != nil {
			writeError(w, err)
			return
		}
		if len(body.Labels) > 0 {
			if err := req.permit(s, channel, protocol.PermissionLabel); err != nil {
				writeError(w, err)
				return
			}
		}
	}
	if body.Claim {
		if err := req.permit(s, channel, protocol.PermissionLabel); err != nil {
			writeError(w, err)
			return
		}
	}
	attachments, perr := s.bindAttachments(req, body.Attachments)
	if perr != nil {
		writeError(w, perr)
		return
	}

	reValue := ""
	if body.Re != nil {
		reValue = *body.Re
	}
	var opts log.UpdateOptions
	if clientID != "" {
		opts.Idempotency = &log.Idempotency{
			Key:    "task:" + req.actor.ID + ":" + clientID,
			Digest: fmt.Sprintf("channel=%s;re=%s", channel, reValue),
		}
	}
	var claim *protocol.TaskClaim
	var preview *log.Message
	err := s.board.Update(req.ctx, req.tenant, opts, func(tx *log.Tx) error {
		if replay, ok := tx.Replay(); ok {
			if len(replay.MessageIDs) == 0 {
				return protocol.NewError(protocol.CodeDuplicateID, "that id was already used for a different write")
			}
			preview = log.CloneMessage(tx.Message(replay.MessageIDs[0]))
			claim = nil
			return nil
		}
		if perr := s.verifyAttachments(req, attachments); perr != nil {
			return perr
		}
		now := tx.Now()
		if body.Re != nil {
			existing := tx.Message(rootID)
			if existing == nil {
				return protocol.NotFound(protocol.CodeMessageNotFound, fmt.Sprintf("message not found: #%d", rootID))
			}
			if existing.Deleted {
				return protocol.NewError(protocol.CodeMessageDeleted, "message was deleted")
			}
			if existing.Task != nil {
				return protocol.NewError(protocol.CodeAlreadyATask, fmt.Sprintf("#%d is a task already", rootID))
			}
		}
		needs, completed, perr := s.validateNeeds(req, tx, body.Needs, self)
		if perr != nil {
			return perr
		}
		waits := len(needs) > 0 && !completed
		if body.Claim && waits && body.Re == nil {
			return protocol.NewError(protocol.CodeTaskWaiting,
				fmt.Sprintf("the task would wait for %s: nothing was made", formatNeeds(needs)))
		}
		if body.Claim && waits && body.Re != nil {
			return protocol.NewError(protocol.CodeTaskWaiting,
				fmt.Sprintf("#%d would wait for %s: it wasn't made a task", rootID, formatNeeds(needs)))
		}
		if body.Claim && assignee != nil && *assignee != req.actor.ID {
			if body.Re == nil {
				return protocol.NewError(protocol.CodeTaskAssigned, fmt.Sprintf("the task would be assigned to %s: nothing was made", *assignee))
			}
			return protocol.NewError(protocol.CodeTaskAssigned, fmt.Sprintf("#%d would be assigned to %s: it wasn't made a task", rootID, *assignee))
		}
		if needs == nil {
			needs = []uint64{}
		}

		offset := 0
		if needsActorSeen(tx.Read(), req.actor) {
			tx.Emit(log.EventActorSeen, actorSeenPayload(req.actor))
			offset++
		}
		thread := log.ThreadKey(rootID)
		if body.Re == nil {
			rootID = tx.NextID(offset)
			thread = log.ThreadKey(rootID)
			text := ""
			if body.Text != nil {
				text = *body.Text
			}
			tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
				ID:          rootID,
				Channel:     channel,
				Text:        text,
				Thread:      thread,
				Labels:      nonNilLabels(body.Labels),
				NoSync:      body.NoSync,
				By:          req.actor.Actored(),
				Attachments: attachments,
				Blind:       !req.can(s, channel, protocol.PermissionRead),
			})
		}
		tx.Emit(log.EventTaskCreated, &log.TaskCreatedPayload{
			ID: rootID, Thread: thread, Needs: needs, For: assignee, By: req.actor.Actored(),
		})
		if len(needs) > 0 && completed {
			tx.Emit(log.EventTaskReady, &log.TaskReadyPayload{ID: rootID, Thread: thread})
		}
		if body.Claim {
			if token == "" {
				generated, err := board.NewToken()
				if err != nil {
					return err
				}
				token = generated
			}
			root := &log.Message{ID: rootID, Thread: thread, Task: &log.Task{State: stateFor(needs, completed), Epoch: 0}}
			events, made := claimEvents(root, req.actor, lease, token, now)
			emit(tx, events)
			claim = &made
		}
		tx.RecordResult(rootID)
		preview, _ = tx.Preview(rootID)
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if preview == nil {
		writeError(w, protocol.NotFound(protocol.CodeMessageNotFound, fmt.Sprintf("message not found: #%d", rootID)))
		return
	}
	var view protocol.MessageView
	err = s.board.Read(req.tenant, func(snapshot *log.Snapshot) error {
		view = messageView(snapshot, preview, &req.actor)
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	response := struct {
		Task  protocol.MessageView `json:"task"`
		Claim *protocol.TaskClaim  `json:"claim,omitempty"`
	}{Task: view, Claim: claim}
	writeJSON(w, http.StatusCreated, response)
}

// stateFor is the state a task is in once its creation events land: ready,
// after task.created and any task.ready.
func stateFor([]uint64, bool) string { return "ready" }

// validateNeeds checks prerequisites against the board as the transaction
// sees it, and reports whether they've all completed. A task not there, one
// the caller can't read, one that's canceled, and the task itself are refused
// alike, so `needs` says nothing of what's out of sight.
func (s *Server) validateNeeds(req *request, tx *log.Tx, refs []string, self *uint64) ([]uint64, bool, *protocol.Error) {
	seen := make(map[uint64]bool, len(refs))
	ids := make([]uint64, 0, len(refs))
	for _, ref := range refs {
		id, valid := protocol.ParseRef(ref)
		if !valid || id == 0 {
			return nil, false, protocol.NewError(protocol.CodeInvalidNeeds, fmt.Sprintf("needs: #%s isn't a task", ref))
		}
		if self != nil && id == *self {
			return nil, false, protocol.NewError(protocol.CodeInvalidNeeds, fmt.Sprintf("needs: #%s is the task itself: it can't wait for itself", ref))
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		message := tx.Message(id)
		if message == nil || message.Task == nil || !req.can(s, message.Channel, protocol.PermissionRead) {
			return nil, false, protocol.NewError(protocol.CodeInvalidNeeds, fmt.Sprintf("needs: #%s isn't a task", ref))
		}
		if message.Task.State == "canceled" {
			return nil, false, protocol.NewError(protocol.CodeInvalidNeeds, fmt.Sprintf("needs: #%s was canceled: it can never complete", ref))
		}
		ids = append(ids, id)
	}
	if len(ids) > protocol.TaskNeedsMax {
		return nil, false, protocol.NewError(protocol.CodeInvalidNeeds, fmt.Sprintf("needs: a task waits for at most %d tasks", protocol.TaskNeedsMax))
	}
	completed := true
	for _, id := range ids {
		if tx.Message(id).Task.State != "completed" {
			completed = false
			break
		}
	}
	return ids, completed, nil
}

// satisfied reports whether every task the task waits for has completed,
// leaving out `but`, which is completing now.
func satisfied(tx *log.Tx, task *log.Task, but uint64) bool {
	for _, id := range task.Needs {
		if id == but {
			continue
		}
		message := tx.Message(id)
		if message == nil || message.Task == nil || message.Task.State != "completed" {
			return false
		}
	}
	return true
}

// readied returns the events that ready each waiting task which waited only
// for id, which is completing now.
func (s *Server) readied(tx *log.Tx, id uint64) []log.Event {
	var events []log.Event
	for _, root := range tx.Tasks() {
		task := root.Task
		if task.State != "waiting" || !containsID(task.Needs, id) {
			continue
		}
		if satisfied(tx, task, id) {
			events = append(events, log.Event{Type: log.EventTaskReady, Payload: &log.TaskReadyPayload{ID: root.ID, Thread: root.Thread}})
		}
	}
	return events
}

// acyclic refuses prerequisites that would make task id wait for itself,
// through however many tasks.
func (s *Server) acyclic(req *request, tx *log.Tx, id uint64, added []uint64) *protocol.Error {
	invalid := func(need uint64, why string) *protocol.Error {
		return protocol.NewError(protocol.CodeInvalidNeeds, fmt.Sprintf("needs: #%d %s", need, why))
	}
	from := make(map[uint64]uint64, len(added))
	level := make([]uint64, 0, len(added))
	for _, need := range added {
		if _, ok := from[need]; ok {
			continue
		}
		from[need] = need
		level = append(level, need)
	}
	for len(level) > 0 {
		next := make([]uint64, 0)
		for _, at := range level {
			message := tx.Message(at)
			if message == nil || message.Task == nil {
				continue
			}
			if message.Task.State == "completed" && req.can(s, message.Channel, protocol.PermissionRead) {
				continue
			}
			for _, need := range message.Task.Needs {
				if need == id {
					return invalid(from[at], fmt.Sprintf("waits for #%d already: they would wait for each other", id))
				}
				if _, seen := from[need]; seen {
					continue
				}
				from[need] = from[at]
				next = append(next, need)
			}
		}
		if len(from) > needsWalkMax {
			return invalid(added[0], fmt.Sprintf("has more than %d tasks behind it and the others: too many to check that none waits for #%d", needsWalkMax, id))
		}
		level = next
	}
	return nil
}

// handleClaimTask answers POST /tasks/{id}/claim.
func (s *Server) handleClaimTask(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	id, perr := pathRef(r)
	if perr != nil {
		writeError(w, perr)
		return
	}
	var body protocol.ClaimRequest
	if err := decodeBody(r, &body, false, true); err != nil {
		writeError(w, err)
		return
	}
	lease, perr := protocol.ValidateLease(body.Lease)
	if perr != nil {
		writeError(w, perr)
		return
	}
	token, perr := claimToken(body.Token)
	if perr != nil {
		writeError(w, perr)
		return
	}
	var channel string
	err := s.board.Read(req.tenant, func(snapshot *log.Snapshot) error {
		root, taskErr := s.readTask(req, snapshot, id)
		if taskErr != nil {
			return taskErr
		}
		channel = root.Channel
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
	var claim protocol.TaskClaim
	var preview *log.Message
	err = s.board.Update(req.ctx, req.tenant, log.UpdateOptions{}, func(tx *log.Tx) error {
		root := tx.Message(id)
		if root == nil || root.Task == nil {
			return protocol.NotFound(protocol.CodeNotATask, fmt.Sprintf("#%d isn't a task", id))
		}
		now := tx.Now()
		if body.Token != "" && root.Task.State == "claimed" && root.Task.Claim != nil && !lapsed(root.Task, now) &&
			subtle.ConstantTimeCompare([]byte(board.HashToken(token)), []byte(root.Task.Claim.TokenHash)) == 1 {
			claim = protocol.TaskClaim{Epoch: root.Task.Epoch, Token: token, Until: protocol.FormatTime(root.Task.Claim.Until)}
			preview, _ = tx.Preview(id)
			return nil
		}
		if err := claimable(root, req.actor, now); err != nil {
			return err
		}
		if claimsActorSeen(tx.Read(), req.actor) {
			tx.Emit(log.EventActorSeen, actorSeenPayload(req.actor))
		}
		events, made := claimEvents(root, req.actor, lease, token, now)
		emit(tx, events)
		claim = made
		preview, _ = tx.Preview(id)
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.respondTask(w, r, req, preview, http.StatusOK, &claim, "")
}

// claimsActorSeen is needsActorSeen, named for a claim's own append.
func claimsActorSeen(snapshot *log.Snapshot, actor protocol.Actor) bool {
	return needsActorSeen(snapshot, actor)
}

func claimToken(value string) (string, *protocol.Error) {
	if value == "" {
		token, err := board.NewToken()
		if err != nil {
			return "", protocol.NewError(protocol.CodeInternal, "could not make a claim token")
		}
		return token, nil
	}
	if err := protocol.ValidateClaimToken(value); err != nil {
		return "", err
	}
	return value, nil
}

// handleClaimReady answers POST /tasks/claim: the oldest ready task in a
// channel or subtree, waiting up to `wait` seconds for one.
func (s *Server) handleClaimReady(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	var body protocol.ClaimReadyRequest
	if err := decodeBody(r, &body, false, false); err != nil {
		writeError(w, err)
		return
	}
	if err := protocol.ValidateChannelOrSubtree(body.Channel); err != nil {
		writeError(w, err)
		return
	}
	lease, perr := protocol.ValidateLease(body.Lease)
	if perr != nil {
		writeError(w, perr)
		return
	}
	wait := 0.0
	if body.Wait != nil {
		wait = *body.Wait
	}
	if wait < 0 || wait > 300 {
		writeError(w, protocol.Invalid("wait: wait is 0 to 300 seconds"))
		return
	}
	token, perr := claimToken(body.Token)
	if perr != nil {
		writeError(w, perr)
		return
	}
	owners := map[string]bool{req.actor.Principal: true}
	for _, value := range body.From {
		if err := protocol.ValidateOwnerFrom(value); err != nil {
			writeError(w, err)
			return
		}
		owners[value] = true
	}
	subtree := strings.HasSuffix(body.Channel, "/")
	channel := strings.TrimSuffix(body.Channel, "/")
	if !subtree {
		if err := req.permit(s, channel, protocol.PermissionLabel); err != nil {
			writeError(w, err)
			return
		}
	}
	deadline := time.Now().Add(time.Duration(wait * float64(time.Second)))
	pause := 25 * time.Millisecond
	for {
		var claim *protocol.TaskClaim
		var preview *log.Message
		err := s.board.Update(req.ctx, req.tenant, log.UpdateOptions{}, func(tx *log.Tx) error {
			now := tx.Now()
			root := s.selectReady(tx, req, channel, subtree, body.Label, owners, now)
			if root == nil {
				return nil
			}
			if claimsActorSeen(tx.Read(), req.actor) {
				tx.Emit(log.EventActorSeen, actorSeenPayload(req.actor))
			}
			events, made := claimEvents(root, req.actor, lease, token, now)
			emit(tx, events)
			claim = &made
			preview, _ = tx.Preview(root.ID)
			return nil
		})
		if err != nil {
			s.fail(w, r, err)
			return
		}
		if preview != nil {
			// A claim made under a token the caller brought may be the one it
			// already holds: it still answers the claim.
			s.respondTask(w, r, req, preview, http.StatusOK, claim, "")
			return
		}
		if req.ctx.Err() != nil {
			writeJSON(w, http.StatusOK, claimReadyResponse{Task: nil})
			return
		}
		if time.Now().After(deadline) {
			writeJSON(w, http.StatusOK, claimReadyResponse{Task: nil})
			return
		}
		timer := time.NewTimer(pause)
		select {
		case <-req.ctx.Done():
			timer.Stop()
			writeJSON(w, http.StatusOK, claimReadyResponse{Task: nil})
			return
		case <-timer.C:
		}
		if pause < 250*time.Millisecond {
			pause *= 2
		}
	}
}

// selectReady picks the task a claim-ready takes: one assigned to the caller
// first, then the oldest unassigned one from an owner it takes tasks from.
func (s *Server) selectReady(tx *log.Tx, req *request, channel string, subtree bool, labels map[string]string, owners map[string]bool, now time.Time) *log.Message {
	var pool *log.Message
	for _, root := range tx.Tasks() {
		if subtree {
			if root.Channel != channel && !strings.HasPrefix(root.Channel, channel+"/") {
				continue
			}
		} else if root.Channel != channel {
			continue
		}
		if stateNow(root.Task, now) != "ready" {
			continue
		}
		if !req.can(s, root.Channel, protocol.PermissionLabel) {
			continue
		}
		if !labelsMapMatch(root.Labels, labels) {
			continue
		}
		if root.Task.For != nil {
			if assignedTo(root.Task, req.actor) {
				return root
			}
			continue
		}
		if pool == nil && owners[root.Task.Owner] {
			pool = root
		}
	}
	return pool
}

// handleRenewTask answers POST /tasks/{id}/renew.
func (s *Server) handleRenewTask(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	id, perr := pathRef(r)
	if perr != nil {
		writeError(w, perr)
		return
	}
	var body protocol.RenewRequest
	if err := decodeBody(r, &body, false, false); err != nil {
		writeError(w, err)
		return
	}
	if perr := validateProof(body.Epoch, body.Token); perr != nil {
		writeError(w, perr)
		return
	}
	lease, perr := protocol.ValidateLease(body.Lease)
	if perr != nil {
		writeError(w, perr)
		return
	}
	channel, err := s.taskChannel(req, id)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if err := req.permit(s, channel, protocol.PermissionRead); err != nil {
		writeError(w, err)
		return
	}
	var until string
	var preview *log.Message
	err = s.board.Update(req.ctx, req.tenant, log.UpdateOptions{}, func(tx *log.Tx) error {
		root := tx.Message(id)
		if root == nil || root.Task == nil {
			return protocol.NotFound(protocol.CodeNotATask, fmt.Sprintf("#%d isn't a task", id))
		}
		now := tx.Now()
		if perr := fenced(root, body.Epoch, body.Token, now); perr != nil {
			return perr
		}
		until = protocol.FormatTime(now.Add(seconds(lease)))
		if needsActorSeen(tx.Read(), req.actor) {
			tx.Emit(log.EventActorSeen, actorSeenPayload(req.actor))
		}
		tx.Emit(log.EventTaskRenewed, &log.TaskRenewedPayload{
			ID: id, Thread: root.Thread, Epoch: body.Epoch, Until: until, By: req.actor.Actored(),
		})
		preview, _ = tx.Preview(id)
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.respondTask(w, r, req, preview, http.StatusOK, nil, until)
}

// handleReleaseTask answers POST /tasks/{id}/release.
func (s *Server) handleReleaseTask(w http.ResponseWriter, r *http.Request) { s.settle(w, r, "release") }

// handleCompleteTask answers POST /tasks/{id}/complete.
func (s *Server) handleCompleteTask(w http.ResponseWriter, r *http.Request) {
	s.settle(w, r, "complete")
}

// handleFailTask answers POST /tasks/{id}/fail.
func (s *Server) handleFailTask(w http.ResponseWriter, r *http.Request) { s.settle(w, r, "fail") }

// settle handles release, complete, and fail: a claim's own writes.
func (s *Server) settle(w http.ResponseWriter, r *http.Request, how string) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	id, perr := pathRef(r)
	if perr != nil {
		writeError(w, perr)
		return
	}
	var body protocol.ClaimedRequest
	if err := decodeBody(r, &body, false, false); err != nil {
		writeError(w, err)
		return
	}
	if perr := validateProof(body.Epoch, body.Token); perr != nil {
		writeError(w, perr)
		return
	}
	var result *string
	var reason string
	switch how {
	case "complete":
		if body.Result != nil {
			if err := protocol.ValidateNote("result", *body.Result); err != nil {
				writeError(w, err)
				return
			}
			result = body.Result
		}
	case "fail":
		if body.Reason == nil {
			writeError(w, protocol.Invalid("reason: a task's reason is required"))
			return
		}
		if err := protocol.ValidateNote("reason", *body.Reason); err != nil {
			writeError(w, err)
			return
		}
		reason = *body.Reason
	}
	channel, err := s.taskChannel(req, id)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if err := req.permit(s, channel, protocol.PermissionRead); err != nil {
		writeError(w, err)
		return
	}
	var preview *log.Message
	err = s.board.Update(req.ctx, req.tenant, log.UpdateOptions{}, func(tx *log.Tx) error {
		root := tx.Message(id)
		if root == nil || root.Task == nil {
			return protocol.NotFound(protocol.CodeNotATask, fmt.Sprintf("#%d isn't a task", id))
		}
		if perr := fenced(root, body.Epoch, body.Token, tx.Now()); perr != nil {
			return perr
		}
		if needsActorSeen(tx.Read(), req.actor) {
			tx.Emit(log.EventActorSeen, actorSeenPayload(req.actor))
		}
		switch how {
		case "release":
			tx.Emit(log.EventTaskReleased, &log.TaskReleasedPayload{ID: id, Thread: root.Thread, Epoch: body.Epoch, By: req.actor.Actored()})
		case "complete":
			tx.Emit(log.EventTaskCompleted, &log.TaskCompletedPayload{
				ID: id, Thread: root.Thread, Epoch: body.Epoch, Result: result, By: req.actor.Actored(),
			})
			emit(tx, s.readied(tx, id))
		case "fail":
			tx.Emit(log.EventTaskFailed, &log.TaskFailedPayload{
				ID: id, Thread: root.Thread, Epoch: body.Epoch, Reason: reason, By: req.actor.Actored(),
			})
		}
		preview, _ = tx.Preview(id)
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.respondTask(w, r, req, preview, http.StatusOK, nil, "")
}

// handleBlockTask answers POST /tasks/{id}/block.
func (s *Server) handleBlockTask(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	id, perr := pathRef(r)
	if perr != nil {
		writeError(w, perr)
		return
	}
	var body protocol.BlockRequest
	if err := decodeBody(r, &body, false, false); err != nil {
		writeError(w, err)
		return
	}
	if err := protocol.ValidateNote("reason", body.Reason); err != nil {
		writeError(w, err)
		return
	}
	if body.Kind != nil {
		if *body.Kind == "" {
			writeError(w, protocol.Invalid("kind: a kind is 1 to 64 lowercase letters, numbers, ., _, and -"))
			return
		}
		if err := protocol.ValidateHoldKind(*body.Kind); err != nil {
			writeError(w, err)
			return
		}
	}
	if (body.Epoch == nil) != (body.Token == nil) {
		writeError(w, protocol.Invalid("a claim is its `epoch` and its `token`: pass both, or neither"))
		return
	}
	if body.Epoch != nil {
		if *body.Epoch == 0 || *body.Token == "" {
			writeError(w, protocol.Invalid("a claim is its `epoch` and its `token`: pass both, or neither"))
			return
		}
	}
	channel, err := s.taskChannel(req, id)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if err := req.permit(s, channel, protocol.PermissionRead); err != nil {
		writeError(w, err)
		return
	}
	var preview *log.Message
	err = s.board.Update(req.ctx, req.tenant, log.UpdateOptions{}, func(tx *log.Tx) error {
		root := tx.Message(id)
		if root == nil || root.Task == nil {
			return protocol.NotFound(protocol.CodeNotATask, fmt.Sprintf("#%d isn't a task", id))
		}
		now := tx.Now()
		if body.Epoch != nil {
			if perr := fenced(root, *body.Epoch, *body.Token, now); perr != nil {
				return perr
			}
		} else {
			if perr := s.owns(req, root); perr != nil {
				return perr
			}
			state := stateNow(root.Task, now)
			switch {
			case state == "claimed":
				return protocol.NewError(protocol.CodeTaskClaimed, fmt.Sprintf("#%d is claimed until %s: only its claim holds it", id, protocol.FormatTime(root.Task.Claim.Until)))
			case state == "blocked":
				reason := ""
				if root.Task.Blocked != nil {
					reason = root.Task.Blocked.Reason
				}
				return protocol.NewError(protocol.CodeTaskBlocked, fmt.Sprintf("#%d is held already: %s", id, reason))
			case finished(root.Task):
				return protocol.NewError(protocol.CodeTaskFinished, fmt.Sprintf("#%d is %s", id, root.Task.State))
			}
		}
		if needsActorSeen(tx.Read(), req.actor) {
			tx.Emit(log.EventActorSeen, actorSeenPayload(req.actor))
		}
		tx.Emit(log.EventTaskBlocked, &log.TaskBlockedPayload{
			ID: id, Thread: root.Thread, Kind: body.Kind, Reason: body.Reason, By: req.actor.Actored(),
		})
		preview, _ = tx.Preview(id)
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.respondTask(w, r, req, preview, http.StatusOK, nil, "")
}

// handleUnblockTask answers POST /tasks/{id}/unblock.
func (s *Server) handleUnblockTask(w http.ResponseWriter, r *http.Request) {
	s.simpleTaskChange(w, r, func(tx *log.Tx, root *log.Message, author log.Author) ([]log.Event, *protocol.Error) {
		if finished(root.Task) {
			return nil, protocol.NewError(protocol.CodeTaskFinished, fmt.Sprintf("#%d is %s", root.ID, root.Task.State))
		}
		if root.Task.State != "blocked" {
			return nil, nil
		}
		events := []log.Event{{Type: log.EventTaskUnblocked, Payload: &log.TaskUnblockedPayload{ID: root.ID, Thread: root.Thread, By: author}}}
		if len(root.Task.Needs) > 0 {
			all := true
			for _, need := range root.Task.Needs {
				message := tx.Message(need)
				if message == nil || message.Task == nil || message.Task.State != "completed" {
					all = false
					break
				}
			}
			if all {
				events = append(events, log.Event{Type: log.EventTaskReady, Payload: &log.TaskReadyPayload{ID: root.ID, Thread: root.Thread}})
			}
		}
		return events, nil
	})
}

// handleReopenTask answers POST /tasks/{id}/reopen.
func (s *Server) handleReopenTask(w http.ResponseWriter, r *http.Request) {
	s.simpleTaskChange(w, r, func(tx *log.Tx, root *log.Message, author log.Author) ([]log.Event, *protocol.Error) {
		if root.Task.State == "completed" || root.Task.State == "canceled" {
			return nil, protocol.NewError(protocol.CodeTaskFinished, fmt.Sprintf("#%d is %s", root.ID, root.Task.State))
		}
		if root.Task.State != "failed" {
			return nil, nil
		}
		return []log.Event{{Type: log.EventTaskReopened, Payload: &log.TaskReopenedPayload{ID: root.ID, Thread: root.Thread, By: author}}}, nil
	})
}

// handleAssignTask answers POST /tasks/{id}/assign.
func (s *Server) handleAssignTask(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	id, perr := pathRef(r)
	if perr != nil {
		writeError(w, perr)
		return
	}
	var body protocol.AssignRequest
	if err := decodeBody(r, &body, false, false); err != nil {
		writeError(w, err)
		return
	}
	if !body.For.Set {
		writeError(w, protocol.Invalid("for: name one of the owner's profiles, or null for anyone who may claim it"))
		return
	}
	var assigned *string
	var preview *log.Message
	err := s.board.Update(req.ctx, req.tenant, log.UpdateOptions{}, func(tx *log.Tx) error {
		root := tx.Message(id)
		if root == nil || root.Task == nil {
			return protocol.NotFound(protocol.CodeNotATask, fmt.Sprintf("#%d isn't a task", id))
		}
		if perr := s.owns(req, root); perr != nil {
			return perr
		}
		var perr *protocol.Error
		assigned, perr = protocol.ResolveAssignee(root.Task.Owner, body.For.Value)
		if perr != nil {
			return perr
		}
		if finished(root.Task) {
			return protocol.NewError(protocol.CodeTaskFinished, fmt.Sprintf("#%d is %s", id, root.Task.State))
		}
		same := (root.Task.For == nil && assigned == nil) || (root.Task.For != nil && assigned != nil && *root.Task.For == *assigned)
		if same {
			preview, _ = tx.Preview(id)
			return nil
		}
		if needsActorSeen(tx.Read(), req.actor) {
			tx.Emit(log.EventActorSeen, actorSeenPayload(req.actor))
		}
		tx.Emit(log.EventTaskAssigned, &log.TaskAssignedPayload{ID: id, Thread: root.Thread, For: assigned, By: req.actor.Actored()})
		preview, _ = tx.Preview(id)
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.respondTask(w, r, req, preview, http.StatusOK, nil, "")
}

// handleSetTaskNeeds answers POST /tasks/{id}/needs.
func (s *Server) handleSetTaskNeeds(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	id, perr := pathRef(r)
	if perr != nil {
		writeError(w, perr)
		return
	}
	var body protocol.NeedsRequest
	if err := decodeBody(r, &body, false, false); err != nil {
		writeError(w, err)
		return
	}
	if body.Needs == nil {
		writeError(w, protocol.Invalid("needs: the tasks it waits for are required"))
		return
	}
	refs := *body.Needs
	if err := protocol.ValidateNeedsShape(refs, &id); err != nil {
		writeError(w, err)
		return
	}
	var preview *log.Message
	err := s.board.Update(req.ctx, req.tenant, log.UpdateOptions{}, func(tx *log.Tx) error {
		root := tx.Message(id)
		if root == nil || root.Task == nil {
			return protocol.NotFound(protocol.CodeNotATask, fmt.Sprintf("#%d isn't a task", id))
		}
		if perr := s.owns(req, root); perr != nil {
			return perr
		}
		task := root.Task
		if finished(task) {
			return protocol.NewError(protocol.CodeTaskFinished, fmt.Sprintf("#%d is %s", id, task.State))
		}
		now := tx.Now()
		over := lapsed(task, now)
		if task.State == "claimed" && !over {
			return protocol.NewError(protocol.CodeTaskClaimed, fmt.Sprintf("#%d is claimed until %s: what it needs changes once no one holds it", id, protocol.FormatTime(task.Claim.Until)))
		}
		needs, completed, perr := s.validateNeeds(req, tx, refs, &id)
		if perr != nil {
			return perr
		}
		if sameIDs(needs, task.Needs) {
			preview, _ = tx.Preview(id)
			return nil
		}
		added := make([]uint64, 0)
		for _, need := range needs {
			if !containsID(task.Needs, need) {
				added = append(added, need)
			}
		}
		if len(added) > 0 {
			if perr := s.acyclic(req, tx, id, added); perr != nil {
				return perr
			}
		}
		if needsActorSeen(tx.Read(), req.actor) {
			tx.Emit(log.EventActorSeen, actorSeenPayload(req.actor))
		}
		if over {
			tx.Emit(log.EventTaskExpired, &log.TaskExpiredPayload{ID: id, Thread: root.Thread, Epoch: task.Epoch})
		}
		tx.Emit(log.EventTaskNeedsChange, &log.TaskNeedsChangedPayload{ID: id, Thread: root.Thread, Needs: needs, By: req.actor.Actored()})
		if len(needs) > 0 && completed && task.State != "blocked" {
			tx.Emit(log.EventTaskReady, &log.TaskReadyPayload{ID: id, Thread: root.Thread})
		}
		preview, _ = tx.Preview(id)
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.respondTask(w, r, req, preview, http.StatusOK, nil, "")
}

// handleCancelTask answers POST /tasks/{id}/cancel.
func (s *Server) handleCancelTask(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	id, perr := pathRef(r)
	if perr != nil {
		writeError(w, perr)
		return
	}
	var body protocol.CancelRequest
	if err := decodeBody(r, &body, false, false); err != nil {
		writeError(w, err)
		return
	}
	if err := protocol.ValidateNote("reason", body.Reason); err != nil {
		writeError(w, err)
		return
	}
	var preview *log.Message
	err := s.board.Update(req.ctx, req.tenant, log.UpdateOptions{}, func(tx *log.Tx) error {
		root := tx.Message(id)
		if root == nil || root.Task == nil {
			return protocol.NotFound(protocol.CodeNotATask, fmt.Sprintf("#%d isn't a task", id))
		}
		if perr := s.owns(req, root); perr != nil {
			return perr
		}
		if root.Task.State == "completed" || root.Task.State == "canceled" {
			return protocol.NewError(protocol.CodeTaskFinished, fmt.Sprintf("#%d is %s", id, root.Task.State))
		}
		if needsActorSeen(tx.Read(), req.actor) {
			tx.Emit(log.EventActorSeen, actorSeenPayload(req.actor))
		}
		tx.Emit(log.EventTaskCanceled, &log.TaskCanceledPayload{ID: id, Thread: root.Thread, Reason: body.Reason, By: req.actor.Actored()})
		preview, _ = tx.Preview(id)
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.respondTask(w, r, req, preview, http.StatusOK, nil, "")
}

// simpleTaskChange handles a route that needs no body but ownership: it loads
// the task, checks the caller owns it, and appends what change decides.
func (s *Server) simpleTaskChange(w http.ResponseWriter, r *http.Request, change func(tx *log.Tx, root *log.Message, author log.Author) ([]log.Event, *protocol.Error)) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	id, perr := pathRef(r)
	if perr != nil {
		writeError(w, perr)
		return
	}
	var preview *log.Message
	err := s.board.Update(req.ctx, req.tenant, log.UpdateOptions{}, func(tx *log.Tx) error {
		root := tx.Message(id)
		if root == nil || root.Task == nil {
			return protocol.NotFound(protocol.CodeNotATask, fmt.Sprintf("#%d isn't a task", id))
		}
		if perr := s.owns(req, root); perr != nil {
			return perr
		}
		events, perr := change(tx, root, req.actor.Actored())
		if perr != nil {
			return perr
		}
		if len(events) > 0 && needsActorSeen(tx.Read(), req.actor) {
			tx.Emit(log.EventActorSeen, actorSeenPayload(req.actor))
		}
		emit(tx, events)
		preview = log.CloneMessage(root)
		if len(events) > 0 {
			preview, _ = tx.Preview(id)
		}
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.respondTask(w, r, req, preview, http.StatusOK, nil, "")
}

// taskChannel returns the channel of a task the caller may read, so a write
// can authorize before its transaction.
func (s *Server) taskChannel(req *request, id uint64) (string, error) {
	var channel string
	err := s.board.Read(req.tenant, func(snapshot *log.Snapshot) error {
		root, perr := s.readTask(req, snapshot, id)
		if perr != nil {
			return perr
		}
		channel = root.Channel
		return nil
	})
	if err != nil {
		if perr, ok := protocol.AsError(err); ok {
			return "", perr
		}
		return "", protocol.NewError(protocol.CodeInternal, "internal error")
	}
	return channel, nil
}

// respondTask writes a task write's answer from the message it previewed.
func (s *Server) respondTask(w http.ResponseWriter, r *http.Request, req *request, preview *log.Message, status int, claim *protocol.TaskClaim, until string) {
	if preview == nil {
		s.fail(w, r, errors.New("a task write answered nothing"))
		return
	}
	var view protocol.MessageView
	err := s.board.Read(req.tenant, func(snapshot *log.Snapshot) error {
		view = messageView(snapshot, preview, &req.actor)
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	switch {
	case claim != nil:
		writeJSON(w, status, claimResponse{Task: view, Claim: *claim})
	case until != "":
		writeJSON(w, status, renewResponse{Task: view, Until: until})
	default:
		writeJSON(w, status, taskResponse{Task: view})
	}
}

// validateProof checks a claim's epoch and token as a request sends them.
func validateProof(epoch uint64, token string) *protocol.Error {
	if epoch == 0 {
		return protocol.Invalid("epoch: a claim's epoch is a positive number")
	}
	if token == "" {
		return protocol.Invalid("token: a claim's token is required")
	}
	return nil
}

// formatNeeds renders prerequisite IDs as "#1, #2".
func formatNeeds(ids []uint64) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = "#" + strconv.FormatUint(id, 10)
	}
	return strings.Join(parts, ", ")
}

func sameIDs(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for _, id := range a {
		if !containsID(b, id) {
			return false
		}
	}
	return true
}

func containsID(ids []uint64, want uint64) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

// channelMatches reports whether a channel is the named one, or under it
// when the name ends in "/".
func channelMatches(channel, want string) bool {
	if strings.HasSuffix(want, "/") {
		base := strings.TrimSuffix(want, "/")
		return channel == base || strings.HasPrefix(channel, base+"/")
	}
	return channel == want
}

// labelFilter is one `key` or `key=value|value` filter.
type labelFilter struct {
	key    string
	values []string
}

// parseLabels parses repeated label query values.
func parseLabels(values []string) ([]labelFilter, *protocol.Error) {
	out := make([]labelFilter, 0, len(values))
	for _, value := range values {
		key, rest, hasValue := strings.Cut(value, "=")
		if err := protocol.ValidateLabelKey(key); err != nil {
			return nil, protocol.Invalidf("label: %s", err.Message)
		}
		filter := labelFilter{key: key}
		if hasValue {
			filter.values = strings.Split(rest, "|")
		}
		out = append(out, filter)
	}
	return out, nil
}

// labelsMapMatch reports whether labels carry every key a claim-ready's label
// map names with its value.
func labelsMapMatch(labels, want map[string]string) bool {
	for key, value := range want {
		if labels[key] != value {
			return false
		}
	}
	return true
}

// labelsMatch reports whether labels (which include board.task) carry every
// filter.
func labelsMatch(labels map[string]string, filters []labelFilter) bool {
	for _, filter := range filters {
		value, ok := labels[filter.key]
		if !ok {
			return false
		}
		if filter.values != nil {
			matched := false
			for _, want := range filter.values {
				if value == want {
					matched = true
					break
				}
			}
			if !matched {
				return false
			}
		}
	}
	return true
}

// matchesWho reports whether a `by`, `owner`, or `for` value names the actor
// or principal. `me` is the caller's own actor.
func matchesWho(snapshot *log.Snapshot, req *request, value, actor, principal string) bool {
	if value == "me" {
		return actor == req.actor.ID
	}
	if value == actor || value == principal {
		return true
	}
	for _, id := range snapshot.ActorsByHandle(value) {
		if id == actor {
			return true
		}
		// A person's handle covers their agents: a message written as
		// "<principal>+claude" is written by the principal too.
		if id == principal {
			return true
		}
		if index := strings.Index(id, "+"); index > 0 && id[:index] == principal {
			return true
		}
	}
	return false
}
