package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
)

// A batch makes several new threads in one channel, each a task, in one
// append: all of them or none. This is upstream's `POST /tasks/batch`; the
// upstream has no separate plan entry, so this is also the smallest consistent
// "plan" a caller can build: a batch whose tasks wait on each other by key is
// the plan.

// batchPlan is what a batch's tasks need, resolved before anything is read:
// the batch's own edges by key, and the refs to tasks already there.
type batchPlan struct {
	keys     map[string]int
	internal [][]int
	external [][]string
}

// planBatch checks a batch's keys and needs, including that no task waits for
// itself through the batch, and returns its edges.
func planBatch(tasks []protocol.BatchTask) (*batchPlan, *protocol.Error) {
	plan := &batchPlan{keys: make(map[string]int, len(tasks)), internal: make([][]int, len(tasks)), external: make([][]string, len(tasks))}
	for at, task := range tasks {
		if task.Key == "" {
			continue
		}
		if err := protocol.ValidateTaskKey(task.Key); err != nil {
			return nil, err
		}
		if _, taken := plan.keys[task.Key]; taken {
			return nil, protocol.NewError(protocol.CodeInvalidNeeds,
				fmt.Sprintf("needs: key %s names two tasks in the batch: a key is one task's", task.Key))
		}
		plan.keys[task.Key] = at
	}
	for at, task := range tasks {
		seen := make(map[string]bool, len(task.Needs))
		for _, need := range task.Needs {
			switch {
			case need.Key != "" && need.Ref != "":
				return nil, protocol.NewError(protocol.CodeInvalidNeeds, "needs: a need is a ref or a key, not both")
			case need.Key != "":
				if err := protocol.ValidateTaskKey(need.Key); err != nil {
					return nil, err
				}
				target, ok := plan.keys[need.Key]
				if !ok {
					return nil, protocol.NewError(protocol.CodeInvalidNeeds,
						fmt.Sprintf("needs: key %s names no task in the batch", need.Key))
				}
				key := "key:" + need.Key
				if seen[key] {
					continue
				}
				seen[key] = true
				plan.internal[at] = append(plan.internal[at], target)
			case need.Ref != "":
				if err := protocol.ValidateRef(need.Ref); err != nil {
					return nil, protocol.NewError(protocol.CodeInvalidNeeds, fmt.Sprintf("needs: %q isn't a task", need.Ref))
				}
				id, ok := protocol.ParseRef(need.Ref)
				if !ok || id == 0 {
					return nil, protocol.NewError(protocol.CodeInvalidNeeds, fmt.Sprintf("needs: %q isn't a task", need.Ref))
				}
				key := fmt.Sprintf("ref:%d", id)
				if seen[key] {
					continue
				}
				seen[key] = true
				plan.external[at] = append(plan.external[at], need.Ref)
			default:
				return nil, protocol.NewError(protocol.CodeInvalidNeeds, "needs: a need is a ref or a key")
			}
		}
		if len(seen) > protocol.TaskNeedsMax {
			return nil, protocol.NewError(protocol.CodeInvalidNeeds,
				fmt.Sprintf("tasks.%d.needs: a task waits for at most %d tasks", at, protocol.TaskNeedsMax))
		}
	}
	if problem := batchCycle(plan); problem != "" {
		return nil, protocol.NewError(protocol.CodeInvalidNeeds, problem)
	}
	return plan, nil
}

// batchCycle reports the first cycle in the batch's own edges.
func batchCycle(plan *batchPlan) string {
	const (
		unseen = iota
		visiting
		done
	)
	state := make([]int, len(plan.internal))
	var visit func(int) string
	visit = func(at int) string {
		switch state[at] {
		case visiting:
			return fmt.Sprintf("needs: tasks.%d waits for itself through the batch: they would wait for each other", at)
		case done:
			return ""
		}
		state[at] = visiting
		for _, next := range plan.internal[at] {
			if problem := visit(next); problem != "" {
				return problem
			}
		}
		state[at] = done
		return ""
	}
	for at := range plan.internal {
		if problem := visit(at); problem != "" {
			return problem
		}
	}
	return ""
}

// batchDigest is what tells one batch request under an `id` from another. A
// fronting board's `request` is used as it is; without one the board hashes
// the batch as given.
func batchDigest(body protocol.CreateTasksRequest) (string, *protocol.Error) {
	if body.Request != "" {
		if err := protocol.ValidateBatchRequest(body.Request); err != nil {
			return "", err
		}
		return "request:" + body.Request, nil
	}
	type digestTask struct {
		Key         string            `json:"key,omitempty"`
		Text        string            `json:"text"`
		Labels      map[string]string `json:"labels,omitempty"`
		NoSync      bool              `json:"noSync,omitempty"`
		Attachments []string          `json:"attachments,omitempty"`
		Needs       []string          `json:"needs,omitempty"`
		For         *string           `json:"for"`
	}
	tasks := make([]digestTask, len(body.Tasks))
	for at, task := range body.Tasks {
		needs := make([]string, 0, len(task.Needs))
		for _, need := range task.Needs {
			if need.Key != "" {
				needs = append(needs, "key:"+need.Key)
			} else {
				needs = append(needs, "ref:"+need.Ref)
			}
		}
		tasks[at] = digestTask{
			Key:         task.Key,
			Text:        task.Text,
			Labels:      task.Labels,
			NoSync:      task.NoSync,
			Attachments: task.Attachments,
			Needs:       needs,
			For:         task.For.Value,
		}
	}
	encoded, err := json.Marshal(struct {
		Channel string       `json:"channel"`
		Tasks   []digestTask `json:"tasks"`
	}{Channel: body.Channel, Tasks: tasks})
	if err != nil {
		return "", protocol.NewError(protocol.CodeInternal, "could not digest the batch")
	}
	sum := sha256.Sum256(encoded)
	return "batch:" + hex.EncodeToString(sum[:]), nil
}

func batchKey(actorID, id string) string { return "taskbatch:" + actorID + ":" + id }

// handleBatchTasks answers POST /tasks/batch.
func (s *Server) handleBatchTasks(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	var body protocol.CreateTasksRequest
	if err := decodeBody(r, &body, false, false); err != nil {
		writeError(w, err)
		return
	}
	if err := protocol.ValidateChannel(body.Channel); err != nil {
		writeError(w, err)
		return
	}
	if len(body.Tasks) == 0 || len(body.Tasks) > protocol.TaskBatchMax {
		writeError(w, protocol.NewError(protocol.CodeBatchTooLarge,
			fmt.Sprintf("a batch makes 1 to %d tasks", protocol.TaskBatchMax)))
		return
	}
	if body.Request != "" {
		if err := protocol.ValidateBatchRequest(body.Request); err != nil {
			writeError(w, err)
			return
		}
	}
	if body.ID != "" {
		if err := protocol.ValidateClientID(body.ID); err != nil {
			writeError(w, err)
			return
		}
	}
	assignees := make([]*string, len(body.Tasks))
	hasLabels := false
	for at, task := range body.Tasks {
		if task.Key != "" {
			if err := protocol.ValidateTaskKey(task.Key); err != nil {
				writeError(w, err)
				return
			}
		}
		if err := protocol.ValidateAttachmentIDs(task.Attachments); err != nil {
			writeError(w, err)
			return
		}
		if err := protocol.ValidateMessageText(task.Text, len(task.Attachments) > 0); err != nil {
			writeError(w, err)
			return
		}
		if err := protocol.ValidateBlocks(task.Text); err != nil {
			writeError(w, err)
			return
		}
		if err := protocol.ValidateMentions(task.Text); err != nil {
			writeError(w, err)
			return
		}
		if err := protocol.ValidateNewLabels(task.Labels); err != nil {
			writeError(w, err)
			return
		}
		if len(task.Labels) > 0 {
			hasLabels = true
		}
		assignee, perr := protocol.ResolveAssignee(req.actor.Principal, task.For.Value)
		if perr != nil {
			writeError(w, perr)
			return
		}
		assignees[at] = assignee
	}
	plan, perr := planBatch(body.Tasks)
	if perr != nil {
		writeError(w, perr)
		return
	}
	if err := req.permit(s, body.Channel, protocol.PermissionPost); err != nil {
		writeError(w, err)
		return
	}
	if hasLabels {
		if err := req.permit(s, body.Channel, protocol.PermissionLabel); err != nil {
			writeError(w, err)
			return
		}
	}
	// Bind every file once, then give each task its own in request order.
	allIDs := make([]string, 0)
	for _, task := range body.Tasks {
		allIDs = append(allIDs, task.Attachments...)
	}
	byID, perr := s.bindAttachmentIDs(req, allIDs)
	if perr != nil {
		writeError(w, perr)
		return
	}
	attachments := make([][]protocol.Attachment, len(body.Tasks))
	for at, task := range body.Tasks {
		seen := make(map[string]bool, len(task.Attachments))
		for _, id := range task.Attachments {
			if seen[id] {
				continue
			}
			seen[id] = true
			attachments[at] = append(attachments[at], byID[id])
		}
	}

	digest, perr := batchDigest(body)
	if perr != nil {
		writeError(w, perr)
		return
	}
	var opts log.UpdateOptions
	if body.ID != "" {
		opts.Idempotency = &log.Idempotency{Key: batchKey(req.actor.ID, body.ID), Digest: digest}
	}
	var rootIDs []uint64
	err := s.board.Update(req.ctx, req.tenant, opts, func(tx *log.Tx) error {
		if replay, ok := tx.Replay(); ok {
			rootIDs = append([]uint64(nil), replay.MessageIDs...)
			return nil
		}
		rootIDs = make([]uint64, len(body.Tasks))
		for _, task := range attachments {
			if perr := s.verifyAttachments(req, task); perr != nil {
				return perr
			}
		}
		seen := needsActorSeen(tx.Read(), req.actor)
		externalIDs := make([][]uint64, len(body.Tasks))
		ready := make([]bool, len(body.Tasks))
		for at := range body.Tasks {
			needs, completed, perr := s.validateNeeds(req, tx, plan.external[at], nil)
			if perr != nil {
				return perr
			}
			externalIDs[at] = needs
			ready[at] = len(needs) > 0 && completed && len(plan.internal[at]) == 0
		}
		offset := 0
		if seen {
			offset = 1
		}
		for at := range body.Tasks {
			rootIDs[at] = tx.NextID(offset)
			// Its post and its task.created, then task.ready when it readies
			// at once.
			offset += 2
			if ready[at] {
				offset++
			}
		}
		internalIDs := make([]uint64, 0, len(body.Tasks))
		for at := range body.Tasks {
			internalIDs = append(internalIDs, rootIDs[at])
		}
		if seen {
			tx.Emit(log.EventActorSeen, actorSeenPayload(req.actor))
		}
		for at, task := range body.Tasks {
			rootID := rootIDs[at]
			thread := log.ThreadKey(rootID)
			tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
				ID:          rootID,
				Channel:     body.Channel,
				Text:        task.Text,
				Thread:      thread,
				Labels:      nonNilLabels(task.Labels),
				NoSync:      task.NoSync,
				By:          req.actor.Actored(),
				Attachments: attachments[at],
				Blind:       !req.can(s, body.Channel, protocol.PermissionRead),
			})
			needs := append([]uint64(nil), externalIDs[at]...)
			for _, target := range plan.internal[at] {
				needs = append(needs, internalIDs[target])
			}
			tx.Emit(log.EventTaskCreated, &log.TaskCreatedPayload{
				ID: rootID, Thread: thread, Needs: needs, For: assignees[at], By: req.actor.Actored(),
			})
			if ready[at] {
				tx.Emit(log.EventTaskReady, &log.TaskReadyPayload{ID: rootID, Thread: thread})
			}
			tx.RecordResult(rootID)
		}
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	tasks := make([]protocol.MessageView, 0, len(rootIDs))
	err = s.board.Read(req.tenant, func(snapshot *log.Snapshot) error {
		for _, id := range rootIDs {
			if message := snapshot.Message(id); message != nil {
				tasks = append(tasks, messageView(snapshot, message, &req.actor))
			}
		}
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, protocol.CreatedTasksPage{Tasks: tasks})
}

// handleCreatedTasks answers GET /tasks/batch/{id}: the caller's own batch as
// its tasks are now.
func (s *Server) handleCreatedTasks(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	id := r.PathValue("id")
	if err := protocol.ValidateClientID(id); err != nil {
		writeError(w, protocol.NotFound(protocol.CodeBatchNotFound, fmt.Sprintf("no batch of yours has id %s", id)))
		return
	}
	request, hasRequest := queryString(r, "request")
	if hasRequest {
		if err := protocol.ValidateBatchRequest(request); err != nil {
			writeError(w, err)
			return
		}
	}
	result, found, err := s.board.ReplayKey(req.tenant, batchKey(req.actor.ID, id))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if !found {
		writeError(w, protocol.NotFound(protocol.CodeBatchNotFound, fmt.Sprintf("no batch of yours has id %s", id)))
		return
	}
	if hasRequest && result.Digest != "request:"+request {
		writeError(w, protocol.NewError(protocol.CodeDuplicateID, "that id was already used for a different write"))
		return
	}
	tasks := make([]protocol.MessageView, 0, len(result.MessageIDs))
	err = s.board.Read(req.tenant, func(snapshot *log.Snapshot) error {
		for _, messageID := range result.MessageIDs {
			if message := snapshot.Message(messageID); message != nil {
				tasks = append(tasks, messageView(snapshot, message, &req.actor))
			}
		}
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, protocol.CreatedTasksPage{Tasks: tasks})
}
