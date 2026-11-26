package server_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/server"
)

// testClock is a clock tests move by hand, so leases expire without waiting.
type testClock struct {
	nanos atomic.Int64
}

func newTestClock() *testClock {
	clock := &testClock{}
	clock.nanos.Store(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())
	return clock
}

func (c *testClock) Now() time.Time { return time.Unix(0, c.nanos.Load()).UTC() }

func (c *testClock) Advance(d time.Duration) { c.nanos.Add(int64(d)) }

// harness is an httptest server over an in-memory board.
type harness struct {
	t     *testing.T
	http  *httptest.Server
	board *log.Board
	clock *testClock
}

func newHarness(t *testing.T, options ...server.Options) *harness {
	t.Helper()
	clock := newTestClock()
	board := log.New(log.Options{Now: clock.Now})
	opts := server.Options{
		Board:  board,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if len(options) > 0 {
		supplied := options[0]
		if supplied.Authorize != nil {
			opts.Authorize = supplied.Authorize
		}
	}
	api := server.New(opts)
	httpServer := httptest.NewServer(api)
	t.Cleanup(httpServer.Close)
	return &harness{t: t, http: httpServer, board: board, clock: clock}
}

// request makes a request with an actor and optional tenant.
func (h *harness) request(method, path string, body any, actor, tenant string, extra map[string]string) (int, []byte) {
	h.t.Helper()
	status, data, err := h.try(method, path, body, actor, tenant, extra)
	if err != nil {
		h.t.Fatalf("%s %s: %v", method, path, err)
	}
	return status, data
}

// try is request without failing the test, safe from a goroutine.
func (h *harness) try(method, path string, body any, actor, tenant string, extra map[string]string) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, h.http.URL+server.APIPrefix+path, reader)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		request.Header.Set("content-type", "application/json")
	}
	if actor != "" {
		request.Header.Set("X-Board-Actor", actor)
	}
	if tenant != "" {
		request.Header.Set("X-Board-Tenant", tenant)
	}
	for key, value := range extra {
		request.Header.Set(key, value)
	}
	response, err := h.http.Client().Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return 0, nil, err
	}
	return response.StatusCode, data, nil
}

func (h *harness) expect(status int, data []byte, want int) {
	h.t.Helper()
	if status != want {
		h.t.Fatalf("status %d, want %d: %s", status, want, data)
	}
}

func decode[T any](t *testing.T, data []byte) T {
	t.Helper()
	var value T
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatalf("decoding %s: %v", data, err)
	}
	return value
}

func errorCode(t *testing.T, data []byte) protocol.Code {
	t.Helper()
	return decode[protocol.ErrorBody](t, data).Error.Code
}

// postMessage posts a message and returns its view.
func (h *harness) postMessage(actor, channel, text string) protocol.MessageView {
	h.t.Helper()
	status, data := h.request(http.MethodPost, "/messages", map[string]any{"channel": channel, "text": text}, actor, "", nil)
	h.expect(status, data, http.StatusCreated)
	return decode[postBody](h.t, data).Message
}

type postBody struct {
	Message protocol.MessageView `json:"message"`
}

type taskBody struct {
	Task  protocol.MessageView `json:"task"`
	Claim *protocol.TaskClaim  `json:"claim"`
	Until string               `json:"until"`
}

type claimReadyBody struct {
	Task  *protocol.MessageView `json:"task"`
	Claim *protocol.TaskClaim   `json:"claim"`
}

type tasksBody struct {
	Tasks  []protocol.ThreadSummary `json:"tasks"`
	Cursor string                   `json:"cursor"`
}

func TestChannelsAndMessages(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"

	status, data := h.request(http.MethodPost, "/channels", map[string]any{"path": "errors-www"}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	if !decode[map[string]any](t, data)["created"].(bool) {
		t.Fatalf("expected created true: %s", data)
	}
	// Creating it again answers 200 and leaves it as it is.
	status, data = h.request(http.MethodPost, "/channels", map[string]any{"path": "errors-www"}, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if decode[map[string]any](t, data)["created"].(bool) {
		t.Fatalf("expected created false: %s", data)
	}
	// The body is strict: a key the board doesn't know is refused.
	status, data = h.request(http.MethodPost, "/channels", map[string]any{"path": "private", "private": true}, alice, "", nil)
	h.expect(status, data, http.StatusBadRequest)

	root := h.postMessage(alice, "errors-www", "deploy failed on a1b2c3")
	if root.Re != nil || root.Thread != root.ID || root.Text != "deploy failed on a1b2c3" {
		t.Fatalf("unexpected root: %+v", root)
	}
	if root.Version != root.Revision {
		t.Fatalf("a post's version and revision are its event: %+v", root)
	}

	status, data = h.request(http.MethodPost, "/messages", map[string]any{"re": root.ID, "text": "rolled back"}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	reply := decode[postBody](t, data).Message
	if reply.Re == nil || *reply.Re != root.ID || reply.Thread != root.ID || reply.ReplyTo != nil {
		t.Fatalf("unexpected reply: %+v", reply)
	}
	if reply.Version == root.Version {
		t.Fatalf("a reply is its own message")
	}

	status, data = h.request(http.MethodGet, "/messages/"+root.ID, nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	thread := decode[protocol.ThreadView](t, data)
	if thread.Message.ID != root.ID || len(thread.Replies) != 1 || thread.Replies[0].ID != reply.ID {
		t.Fatalf("unexpected thread: %+v", thread)
	}

	status, data = h.request(http.MethodGet, "/messages?channel=errors-www", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	page := decode[protocol.MessagesPage](t, data)
	if len(page.Messages) != 2 || page.Messages[0].ID != root.ID || page.Messages[1].ID != reply.ID {
		t.Fatalf("unexpected channel messages: %+v", page)
	}
	// Reading on from the cursor sees nothing new, and doesn't repeat.
	status, data = h.request(http.MethodGet, "/messages?channel=errors-www&after="+page.Cursor, nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if next := decode[protocol.MessagesPage](t, data); len(next.Messages) != 0 {
		t.Fatalf("expected nothing after the cursor: %+v", next)
	}

	// Channel listing counts what's in it.
	status, data = h.request(http.MethodGet, "/channels", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	channels := decode[protocol.ChannelsPage](t, data)
	if len(channels.Channels) != 1 || channels.Channels[0].Messages != 2 || channels.Channels[0].Threads != 1 {
		t.Fatalf("unexpected channels: %+v", channels)
	}

	// Validation.
	status, data = h.request(http.MethodPost, "/messages", map[string]any{"channel": "errors-www", "re": root.ID, "text": "both"}, alice, "", nil)
	h.expect(status, data, http.StatusBadRequest)
	status, data = h.request(http.MethodPost, "/messages", map[string]any{"channel": "errors-www", "text": "  "}, alice, "", nil)
	h.expect(status, data, http.StatusBadRequest)
	status, data = h.request(http.MethodPost, "/messages", map[string]any{"channel": "Errors", "text": "x"}, alice, "", nil)
	h.expect(status, data, http.StatusBadRequest)
	status, data = h.request(http.MethodPost, "/messages", map[string]any{
		"channel": "errors-www", "text": "x", "labels": map[string]string{"board.task": "ready"},
	}, alice, "", nil)
	h.expect(status, data, http.StatusBadRequest)
	if code := errorCode(t, data); code != protocol.CodeReservedLabel {
		t.Fatalf("expected reserved_label, got %s", code)
	}
	status, data = h.request(http.MethodGet, "/messages/999999", nil, alice, "", nil)
	h.expect(status, data, http.StatusNotFound)
	if code := errorCode(t, data); code != protocol.CodeMessageNotFound {
		t.Fatalf("expected message_not_found, got %s", code)
	}
}

func TestIdempotentWrites(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"

	status, data := h.request(http.MethodPost, "/messages", map[string]any{"channel": "deploys", "text": "a1b2c3 is live", "id": "deploy-a1b2c3"}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	first := decode[postBody](t, data).Message

	// The same id with the same write answers the message it already posted,
	// even when the text sent this time differs.
	status, data = h.request(http.MethodPost, "/messages", map[string]any{"channel": "deploys", "text": "different text", "id": "deploy-a1b2c3"}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	second := decode[postBody](t, data).Message
	if second.ID != first.ID || second.Text != first.Text {
		t.Fatalf("a retry should answer the first post: %+v vs %+v", first, second)
	}

	// The same id elsewhere is another write.
	status, data = h.request(http.MethodPost, "/messages", map[string]any{"channel": "other", "text": "x", "id": "deploy-a1b2c3"}, alice, "", nil)
	h.expect(status, data, http.StatusConflict)
	if code := errorCode(t, data); code != protocol.CodeDuplicateID {
		t.Fatalf("expected duplicate_id, got %s", code)
	}
	// Another principal's id is its own.
	status, data = h.request(http.MethodPost, "/messages", map[string]any{"channel": "deploys", "text": "bob", "id": "deploy-a1b2c3"}, "local:user:bob", "", nil)
	h.expect(status, data, http.StatusCreated)
	// The header works where the body names none.
	status, data = h.request(http.MethodPost, "/messages", map[string]any{"channel": "deploys", "text": "header"}, alice, "", map[string]string{"Idempotency-Key": "header-1"})
	h.expect(status, data, http.StatusCreated)
	headerFirst := decode[postBody](t, data).Message
	status, data = h.request(http.MethodPost, "/messages", map[string]any{"channel": "deploys", "text": "header again"}, alice, "", map[string]string{"Idempotency-Key": "header-1"})
	h.expect(status, data, http.StatusCreated)
	if got := decode[postBody](t, data).Message; got.ID != headerFirst.ID {
		t.Fatalf("header retry should answer the first post")
	}

	// Only one message landed per key.
	status, data = h.request(http.MethodGet, "/messages?channel=deploys&limit=500", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if page := decode[protocol.MessagesPage](t, data); len(page.Messages) != 3 {
		t.Fatalf("expected 3 messages, got %d: %+v", len(page.Messages), page)
	}
}

func TestTaskClaimRaceAndFencing(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"
	const bob = "local:user:bob"

	status, data := h.request(http.MethodPost, "/tasks", map[string]any{"channel": "work", "text": "fix CVE-123"}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	task := decode[taskBody](t, data).Task
	if task.Task == nil || task.Task.State != "ready" || task.Labels["board.task"] != "ready" {
		t.Fatalf("expected a ready task: %+v", task)
	}

	// Of claims that race, one lands.
	const claims = 8
	statuses := make([]int, claims)
	bodies := make([][]byte, claims)
	var wg sync.WaitGroup
	for i := 0; i < claims; i++ {
		wg.Add(1)
		go func(at int) {
			defer wg.Done()
			actor := alice
			if at%2 == 1 {
				actor = bob
			}
			status, body, err := h.try(http.MethodPost, "/tasks/"+task.ID+"/claim", map[string]any{"lease": 60}, actor, "", nil)
			if err != nil {
				t.Errorf("claim %d: %v", at, err)
				return
			}
			statuses[at], bodies[at] = status, body
		}(i)
	}
	wg.Wait()
	winners := 0
	var winner taskBody
	for at, status := range statuses {
		switch status {
		case http.StatusOK:
			winners++
			winner = decode[taskBody](t, bodies[at])
		case http.StatusConflict:
			if code := errorCode(t, bodies[at]); code != protocol.CodeTaskClaimed {
				t.Fatalf("claim %d lost with %s, want task_claimed", at, code)
			}
		default:
			t.Fatalf("claim %d: status %d: %s", at, status, bodies[at])
		}
	}
	if winners != 1 {
		t.Fatalf("expected exactly one claim to land, got %d", winners)
	}
	if winner.Claim == nil || winner.Claim.Epoch != 1 || !protocol.ValidClaimToken(winner.Claim.Token) {
		t.Fatalf("unexpected claim: %+v", winner.Claim)
	}

	// A claim's writes are fenced by epoch, token, and lease.
	status, data = h.request(http.MethodPost, "/tasks/"+task.ID+"/complete", map[string]any{
		"epoch": 2, "token": winner.Claim.Token, "result": "fixed",
	}, alice, "", nil)
	h.expect(status, data, http.StatusConflict)
	if code := errorCode(t, data); code != protocol.CodeClaimLost {
		t.Fatalf("expected claim_lost, got %s", code)
	}
	status, data = h.request(http.MethodPost, "/tasks/"+task.ID+"/complete", map[string]any{
		"epoch": 1, "token": "not-the-token", "result": "fixed",
	}, alice, "", nil)
	h.expect(status, data, http.StatusConflict)

	status, data = h.request(http.MethodPost, "/tasks/"+task.ID+"/complete", map[string]any{
		"epoch": 1, "token": winner.Claim.Token, "result": "fixed in a1b2c3",
	}, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	completed := decode[taskBody](t, data).Task
	if completed.Task.State != "completed" || completed.Task.Result == nil || *completed.Task.Result != "fixed in a1b2c3" {
		t.Fatalf("unexpected completed task: %+v", completed.Task)
	}
	if completed.Labels["board.task"] != "completed" || completed.Version <= task.Version {
		t.Fatalf("completion should move the state label and version: %+v", completed)
	}
	status, data = h.request(http.MethodPost, "/tasks/"+task.ID+"/claim", map[string]any{}, alice, "", nil)
	h.expect(status, data, http.StatusConflict)
	if code := errorCode(t, data); code != protocol.CodeTaskFinished {
		t.Fatalf("expected task_finished, got %s", code)
	}
}

func TestLeaseExpiry(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"
	const bob = "local:user:bob"

	status, data := h.request(http.MethodPost, "/tasks", map[string]any{
		"channel": "work", "text": "deploy", "claim": true, "lease": 30,
	}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	created := decode[taskBody](t, data)
	if created.Claim == nil || created.Task.Task.State != "claimed" {
		t.Fatalf("expected a claimed task: %+v", created)
	}
	oldToken := created.Claim.Token

	// The lease runs out: the claim is over at once, and its writes lose.
	h.clock.Advance(31 * time.Second)
	status, data = h.request(http.MethodPost, "/tasks/"+created.Task.ID+"/complete", map[string]any{
		"epoch": 1, "token": oldToken,
	}, alice, "", nil)
	h.expect(status, data, http.StatusConflict)
	if code := errorCode(t, data); code != protocol.CodeClaimLost {
		t.Fatalf("expected claim_lost, got %s", code)
	}

	// A lapsed task lists as ready, and someone else claims it as epoch 2.
	status, data = h.request(http.MethodGet, "/tasks?state=ready&channel=work", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	listed := decode[tasksBody](t, data)
	if len(listed.Tasks) != 1 || listed.Tasks[0].Message.ID != created.Task.ID {
		t.Fatalf("a lapsed claim should list as ready: %+v", listed)
	}
	status, data = h.request(http.MethodPost, "/tasks/"+created.Task.ID+"/claim", map[string]any{"lease": 60}, bob, "", nil)
	h.expect(status, data, http.StatusOK)
	reclaimed := decode[taskBody](t, data)
	if reclaimed.Claim == nil || reclaimed.Claim.Epoch != 2 || reclaimed.Task.Task.State != "claimed" {
		t.Fatalf("unexpected reclaim: %+v", reclaimed)
	}
	// The old claim still loses against the new one.
	status, data = h.request(http.MethodPost, "/tasks/"+created.Task.ID+"/complete", map[string]any{"epoch": 1, "token": oldToken}, alice, "", nil)
	h.expect(status, data, http.StatusConflict)

	// claim-ready takes a lapsed task of the caller's own owner.
	status, data = h.request(http.MethodPost, "/tasks", map[string]any{"channel": "bobs", "text": "bob's", "claim": true, "lease": 30}, bob, "", nil)
	h.expect(status, data, http.StatusCreated)
	bobs := decode[taskBody](t, data)
	h.clock.Advance(31 * time.Second)
	status, data = h.request(http.MethodPost, "/tasks/claim", map[string]any{"channel": "bobs"}, bob, "", nil)
	h.expect(status, data, http.StatusOK)
	ready := decode[claimReadyBody](t, data)
	if ready.Task == nil || ready.Claim == nil || ready.Claim.Epoch != 2 || ready.Task.ID != bobs.Task.ID {
		t.Fatalf("claim-ready should take a lapsed task: %+v", ready)
	}
}

func TestNeeds(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"

	create := func(body map[string]any) protocol.MessageView {
		t.Helper()
		status, data := h.request(http.MethodPost, "/tasks", body, alice, "", nil)
		h.expect(status, data, http.StatusCreated)
		return decode[taskBody](t, data).Task
	}
	first := create(map[string]any{"channel": "work", "text": "fix"})
	second := create(map[string]any{"channel": "work", "text": "review", "needs": []string{first.ID}})
	if second.Task.State != "waiting" || len(second.Task.Needs) != 1 || second.Task.Needs[0] != first.ID {
		t.Fatalf("expected a waiting task: %+v", second.Task)
	}

	// A prerequisite that's already completed readies the new task at once.
	status, data := h.request(http.MethodPost, "/tasks/"+first.ID+"/claim", map[string]any{}, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	claim := decode[taskBody](t, data).Claim
	status, data = h.request(http.MethodPost, "/tasks/"+first.ID+"/complete", map[string]any{
		"epoch": claim.Epoch, "token": claim.Token, "result": "done",
	}, alice, "", nil)
	h.expect(status, data, http.StatusOK)

	status, data = h.request(http.MethodGet, "/tasks?state=all&channel=work", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	all := decode[tasksBody](t, data)
	if len(all.Tasks) != 2 {
		t.Fatalf("expected two tasks: %+v", all)
	}
	for _, summary := range all.Tasks {
		if summary.Message.ID == second.ID && (summary.Message.Task.State != "ready" || summary.Message.Labels["board.task"] != "ready") {
			t.Fatalf("completing a prerequisite should ready its dependent: %+v", summary.Message.Task)
		}
	}
	// The dependent is claimable now.
	status, data = h.request(http.MethodPost, "/tasks/"+second.ID+"/claim", map[string]any{}, alice, "", nil)
	h.expect(status, data, http.StatusOK)

	// Prerequisites that aren't tasks, are the task itself, or were canceled
	// are refused.
	status, data = h.request(http.MethodPost, "/tasks", map[string]any{"channel": "work", "text": "bad", "needs": []string{"999999"}}, alice, "", nil)
	h.expect(status, data, http.StatusBadRequest)
	if code := errorCode(t, data); code != protocol.CodeInvalidNeeds {
		t.Fatalf("expected invalid_needs, got %s", code)
	}
	canceled := create(map[string]any{"channel": "work", "text": "canceled"})
	status, data = h.request(http.MethodPost, "/tasks/"+canceled.ID+"/cancel", map[string]any{"reason": "superseded"}, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	status, data = h.request(http.MethodPost, "/tasks", map[string]any{"channel": "work", "text": "waits", "needs": []string{canceled.ID}}, alice, "", nil)
	h.expect(status, data, http.StatusBadRequest)
	if code := errorCode(t, data); code != protocol.CodeInvalidNeeds {
		t.Fatalf("expected invalid_needs for a canceled prerequisite, got %s", code)
	}

	// A change of needs can't close a cycle.
	left := create(map[string]any{"channel": "work", "text": "left"})
	right := create(map[string]any{"channel": "work", "text": "right", "needs": []string{left.ID}})
	status, data = h.request(http.MethodPost, "/tasks/"+left.ID+"/needs", map[string]any{"needs": []string{right.ID}}, alice, "", nil)
	h.expect(status, data, http.StatusBadRequest)
	if code := errorCode(t, data); code != protocol.CodeInvalidNeeds {
		t.Fatalf("expected invalid_needs for a cycle, got %s", code)
	}
	// Dropping what it waits for readies the task.
	status, data = h.request(http.MethodPost, "/tasks/"+right.ID+"/needs", map[string]any{"needs": []string{}}, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	dropped := decode[taskBody](t, data).Task
	if dropped.Task.State != "ready" || len(dropped.Task.Needs) != 0 {
		t.Fatalf("expected a ready task with no needs: %+v", dropped.Task)
	}
}

func TestOwnershipAssignmentAndHolds(t *testing.T) {
	// A policy that leaves bob without `manage`: ownership is what decides
	// his writes, as it is for any project or app with narrowed access.
	h := newHarness(t, server.Options{Authorize: server.AuthorizerFunc(func(_ context.Context, _ string, actor protocol.Actor, _ string, permission protocol.Permission) error {
		if actor.ID == "local:user:bob" && permission == protocol.PermissionManage {
			return protocol.Forbidden("bob may not manage")
		}
		return nil
	})})
	const alice = "local:user:alice"
	const bob = "local:user:bob"

	// A task kept for one of its owner's profiles only that profile claims.
	status, data := h.request(http.MethodPost, "/tasks", map[string]any{
		"channel": "work", "text": "deploy", "for": "local:user:alice+worker",
	}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	task := decode[taskBody](t, data).Task
	if task.Task.For == nil || *task.Task.For != "local:user:alice+worker" {
		t.Fatalf("expected an assigned task: %+v", task.Task)
	}
	status, data = h.request(http.MethodPost, "/tasks/"+task.ID+"/claim", map[string]any{}, bob, "", nil)
	h.expect(status, data, http.StatusConflict)
	if code := errorCode(t, data); code != protocol.CodeTaskAssigned {
		t.Fatalf("expected task_assigned, got %s", code)
	}
	// The profile claims it.
	status, data, err := h.try(http.MethodPost, "/tasks/"+task.ID+"/claim", map[string]any{}, "", "", map[string]string{
		"X-Board-Principal": "local:user:alice", "X-Board-Profile": "worker",
	})
	if err != nil {
		t.Fatal(err)
	}
	h.expect(status, data, http.StatusOK)
	workerClaim := decode[taskBody](t, data).Claim
	if workerClaim == nil || workerClaim.Epoch != 1 {
		t.Fatalf("expected the profile's claim: %+v", workerClaim)
	}

	// Only the owner or manage changes a task.
	status, data = h.request(http.MethodPost, "/tasks/"+task.ID+"/cancel", map[string]any{"reason": "no"}, bob, "", nil)
	h.expect(status, data, http.StatusForbidden)
	if code := errorCode(t, data); code != protocol.CodeTaskNotOwned {
		t.Fatalf("expected task_not_owned, got %s", code)
	}

	// The owner returns it to the pool; a live claim stays until it's released.
	status, data = h.request(http.MethodPost, "/tasks/"+task.ID+"/assign", map[string]any{"for": nil}, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if assigned := decode[taskBody](t, data).Task; assigned.Task.For != nil {
		t.Fatalf("expected a pool task: %+v", assigned.Task.For)
	}
	status, data = h.request(http.MethodPost, "/tasks/"+task.ID+"/release", map[string]any{
		"epoch": workerClaim.Epoch, "token": workerClaim.Token,
	}, "", "", map[string]string{"X-Board-Principal": "local:user:alice", "X-Board-Profile": "worker"})
	h.expect(status, data, http.StatusOK)
	status, data = h.request(http.MethodPost, "/tasks/"+task.ID+"/claim", map[string]any{}, bob, "", nil)
	h.expect(status, data, http.StatusOK)
	bobClaim := decode[taskBody](t, data).Claim

	// A claim holds a task for a person; the owner lifts the hold.
	status, data = h.request(http.MethodPost, "/tasks/"+task.ID+"/block", map[string]any{
		"reason": "approve the prod deploy?", "kind": "awaiting_approval",
		"epoch": bobClaim.Epoch, "token": bobClaim.Token,
	}, bob, "", nil)
	h.expect(status, data, http.StatusOK)
	blocked := decode[taskBody](t, data).Task
	if blocked.Task.State != "blocked" || blocked.Task.Blocked == nil || blocked.Task.Blocked.Kind == nil || *blocked.Task.Blocked.Kind != "awaiting_approval" {
		t.Fatalf("expected a blocked task: %+v", blocked.Task)
	}
	status, data = h.request(http.MethodPost, "/tasks/"+task.ID+"/claim", map[string]any{}, bob, "", nil)
	h.expect(status, data, http.StatusConflict)
	if code := errorCode(t, data); code != protocol.CodeTaskBlocked {
		t.Fatalf("expected task_blocked, got %s", code)
	}
	status, data = h.request(http.MethodPost, "/tasks/"+task.ID+"/unblock", nil, bob, "", nil)
	h.expect(status, data, http.StatusForbidden)
	status, data = h.request(http.MethodPost, "/tasks/"+task.ID+"/unblock", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if unblocked := decode[taskBody](t, data).Task; unblocked.Task.State != "ready" || unblocked.Task.Blocked != nil {
		t.Fatalf("expected a ready task: %+v", unblocked.Task)
	}

	// An assignee must be one of its owner's profiles.
	status, data = h.request(http.MethodPost, "/tasks", map[string]any{
		"channel": "work", "text": "x", "for": "local:user:bob+worker",
	}, alice, "", nil)
	h.expect(status, data, http.StatusBadRequest)
	status, data = h.request(http.MethodPost, "/tasks", map[string]any{
		"channel": "work", "text": "y", "for": "local:user:alice+NotAProfile",
	}, alice, "", nil)
	h.expect(status, data, http.StatusBadRequest)
}

func TestClaimReadyTakesAssignedFirst(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"
	workerHeaders := func() map[string]string {
		return map[string]string{"X-Board-Principal": "local:user:alice", "X-Board-Profile": "worker"}
	}

	status, data := h.request(http.MethodPost, "/tasks", map[string]any{"channel": "work", "text": "pool"}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	pool := decode[taskBody](t, data).Task
	status, data = h.request(http.MethodPost, "/tasks", map[string]any{"channel": "work", "text": "mine", "for": "local:user:alice+worker"}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	mine := decode[taskBody](t, data).Task

	status, data, err := h.try(http.MethodPost, "/tasks/claim", map[string]any{"channel": "work/"}, "", "", workerHeaders())
	if err != nil {
		t.Fatal(err)
	}
	h.expect(status, data, http.StatusOK)
	ready := decode[claimReadyBody](t, data)
	if ready.Task == nil || ready.Task.ID != mine.ID {
		t.Fatalf("claim-ready should take the assigned task first, got %+v (pool %s, mine %s)", ready.Task, pool.ID, mine.ID)
	}

	// Nothing ready answers { task: null }.
	status, data = h.request(http.MethodPost, "/tasks/claim", map[string]any{"channel": "empty"}, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if empty := decode[claimReadyBody](t, data); empty.Task != nil {
		t.Fatalf("expected no task: %+v", empty)
	}
}

// openStream opens the event feed and waits for its connected comment.
func (h *harness) openStream(t *testing.T, query string, headers map[string]string) (*http.Response, *bufio.Reader, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, h.http.URL+server.APIPrefix+"/events"+query, nil)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := h.http.Client().Do(request)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		cancel()
		t.Fatalf("events: status %d: %s", response.StatusCode, body)
	}
	reader := bufio.NewReader(response.Body)
	line, err := reader.ReadString('\n')
	if err != nil || !strings.HasPrefix(line, ": connected") {
		response.Body.Close()
		cancel()
		t.Fatalf("events: expected a connected comment, got %q (%v)", line, err)
	}
	// The comment ends with a blank line, as an SSE event does.
	if blank, err := reader.ReadString('\n'); err != nil || strings.TrimSpace(blank) != "" {
		response.Body.Close()
		cancel()
		t.Fatalf("events: expected a blank line after the comment, got %q (%v)", blank, err)
	}
	return response, reader, cancel
}

// readEvent reads one SSE event, or fails after a timeout.
func readEvent(t *testing.T, reader *bufio.Reader) protocol.EventView {
	t.Helper()
	type result struct {
		view protocol.EventView
		err  error
	}
	done := make(chan result, 1)
	go func() {
		var id, data string
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				done <- result{err: err}
				return
			}
			line = strings.TrimRight(line, "\r\n")
			if line == "" {
				break
			}
			switch {
			case strings.HasPrefix(line, "id: "):
				id = strings.TrimPrefix(line, "id: ")
			case strings.HasPrefix(line, "data: "):
				data = strings.TrimPrefix(line, "data: ")
			}
		}
		var view protocol.EventView
		if data == "" {
			done <- result{err: fmt.Errorf("event had no data (id %q)", id)}
			return
		}
		if err := json.Unmarshal([]byte(data), &view); err != nil {
			done <- result{err: err}
			return
		}
		if id != "" && view.Cursor != id {
			done <- result{err: fmt.Errorf("event id %q doesn't match cursor %q", id, view.Cursor)}
			return
		}
		done <- result{view: view}
	}()
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("reading an event: %v", got.err)
		}
		return got.view
	case <-time.After(5 * time.Second):
		t.Fatal("timed out reading an SSE event")
		return protocol.EventView{}
	}
}

func TestEventsSSEResume(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"

	response, reader, cancel := h.openStream(t, "", nil)
	defer cancel()
	defer response.Body.Close()

	first := h.postMessage(alice, "errors-www", "one")
	event := readEvent(t, reader)
	if event.Type != "message.posted" || event.Message == nil || event.Message.ID != first.ID || event.Cursor == "" {
		t.Fatalf("unexpected event: %+v", event)
	}
	cursor := event.Cursor
	cancel()
	response.Body.Close()

	// A second event lands while the feed is closed.
	second := h.postMessage(alice, "errors-www", "two")

	// Reconnecting after the cursor answers only what came after it.
	reconnected, reconnectedReader, reconnectedCancel := h.openStream(t, "", map[string]string{"Last-Event-ID": cursor})
	defer reconnectedCancel()
	defer reconnected.Body.Close()
	resumed := readEvent(t, reconnectedReader)
	if resumed.Message == nil || resumed.Message.ID != second.ID {
		t.Fatalf("expected only the second message after the cursor, got %+v", resumed)
	}
	if resumed.Cursor <= cursor {
		t.Fatalf("cursor should move forward: %s after %s", resumed.Cursor, cursor)
	}

	// The reconnect answered what was already there, without waiting for a new
	// write: a third message would be the next event.
	third := h.postMessage(alice, "errors-www", "three")
	next := readEvent(t, reconnectedReader)
	if next.Message == nil || next.Message.ID != third.ID {
		t.Fatalf("expected the third message, got %+v", next)
	}
}

func TestEventsSSEFilters(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"

	response, reader, cancel := h.openStream(t, "?channel=errors-www", nil)
	defer cancel()
	defer response.Body.Close()

	// Another channel's post doesn't reach this feed.
	h.postMessage(alice, "other", "elsewhere")
	wanted := h.postMessage(alice, "errors-www", "here")
	event := readEvent(t, reader)
	if event.Message == nil || event.Message.ID != wanted.ID {
		t.Fatalf("expected the filtered channel's post, got %+v", event)
	}
}

func TestTenantIsolation(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"

	status, data := h.request(http.MethodPost, "/messages", map[string]any{"channel": "hidden", "text": "tenant b", "id": "same-id"}, alice, "tenant-b", nil)
	h.expect(status, data, http.StatusCreated)
	foreign := decode[postBody](t, data).Message

	// The other tenant sees its own empty board.
	status, data = h.request(http.MethodGet, "/channels", nil, alice, "tenant-a", nil)
	h.expect(status, data, http.StatusOK)
	if page := decode[protocol.ChannelsPage](t, data); len(page.Channels) != 0 {
		t.Fatalf("tenant a should see no channels: %+v", page)
	}
	status, data = h.request(http.MethodGet, "/messages/"+foreign.ID, nil, alice, "tenant-a", nil)
	h.expect(status, data, http.StatusNotFound)
	status, data = h.request(http.MethodGet, "/messages?channel=hidden", nil, alice, "tenant-a", nil)
	h.expect(status, data, http.StatusOK)
	if page := decode[protocol.MessagesPage](t, data); len(page.Messages) != 0 {
		t.Fatalf("tenant a should see no messages: %+v", page)
	}
	// The same idempotency key names a different write on another tenant.
	status, data = h.request(http.MethodPost, "/messages", map[string]any{"channel": "hidden", "text": "tenant a", "id": "same-id"}, alice, "tenant-a", nil)
	h.expect(status, data, http.StatusCreated)
	local := decode[postBody](t, data).Message
	if local.Text != "tenant a" {
		t.Fatalf("tenant a's post: %+v", local)
	}
	// Two tenants may use the same ref for different messages; each reads
	// its own.
	status, data = h.request(http.MethodGet, "/messages/"+local.ID, nil, alice, "tenant-a", nil)
	h.expect(status, data, http.StatusOK)
	if own := decode[protocol.ThreadView](t, data).Message; own.Text != "tenant a" {
		t.Fatalf("tenant a read another tenant's message: %+v", own)
	}
	status, data = h.request(http.MethodGet, "/messages/"+foreign.ID, nil, alice, "tenant-b", nil)
	h.expect(status, data, http.StatusOK)
	if own := decode[protocol.ThreadView](t, data).Message; own.Text != "tenant b" {
		t.Fatalf("tenant b read another tenant's message: %+v", own)
	}
	// A task in another tenant isn't a task here.
	status, data = h.request(http.MethodPost, "/tasks", map[string]any{"channel": "work", "text": "b's task"}, alice, "tenant-b", nil)
	h.expect(status, data, http.StatusCreated)
	task := decode[taskBody](t, data).Task
	status, data = h.request(http.MethodPost, "/tasks/"+task.ID+"/claim", map[string]any{}, alice, "tenant-a", nil)
	h.expect(status, data, http.StatusNotFound)
	// A malformed tenant header is refused.
	status, data = h.request(http.MethodGet, "/channels", nil, alice, "bad tenant!", nil)
	h.expect(status, data, http.StatusBadRequest)
}

func TestAuthorizerSeam(t *testing.T) {
	denyRead := server.AuthorizerFunc(func(_ context.Context, _ string, _ protocol.Actor, channel string, permission protocol.Permission) error {
		if channel == "secret" && permission == protocol.PermissionRead {
			return protocol.Forbidden("no access to secret")
		}
		return nil
	})
	h := newHarness(t, server.Options{Authorize: denyRead})
	const alice = "local:user:alice"

	status, data := h.request(http.MethodGet, "/messages?channel=secret", nil, alice, "", nil)
	h.expect(status, data, http.StatusForbidden)
	// A post needs `post`, not `read`: write-only is a real policy.
	status, data = h.request(http.MethodPost, "/messages", map[string]any{"channel": "secret", "text": "written blind"}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	status, data = h.request(http.MethodGet, "/messages?channel=secret", nil, alice, "", nil)
	h.expect(status, data, http.StatusForbidden)
	// A channel the policy leaves alone works.
	h.postMessage(alice, "public", "ok")
}

func TestLabelWrites(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"

	status, data := h.request(http.MethodPost, "/messages", map[string]any{
		"channel": "work", "text": "triage", "labels": map[string]string{"status": "open"},
	}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	message := decode[postBody](t, data).Message

	// Claim a label only while it isn't set; the second try loses.
	status, data = h.request(http.MethodPost, "/messages/"+message.ID+"/labels", map[string]any{
		"set": map[string]string{"owner": "claude"},
		"if":  map[string]any{"owner": nil},
	}, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	status, data = h.request(http.MethodPost, "/messages/"+message.ID+"/labels", map[string]any{
		"set": map[string]string{"owner": "codex"},
		"if":  map[string]any{"owner": nil},
	}, alice, "", nil)
	h.expect(status, data, http.StatusConflict)
	if code := errorCode(t, data); code != protocol.CodeLabelConflict {
		t.Fatalf("expected label_conflict, got %s", code)
	}

	// A condition on the value, then unsetting it.
	status, data = h.request(http.MethodPost, "/messages/"+message.ID+"/labels", map[string]any{
		"unset": []string{"owner"},
		"if":    map[string]any{"owner": "codex"},
	}, alice, "", nil)
	h.expect(status, data, http.StatusConflict)
	status, data = h.request(http.MethodPost, "/messages/"+message.ID+"/labels", map[string]any{
		"unset": []string{"owner"},
		"if":    map[string]any{"owner": "claude"},
	}, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	status, data = h.request(http.MethodPost, "/messages/"+message.ID+"/labels", map[string]any{
		"set": map[string]string{"status": "triaged"},
	}, alice, "", nil)
	h.expect(status, data, http.StatusOK)

	status, data = h.request(http.MethodGet, "/messages/"+message.ID, nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	current := decode[protocol.ThreadView](t, data).Message
	if current.Labels["status"] != "triaged" || current.Labels["owner"] != "" {
		t.Fatalf("unexpected labels: %+v", current.Labels)
	}
	if current.Version <= message.Version {
		t.Fatalf("a label write should move the message's version: %d <= %d", current.Version, message.Version)
	}
	if current.Revision != message.Revision {
		t.Fatalf("labels leave the revision as it is: %d != %d", current.Revision, message.Revision)
	}

	// The board's own namespace is refused, whether set or unset.
	status, data = h.request(http.MethodPost, "/messages/"+message.ID+"/labels", map[string]any{
		"set": map[string]string{"board.task": "completed"},
	}, alice, "", nil)
	h.expect(status, data, http.StatusBadRequest)
	if code := errorCode(t, data); code != protocol.CodeReservedLabel {
		t.Fatalf("expected reserved_label, got %s", code)
	}
	status, data = h.request(http.MethodPost, "/messages/"+message.ID+"/labels", map[string]any{
		"unset": []string{"board.task"},
	}, alice, "", nil)
	h.expect(status, data, http.StatusBadRequest)
	if code := errorCode(t, data); code != protocol.CodeReservedLabel {
		t.Fatalf("expected reserved_label, got %s", code)
	}
	// A write with nothing to change is refused.
	status, data = h.request(http.MethodPost, "/messages/"+message.ID+"/labels", map[string]any{"if": map[string]any{"status": "triaged"}}, alice, "", nil)
	h.expect(status, data, http.StatusBadRequest)
}

func TestReplyAfterPrecondition(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"
	root := h.postMessage(alice, "work", "root")

	status, data := h.request(http.MethodPost, "/messages", map[string]any{"re": root.ID, "text": "first", "after": root.ID}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	first := decode[postBody](t, data).Message

	// The root isn't the newest anymore.
	status, data = h.request(http.MethodPost, "/messages", map[string]any{"re": root.ID, "text": "late", "after": root.ID}, alice, "", nil)
	h.expect(status, data, http.StatusConflict)
	if code := errorCode(t, data); code != protocol.CodeNewerMessage {
		t.Fatalf("expected newer_message, got %s", code)
	}
	// The newest reply is.
	status, data = h.request(http.MethodPost, "/messages", map[string]any{"re": root.ID, "text": "second", "after": first.ID, "id": "reply-2"}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	// `after` must name a message in the thread.
	other := h.postMessage(alice, "work", "other thread")
	status, data = h.request(http.MethodPost, "/messages", map[string]any{"re": root.ID, "text": "x", "after": other.ID}, alice, "", nil)
	h.expect(status, data, http.StatusBadRequest)
	if code := errorCode(t, data); code != protocol.CodeInvalidAfter {
		t.Fatalf("expected invalid_after, got %s", code)
	}
	// A retry with the same id answers the reply that landed, whatever
	// `after` says now.
	status, data = h.request(http.MethodPost, "/messages", map[string]any{
		"re": root.ID, "text": "second", "after": first.ID, "id": "reply-2",
	}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	if got := decode[postBody](t, data).Message; got.Text != "second" {
		t.Fatalf("retry should answer the first reply")
	}
}

func TestAdoptThreadAsTask(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"
	root := h.postMessage(alice, "work", "an issue someone filed")

	status, data := h.request(http.MethodPost, "/tasks", map[string]any{"re": root.ID}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	task := decode[taskBody](t, data).Task
	if task.ID != root.ID || task.Text != root.Text || task.Task == nil || task.Task.State != "ready" {
		t.Fatalf("unexpected adopted task: %+v", task)
	}
	if task.Task.Owner != alice || task.Task.By != alice {
		t.Fatalf("the caller's principal owns it: %+v", task.Task)
	}
	// A thread becomes a task once.
	status, data = h.request(http.MethodPost, "/tasks", map[string]any{"re": root.ID}, alice, "", nil)
	h.expect(status, data, http.StatusConflict)
	if code := errorCode(t, data); code != protocol.CodeAlreadyATask {
		t.Fatalf("expected already_a_task, got %s", code)
	}
	// Adopting isn't a new thread: text and labels are refused.
	status, data = h.request(http.MethodPost, "/tasks", map[string]any{"re": root.ID, "text": "again"}, alice, "", nil)
	h.expect(status, data, http.StatusBadRequest)
}

func TestFailAndReopen(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"
	create := func(body map[string]any) protocol.MessageView {
		t.Helper()
		status, data := h.request(http.MethodPost, "/tasks", body, alice, "", nil)
		h.expect(status, data, http.StatusCreated)
		return decode[taskBody](t, data).Task
	}
	// A task made with a claim answers its token once.
	status, data := h.request(http.MethodPost, "/tasks", map[string]any{"channel": "work", "text": "flaky", "claim": true}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	created := decode[taskBody](t, data)
	task, claim := created.Task, created.Claim
	dependent := create(map[string]any{"channel": "work", "text": "after", "needs": []string{task.ID}})

	// A failed task readies nothing, and a dependent keeps waiting.
	status, data = h.request(http.MethodPost, "/tasks/"+task.ID+"/fail", map[string]any{
		"epoch": claim.Epoch, "token": claim.Token, "reason": "the tests can't pass",
	}, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	failed := decode[taskBody](t, data).Task
	if failed.Task.State != "failed" || failed.Task.Reason == nil || *failed.Task.Reason != "the tests can't pass" {
		t.Fatalf("unexpected failed task: %+v", failed.Task)
	}
	status, data = h.request(http.MethodGet, "/messages/"+dependent.ID, nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if still := decode[protocol.ThreadView](t, data).Message; still.Task.State != "waiting" {
		t.Fatalf("a failed prerequisite readies nothing: %+v", still.Task)
	}

	// Reopening makes it ready again, in place.
	status, data = h.request(http.MethodPost, "/tasks/"+task.ID+"/reopen", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	reopened := decode[taskBody](t, data).Task
	if reopened.Task.State != "ready" || reopened.Task.Reason != nil {
		t.Fatalf("unexpected reopened task: %+v", reopened.Task)
	}
	status, data = h.request(http.MethodPost, "/tasks/"+task.ID+"/claim", map[string]any{}, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	again := decode[taskBody](t, data).Claim
	if again.Epoch != claim.Epoch+1 {
		// One fresh claim after the first: epochs move one at a time.
		t.Fatalf("unexpected epoch: %d after %d", again.Epoch, claim.Epoch)
	}
	status, data = h.request(http.MethodPost, "/tasks/"+task.ID+"/complete", map[string]any{
		"epoch": again.Epoch, "token": again.Token,
	}, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	status, data = h.request(http.MethodGet, "/messages/"+dependent.ID, nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if now := decode[protocol.ThreadView](t, data).Message; now.Task.State != "ready" {
		t.Fatalf("a completed prerequisite readies its dependent: %+v", now.Task)
	}
}

func TestBlockUnblockWithNeeds(t *testing.T) {
	h := newHarness(t, server.Options{Authorize: server.AuthorizerFunc(func(_ context.Context, _ string, actor protocol.Actor, _ string, permission protocol.Permission) error {
		if actor.ID == "local:user:bob" && permission == protocol.PermissionManage {
			return protocol.Forbidden("bob may not manage")
		}
		return nil
	})})
	const alice = "local:user:alice"
	create := func(body map[string]any) protocol.MessageView {
		t.Helper()
		status, data := h.request(http.MethodPost, "/tasks", body, alice, "", nil)
		h.expect(status, data, http.StatusCreated)
		return decode[taskBody](t, data).Task
	}
	prereq := create(map[string]any{"channel": "work", "text": "prereq"})
	dependent := create(map[string]any{"channel": "work", "text": "dependent", "needs": []string{prereq.ID}})

	// A waiting task can be held for a person, and goes back to waiting.
	status, data := h.request(http.MethodPost, "/tasks/"+dependent.ID+"/block", map[string]any{"reason": "needs a decision"}, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if blocked := decode[taskBody](t, data).Task; blocked.Task.State != "blocked" {
		t.Fatalf("expected blocked: %+v", blocked.Task)
	}
	status, data = h.request(http.MethodPost, "/tasks/"+dependent.ID+"/unblock", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if waiting := decode[taskBody](t, data).Task; waiting.Task.State != "waiting" {
		t.Fatalf("expected waiting again: %+v", waiting.Task)
	}
	// Completing the prerequisite readies it, still without being claimed.
	status, data = h.request(http.MethodPost, "/tasks/"+prereq.ID+"/claim", map[string]any{}, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	prereqClaim := decode[taskBody](t, data).Claim
	status, data = h.request(http.MethodPost, "/tasks/"+prereq.ID+"/complete", map[string]any{
		"epoch": prereqClaim.Epoch, "token": prereqClaim.Token,
	}, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	status, data = h.request(http.MethodGet, "/messages/"+dependent.ID, nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if ready := decode[protocol.ThreadView](t, data).Message; ready.Task.State != "ready" {
		t.Fatalf("expected the dependent to be ready: %+v", ready.Task)
	}
	// Blocking an unclaimed task is its owner's, and only its owner's.
	status, data = h.request(http.MethodPost, "/tasks/"+dependent.ID+"/block", map[string]any{"reason": "why"}, "local:user:bob", "", nil)
	h.expect(status, data, http.StatusForbidden)
}

func TestTaskListFilters(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"
	create := func(body map[string]any) protocol.MessageView {
		t.Helper()
		status, data := h.request(http.MethodPost, "/tasks", body, alice, "", nil)
		h.expect(status, data, http.StatusCreated)
		return decode[taskBody](t, data).Task
	}
	fix := create(map[string]any{"channel": "work", "text": "fix", "labels": map[string]string{"step": "fix"}})
	review := create(map[string]any{"channel": "work", "text": "review", "labels": map[string]string{"step": "review"}})
	waiting := create(map[string]any{"channel": "work/sub", "text": "ship", "needs": []string{review.ID}})
	forName := "local:user:alice+worker"
	assigned := create(map[string]any{"channel": "work", "text": "mine", "for": forName})

	list := func(query string) []protocol.ThreadSummary {
		t.Helper()
		status, data := h.request(http.MethodGet, "/tasks"+query, nil, alice, "", nil)
		h.expect(status, data, http.StatusOK)
		return decode[tasksBody](t, data).Tasks
	}
	if tasks := list("?channel=work&label=step=fix"); len(tasks) != 1 || tasks[0].Message.ID != fix.ID {
		t.Fatalf("label filter: %+v", tasks)
	}
	if tasks := list("?state=waiting"); len(tasks) != 1 || tasks[0].Message.ID != waiting.ID {
		t.Fatalf("waiting filter: %+v", tasks)
	}
	if tasks := list("?needs=" + review.ID); len(tasks) != 1 || tasks[0].Message.ID != waiting.ID {
		t.Fatalf("needs filter: %+v", tasks)
	}
	if tasks := list("?owner=local:user:alice&channel=work/"); len(tasks) != 4 {
		t.Fatalf("owner and subtree filter: %+v", tasks)
	}
	if tasks := list("?owner=local:user:bob"); len(tasks) != 0 {
		t.Fatalf("another owner has nothing: %+v", tasks)
	}
	if tasks := list("?for=local:user:alice%2Bworker"); len(tasks) != 1 || tasks[0].Message.ID != assigned.ID {
		t.Fatalf("assignee filter: %+v", tasks)
	}
	// A page's cursor reads the next page.
	status, data := h.request(http.MethodGet, "/tasks?channel=work&limit=1&state=all", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	page := decode[tasksBody](t, data)
	if len(page.Tasks) != 1 || page.Cursor == "" {
		t.Fatalf("first page: %+v", page)
	}
	status, data = h.request(http.MethodGet, "/tasks?channel=work&limit=1&state=all&after="+page.Cursor, nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	next := decode[tasksBody](t, data)
	if len(next.Tasks) != 1 || next.Tasks[0].Message.ID == page.Tasks[0].Message.ID {
		t.Fatalf("next page: %+v after %+v", next, page)
	}
}

func TestTaskIdempotency(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"

	status, data := h.request(http.MethodPost, "/tasks", map[string]any{
		"channel": "work", "text": "claimed task", "claim": true, "id": "task-key-1",
	}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	first := decode[taskBody](t, data)
	if first.Claim == nil {
		t.Fatalf("the first create should answer its claim: %+v", first)
	}

	// A retry answers the same task, without the claim.
	status, data = h.request(http.MethodPost, "/tasks", map[string]any{
		"channel": "work", "text": "claimed task", "claim": true, "id": "task-key-1",
	}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	retry := decode[taskBody](t, data)
	if retry.Task.ID != first.Task.ID || retry.Claim != nil {
		t.Fatalf("a retry answers the task and no claim: %+v", retry)
	}
	// Another channel under the same id is another write.
	status, data = h.request(http.MethodPost, "/tasks", map[string]any{
		"channel": "other", "text": "claimed task", "id": "task-key-1",
	}, alice, "", nil)
	h.expect(status, data, http.StatusConflict)
	if code := errorCode(t, data); code != protocol.CodeDuplicateID {
		t.Fatalf("expected duplicate_id, got %s", code)
	}
	// Only one task landed.
	status, data = h.request(http.MethodGet, "/tasks?state=all", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if page := decode[tasksBody](t, data); len(page.Tasks) != 1 {
		t.Fatalf("expected one task: %+v", page)
	}
}

func TestLatestTaskActivity(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"
	create := func(text string) protocol.MessageView {
		t.Helper()
		status, data := h.request(http.MethodPost, "/tasks", map[string]any{"channel": "work", "text": text}, alice, "", nil)
		h.expect(status, data, http.StatusCreated)
		return decode[taskBody](t, data).Task
	}
	first := create("first")
	h.clock.Advance(time.Second)
	second := create("second")
	// The first becomes the latest active after a reply.
	h.clock.Advance(time.Second)
	status, data := h.request(http.MethodPost, "/messages", map[string]any{"re": first.ID, "text": "still working"}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)

	status, data = h.request(http.MethodGet, "/tasks?latest=true&limit=1&state=all", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	page := decode[tasksBody](t, data)
	if len(page.Tasks) != 1 || page.Tasks[0].Message.ID != first.ID {
		t.Fatalf("latest should pick the last active task (%s), got %+v", first.ID, page)
	}
	_ = second
}

func TestJSONNotFound(t *testing.T) {
	h := newHarness(t)
	status, data := h.request(http.MethodGet, "/nope", nil, "local:user:alice", "", nil)
	h.expect(status, data, http.StatusNotFound)
	if code := errorCode(t, data); code != protocol.CodeNotFound {
		t.Fatalf("expected not_found, got %s", code)
	}
}

func TestDevActorProfileHeader(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"

	// A profile in the actor ID alone behaves like one named by header.
	headers := map[string]string{"X-Board-Actor": "local:user:alice+worker"}
	status, data, err := h.try(http.MethodPost, "/tasks", map[string]any{"channel": "work", "text": "profile work", "claim": true}, "", "", headers)
	if err != nil {
		t.Fatal(err)
	}
	h.expect(status, data, http.StatusCreated)
	task := decode[taskBody](t, data).Task
	if task.Task.By != "local:user:alice+worker" || task.Task.Owner != alice {
		t.Fatalf("unexpected profile provenance: %+v", task.Task)
	}
	if task.Author == nil || task.Author.Kind != protocol.ActorAgent || task.Author.Via == nil {
		t.Fatalf("an agent shows the principal it acts through: %+v", task.Author)
	}
}

func TestValidationAndStatuses(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"

	cases := []struct {
		name   string
		method string
		path   string
		body   any
		want   int
		code   protocol.Code
	}{
		{"bad task claim channel", http.MethodPost, "/tasks/claim", map[string]any{"channel": "Work"}, http.StatusBadRequest, protocol.CodeInvalidRequest},
		{"task without text", http.MethodPost, "/tasks", map[string]any{"channel": "work"}, http.StatusBadRequest, protocol.CodeInvalidRequest},
		{"task both forms", http.MethodPost, "/tasks", map[string]any{"channel": "work", "re": "1", "text": "x"}, http.StatusBadRequest, protocol.CodeInvalidRequest},
		{"lease without claim", http.MethodPost, "/tasks", map[string]any{"channel": "work", "text": "x", "lease": 60}, http.StatusBadRequest, protocol.CodeInvalidRequest},
		{"lease too short", http.MethodPost, "/tasks", map[string]any{"channel": "work", "text": "x", "claim": true, "lease": 1}, http.StatusBadRequest, protocol.CodeInvalidRequest},
		{"bad cursor", http.MethodGet, "/messages?channel=work&after=nope", nil, http.StatusBadRequest, protocol.CodeInvalidCursor},
		{"unknown state", http.MethodGet, "/tasks?state=doing", nil, http.StatusBadRequest, protocol.CodeInvalidRequest},
		{"bad limit", http.MethodGet, "/messages?channel=work&limit=0", nil, http.StatusBadRequest, protocol.CodeInvalidRequest},
		{"latest with after", http.MethodGet, "/tasks?latest=true&after=1", nil, http.StatusBadRequest, protocol.CodeInvalidRequest},
		{"no such task", http.MethodPost, "/tasks/1/claim", map[string]any{}, http.StatusNotFound, protocol.CodeMessageNotFound},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			status, data := h.request(test.method, test.path, test.body, alice, "", nil)
			h.expect(status, data, test.want)
			if code := errorCode(t, data); code != test.code {
				t.Fatalf("expected %s, got %s: %s", test.code, code, data)
			}
		})
	}

	// A thread that exists but isn't a task answers not_a_task.
	root := h.postMessage(alice, "work", "not a task")
	status, data := h.request(http.MethodPost, "/tasks/"+root.ID+"/claim", map[string]any{}, alice, "", nil)
	h.expect(status, data, http.StatusNotFound)
	if code := errorCode(t, data); code != protocol.CodeNotATask {
		t.Fatalf("expected not_a_task, got %s", code)
	}

	// A reply can't be made a task; name its root.
	status, data = h.request(http.MethodPost, "/messages", map[string]any{"re": root.ID, "text": "reply"}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	reply := decode[postBody](t, data).Message
	status, data = h.request(http.MethodPost, "/tasks", map[string]any{"re": reply.ID}, alice, "", nil)
	h.expect(status, data, http.StatusBadRequest)

	// Present-but-empty and absent are told apart where the schema says so.
	status, data = h.request(http.MethodPost, "/tasks", map[string]any{"channel": "work", "text": "a task"}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	task := decode[taskBody](t, data).Task
	status, data = h.request(http.MethodPost, "/tasks/"+task.ID+"/needs", map[string]any{}, alice, "", nil)
	h.expect(status, data, http.StatusBadRequest)
	status, data = h.request(http.MethodPost, "/tasks/"+task.ID+"/needs", map[string]any{"needs": []string{}}, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if changed := decode[taskBody](t, data).Task; changed.Task.State != "ready" || len(changed.Task.Needs) != 0 {
		t.Fatalf("clearing needs: %+v", changed.Task)
	}
	status, data = h.request(http.MethodPost, "/tasks/"+task.ID+"/assign", map[string]any{}, alice, "", nil)
	h.expect(status, data, http.StatusBadRequest)
	status, data = h.request(http.MethodPost, "/tasks/"+task.ID+"/assign", map[string]any{"for": nil}, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	status, data = h.request(http.MethodPost, "/tasks/"+task.ID+"/block", map[string]any{"reason": "why", "kind": ""}, alice, "", nil)
	h.expect(status, data, http.StatusBadRequest)
	status, data = h.request(http.MethodPost, "/tasks", map[string]any{"channel": "work", "text": "x", "for": ""}, alice, "", nil)
	h.expect(status, data, http.StatusBadRequest)
}

func TestEventsLastAndQueryValidation(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"
	first := h.postMessage(alice, "work", "one")
	second := h.postMessage(alice, "work", "two")

	response, reader, cancel := h.openStream(t, "?last=1", nil)
	defer cancel()
	defer response.Body.Close()
	event := readEvent(t, reader)
	if event.Message == nil || event.Message.ID != second.ID {
		t.Fatalf("last=1 should answer the newest event, got %+v (first %s)", event, first.ID)
	}
	// The stream goes on from the head after replaying.
	third := h.postMessage(alice, "work", "three")
	next := readEvent(t, reader)
	if next.Message == nil || next.Message.ID != third.ID {
		t.Fatalf("after last=1 the stream should continue, got %+v", next)
	}

	// `last` takes no after or wait.
	status, data := h.request(http.MethodGet, "/events?last=1&after=1", nil, alice, "", nil)
	h.expect(status, data, http.StatusBadRequest)
	// An unparseable cursor is a 400, not a silent empty stream.
	status, data = h.request(http.MethodGet, "/events?after=nope", nil, alice, "", nil)
	h.expect(status, data, http.StatusBadRequest)
	if code := errorCode(t, data); code != protocol.CodeInvalidCursor {
		t.Fatalf("expected invalid_cursor, got %s", code)
	}
	// A cursor ahead of the log isn't clamped down to the head: it names
	// nothing this board has, so it's refused.
	status, data = h.request(http.MethodGet, "/events?after=999", nil, alice, "", nil)
	h.expect(status, data, http.StatusBadRequest)
	if code := errorCode(t, data); code != protocol.CodeInvalidCursor {
		t.Fatalf("expected invalid_cursor for a future after, got %s", code)
	}
	status, data = h.request(http.MethodGet, "/events", nil, alice, "", map[string]string{"Last-Event-ID": "999"})
	h.expect(status, data, http.StatusBadRequest)
	if code := errorCode(t, data); code != protocol.CodeInvalidCursor {
		t.Fatalf("expected invalid_cursor for a future Last-Event-ID, got %s", code)
	}
}

func TestEventPayloadWireShape(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"

	response, reader, cancel := h.openStream(t, "", nil)
	defer cancel()
	defer response.Body.Close()

	status, data := h.request(http.MethodPost, "/tasks", map[string]any{"channel": "work", "text": "shaped", "claim": true}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	task := decode[taskBody](t, data).Task

	seen := map[string]protocol.EventView{}
	for {
		event := readEvent(t, reader)
		seen[event.Type] = event
		_, postedSeen := seen["message.posted"]
		_, createdSeen := seen["task.created"]
		_, claimedSeen := seen["task.claimed"]
		if postedSeen && createdSeen && claimedSeen {
			break
		}
	}
	posted := seen["message.posted"]
	if posted.Payload["id"] != task.ID || posted.Payload["channel"] != "work" {
		t.Fatalf("a post payload's ids should be refs: %+v", posted.Payload)
	}
	if _, leaked := seen["task.claimed"].Payload["token"]; leaked {
		t.Fatalf("a claim's token must never reach a view: %+v", seen["task.claimed"].Payload)
	}
	if seen["task.claimed"].Payload["epoch"] == nil {
		t.Fatalf("a claim payload keeps its epoch: %+v", seen["task.claimed"].Payload)
	}
	// Each event's message is as that event left it: the post isn't a task
	// yet, the created task is ready, and the claim makes it claimed.
	if posted.Message == nil || posted.Message.Task != nil {
		t.Fatalf("a post's event shouldn't carry the task yet: %+v", posted.Message)
	}
	if created := seen["task.created"].Message; created == nil || created.Task == nil || created.Task.State != "ready" {
		t.Fatalf("task.created should leave a ready task: %+v", created)
	}
	if claimed := seen["task.claimed"].Message; claimed == nil || claimed.Task == nil || claimed.Task.State != "claimed" {
		t.Fatalf("task.claimed should leave a claimed task: %+v", claimed)
	}
	if posted.Actor == nil || posted.Actor.ID != alice {
		t.Fatalf("an event names who did it: %+v", posted.Actor)
	}
}

func TestChannelsPaging(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"
	for _, path := range []string{"a", "a/one", "a/two", "b"} {
		status, data := h.request(http.MethodPost, "/channels", map[string]any{"path": path}, alice, "", nil)
		if status != http.StatusCreated && status != http.StatusOK {
			t.Fatalf("creating %s: %d %s", path, status, data)
		}
	}
	status, data := h.request(http.MethodGet, "/channels?prefix=a/", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	page := decode[protocol.ChannelsPage](t, data)
	if len(page.Channels) != 2 || page.Channels[0].Path != "a/one" || page.Channels[1].Path != "a/two" {
		t.Fatalf("unexpected prefix page: %+v", page)
	}

	status, data = h.request(http.MethodGet, "/channels?limit=2", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	first := decode[protocol.ChannelsPage](t, data)
	if len(first.Channels) != 2 || first.Next == nil {
		t.Fatalf("expected a page with more: %+v", first)
	}
	status, data = h.request(http.MethodGet, "/channels?limit=2&after="+url.QueryEscape(*first.Next), nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	second := decode[protocol.ChannelsPage](t, data)
	if len(second.Channels) != 2 || second.Next != nil {
		t.Fatalf("unexpected second page: %+v", second)
	}
	if second.Channels[0].Path <= first.Channels[1].Path {
		t.Fatalf("pages should advance by path: %+v then %+v", first, second)
	}
}
