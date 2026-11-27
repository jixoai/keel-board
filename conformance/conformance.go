// Package conformance is the board's contract test suite. It runs the same
// assertions against any store.EventStore and proves that the log, the HTTP
// surface, and the server's auth/tenant seams behave the same over the
// in-memory store and PostgreSQL.
//
// The suite creates every tenant itself and never assumes an empty store. An
// implementation supplies Options.Store: each call returns a handle to the
// same durable log (a new PostgreSQL pool on the same schema, or the same
// in-memory store), so the suite can model two processes sharing one log.
//
//	go test ./conformance
//	KEEL_BOARD_TEST_DSN=postgres://... go test ./conformance
//
// PostgreSQL runs only when KEEL_BOARD_TEST_DSN (or TEST_DSN) is set; CI must
// set it, as it does for the repository's other PostgreSQL tests.
package conformance

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/server"
	"github.com/jixoai/keel-board/store"
)

// Options configure one conformance run.
type Options struct {
	// Name labels the implementation in subtest names.
	Name string
	// Store returns a handle to one durable log. The suite may call it more
	// than once to model separate processes: every handle must see the same
	// log, and each PostgreSQL handle is a separate pool on one schema.
	// Required.
	Store func(t *testing.T) store.EventStore
}

// Run runs the whole conformance suite. It fails t on the first contract
// violation; a store that implements it correctly leaves every subtest green.
func Run(t *testing.T, opts Options) {
	t.Helper()
	if opts.Store == nil {
		t.Fatal("conformance: Options.Store is required")
	}
	if strings.TrimSpace(opts.Name) == "" {
		opts.Name = "store"
	}
	t.Run(opts.Name+"/store_handles_share_the_log", func(t *testing.T) { testStoreHandlesShareTheLog(t, opts) })
	t.Run(opts.Name+"/append_load_history", func(t *testing.T) { testAppendLoadHistory(t, opts) })
	t.Run(opts.Name+"/conditional_append", func(t *testing.T) { testConditionalAppend(t, opts) })
	t.Run(opts.Name+"/idempotency", func(t *testing.T) { testIdempotency(t, opts) })
	t.Run(opts.Name+"/message_thread", func(t *testing.T) { testMessageThread(t, opts) })
	t.Run(opts.Name+"/idempotent_post", func(t *testing.T) { testIdempotentPost(t, opts) })
	t.Run(opts.Name+"/concurrent_claim", func(t *testing.T) { testConcurrentClaim(t, opts) })
	t.Run(opts.Name+"/tenant_isolation", func(t *testing.T) { testTenantIsolation(t, opts) })
	t.Run(opts.Name+"/unknown_event", func(t *testing.T) { testUnknownEvent(t, opts) })
	t.Run(opts.Name+"/sse_cursor", func(t *testing.T) { testSSECursor(t, opts) })
	t.Run(opts.Name+"/seams", func(t *testing.T) { testSeams(t, opts) })
}

// FakeAuthorizer is the switchable authorizer the seam test uses. It allows
// every permission except those explicitly denied for a channel.
type FakeAuthorizer struct {
	// Denied maps a channel to the permissions it refuses. A nil map allows
	// everything.
	Denied map[string]map[protocol.Permission]bool
}

// Authorize implements server.Authorizer.
func (a FakeAuthorizer) Authorize(_ context.Context, _ string, _ protocol.Actor, channel string, permission protocol.Permission) error {
	if denied := a.Denied[channel]; denied != nil && denied[permission] {
		return protocol.Forbidden(fmt.Sprintf("%s is not allowed in %s", permission, channel))
	}
	return nil
}

// HeaderActor is an ActorFunc that refuses a request without header, and
// otherwise resolves it with server.DevActor. It proves a host can replace
// the development actor seam without touching the handlers.
func HeaderActor(header string) server.ActorFunc {
	return func(r *http.Request) (protocol.Actor, error) {
		if strings.TrimSpace(r.Header.Get(header)) == "" {
			return protocol.Actor{}, protocol.NewError(protocol.CodeUnauthorized, "a credential is required")
		}
		return server.DevActor(r)
	}
}

// HeaderTenant is a TenantFunc that refuses a request without header, and
// otherwise resolves the named board. It proves a host can replace the tenant
// seam, and that the board isolates whatever it returns.
func HeaderTenant(header string) server.TenantFunc {
	return func(r *http.Request) (string, error) {
		tenant := strings.TrimSpace(r.Header.Get(header))
		if tenant == "" {
			return "", protocol.Invalid("the request names no board")
		}
		return tenant, nil
	}
}

// recordingAuthorizer remembers what the server told it, proving the
// authorizer seam receives the resolved tenant and actor.
type recordingAuthorizer struct {
	mu    sync.Mutex
	calls []authCall
}

type authCall struct {
	tenant     string
	actor      protocol.ActorID
	channel    string
	permission protocol.Permission
}

// Authorize implements server.Authorizer.
func (r *recordingAuthorizer) Authorize(_ context.Context, tenant string, actor protocol.Actor, channel string, permission protocol.Permission) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, authCall{tenant: tenant, actor: actor.ID, channel: channel, permission: permission})
	return nil
}

// saw reports whether one call matched every dimension.
func (r *recordingAuthorizer) saw(tenant string, actor protocol.ActorID, channel string, permission protocol.Permission) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, call := range r.calls {
		if call.tenant == tenant && call.actor == actor && call.channel == channel && call.permission == permission {
			return true
		}
	}
	return false
}

// testStoreHandlesShareTheLog locks the Options.Store contract: two handles
// from one factory call must see one durable log, as two processes would.
func testStoreHandlesShareTheLog(t *testing.T, opts Options) {
	t.Helper()
	first, second := opts.Store(t), opts.Store(t)
	tenant := tenantName(t)
	appendOne(t, first, tenant, 0, 1)
	records, err := second.Load(t.Context(), tenant)
	if err != nil {
		t.Fatalf("second handle's load: %v", err)
	}
	if len(records) != 1 || records[0].Seq != 1 {
		t.Fatalf("the second handle doesn't see the first's log: %+v", records)
	}
	if head, ok := second.(store.HeadStore); ok {
		got, err := head.Head(t.Context(), tenant)
		if err != nil || got != 1 {
			t.Fatalf("the second handle's head = %d, %v", got, err)
		}
	}
}

// testAppendLoadHistory is the EventStore contract: an append is
// all-or-nothing, seqs are contiguous and per tenant, and History pages.
func testAppendLoadHistory(t *testing.T, opts Options) {
	t.Helper()
	ctx := t.Context()
	events := opts.Store(t)
	tenant := tenantName(t)
	at := time.Date(2026, 1, 2, 3, 4, 5, 6000, time.UTC)
	records := []store.Record{
		{Seq: 1, Type: "conformance.event", Channel: "work", Payload: []byte(`{"n":1,"text":"one"}`), At: at},
		{Seq: 2, Type: "conformance.event", Channel: "work", Payload: []byte(`{"n":2,"text":"two"}`), At: at.Add(time.Second)},
	}
	result, err := events.Append(ctx, tenant, records, 0)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if result.Replayed || result.FirstSeq != 1 || result.LastSeq != 2 {
		t.Fatalf("append result: %+v", result)
	}

	loaded, err := events.Load(ctx, tenant)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(loaded) != 2 || loaded[0].Seq != 1 || loaded[1].Seq != 2 {
		t.Fatalf("loaded records: %+v", loaded)
	}
	if loaded[0].Type != "conformance.event" || loaded[1].Channel != "work" {
		t.Fatalf("loaded metadata: %+v", loaded)
	}
	if !sameJSON(t, loaded[0].Payload, `{"n":1,"text":"one"}`) {
		t.Fatalf("payload changed: %s", loaded[0].Payload)
	}
	if !loaded[0].At.Equal(at) {
		t.Fatalf("append time: %v, want %v", loaded[0].At, at)
	}

	historian, ok := events.(store.HistoryStore)
	if !ok {
		t.Fatalf("%T doesn't implement store.HistoryStore", events)
	}
	page, err := historian.History(ctx, tenant, 0, 1)
	if err != nil {
		t.Fatalf("history after 0: %v", err)
	}
	if len(page) != 1 || page[0].Seq != 1 {
		t.Fatalf("first history page: %+v", page)
	}
	page, err = historian.History(ctx, tenant, 1, 10)
	if err != nil {
		t.Fatalf("history after 1: %v", err)
	}
	if len(page) != 1 || page[0].Seq != 2 {
		t.Fatalf("second history page: %+v", page)
	}
	page, err = historian.History(ctx, tenant, 2, 10)
	if err != nil {
		t.Fatalf("history after head: %v", err)
	}
	if len(page) != 0 {
		t.Fatalf("history past the head: %+v", page)
	}

	// A tenant with no events is empty, not an error, and is not another
	// tenant's log.
	other, err := events.Load(ctx, tenantName(t))
	if err != nil {
		t.Fatalf("load another tenant: %v", err)
	}
	if len(other) != 0 {
		t.Fatalf("another tenant's log leaked: %+v", other)
	}
	if head, ok := events.(store.HeadStore); ok {
		got, err := head.Head(ctx, tenant)
		if err != nil {
			t.Fatalf("head: %v", err)
		}
		if got != 2 {
			t.Fatalf("head = %d, want 2", got)
		}
	}
}

// testConditionalAppend proves a stale head and a non-contiguous batch are
// refused without writing.
func testConditionalAppend(t *testing.T, opts Options) {
	t.Helper()
	ctx := t.Context()
	events := opts.Store(t)
	tenant := tenantName(t)
	appendOne(t, events, tenant, 0, 1)

	if _, err := events.Append(ctx, tenant, []store.Record{conformanceRecord(2, 2)}, 0); !errors.Is(err, store.ErrHeadMoved) {
		t.Fatalf("a stale head: %v", err)
	}
	gap := []store.Record{conformanceRecord(2, 2), conformanceRecord(4, 4)}
	if _, err := events.Append(ctx, tenant, gap, 1); !errors.Is(err, store.ErrHeadMoved) {
		t.Fatalf("a batch with a gap: %v", err)
	}
	loaded, err := events.Load(ctx, tenant)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(loaded) != 1 {
		t.Fatalf("a refused batch wrote: %+v", loaded)
	}
	appendOne(t, events, tenant, 1, 2)
}

// testIdempotency is the store-level keyed append contract: the same key and
// digest answers the first result without writing; another digest is refused.
func testIdempotency(t *testing.T, opts Options) {
	t.Helper()
	ctx := t.Context()
	events := opts.Store(t)
	tenant := tenantName(t)
	keyed := conformanceRecord(1, 1)
	keyed.RequestKey = "conformance:key"
	keyed.Digest = "digest-one"
	keyed.RequestResult = []byte(`{"firstSeq":1,"lastSeq":1,"messageIds":[1]}`)
	first, err := events.Append(ctx, tenant, []store.Record{keyed}, 0)
	if err != nil {
		t.Fatalf("first keyed append: %v", err)
	}
	if first.Replayed || first.FirstSeq != 1 || first.LastSeq != 1 || len(first.MessageIDs) != 1 || first.MessageIDs[0] != 1 {
		t.Fatalf("first keyed append: %+v", first)
	}

	// A retry answers even with a stale expected head: the key had already
	// landed.
	retry := conformanceRecord(2, 2)
	retry.RequestKey = "conformance:key"
	retry.Digest = "digest-one"
	replayed, err := events.Append(ctx, tenant, []store.Record{retry}, 99)
	if err != nil {
		t.Fatalf("keyed retry: %v", err)
	}
	if !replayed.Replayed || replayed.FirstSeq != 1 || replayed.LastSeq != 1 || len(replayed.MessageIDs) != 1 || replayed.MessageIDs[0] != 1 {
		t.Fatalf("keyed retry: %+v", replayed)
	}
	other := conformanceRecord(2, 2)
	other.RequestKey = "conformance:key"
	other.Digest = "digest-two"
	if _, err := events.Append(ctx, tenant, []store.Record{other}, 1); !errors.Is(err, store.ErrDuplicateRequest) {
		t.Fatalf("another digest: %v", err)
	}
	loaded, err := events.Load(ctx, tenant)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(loaded) != 1 {
		t.Fatalf("a retry wrote: %+v", loaded)
	}

	requests, ok := events.(store.RequestStore)
	if !ok {
		t.Fatalf("%T doesn't implement store.RequestStore", events)
	}
	stored, found, err := requests.Request(ctx, tenant, "conformance:key")
	if err != nil {
		t.Fatalf("request lookup: %v", err)
	}
	if !found || stored.Digest != "digest-one" || stored.Result.LastSeq != 1 {
		t.Fatalf("stored request: %+v, found %v", stored, found)
	}
	if _, found, err := requests.Request(ctx, tenant, "absent"); err != nil || found {
		t.Fatalf("an absent request: found %v, %v", found, err)
	}
}

// testMessageThread posts a root and a reply through the HTTP surface over the
// store, and reads them back.
func testMessageThread(t *testing.T, opts Options) {
	t.Helper()
	h := newHarness(t, opts)
	tenant := tenantName(t)
	h.tenant = tenant
	const actor = "local:user:alice"

	root := h.post(actor, "work", "root")
	if root.Text != "root" || root.Re != nil {
		t.Fatalf("root: %+v", root)
	}
	status, data := h.request(http.MethodPost, "/messages", map[string]any{
		"re":   root.ID,
		"text": "reply",
	}, actor, nil)
	if status != http.StatusCreated {
		t.Fatalf("reply: %d %s", status, data)
	}
	reply := decode[postBody](t, data).Message
	if reply.Re == nil || *reply.Re != root.ID || reply.Thread != string(root.ID) {
		t.Fatalf("reply shape: %+v", reply)
	}

	status, data = h.request(http.MethodGet, "/messages/"+string(root.ID), nil, actor, nil)
	if status != http.StatusOK {
		t.Fatalf("thread read: %d %s", status, data)
	}
	thread := decode[protocol.ThreadView](t, data)
	if thread.Message.ID != root.ID || len(thread.Replies) != 1 || thread.Replies[0].ID != reply.ID {
		t.Fatalf("thread: %+v", thread)
	}

	// The durable log names both posts: their IDs are their seqs.
	events, err := opts.Store(t).Load(t.Context(), tenant)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	posted := 0
	for _, record := range events {
		if record.Type == string(log.EventMessagePosted) {
			posted++
		}
	}
	if posted != 2 {
		t.Fatalf("expected two message.posted events, got %d: %+v", posted, events)
	}
}

// testIdempotentPost proves the HTTP idempotency key is per tenant and per
// actor, answers the first write, and refuses another request.
func testIdempotentPost(t *testing.T, opts Options) {
	t.Helper()
	h := newHarness(t, opts)
	tenant := tenantName(t)
	h.tenant = tenant
	const actor = "local:user:alice"
	headers := map[string]string{"Idempotency-Key": "conformance-post"}

	status, data := h.request(http.MethodPost, "/messages", map[string]any{"channel": "work", "text": "once"}, actor, headers)
	if status != http.StatusCreated {
		t.Fatalf("first post: %d %s", status, data)
	}
	first := decode[postBody](t, data).Message
	status, data = h.request(http.MethodPost, "/messages", map[string]any{"channel": "work", "text": "once"}, actor, headers)
	if status != http.StatusCreated {
		t.Fatalf("retry: %d %s", status, data)
	}
	again := decode[postBody](t, data).Message
	if again.ID != first.ID {
		t.Fatalf("a retry made another message: %s vs %s", again.ID, first.ID)
	}

	// The place digest names the channel, so a different body has to name a
	// different place to count as another request.
	status, data = h.request(http.MethodPost, "/messages", map[string]any{"channel": "other", "text": "different"}, actor, headers)
	if status != http.StatusConflict {
		t.Fatalf("same key, different body: %d %s", status, data)
	}
	if code := errorCode(t, data); code != protocol.CodeDuplicateID {
		t.Fatalf("expected duplicate_id, got %s", code)
	}

	// The key is per actor: another actor's same key is another write.
	status, data = h.request(http.MethodPost, "/messages", map[string]any{"channel": "work", "text": "once"}, "local:user:bob", headers)
	if status != http.StatusCreated {
		t.Fatalf("same key, another actor: %d %s", status, data)
	}
	bob := decode[postBody](t, data).Message
	if bob.ID == first.ID {
		t.Fatalf("another actor replayed %s's write", first.ID)
	}

	events, err := opts.Store(t).Load(t.Context(), tenant)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	posted := 0
	for _, record := range events {
		if record.Type == string(log.EventMessagePosted) {
			posted++
		}
	}
	if posted != 2 {
		t.Fatalf("expected two posts (one retry was answered, one actor was distinct), got %d", posted)
	}
}

// testConcurrentClaim runs the same claim on two boards sharing the log, as
// two processes would: exactly one wins and one task.claimed event lands.
func testConcurrentClaim(t *testing.T, opts Options) {
	t.Helper()
	first := newHarness(t, opts)
	second := newHarness(t, opts)
	tenant := tenantName(t)
	first.tenant, second.tenant = tenant, tenant
	const actor = "local:user:alice"

	task := first.createTask(actor, "work", "claim me")
	start := make(chan struct{})
	type outcome struct {
		status int
		body   []byte
		err    error
	}
	results := make(chan outcome, 2)
	var wait sync.WaitGroup
	for _, h := range []*harness{first, second} {
		wait.Add(1)
		go func(h *harness) {
			defer wait.Done()
			<-start
			status, data, err := h.try(http.MethodPost, "/tasks/"+string(task.ID)+"/claim", nil, actor, nil)
			results <- outcome{status: status, body: data, err: err}
		}(h)
	}
	close(start)
	wait.Wait()
	close(results)

	won, claimed := 0, 0
	for result := range results {
		if result.err != nil {
			t.Fatalf("claim request: %v", result.err)
		}
		switch result.status {
		case http.StatusOK:
			won++
		case http.StatusConflict:
			claimed++
			if code := errorCode(t, result.body); code != protocol.CodeTaskClaimed {
				t.Fatalf("a losing claim should be task_claimed, got %s", code)
			}
		default:
			t.Fatalf("claim: %d %s", result.status, result.body)
		}
	}
	if won != 1 || claimed != 1 {
		t.Fatalf("concurrent claims: %d won, %d refused", won, claimed)
	}

	events, err := opts.Store(t).Load(t.Context(), tenant)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	claims := 0
	for _, record := range events {
		if record.Type == string(log.EventTaskClaimed) {
			claims++
		}
	}
	if claims != 1 {
		t.Fatalf("expected one task.claimed event, got %d", claims)
	}
}

// testTenantIsolation proves logs, reads, cursors and idempotency keys are
// scoped to one tenant.
func testTenantIsolation(t *testing.T, opts Options) {
	t.Helper()
	h := newHarness(t, opts)
	tenantA, tenantB := tenantName(t), tenantName(t)
	const actor = "local:user:alice"

	h.tenant = tenantA
	message := h.post(actor, "work", "for A only")
	_ = h.post(actor, "work", "A's cursor source")

	// Tenant B can't read A by ID, and its channel is empty.
	h.tenant = tenantB
	status, data := h.request(http.MethodGet, "/messages/"+string(message.ID), nil, actor, nil)
	if status != http.StatusNotFound {
		t.Fatalf("cross-tenant read: %d %s", status, data)
	}
	status, data = h.request(http.MethodGet, "/messages?channel=work", nil, actor, nil)
	if status != http.StatusOK {
		t.Fatalf("B channel read: %d %s", status, data)
	}
	if page := decode[protocol.MessagesPage](t, data); len(page.Messages) != 0 {
		t.Fatalf("B saw A's messages: %+v", page.Messages)
	}

	// A cursor is a tenant's seq. A's cursor is ahead of B's empty log, so B
	// refuses it instead of clamping.
	status, data = h.request(http.MethodGet, "/events", nil, actor, map[string]string{"Last-Event-ID": string(message.ID)})
	if status != http.StatusBadRequest {
		t.Fatalf("cross-tenant cursor: %d %s", status, data)
	}
	if code := errorCode(t, data); code != protocol.CodeInvalidCursor {
		t.Fatalf("expected invalid_cursor, got %s", code)
	}

	// The same idempotency key in another tenant is another write.
	headers := map[string]string{"Idempotency-Key": "conformance-cross-tenant"}
	for _, tenant := range []string{tenantA, tenantB} {
		h.tenant = tenant
		status, data = h.request(http.MethodPost, "/messages", map[string]any{"channel": "work", "text": "same key"}, actor, headers)
		if status != http.StatusCreated {
			t.Fatalf("tenant %s key: %d %s", tenant, status, data)
		}
	}

	events, err := opts.Store(t).Load(t.Context(), tenantA)
	if err != nil {
		t.Fatalf("load A: %v", err)
	}
	other, err := opts.Store(t).Load(t.Context(), tenantB)
	if err != nil {
		t.Fatalf("load B: %v", err)
	}
	if len(events) == 0 || len(other) == 0 {
		t.Fatalf("both tenants should hold their own events: %d / %d", len(events), len(other))
	}
}

// testUnknownEvent proves a kind this build doesn't fold stays in the log
// without blocking the projection: a later version can read it.
func testUnknownEvent(t *testing.T, opts Options) {
	t.Helper()
	h := newHarness(t, opts)
	h.tenant = tenantName(t)
	const actor = "local:user:alice"
	message := h.post(actor, "work", "known")

	events := opts.Store(t)
	loaded, err := events.Load(t.Context(), h.tenant)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	head := uint64(len(loaded))
	future := store.Record{
		Seq:     head + 1,
		Type:    "conformance.future_event",
		Channel: "work",
		Payload: []byte(`{"v":1,"note":"unknown to this build"}`),
	}
	if _, err := events.Append(t.Context(), h.tenant, []store.Record{future}, head); err != nil {
		t.Fatalf("append an unknown kind: %v", err)
	}

	// A new board folds the whole log, including the unknown kind, and still
	// serves what it knows.
	fresh := newHarness(t, opts)
	fresh.tenant = h.tenant
	status, data := fresh.request(http.MethodGet, "/messages/"+string(message.ID), nil, actor, nil)
	if status != http.StatusOK {
		t.Fatalf("read past an unknown kind: %d %s", status, data)
	}
	thread := decode[protocol.ThreadView](t, data)
	if thread.Message.ID != message.ID || thread.Message.Text != "known" {
		t.Fatalf("known message after an unknown kind: %+v", thread.Message)
	}
	if thread.Cursor != protocol.FormatRef(head+1) {
		t.Fatalf("the read cursor should include the unknown event: %s, want %d", thread.Cursor, head+1)
	}
}

// testSSECursor proves the stream's id is the board seq: a reconnect with
// Last-Event-ID replays only what came after it, and a cursor ahead of the log
// is refused.
func testSSECursor(t *testing.T, opts Options) {
	t.Helper()
	h := newHarness(t, opts)
	h.tenant = tenantName(t)
	const actor = "local:user:alice"

	response, reader, cancel := h.openStream(nil)
	defer cancel()
	defer response.Body.Close()
	first := h.post(actor, "work", "first")
	event := readEvent(t, reader)
	if event.Message == nil || event.Message.ID != first.ID {
		t.Fatalf("first streamed event: %+v", event)
	}
	cursor := event.Cursor
	if cursor == "" {
		t.Fatal("an SSE event should carry an id")
	}
	cancel()
	response.Body.Close()

	second := h.post(actor, "work", "second")
	reconnected, reconnectedReader, reconnectCancel := h.openStream(map[string]string{"Last-Event-ID": cursor})
	defer reconnectCancel()
	defer reconnected.Body.Close()
	resumed := readEvent(t, reconnectedReader)
	if resumed.Message == nil || resumed.Message.ID != second.ID {
		t.Fatalf("reconnect should answer only the second post: %+v", resumed)
	}
	if !cursorIsAfter(resumed.Cursor, cursor) {
		t.Fatalf("resumed cursor %s should be after %s", resumed.Cursor, cursor)
	}

	// A cursor ahead of the log names nothing on this board.
	status, data := h.request(http.MethodGet, "/events", nil, actor, map[string]string{"Last-Event-ID": "999999"})
	if status != http.StatusBadRequest {
		t.Fatalf("future cursor: %d %s", status, data)
	}
	if code := errorCode(t, data); code != protocol.CodeInvalidCursor {
		t.Fatalf("expected invalid_cursor, got %s", code)
	}
}

// testSeams proves the server's actor, tenant and authorizer seams switch
// without changing a handler.
func testSeams(t *testing.T, opts Options) {
	t.Helper()
	tenant := tenantName(t)
	// A board that allows everything is the baseline.
	open := newHarness(t, opts)
	open.tenant = tenant
	const actor = "local:user:alice"
	open.post(actor, "work", "allowed")

	// The same board behind a fake authorizer: reads pass, writing to the
	// denied channel is 403.
	restricted := newHarnessAt(t, open.board)
	restricted.tenant = tenant
	restricted.serve(t, server.Options{Authorize: FakeAuthorizer{Denied: map[string]map[protocol.Permission]bool{
		"closed": {protocol.PermissionPost: true, protocol.PermissionLabel: true},
	}}})
	status, data := restricted.request(http.MethodPost, "/messages", map[string]any{"channel": "closed", "text": "no"}, actor, nil)
	if status != http.StatusForbidden {
		t.Fatalf("denied post: %d %s", status, data)
	}
	status, data = restricted.request(http.MethodGet, "/messages?channel=work", nil, actor, nil)
	if status != http.StatusOK {
		t.Fatalf("allowed read: %d %s", status, data)
	}

	// A switched actor seam refuses a request without its credential.
	auth := newHarnessAt(t, open.board)
	auth.tenant = tenant
	auth.serve(t, server.Options{Actor: HeaderActor("X-Conformance-Credential")})
	status, data = auth.request(http.MethodGet, "/messages?channel=work", nil, "", nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("missing credential: %d %s", status, data)
	}
	status, _ = auth.request(http.MethodGet, "/messages?channel=work", nil, actor, map[string]string{"X-Conformance-Credential": "yes"})
	if status != http.StatusOK {
		t.Fatalf("with credential: %d", status)
	}

	// A switched tenant seam refuses a request without a board, and names one
	// when it has it.
	names := newHarnessAt(t, open.board)
	names.serve(t, server.Options{Tenant: HeaderTenant("X-Conformance-Tenant")})
	status, data = names.request(http.MethodGet, "/messages?channel=work", nil, actor, nil)
	if status != http.StatusBadRequest {
		t.Fatalf("missing tenant: %d %s", status, data)
	}
	status, _ = names.request(http.MethodGet, "/messages?channel=work", nil, actor, map[string]string{"X-Conformance-Tenant": tenant})
	if status != http.StatusOK {
		t.Fatalf("named tenant: %d", status)
	}

	// The authorizer receives the server-resolved tenant and actor, not an
	// empty placeholder or another request's caller.
	recorder := &recordingAuthorizer{}
	recorded := newHarnessAt(t, open.board)
	recorded.tenant = tenant
	recorded.serve(t, server.Options{Authorize: recorder})
	status, data = recorded.request(http.MethodPost, "/messages", map[string]any{"channel": "work", "text": "seen"}, actor, nil)
	if status != http.StatusCreated {
		t.Fatalf("recorded post: %d %s", status, data)
	}
	if !recorder.saw(tenant, protocol.ActorID(actor), "work", protocol.PermissionPost) {
		t.Fatalf("the authorizer didn't receive tenant=%q actor=%q post work: %+v", tenant, actor, recorder.calls)
	}
}

// harness is one process: a board over the log and the HTTP server above it.
type harness struct {
	t      *testing.T
	board  *log.Board
	clock  *clock
	server *httptest.Server
	tenant string
}

// newHarness opens a board and its HTTP server over one handle to the log.
func newHarness(t *testing.T, opts Options) *harness {
	t.Helper()
	clock := newClock()
	return newHarnessAtClock(t, log.New(log.Options{Store: opts.Store(t), Now: clock.Now, Freshness: -1}), clock)
}

// newHarnessAt runs an HTTP server over an existing board. It models another
// process sharing the same board state for the seam tests.
func newHarnessAt(t *testing.T, board *log.Board) *harness {
	t.Helper()
	return newHarnessAtClock(t, board, newClock())
}

func newHarnessAtClock(t *testing.T, board *log.Board, clock *clock) *harness {
	t.Helper()
	h := &harness{t: t, board: board, clock: clock}
	h.serve(t, server.Options{})
	return h
}

// serve (re)builds the HTTP server over the harness's board.
func (h *harness) serve(t *testing.T, overrides server.Options) {
	t.Helper()
	if h.server != nil {
		h.server.Close()
	}
	opts := server.Options{
		Board:     h.board,
		Now:       h.clock.Now,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		Authorize: server.AllowAll{},
	}
	if overrides.Authorize != nil {
		opts.Authorize = overrides.Authorize
	}
	if overrides.Actor != nil {
		opts.Actor = overrides.Actor
	}
	if overrides.Tenant != nil {
		opts.Tenant = overrides.Tenant
	}
	h.server = httptest.NewServer(server.New(opts))
	t.Cleanup(h.server.Close)
}

// request makes a request with a body, actor and extra headers, and fails on
// a transport error.
func (h *harness) request(method, path string, body any, actor string, headers map[string]string) (int, []byte) {
	h.t.Helper()
	status, data, err := h.try(method, path, body, actor, headers)
	if err != nil {
		h.t.Fatalf("%s %s: %v", method, path, err)
	}
	return status, data
}

// try is request without failing the caller, safe from a goroutine.
func (h *harness) try(method, path string, body any, actor string, headers map[string]string) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, h.server.URL+server.APIPrefix+path, reader)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		request.Header.Set("content-type", "application/json")
	}
	if actor != "" {
		request.Header.Set("X-Board-Actor", actor)
	}
	if h.tenant != "" {
		request.Header.Set("X-Board-Tenant", h.tenant)
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := h.server.Client().Do(request)
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

// post posts a message and returns its view.
func (h *harness) post(actor, channel, text string) protocol.MessageView {
	h.t.Helper()
	status, data := h.request(http.MethodPost, "/messages", map[string]any{"channel": channel, "text": text}, actor, nil)
	if status != http.StatusCreated {
		h.t.Fatalf("post: %d %s", status, data)
	}
	return decode[postBody](h.t, data).Message
}

// createTask makes a ready task and returns its root view.
func (h *harness) createTask(actor, channel, text string) protocol.MessageView {
	h.t.Helper()
	status, data := h.request(http.MethodPost, "/tasks", map[string]any{"channel": channel, "text": text}, actor, nil)
	if status != http.StatusCreated {
		h.t.Fatalf("create task: %d %s", status, data)
	}
	return decode[taskBody](h.t, data).Task
}

// openStream opens GET /events with the harness's tenant and optional
// headers. It returns the response, a reader on its body, and a cancel that
// closes the connection.
func (h *harness) openStream(headers map[string]string) (*http.Response, *bufio.Reader, context.CancelFunc) {
	h.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, h.server.URL+server.APIPrefix+"/events", nil)
	if err != nil {
		cancel()
		h.t.Fatalf("stream request: %v", err)
	}
	if h.tenant != "" {
		request.Header.Set("X-Board-Tenant", h.tenant)
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := h.server.Client().Do(request)
	if err != nil {
		cancel()
		h.t.Fatalf("opening stream: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(response.Body)
		response.Body.Close()
		cancel()
		h.t.Fatalf("stream: %d %s", response.StatusCode, data)
	}
	reader := bufio.NewReader(response.Body)
	// The stream opens with a comment and the blank line that ends it.
	line, err := reader.ReadString('\n')
	if err != nil || !strings.HasPrefix(line, ": connected") {
		response.Body.Close()
		cancel()
		h.t.Fatalf("events: expected a connected comment, got %q (%v)", line, err)
	}
	if blank, err := reader.ReadString('\n'); err != nil || strings.TrimSpace(blank) != "" {
		response.Body.Close()
		cancel()
		h.t.Fatalf("events: expected a blank line after the comment, got %q (%v)", blank, err)
	}
	return response, reader, cancel
}

// readEvent reads one SSE event, skipping comments and id-only cursor frames,
// with a timeout so a broken feed fails instead of hanging.
func readEvent(t *testing.T, reader *bufio.Reader) protocol.EventView {
	t.Helper()
	type result struct {
		view protocol.EventView
		err  error
	}
	done := make(chan result, 1)
	go func() {
		for {
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
			if data == "" {
				// An invisible event only advances the cursor.
				continue
			}
			var view protocol.EventView
			if err := json.Unmarshal([]byte(data), &view); err != nil {
				done <- result{err: err}
				return
			}
			if id != "" && view.Cursor != id {
				done <- result{err: fmt.Errorf("event id %q doesn't match cursor %q", id, view.Cursor)}
				return
			}
			done <- result{view: view}
			return
		}
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

// postBody is a post's answer.
type postBody struct {
	Message protocol.MessageView `json:"message"`
}

// taskBody is a task write's answer.
type taskBody struct {
	Task  protocol.MessageView `json:"task"`
	Claim *protocol.TaskClaim  `json:"claim"`
}

// decode unmarshals a body and fails the test on a malformed one.
func decode[T any](t *testing.T, data []byte) T {
	t.Helper()
	var value T
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatalf("decoding %s: %v", data, err)
	}
	return value
}

// errorCode reads a failure body's code.
func errorCode(t *testing.T, data []byte) protocol.Code {
	t.Helper()
	return decode[protocol.ErrorBody](t, data).Error.Code
}

// sameJSON compares two payloads by JSON value.
func sameJSON(t *testing.T, got []byte, want string) bool {
	t.Helper()
	var left, right any
	if err := json.Unmarshal(got, &left); err != nil {
		t.Fatalf("decoding %s: %v", got, err)
	}
	if err := json.Unmarshal([]byte(want), &right); err != nil {
		t.Fatalf("decoding %s: %v", want, err)
	}
	return reflect.DeepEqual(left, right)
}

// appendOne appends one record at expectedHead and fails on any error.
func appendOne(t *testing.T, events store.EventStore, tenant string, expectedHead, seq uint64) {
	t.Helper()
	if _, err := events.Append(t.Context(), tenant, []store.Record{conformanceRecord(seq, seq)}, expectedHead); err != nil {
		t.Fatalf("append #%d: %v", seq, err)
	}
}

// conformanceRecord is one valid record for the store contract.
func conformanceRecord(seq, n uint64) store.Record {
	return store.Record{
		Seq:     seq,
		Type:    "conformance.event",
		Channel: "work",
		Payload: []byte(fmt.Sprintf(`{"n":%d}`, n)),
	}
}

// cursorIsAfter compares two opaque refs, for a test that only needs order.
func cursorIsAfter(after, before string) bool {
	left, errLeft := strconv.ParseUint(after, 10, 64)
	right, errRight := strconv.ParseUint(before, 10, 64)
	return errLeft == nil && errRight == nil && left > right
}

// clock is a hand-moved board clock, so leases expire without waiting.
type clock struct {
	nanos atomic.Int64
}

// newClock starts the suite's clock at a fixed instant.
func newClock() *clock {
	c := &clock{}
	c.nanos.Store(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())
	return c
}

// Now implements log.Clock.
func (c *clock) Now() time.Time { return time.Unix(0, c.nanos.Load()).UTC() }

// tenantSeq keeps generated tenant names unique across subtests.
var tenantSeq atomic.Uint64

// tenantName returns a fresh tenant name a store keeps to itself.
func tenantName(t *testing.T) string {
	t.Helper()
	base := strings.ToLower(t.Name())
	var built strings.Builder
	for _, c := range base {
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' {
			built.WriteRune(c)
		} else {
			built.WriteByte('_')
		}
	}
	name := built.String()
	if len(name) > 40 {
		name = name[len(name)-40:]
	}
	return fmt.Sprintf("%s_%d_%d", name, time.Now().UnixNano(), tenantSeq.Add(1))
}
