package sidecartest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/chatsdk"
	"github.com/jixoai/keel-board/sync/chatsdk/sidecartest"
)

type fullAdapter struct {
	mu        sync.Mutex
	sends     []boardsync.OutboundMessage
	edits     []string
	deletes   []string
	reactions []string
}

func (a *fullAdapter) Platform() string { return "telegram" }

func (a *fullAdapter) Send(_ context.Context, key boardsync.Key, target boardsync.ResolvedTarget, msg boardsync.OutboundMessage) (boardsync.Posted, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sends = append(a.sends, msg)
	return boardsync.Posted{Thread: postedThread(target), Message: "posted-1"}, nil
}

func (a *fullAdapter) Edit(_ context.Context, _ boardsync.Key, _ boardsync.ThreadRef, message string, _ boardsync.OutboundMessage) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.edits = append(a.edits, message)
	return nil
}

func (a *fullAdapter) Delete(_ context.Context, _ boardsync.Key, _ boardsync.ThreadRef, message string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.deletes = append(a.deletes, message)
	return nil
}

func (a *fullAdapter) React(_ context.Context, _ boardsync.Key, _ boardsync.ThreadRef, message, emoji string, _ bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.reactions = append(a.reactions, message+":"+emoji)
	return nil
}

func (a *fullAdapter) ResolveChannel(_ context.Context, _ boardsync.Key, name string) (string, error) {
	if name != "#support" {
		return "", errors.New("unknown channel")
	}
	return "-100", nil
}

func (a *fullAdapter) ResolveTarget(_ context.Context, _ boardsync.Key, target string) (boardsync.ResolvedTarget, error) {
	if target != "-100:42" {
		return boardsync.ResolvedTarget{}, errors.New("unknown target")
	}
	return boardsync.ResolvedTarget{Key: "telegram", Platform: "telegram", Channel: "-100", Thread: "-100:42", Kind: boardsync.TargetThread}, nil
}

func (a *fullAdapter) counts() (sends, edits, deletes, reactions int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.sends), len(a.edits), len(a.deletes), len(a.reactions)
}

type sendOnlyAdapter struct{}

func (sendOnlyAdapter) Platform() string { return "telegram" }

func (sendOnlyAdapter) Send(_ context.Context, _ boardsync.Key, target boardsync.ResolvedTarget, _ boardsync.OutboundMessage) (boardsync.Posted, error) {
	return boardsync.Posted{Thread: postedThread(target), Message: "posted-1"}, nil
}

// postedThread models a platform that creates a thread for a channel root.
func postedThread(target boardsync.ResolvedTarget) string {
	if target.Thread != "" {
		return target.Thread
	}
	return target.Channel + ":created"
}

func newRemote(t *testing.T, sidecar *sidecartest.Sidecar, caps boardsync.Capabilities) *chatsdk.RemoteAdapter {
	t.Helper()
	client, err := chatsdk.NewClient(chatsdk.ClientConfig{
		BaseURL: sidecar.URL(), Secret: "fixture-secret", Key: "telegram",
		Platform: "telegram", SelfIDs: []string{"telegram:user:bot"},
		Retry: boardsync.RetryPolicy{MaxAttempts: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := chatsdk.NewRemoteAdapter(client, "telegram", caps)
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

func TestSidecarFullAdapterShape(t *testing.T) {
	adapter := &fullAdapter{}
	sidecar, err := sidecartest.New("telegram", "telegram", "fixture-secret", adapter)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sidecar.Close)
	remote := newRemote(t, sidecar, boardsync.Capabilities{Edits: true, Deletes: true, Reactions: true})
	ctx := context.Background()
	target := boardsync.ResolvedTarget{Key: "telegram", Platform: "telegram", Channel: "-100", Thread: "-100:42", Kind: boardsync.TargetThread}
	posted, err := remote.Send(ctx, "telegram", target, boardsync.OutboundMessage{Markdown: "hello"})
	if err != nil || posted.Message != "posted-1" {
		t.Fatalf("send = %+v, %v", posted, err)
	}
	if err := remote.Edit(ctx, "telegram", target.Ref(), "posted-1", boardsync.OutboundMessage{Markdown: "fixed"}); err != nil {
		t.Fatal(err)
	}
	if err := remote.Delete(ctx, "telegram", target.Ref(), "posted-1"); err != nil {
		t.Fatal(err)
	}
	if err := remote.React(ctx, "telegram", target.Ref(), "posted-1", "thumbsup", true); err != nil {
		t.Fatal(err)
	}
	sends, edits, deletes, reactions := adapter.counts()
	if sends != 1 || edits != 1 || deletes != 1 || reactions != 1 {
		t.Fatalf("adapter calls = %d/%d/%d/%d", sends, edits, deletes, reactions)
	}
	if len(sidecar.Deliveries()) != 4 {
		t.Fatalf("deliveries = %d", len(sidecar.Deliveries()))
	}
	channel, err := remote.ResolveChannel(ctx, "telegram", "#support")
	if err != nil || channel != "-100" {
		t.Fatalf("resolve channel = %q, %v", channel, err)
	}
	resolved, err := remote.ResolveTarget(ctx, "telegram", "-100:42")
	if err != nil || resolved.Thread != "-100:42" {
		t.Fatalf("resolve target = %+v, %v", resolved, err)
	}
	if len(sidecar.Resolves()) != 2 {
		t.Fatalf("resolves = %d", len(sidecar.Resolves()))
	}
}

func TestSidecarChannelRootReturnsTheNewThread(t *testing.T) {
	adapter := &fullAdapter{}
	sidecar, err := sidecartest.New("telegram", "telegram", "fixture-secret", adapter)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sidecar.Close)
	remote := newRemote(t, sidecar, boardsync.Capabilities{StartThreads: true})
	channel := boardsync.ResolvedTarget{Key: "telegram", Platform: "telegram", Channel: "-100", Kind: boardsync.TargetChannel}
	posted, err := remote.Send(context.Background(), "telegram", channel, boardsync.OutboundMessage{Markdown: "root"})
	if err != nil {
		t.Fatalf("channel root: %v", err)
	}
	if posted.Thread != "-100:created" || posted.Message != "posted-1" {
		t.Fatalf("posted = %+v", posted)
	}
	thread := boardsync.ResolvedTarget{
		Key: "telegram", Platform: "telegram", Channel: "-100",
		Thread: posted.Thread, Kind: boardsync.TargetThread,
	}
	reply, err := remote.Send(context.Background(), "telegram", thread, boardsync.OutboundMessage{Markdown: "reply"})
	if err != nil || reply.Thread != "-100:created" {
		t.Fatalf("reply = %+v, %v", reply, err)
	}
}

func TestSidecarIdempotentDelivery(t *testing.T) {
	adapter := &fullAdapter{}
	sidecar, err := sidecartest.New("telegram", "telegram", "fixture-secret", adapter)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sidecar.Close)
	client, err := chatsdk.NewClient(chatsdk.ClientConfig{
		BaseURL: sidecar.URL(), Secret: "fixture-secret", Key: "telegram",
		Platform: "telegram", SelfIDs: []string{"telegram:user:bot"},
		Retry: boardsync.RetryPolicy{MaxAttempts: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	target := boardsync.ResolvedTarget{Key: "telegram", Platform: "telegram", Channel: "-100", Thread: "-100:42", Kind: boardsync.TargetThread}
	request := chatsdk.DeliverRequest{
		DeliveryID: "delivery:fixture:1", Key: "telegram", Target: target,
		Kind: chatsdk.DeliverMessage, Body: chatsdk.OutboundBody{Markdown: "hello"},
	}
	first, err := client.Deliver(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := client.Deliver(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Thread != second.Thread || first.Message != second.Message || first.Message != "posted-1" {
		t.Fatalf("retry = %+v / %+v", second, first)
	}
	if sends, _, _, _ := adapter.counts(); sends != 1 {
		t.Fatalf("adapter was called %d times for one delivery ID", sends)
	}
}

func TestSidecarMinimalAdapterShape(t *testing.T) {
	sidecar, err := sidecartest.New("telegram", "telegram", "fixture-secret", sendOnlyAdapter{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sidecar.Close)
	remote := newRemote(t, sidecar, boardsync.Capabilities{Edits: true})
	ctx := context.Background()
	target := boardsync.ResolvedTarget{Key: "telegram", Platform: "telegram", Channel: "-100", Thread: "-100:42", Kind: boardsync.TargetThread}
	if _, err := remote.Send(ctx, "telegram", target, boardsync.OutboundMessage{Markdown: "hello"}); err != nil {
		t.Fatal(err)
	}
	if err := remote.Edit(ctx, "telegram", target.Ref(), "m1", boardsync.OutboundMessage{Markdown: "fixed"}); !errors.Is(err, boardsync.ErrUnsupported) {
		t.Fatalf("minimal edit = %v", err)
	}
	if _, err := remote.ResolveTarget(ctx, "telegram", "-100:42"); !errors.Is(err, boardsync.ErrUnsupported) {
		t.Fatalf("minimal resolve = %v", err)
	}
}

func TestSidecarInboundPush(t *testing.T) {
	sidecar, err := sidecartest.New("telegram", "telegram", "fixture-secret", sendOnlyAdapter{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sidecar.Close)
	var got []boardsync.Record
	board := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/board/sync/bridge/telegram/events" {
			http.NotFound(w, r)
			return
		}
		body, _ := io.ReadAll(r.Body)
		if err := chatsdk.VerifyRequestV1("fixture-secret", r.Header, body, time.Now(), 0); err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		records, err := chatsdk.DecodeInbound(body, "telegram", "telegram")
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		got = records
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]int{"stored": len(records)})
	}))
	t.Cleanup(board.Close)
	event := chatsdk.InboundEvent{
		Dedupe: "m1",
		Event: boardsync.Event{
			Kind: boardsync.KindMessage, Type: boardsync.TypeMessage,
			Channel: "-100", Thread: "-100:42", Message: "42", Text: "hello",
			Author: boardsync.Author{ID: "telegram:user:7", Name: "Ada"},
		},
	}
	result, err := sidecar.PushInbound(context.Background(), board.URL, []chatsdk.InboundEvent{event})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != http.StatusOK {
		t.Fatalf("status = %d body %s", result.Status, result.Body)
	}
	if len(got) != 1 || got[0].Dedupe != "m1" || got[0].Event.Key != "telegram" {
		t.Fatalf("records = %+v", got)
	}
}

func TestSidecarRejectsNonceReplay(t *testing.T) {
	sidecar, err := sidecartest.New("telegram", "telegram", "fixture-secret", sendOnlyAdapter{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sidecar.Close)
	body := []byte(`{"deliveryId":"d1","key":"telegram","target":{"key":"telegram","platform":"telegram","channel":"-100","thread":"-100:42","kind":"thread"},"kind":"message","body":{"markdown":"hi"}}`)
	nonce := "fixture-nonce-1"
	request := func() *http.Request {
		request, err := http.NewRequest(http.MethodPost, sidecar.URL()+"/deliver", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		timestamp := time.Now().Unix()
		request.Header.Set(chatsdk.HeaderTimestamp, fmt.Sprintf("%d", timestamp))
		request.Header.Set(chatsdk.HeaderNonce, nonce)
		request.Header.Set(chatsdk.HeaderSignature, chatsdk.SignV1("fixture-secret", timestamp, nonce, body))
		return request
	}
	for i, want := range []int{http.StatusOK, http.StatusConflict} {
		response, err := http.DefaultClient.Do(request())
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != want {
			t.Fatalf("attempt %d status = %d, want %d", i, response.StatusCode, want)
		}
	}
}

func TestSidecarRefusesBadSignature(t *testing.T) {
	sidecar, err := sidecartest.New("telegram", "telegram", "fixture-secret", sendOnlyAdapter{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sidecar.Close)
	body := []byte(`{"deliveryId":"d1","key":"telegram","target":{"key":"telegram","platform":"telegram","channel":"-100","kind":"channel"},"kind":"message","body":{"markdown":"hi"}}`)
	request, err := http.NewRequest(http.MethodPost, sidecar.URL()+"/deliver", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(chatsdk.HeaderTimestamp, "not-a-number")
	request.Header.Set(chatsdk.HeaderNonce, "nonce-0123456789")
	request.Header.Set(chatsdk.HeaderSignature, "v1=deadbeef")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d", response.StatusCode)
	}
}

func TestSidecarRejectsInvalidDelivery(t *testing.T) {
	sidecar, err := sidecartest.New("telegram", "telegram", "fixture-secret", sendOnlyAdapter{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sidecar.Close)
	body := []byte(`{"deliveryId":"d1","key":"telegram","target":{"key":"telegram","platform":"telegram","channel":"-100","thread":"-100:42","kind":"thread"},"kind":"message","body":{}}`)
	request, err := http.NewRequest(http.MethodPost, sidecar.URL()+"/deliver", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	timestamp := time.Now().Unix()
	nonce := "fixture-invalid-1"
	request.Header.Set(chatsdk.HeaderTimestamp, fmt.Sprintf("%d", timestamp))
	request.Header.Set(chatsdk.HeaderNonce, nonce)
	request.Header.Set(chatsdk.HeaderSignature, chatsdk.SignV1("fixture-secret", timestamp, nonce, body))
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("invalid delivery = %d", response.StatusCode)
	}
}
