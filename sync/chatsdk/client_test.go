package chatsdk_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/chatsdk"
)

func fastRetry(attempts int) boardsync.RetryPolicy {
	return boardsync.RetryPolicy{MaxAttempts: attempts}
}

func newClient(t *testing.T, baseURL string, mutate func(*chatsdk.ClientConfig)) *chatsdk.Client {
	t.Helper()
	cfg := chatsdk.ClientConfig{
		BaseURL:  baseURL,
		Secret:   bridgeSecret,
		Key:      "telegram",
		Platform: "telegram",
		SelfIDs:  []string{"telegram:user:bot"},
		Retry:    fastRetry(3),
	}
	if mutate != nil {
		mutate(&cfg)
	}
	client, err := chatsdk.NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return client
}

func signedSidecar(t *testing.T, handle func(r *http.Request, body []byte) (int, any)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read", http.StatusInternalServerError)
			return
		}
		if err := chatsdk.VerifyRequestV1(bridgeSecret, r.Header, body, time.Now(), 0); err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		status, answer := handle(r, body)
		if status == 0 {
			status = http.StatusOK
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if answer != nil {
			_ = json.NewEncoder(w).Encode(answer)
		}
	}))
}

func TestNewClientValidation(t *testing.T) {
	cases := []chatsdk.ClientConfig{
		{BaseURL: "", Secret: bridgeSecret, Key: "telegram"},
		{BaseURL: "ftp://bridge.example", Secret: bridgeSecret, Key: "telegram"},
		{BaseURL: "http://bridge.example/telegram", Secret: bridgeSecret, Key: "telegram"},
		{BaseURL: "https://bridge.example/?x=1", Secret: bridgeSecret, Key: "telegram"},
		{BaseURL: "https://bridge.example/#frag", Secret: bridgeSecret, Key: "telegram"},
		{BaseURL: "https://user:pass@bridge.example", Secret: bridgeSecret, Key: "telegram"},
		{BaseURL: "https://bridge.example", Secret: "", Key: "telegram"},
		{BaseURL: "https://bridge.example", Secret: bridgeSecret, Key: "Telegram!"},
	}
	for i, cfg := range cases {
		if _, err := chatsdk.NewClient(cfg); err == nil {
			t.Fatalf("case %d accepted %+v", i, cfg)
		}
	}
	client, err := chatsdk.NewClient(chatsdk.ClientConfig{BaseURL: "http://127.0.0.1:9", Secret: bridgeSecret, Key: "telegram"})
	if err != nil || client == nil {
		t.Fatalf("loopback http = %v", err)
	}
}

func TestClientDeliverRetriesTransientWithFreshSignature(t *testing.T) {
	var attempts atomic.Int64
	var mu sync.Mutex
	var nonces []string
	var deliveries []string
	server := signedSidecar(t, func(r *http.Request, body []byte) (int, any) {
		n := attempts.Add(1)
		var request chatsdk.DeliverRequest
		if err := json.Unmarshal(body, &request); err != nil {
			t.Fatalf("deliver body: %v", err)
		}
		mu.Lock()
		nonces = append(nonces, r.Header.Get(chatsdk.HeaderNonce))
		deliveries = append(deliveries, request.DeliveryID)
		mu.Unlock()
		if n < 3 {
			return http.StatusServiceUnavailable, chatsdk.BridgeError{Code: chatsdk.ErrorTransient}
		}
		return http.StatusOK, boardsync.Posted{Thread: "-100:42", Message: "43"}
	})
	t.Cleanup(server.Close)
	client := newClient(t, server.URL, nil)
	target := boardsync.ResolvedTarget{Key: "telegram", Platform: "telegram", Channel: "-100", Thread: "-100:42", Kind: boardsync.TargetThread}
	posted, err := client.Deliver(context.Background(), chatsdk.DeliverRequest{
		DeliveryID: "delivery:1",
		Target:     target,
		Kind:       chatsdk.DeliverMessage,
		Body:       chatsdk.OutboundBody{Markdown: "ship it"},
	})
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if posted.Thread != "-100:42" || posted.Message != "43" {
		t.Fatalf("posted = %+v", posted)
	}
	if attempts.Load() != 3 {
		t.Fatalf("attempts = %d, want 3", attempts.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(deliveries) != 3 {
		t.Fatalf("deliveries = %v", deliveries)
	}
	for _, id := range deliveries {
		if id != "delivery:1" {
			t.Fatalf("retry changed the delivery ID: %v", deliveries)
		}
	}
	seen := make(map[string]struct{}, len(nonces))
	for _, nonce := range nonces {
		if nonce == "" {
			t.Fatal("an attempt carried no nonce")
		}
		if _, duplicate := seen[nonce]; duplicate {
			t.Fatalf("retry reused a nonce: %v", nonces)
		}
		seen[nonce] = struct{}{}
	}
}

func TestClientDeliverRefusesPermanentAndAuth(t *testing.T) {
	cases := []struct {
		status   int
		answer   any
		attempts int64
	}{
		{status: http.StatusUnprocessableEntity, answer: chatsdk.BridgeError{Code: chatsdk.ErrorPermanent}, attempts: 1},
		{status: http.StatusUnauthorized, answer: chatsdk.BridgeError{Code: chatsdk.ErrorAuth}, attempts: 1},
		{status: http.StatusUnprocessableEntity, answer: chatsdk.BridgeError{Code: chatsdk.ErrorUnsupported}, attempts: 1},
	}
	for _, tc := range cases {
		var attempts atomic.Int64
		server := signedSidecar(t, func(*http.Request, []byte) (int, any) {
			attempts.Add(1)
			return tc.status, tc.answer
		})
		client := newClient(t, server.URL, nil)
		_, err := client.Deliver(context.Background(), chatsdk.DeliverRequest{
			DeliveryID: "delivery:1",
			Target:     boardsync.ResolvedTarget{Key: "telegram", Platform: "telegram", Channel: "-100", Kind: boardsync.TargetChannel},
			Kind:       chatsdk.DeliverMessage,
			Body:       chatsdk.OutboundBody{Markdown: "hi"},
		})
		server.Close()
		if err == nil || !boardsync.IsPermanent(err) {
			t.Fatalf("status %d err = %v, want permanent", tc.status, err)
		}
		if attempts.Load() != tc.attempts {
			t.Fatalf("status %d attempts = %d, want %d", tc.status, attempts.Load(), tc.attempts)
		}
	}
}

func TestClientResolve(t *testing.T) {
	server := signedSidecar(t, func(_ *http.Request, body []byte) (int, any) {
		var request chatsdk.ResolveRequest
		if err := json.Unmarshal(body, &request); err != nil {
			t.Fatalf("resolve body: %v", err)
		}
		if request.Name == "#support" {
			return http.StatusOK, chatsdk.ResolveResponse{Channel: "-100"}
		}
		return http.StatusOK, chatsdk.ResolveResponse{Target: &boardsync.ResolvedTarget{
			Key: "telegram", Platform: "telegram", Channel: "-100", Thread: "-100:42", Kind: boardsync.TargetThread,
		}}
	})
	t.Cleanup(server.Close)
	client := newClient(t, server.URL, nil)
	channel, err := client.Resolve(context.Background(), chatsdk.ResolveRequest{Name: "#support"})
	if err != nil || channel.Channel != "-100" {
		t.Fatalf("resolve name = %+v, %v", channel, err)
	}
	target, err := client.Resolve(context.Background(), chatsdk.ResolveRequest{Target: "-100:42"})
	if err != nil || target.Target == nil || target.Target.Thread != "-100:42" {
		t.Fatalf("resolve target = %+v, %v", target, err)
	}
}

func TestClientRefusesRedirect(t *testing.T) {
	var leaked atomic.Bool
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		leaked.Store(true)
	}))
	t.Cleanup(other.Close)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+"/deliver", http.StatusTemporaryRedirect)
	}))
	t.Cleanup(server.Close)
	client := newClient(t, server.URL, nil)
	_, err := client.Deliver(context.Background(), chatsdk.DeliverRequest{
		DeliveryID: "delivery:1",
		Target:     boardsync.ResolvedTarget{Key: "telegram", Platform: "telegram", Channel: "-100", Kind: boardsync.TargetChannel},
		Kind:       chatsdk.DeliverMessage,
		Body:       chatsdk.OutboundBody{Markdown: "hi"},
	})
	if err == nil {
		t.Fatal("redirect was accepted")
	}
	if leaked.Load() {
		t.Fatal("redirect left the configured origin")
	}
}

func TestClientTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
	}))
	t.Cleanup(server.Close)
	client := newClient(t, server.URL, func(c *chatsdk.ClientConfig) {
		c.Timeout = 20 * time.Millisecond
		c.Retry = fastRetry(1)
	})
	_, err := client.Resolve(context.Background(), chatsdk.ResolveRequest{Name: "#support"})
	if err == nil {
		t.Fatal("timeout was accepted")
	}
}

func TestRemoteAdapterOutboundShapes(t *testing.T) {
	var requests []chatsdk.DeliverRequest
	server := signedSidecar(t, func(_ *http.Request, body []byte) (int, any) {
		var request chatsdk.DeliverRequest
		if err := json.Unmarshal(body, &request); err != nil {
			t.Fatalf("deliver body: %v", err)
		}
		requests = append(requests, request)
		return http.StatusOK, boardsync.Posted{Thread: "-100:42", Message: "43"}
	})
	t.Cleanup(server.Close)
	client := newClient(t, server.URL, nil)
	adapter, err := chatsdk.NewRemoteAdapter(client, "telegram", boardsync.Capabilities{
		StartThreads: true, Edits: true, Deletes: true, Reactions: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	target := boardsync.ResolvedTarget{Key: "telegram", Platform: "telegram", Channel: "-100", Thread: "-100:42", Kind: boardsync.TargetThread}
	ctx := context.Background()
	if _, err := adapter.Send(ctx, "telegram", target, boardsync.OutboundMessage{Markdown: "hello", BoardMessage: 9, BoardThread: 9}); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Edit(ctx, "telegram", target.Ref(), "43", boardsync.OutboundMessage{Markdown: "fixed"}); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Delete(ctx, "telegram", target.Ref(), "43"); err != nil {
		t.Fatal(err)
	}
	if err := adapter.React(ctx, "telegram", target.Ref(), "43", "thumbsup", true); err != nil {
		t.Fatal(err)
	}
	if len(requests) != 4 {
		t.Fatalf("requests = %d", len(requests))
	}
	if requests[0].Kind != chatsdk.DeliverMessage || requests[0].Body.Markdown != "hello" || requests[0].Body.BoardMessage != 9 {
		t.Fatalf("message = %+v", requests[0])
	}
	if requests[1].Kind != chatsdk.DeliverEdit || requests[1].Message != "43" || requests[1].Body.Markdown != "fixed" {
		t.Fatalf("edit = %+v", requests[1])
	}
	if requests[2].Kind != chatsdk.DeliverDelete || requests[2].Message != "43" {
		t.Fatalf("delete = %+v", requests[2])
	}
	if requests[3].Kind != chatsdk.DeliverReaction || requests[3].Emoji != "thumbsup" || !requests[3].Added {
		t.Fatalf("reaction = %+v", requests[3])
	}
	for i := 1; i < len(requests); i++ {
		if requests[i].DeliveryID == requests[0].DeliveryID {
			t.Fatal("distinct deliveries shared one ID")
		}
	}

	// A minimal adapter refuses changes it doesn't claim.
	minimal, err := chatsdk.NewRemoteAdapter(client, "telegram", boardsync.Capabilities{})
	if err != nil {
		t.Fatal(err)
	}
	if err := minimal.Edit(ctx, "telegram", target.Ref(), "43", boardsync.OutboundMessage{Markdown: "x"}); !errors.Is(err, boardsync.ErrUnsupported) {
		t.Fatalf("minimal edit = %v", err)
	}
	if err := minimal.Delete(ctx, "telegram", target.Ref(), "43"); !errors.Is(err, boardsync.ErrUnsupported) {
		t.Fatalf("minimal delete = %v", err)
	}
	if err := minimal.React(ctx, "telegram", target.Ref(), "43", "x", true); !errors.Is(err, boardsync.ErrUnsupported) {
		t.Fatalf("minimal react = %v", err)
	}
	if _, err := minimal.Send(ctx, "telegram", target, boardsync.OutboundMessage{Markdown: "x", Files: []boardsync.OutboundFile{{Filename: "a"}}}); !errors.Is(err, boardsync.ErrUnsupported) {
		t.Fatalf("file send = %v", err)
	}
}

func TestRemoteAdapterResolveAndRawWebhookRefusal(t *testing.T) {
	server := signedSidecar(t, func(_ *http.Request, body []byte) (int, any) {
		var request chatsdk.ResolveRequest
		if err := json.Unmarshal(body, &request); err != nil {
			t.Fatal(err)
		}
		if request.Name != "" {
			return http.StatusOK, chatsdk.ResolveResponse{Channel: "-100"}
		}
		return http.StatusOK, chatsdk.ResolveResponse{Target: &boardsync.ResolvedTarget{
			Channel: "-100", Thread: "-100:42", Kind: boardsync.TargetThread,
		}}
	})
	t.Cleanup(server.Close)
	client := newClient(t, server.URL, nil)
	adapter, err := chatsdk.NewRemoteAdapter(client, "telegram", boardsync.Capabilities{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	channel, err := adapter.ResolveChannel(ctx, "telegram", "#support")
	if err != nil || channel != "-100" {
		t.Fatalf("channel = %q, %v", channel, err)
	}
	target, err := adapter.ResolveTarget(ctx, "telegram", "-100:42")
	if err != nil || target.Thread != "-100:42" {
		t.Fatalf("target = %+v, %v", target, err)
	}
	if _, err := adapter.ResolveTarget(ctx, "other", "-100:42"); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("foreign key = %v", err)
	}
	if err := adapter.Verify(ctx, &boardsync.WebhookRequest{Method: http.MethodPost, Body: []byte(`{}`), Header: http.Header{"X": {"y"}}}); !errors.Is(err, boardsync.ErrUnsupported) {
		t.Fatalf("raw verify = %v", err)
	}
	if _, err := adapter.Decode(ctx, "telegram", &boardsync.WebhookRequest{}); !errors.Is(err, boardsync.ErrUnsupported) {
		t.Fatalf("raw decode = %v", err)
	}
}

func TestRemoteAdapterDeliveryIDIsStable(t *testing.T) {
	var ids []string
	server := signedSidecar(t, func(_ *http.Request, body []byte) (int, any) {
		var request chatsdk.DeliverRequest
		if err := json.Unmarshal(body, &request); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, request.DeliveryID)
		return http.StatusOK, boardsync.Posted{Thread: "-100:42", Message: "43"}
	})
	t.Cleanup(server.Close)
	client := newClient(t, server.URL, nil)
	adapter, err := chatsdk.NewRemoteAdapter(client, "telegram", boardsync.Capabilities{Edits: true})
	if err != nil {
		t.Fatal(err)
	}
	target := boardsync.ResolvedTarget{Key: "telegram", Platform: "telegram", Channel: "-100", Thread: "-100:42", Kind: boardsync.TargetThread}
	ctx := context.Background()
	message := boardsync.OutboundMessage{Markdown: "same", BoardMessage: 9}
	if _, err := adapter.Send(ctx, "telegram", target, message); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.Send(ctx, "telegram", target, message); err != nil {
		t.Fatal(err)
	}
	message.Markdown = "changed"
	if _, err := adapter.Send(ctx, "telegram", target, message); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 3 {
		t.Fatalf("ids = %v", ids)
	}
	if ids[0] != ids[1] {
		t.Fatalf("retry IDs differ: %q / %q", ids[0], ids[1])
	}
	if ids[2] == ids[0] || strings.TrimSpace(ids[2]) == "" {
		t.Fatalf("changed content reused an ID: %v", ids)
	}
}

func TestClientResolveFillsMinimalTarget(t *testing.T) {
	server := signedSidecar(t, func(_ *http.Request, body []byte) (int, any) {
		var request chatsdk.ResolveRequest
		if err := json.Unmarshal(body, &request); err != nil {
			t.Fatal(err)
		}
		return http.StatusOK, chatsdk.ResolveResponse{Target: &boardsync.ResolvedTarget{
			Channel: "-100", Thread: "-100:42", Kind: boardsync.TargetThread,
		}}
	})
	t.Cleanup(server.Close)
	client := newClient(t, server.URL, nil)
	response, err := client.Resolve(context.Background(), chatsdk.ResolveRequest{Target: "-100:42"})
	if err != nil {
		t.Fatalf("minimal target: %v", err)
	}
	if response.Target.Key != "telegram" || response.Target.Platform != "telegram" {
		t.Fatalf("target wasn't completed: %+v", response.Target)
	}
	if err := response.Target.Validate(); err != nil {
		t.Fatalf("target invalid: %v", err)
	}
}

func TestClientClassifiesLargeErrorBeforeSize(t *testing.T) {
	var attempts atomic.Int64
	server := signedSidecar(t, func(*http.Request, []byte) (int, any) {
		attempts.Add(1)
		return http.StatusServiceUnavailable, strings.Repeat("x", chatsdk.MaxResponseBytes+1)
	})
	t.Cleanup(server.Close)
	client := newClient(t, server.URL, func(c *chatsdk.ClientConfig) { c.Retry = fastRetry(2) })
	_, err := client.Resolve(context.Background(), chatsdk.ResolveRequest{Name: "#support"})
	if err == nil {
		t.Fatal("large 503 was accepted")
	}
	if boardsync.IsPermanent(err) {
		t.Fatalf("a large 503 became permanent: %v", err)
	}
	var failure *chatsdk.BridgeError
	if !errors.As(err, &failure) || failure.Code != chatsdk.ErrorTransient {
		t.Fatalf("error = %v", err)
	}
	if attempts.Load() != 2 {
		t.Fatalf("attempts = %d, want 2", attempts.Load())
	}
}

func TestClientOversizedSuccessIsPermanent(t *testing.T) {
	var attempts atomic.Int64
	server := signedSidecar(t, func(*http.Request, []byte) (int, any) {
		attempts.Add(1)
		return http.StatusOK, strings.Repeat("x", chatsdk.MaxResponseBytes+1)
	})
	t.Cleanup(server.Close)
	client := newClient(t, server.URL, nil)
	_, err := client.Resolve(context.Background(), chatsdk.ResolveRequest{Name: "#support"})
	if err == nil || !boardsync.IsPermanent(err) {
		t.Fatalf("oversized success = %v, want permanent", err)
	}
	if attempts.Load() != 1 {
		t.Fatalf("attempts = %d, want 1", attempts.Load())
	}
}

func TestClientCustomHTTPClientStillRefusesRedirect(t *testing.T) {
	var leaked atomic.Bool
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		leaked.Store(true)
	}))
	t.Cleanup(other.Close)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+"/deliver", http.StatusTemporaryRedirect)
	}))
	t.Cleanup(server.Close)
	client := newClient(t, server.URL, func(c *chatsdk.ClientConfig) {
		c.HTTPClient = &http.Client{} // a caller that would otherwise follow
	})
	_, err := client.Resolve(context.Background(), chatsdk.ResolveRequest{Name: "#support"})
	if err == nil {
		t.Fatal("redirect was accepted")
	}
	if leaked.Load() {
		t.Fatal("redirect left the configured origin through a custom client")
	}
}

func TestRemoteAdapterDeliveryIsSingleAttempt(t *testing.T) {
	var attempts atomic.Int64
	server := signedSidecar(t, func(*http.Request, []byte) (int, any) {
		attempts.Add(1)
		return http.StatusServiceUnavailable, chatsdk.BridgeError{Code: chatsdk.ErrorTransient}
	})
	t.Cleanup(server.Close)
	client := newClient(t, server.URL, func(c *chatsdk.ClientConfig) { c.Retry = fastRetry(5) })
	adapter, err := chatsdk.NewRemoteAdapter(client, "telegram", boardsync.Capabilities{})
	if err != nil {
		t.Fatal(err)
	}
	target := boardsync.ResolvedTarget{Key: "telegram", Platform: "telegram", Channel: "-100", Thread: "-100:42", Kind: boardsync.TargetThread}
	if _, err := adapter.Send(context.Background(), "telegram", target, boardsync.OutboundMessage{Markdown: "hi"}); err == nil {
		t.Fatal("503 was accepted")
	}
	if attempts.Load() != 1 {
		t.Fatalf("outbound short-circuit made %d attempts, want one", attempts.Load())
	}
}

func TestClientRejectsCrossKeyOversizedRequestAndInvalidPosted(t *testing.T) {
	var attempts atomic.Int64
	server := signedSidecar(t, func(*http.Request, []byte) (int, any) {
		attempts.Add(1)
		return http.StatusOK, boardsync.Posted{}
	})
	t.Cleanup(server.Close)
	client := newClient(t, server.URL, nil)
	target := boardsync.ResolvedTarget{Key: "telegram", Platform: "telegram", Channel: "-100", Thread: "-100:42", Kind: boardsync.TargetThread}
	if _, err := client.Deliver(context.Background(), chatsdk.DeliverRequest{
		DeliveryID: "d1", Key: "other", Target: boardsync.ResolvedTarget{
			Key: "other", Platform: "telegram", Channel: "-100", Thread: "-100:42", Kind: boardsync.TargetThread,
		}, Kind: chatsdk.DeliverMessage, Body: chatsdk.OutboundBody{Markdown: "hi"},
	}); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("cross-key = %v", err)
	}
	if _, err := client.Deliver(context.Background(), chatsdk.DeliverRequest{
		DeliveryID: "d1", Key: "telegram", Target: target, Kind: chatsdk.DeliverMessage,
		Body: chatsdk.OutboundBody{Markdown: strings.Repeat("x", boardsync.MaxTextBytes+1)},
	}); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("oversized request = %v", err)
	}
	if attempts.Load() != 0 {
		t.Fatalf("refused requests reached the sidecar: %d", attempts.Load())
	}
	if _, err := client.Deliver(context.Background(), chatsdk.DeliverRequest{
		DeliveryID: "d1", Key: "telegram", Target: target, Kind: chatsdk.DeliverMessage,
		Body: chatsdk.OutboundBody{Markdown: "hi"},
	}); err == nil || !boardsync.IsPermanent(err) {
		t.Fatalf("invalid posted = %v, want permanent", err)
	}
	if attempts.Load() != 1 {
		t.Fatalf("invalid posted attempts = %d", attempts.Load())
	}
}
