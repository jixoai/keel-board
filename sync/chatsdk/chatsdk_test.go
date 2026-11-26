package chatsdk_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/chatsdk"
)

const testSecret = "generic-webhook-secret"

func signedRequest(t *testing.T, body []byte) *boardsync.WebhookRequest {
	t.Helper()
	header := http.Header{}
	header.Set("X-Webhook-Signature", "sha256="+sign(body))
	header.Set("Content-Type", "application/json")
	return &boardsync.WebhookRequest{Method: http.MethodPost, Path: "/webhooks/telegram", Header: header, Body: body}
}

func sign(body []byte) string {
	mac := hmac.New(sha256.New, []byte(testSecret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func TestHMACVerifier(t *testing.T) {
	body := []byte(`{"update_id":1}`)
	verifier := chatsdk.HMACVerifier{Secret: testSecret}
	if err := verifier.Verify(context.Background(), signedRequest(t, body)); err != nil {
		t.Fatalf("valid: %v", err)
	}
	bad := signedRequest(t, body)
	bad.Header.Set("X-Webhook-Signature", "sha256=deadbeef")
	if err := verifier.Verify(context.Background(), bad); !errors.Is(err, boardsync.ErrUnauthorized) {
		t.Fatalf("bad signature = %v", err)
	}
	if err := (chatsdk.HMACVerifier{}).Verify(context.Background(), signedRequest(t, body)); !errors.Is(err, boardsync.ErrUnauthorized) {
		t.Fatalf("missing secret = %v", err)
	}
	timestamped := chatsdk.HMACVerifier{
		Secret: testSecret, TimestampHeader: "X-Webhook-Timestamp",
		Now: func() time.Time { return time.Unix(1727000400, 0) },
	}
	request := signedRequest(t, body)
	request.Header.Set("X-Webhook-Timestamp", "1726990000")
	if err := timestamped.Verify(context.Background(), request); !errors.Is(err, boardsync.ErrUnauthorized) {
		t.Fatalf("stale timestamp = %v", err)
	}
}

func TestProviderReceiveAndSend(t *testing.T) {
	adapter := &chatsdk.FuncAdapter{
		PlatformName: "telegram",
		VerifyFunc: func(_ context.Context, req *boardsync.WebhookRequest) error {
			return (chatsdk.HMACVerifier{Secret: testSecret}).Verify(context.Background(), req)
		},
		DecodeFunc: func(_ context.Context, key boardsync.Key, _ *boardsync.WebhookRequest) (chatsdk.DecodeResult, error) {
			event := boardsync.Event{
				Key: key, Platform: "telegram",
				Kind: boardsync.KindMessage, Type: boardsync.TypeMessage,
				Channel: "-1001234567890", Thread: "-1001234567890:42",
				Message: "42", Text: "hello", IsRoot: true,
				Author: boardsync.Author{ID: "telegram:user:7", Handle: "ada", Name: "Ada"},
			}
			event.ID = boardsync.MessageDedupe(key, event.Thread, event.Message)
			return chatsdk.DecodeResult{
				Response: boardsync.OK(),
				Records:  []boardsync.Record{{Dedupe: event.ID, Event: event}},
			}, nil
		},
		SendFunc: func(_ context.Context, key boardsync.Key, target boardsync.ResolvedTarget, msg boardsync.OutboundMessage) (boardsync.Posted, error) {
			if key != "telegram" || target.Channel != "-1001234567890" || msg.Markdown != "hi" {
				t.Fatalf("send = %s %+v %+v", key, target, msg)
			}
			return boardsync.Posted{Thread: "-1001234567890:99", Message: "99"}, nil
		},
		TargetFunc: func(_ context.Context, key boardsync.Key, target string) (boardsync.ResolvedTarget, error) {
			return boardsync.ResolvedTarget{Key: key, Platform: "telegram", Channel: "-1001234567890", Thread: target, Kind: boardsync.TargetThread}, nil
		},
	}
	p, err := chatsdk.New(chatsdk.Config{Key: "telegram", Adapter: adapter, Caps: boardsync.Capabilities{StartThreads: false}})
	if err != nil {
		t.Fatal(err)
	}
	intake, err := p.Receive(context.Background(), "telegram", signedRequest(t, []byte(`{"update_id":1}`)))
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if len(intake.Records) != 1 {
		t.Fatalf("records = %d", len(intake.Records))
	}
	event := intake.Records[0].Event
	if event.Key != "telegram" || event.Platform != "telegram" || event.ID == "" {
		t.Fatalf("event = %+v", event)
	}
	target, err := p.ResolveTarget(context.Background(), "telegram", "-1001234567890:1")
	if err != nil || target.Channel != "-1001234567890" {
		t.Fatalf("ResolveTarget = %+v, %v", target, err)
	}
	posted, err := p.Post(context.Background(), "telegram", target, boardsync.OutboundMessage{Markdown: "hi"})
	if err != nil || posted.Message != "99" {
		t.Fatalf("Post = %+v, %v", posted, err)
	}
	if p.Capabilities().StartThreads {
		t.Fatal("StartThreads should follow the config")
	}
}

func TestProviderRefusesBadSignature(t *testing.T) {
	adapter := &chatsdk.FuncAdapter{
		PlatformName: "telegram",
		VerifyFunc: func(_ context.Context, req *boardsync.WebhookRequest) error {
			return (chatsdk.HMACVerifier{Secret: testSecret}).Verify(context.Background(), req)
		},
	}
	p, err := chatsdk.New(chatsdk.Config{Key: "telegram", Adapter: adapter})
	if err != nil {
		t.Fatal(err)
	}
	request := signedRequest(t, []byte(`{}`))
	request.Header.Set("X-Webhook-Signature", "sha256=bad")
	if _, err := p.Receive(context.Background(), "telegram", request); !errors.Is(err, boardsync.ErrUnauthorized) {
		t.Fatalf("Receive = %v", err)
	}
}

func TestProviderOptionalOps(t *testing.T) {
	var calls []string
	adapter := &chatsdk.FuncAdapter{
		PlatformName: "discord",
		VerifyFunc:   func(context.Context, *boardsync.WebhookRequest) error { return nil },
		EditFunc: func(context.Context, boardsync.Key, boardsync.ThreadRef, string, boardsync.OutboundMessage) error {
			calls = append(calls, "edit")
			return nil
		},
		DeleteFunc: func(context.Context, boardsync.Key, boardsync.ThreadRef, string) error {
			calls = append(calls, "delete")
			return nil
		},
		ReactFunc: func(context.Context, boardsync.Key, boardsync.ThreadRef, string, string, bool) error {
			calls = append(calls, "react")
			return nil
		},
	}
	p, err := chatsdk.New(chatsdk.Config{Key: "discord", Adapter: adapter})
	if err != nil {
		t.Fatal(err)
	}
	thread := boardsync.ThreadRef{Key: "discord", Platform: "discord", Channel: "c1", Thread: "c1:t1"}
	ctx := context.Background()
	if err := p.Edit(ctx, "discord", thread, "m1", boardsync.OutboundMessage{Markdown: "x"}); err != nil {
		t.Fatal(err)
	}
	if err := p.Delete(ctx, "discord", thread, "m1"); err != nil {
		t.Fatal(err)
	}
	if err := p.React(ctx, "discord", thread, "m1", "eyes", true); err != nil {
		t.Fatal(err)
	}
	if strings.Join(calls, ",") != "edit,delete,react" {
		t.Fatalf("calls = %v", calls)
	}
}
