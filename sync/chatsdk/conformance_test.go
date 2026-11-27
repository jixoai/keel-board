package chatsdk_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"testing"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/chatsdk"
	"github.com/jixoai/keel-board/sync/chatsdk/sidecartest"
	"github.com/jixoai/keel-board/sync/conformance"
)

func TestProviderConformance(t *testing.T) {
	caps := boardsync.Capabilities{StartThreads: true, Edits: true, Deletes: true, Reactions: true}
	sidecarAdapter := &chatsdk.FuncAdapter{
		PlatformName: "telegram",
		SendFunc: func(_ context.Context, _ boardsync.Key, target boardsync.ResolvedTarget, msg boardsync.OutboundMessage) (boardsync.Posted, error) {
			return boardsync.Posted{Thread: target.Thread, Message: "sidecar-" + msg.Markdown}, nil
		},
		EditFunc: func(context.Context, boardsync.Key, boardsync.ThreadRef, string, boardsync.OutboundMessage) error {
			return nil
		},
		DeleteFunc: func(context.Context, boardsync.Key, boardsync.ThreadRef, string) error { return nil },
		ReactFunc:  func(context.Context, boardsync.Key, boardsync.ThreadRef, string, string, bool) error { return nil },
		ChannelFunc: func(_ context.Context, _ boardsync.Key, name string) (string, error) {
			return "-100", nil
		},
		TargetFunc: func(_ context.Context, key boardsync.Key, _ string) (boardsync.ResolvedTarget, error) {
			return boardsync.ResolvedTarget{Key: key, Platform: "telegram", Channel: "-100", Thread: "-100:42", Kind: boardsync.TargetThread}, nil
		},
	}
	sidecar, err := sidecartest.New("telegram", "telegram", bridgeSecret, sidecarAdapter)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sidecar.Close)
	client, err := chatsdk.NewClient(chatsdk.ClientConfig{
		BaseURL: sidecar.URL(), Secret: bridgeSecret, Key: "telegram",
		Retry: boardsync.RetryPolicy{MaxAttempts: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	remote, err := chatsdk.NewRemoteAdapter(client, "telegram", caps)
	if err != nil {
		t.Fatal(err)
	}
	bridge := &chatsdk.FuncAdapter{
		PlatformName: "telegram",
		VerifyFunc: func(_ context.Context, request *boardsync.WebhookRequest) error {
			return chatsdk.VerifyRequestV1(bridgeSecret, request.Header, request.Body, time.Now(), 0)
		},
		DecodeFunc: func(_ context.Context, key boardsync.Key, request *boardsync.WebhookRequest) (chatsdk.DecodeResult, error) {
			records, err := chatsdk.DecodeInbound(request.Body, key, "telegram")
			if err != nil {
				return chatsdk.DecodeResult{}, err
			}
			return chatsdk.DecodeResult{Records: records, Response: boardsync.OK()}, nil
		},
		SendFunc:    remote.Send,
		EditFunc:    remote.Edit,
		DeleteFunc:  remote.Delete,
		ReactFunc:   remote.React,
		ChannelFunc: remote.ResolveChannel,
		TargetFunc:  remote.ResolveTarget,
	}
	provider, err := chatsdk.New(chatsdk.Config{Key: "telegram", Adapter: bridge, Caps: caps})
	if err != nil {
		t.Fatal(err)
	}
	batch := chatsdk.InboundBatch{
		Version: chatsdk.BridgeVersion,
		Events: []chatsdk.InboundEvent{{
			Dedupe: "telegram:update:1",
			Event: boardsync.Event{
				Kind: boardsync.KindMessage, Type: boardsync.TypeMessage,
				Channel: "-100", Thread: "-100:42", Message: "42", IsRoot: true, Text: "hello",
				Author: boardsync.Author{ID: "telegram:user:7", Name: "Ada"},
			},
		}},
	}
	body, err := json.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	inbound := func() *boardsync.WebhookRequest {
		timestamp := time.Now().Unix()
		nonce := "nonce-0123456789"
		header := http.Header{}
		header.Set(chatsdk.HeaderTimestamp, strconv.FormatInt(timestamp, 10))
		header.Set(chatsdk.HeaderNonce, nonce)
		header.Set(chatsdk.HeaderSignature, chatsdk.SignV1(bridgeSecret, timestamp, nonce, body))
		return &boardsync.WebhookRequest{Method: http.MethodPost, Path: "/v1/board/sync/bridge/telegram/events", Header: header, Body: body}
	}
	conformance.Run(t, conformance.Fixture{
		Provider: provider,
		Key:      "telegram",
		Inbound:  func(*testing.T) *boardsync.WebhookRequest { return inbound() },
		BadSignature: func(t *testing.T) *boardsync.WebhookRequest {
			request := inbound()
			request.Header.Set(chatsdk.HeaderSignature, "v1=deadbeef")
			return request
		},
		Target: boardsync.ResolvedTarget{
			Key: "telegram", Platform: "telegram", Channel: "-100",
			Thread: "-100:42", Kind: boardsync.TargetThread,
		},
		Markdown: "**Ada**: ship it",
		Resolve:  "-100:42",
		ExerciseChanges: func(t *testing.T, provider boardsync.Provider) {
			thread := boardsync.ThreadRef{Key: "telegram", Platform: "telegram", Channel: "-100", Thread: "-100:42"}
			if err := provider.(boardsync.Editor).Edit(t.Context(), "telegram", thread, "m1", boardsync.OutboundMessage{Markdown: "edited"}); err != nil {
				t.Fatal(err)
			}
			if err := provider.(boardsync.Deleter).Delete(t.Context(), "telegram", thread, "m1"); err != nil {
				t.Fatal(err)
			}
			if err := provider.(boardsync.Reactor).React(t.Context(), "telegram", thread, "m1", "thumbsup", true); err != nil {
				t.Fatal(err)
			}
		},
	})
}
