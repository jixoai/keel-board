package main

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/chatsdk"
	"github.com/jixoai/keel-board/sync/chatsdk/sidecartest"
)

const (
	chattyKey          boardsync.Key    = "chatty"
	chattyBridgeSecret boardsync.Secret = "chatty-e2e-bridge-secret"
	chattyBotID                         = "chatty:user:sidecar-bot"
)

// newChattyProvider starts the reference Chat SDK sidecar fixture and builds
// the real bridge client + remote adapter the board uses for key "chatty".
func newChattyProvider() (boardsync.Provider, *sidecartest.Sidecar, error) {
	adapter := &chattyAdapter{}
	sidecar, err := sidecartest.New(chattyKey, "chatty", chattyBridgeSecret, adapter)
	if err != nil {
		return nil, nil, err
	}
	client, err := chatsdk.NewClient(chatsdk.ClientConfig{
		BaseURL:  sidecar.URL(),
		Secret:   chattyBridgeSecret,
		Key:      chattyKey,
		Platform: "chatty",
		SelfIDs:  []string{chattyBotID},
		HTTPClient: &http.Client{
			Transport: &http.Transport{Proxy: nil},
		},
	})
	if err != nil {
		sidecar.Close()
		return nil, nil, err
	}
	caps := boardsync.Capabilities{
		StartThreads: true, Edits: true, Deletes: true, Reactions: true,
	}
	remote, err := chatsdk.NewRemoteAdapter(client, "chatty", caps)
	if err != nil {
		sidecar.Close()
		return nil, nil, err
	}
	provider, err := chatsdk.New(chatsdk.Config{
		Key: chattyKey, Adapter: remote, SelfIDs: []string{chattyBotID}, Caps: caps,
	})
	if err != nil {
		sidecar.Close()
		return nil, nil, err
	}
	return provider, sidecar, nil
}

// chattyAdapter is the sidecar's fake platform: it records nothing itself (the
// fixture does) and answers deliveries with stable native IDs.
type chattyAdapter struct {
	mu   sync.Mutex
	next int64
}

func (a *chattyAdapter) Platform() string { return "chatty" }

func (a *chattyAdapter) Send(_ context.Context, _ boardsync.Key, target boardsync.ResolvedTarget, _ boardsync.OutboundMessage) (boardsync.Posted, error) {
	a.mu.Lock()
	a.next++
	n := a.next
	a.mu.Unlock()
	thread := target.Thread
	if thread == "" {
		thread = target.Channel + ":chatty-thread-1"
	}
	return boardsync.Posted{Thread: thread, Message: fmt.Sprintf("chatty-m-%d", n)}, nil
}

func (a *chattyAdapter) Edit(context.Context, boardsync.Key, boardsync.ThreadRef, string, boardsync.OutboundMessage) error {
	return nil
}

func (a *chattyAdapter) Delete(context.Context, boardsync.Key, boardsync.ThreadRef, string) error {
	return nil
}

func (a *chattyAdapter) React(context.Context, boardsync.Key, boardsync.ThreadRef, string, string, bool) error {
	return nil
}

func (a *chattyAdapter) ResolveTarget(_ context.Context, key boardsync.Key, target string) (boardsync.ResolvedTarget, error) {
	target = strings.TrimSpace(target)
	channel, thread, found := strings.Cut(target, ":")
	if channel == "" {
		return boardsync.ResolvedTarget{}, fmt.Errorf("%w: chatty target %q has no channel", boardsync.ErrBadRequest, target)
	}
	resolved := boardsync.ResolvedTarget{Key: key, Platform: "chatty", Channel: channel}
	if found && strings.TrimSpace(thread) != "" {
		resolved.Thread = strings.TrimSpace(thread)
		resolved.Kind = boardsync.TargetThread
	} else {
		resolved.Kind = boardsync.TargetChannel
	}
	return resolved, nil
}
