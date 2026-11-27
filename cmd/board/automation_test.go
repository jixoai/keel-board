package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jixoai/keel-board/blob"
	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/server"
	"github.com/jixoai/keel-board/store"
)

// TestAutomationWorkersSmoke starts the same watcher and schedule workers
// `board serve` starts, then drives one watcher end to end through the HTTP
// management API. It proves the worker startup/shutdown wiring, not just the
// packages in isolation.
func TestAutomationWorkersSmoke(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	board := log.New(log.Options{Store: store.NewMemory()})
	runtime, err := buildAutomation(context.Background(), &automationFlags{
		tenant: "default", poll: 10 * time.Millisecond, lease: time.Minute,
	}, "")
	if err != nil {
		t.Fatalf("build automation: %v", err)
	}
	api := devServerWithAutomation(logger, board, blob.NewMemory(), nil, runtime.options(logger))
	httpServer := httptest.NewServer(api)
	defer httpServer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	stop, err := startAutomationWorkers(ctx, logger, runtime, board, server.AllowAll{})
	if err != nil {
		cancel()
		t.Fatalf("start workers: %v", err)
	}
	defer func() {
		cancel()
		stop()
	}()

	post := func(path string, body any) int {
		t.Helper()
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		response, err := http.Post(httpServer.URL+server.APIPrefix+path, "application/json", bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("POST %s: %v", path, err)
		}
		defer response.Body.Close()
		_, _ = io.Copy(io.Discard, response.Body)
		return response.StatusCode
	}
	if status := post("/watchers", map[string]any{
		"id": "smoke", "channel": "work",
		"action": map[string]any{"name": "post", "params": map[string]string{"text": "wired"}},
	}); status != http.StatusCreated {
		t.Fatalf("create watcher = %d", status)
	}
	if status := post("/messages", map[string]any{"channel": "work", "text": "trigger"}); status != http.StatusCreated {
		t.Fatalf("post message = %d", status)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		var found bool
		if err := board.Read("default", func(snapshot *log.Snapshot) error {
			for _, message := range snapshot.MessagesInChannel("work") {
				if message.Text == "wired" && message.By.Actor == "local:bot:smoke" {
					found = true
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the watcher action never posted")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
