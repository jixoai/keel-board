package chatsdk

import (
	"context"
	"errors"
	"strings"
	"testing"

	boardsync "github.com/jixoai/keel-board/sync"
)

func TestAttemptRefusesOversizedBodyBeforeNetwork(t *testing.T) {
	client, err := NewClient(ClientConfig{
		BaseURL: "http://127.0.0.1:9", Secret: "secret", Key: "telegram",
	})
	if err != nil {
		t.Fatal(err)
	}
	payload := map[string]string{"x": strings.Repeat("x", MaxBodyBytes)}
	err = client.attempt(context.Background(), "/deliver", payload, &struct{}{})
	if !errors.Is(err, boardsync.ErrBadRequest) || !boardsync.IsPermanent(err) {
		t.Fatalf("oversized body = %v", err)
	}
}
