package sync_test

import (
	"context"
	"testing"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/fake"
)

func TestRegistryDropsSelfAuthoredRecord(t *testing.T) {
	provider := fake.New("slack", boardsync.Capabilities{})
	provider.SelfIDsValue = []string{"slack:user:U1"}
	provider.ReceiveFunc = func(context.Context, boardsync.Key, *boardsync.WebhookRequest) (boardsync.Intake, error) {
		self := inboxEvent("self", "C1:T1")
		self.Author = boardsync.Author{ID: "slack:user:U1", Handle: "app", Name: "app", Bot: true}
		other := inboxEvent("other", "C1:T1")
		other.Author = boardsync.Author{ID: "slack:user:U2", Handle: "ada", Name: "Ada"}
		return boardsync.Intake{
			Records:  []boardsync.Record{{Dedupe: "self", Event: self}, {Dedupe: "other", Event: other}},
			Response: boardsync.OK(),
		}, nil
	}
	registry := boardsync.NewRegistry()
	if err := registry.Add(boardsync.Entry{
		Tenant: "t1", Key: "slack", Platform: "slack", Provider: provider, Sync: boardsync.Noop,
		Routes: func(context.Context, string) (bool, error) { return true, nil },
	}); err != nil {
		t.Fatal(err)
	}
	inbox := boardsync.NewMemoryInbox(boardsync.DefaultRetry, 0)
	response, err := registry.HandleWebhook(context.Background(), "t1", "slack", &boardsync.WebhookRequest{
		Method: "POST", Path: "/webhooks/slack",
		Header: map[string][]string{"Content-Type": {"application/json"}}, Body: []byte("{}"),
	}, nil, inbox)
	if err != nil || response.Status != 200 {
		t.Fatalf("webhook = %+v, %v", response, err)
	}
	if pending := inbox.Pending("slack"); pending != 1 {
		t.Fatalf("inbox pending = %d, want only the other author", pending)
	}
}
