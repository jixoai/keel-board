package slack_test

import (
	"testing"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/conformance"
	"github.com/jixoai/keel-board/sync/slack"
)

func TestProviderConformance(t *testing.T) {
	fake := newFakeSlack(t)
	provider := fake.provider(t)
	body := fixture(t, "message.json")
	conformance.Run(t, conformance.Fixture{
		Provider: provider,
		Key:      "slack",
		Inbound: func(t *testing.T) *boardsync.WebhookRequest {
			return signedRequest(t, body)
		},
		BadSignature: func(t *testing.T) *boardsync.WebhookRequest {
			request := signedRequest(t, body)
			request.Header.Set(slack.SignatureHeader, "v0=deadbeef")
			return request
		},
		Target: boardsync.ResolvedTarget{
			Key: "slack", Platform: "slack", Channel: "C0123ABC",
			Thread: "C0123ABC:1727000000.123456", Kind: boardsync.TargetThread,
		},
		Markdown: "**Ada**: ship it",
		Resolve:  "C0123ABC:1727000000.123456",
		ExerciseChanges: func(t *testing.T, provider boardsync.Provider) {
			editor, ok := provider.(boardsync.Editor)
			if !ok {
				t.Fatal("slack provider has no editor")
			}
			thread := boardsync.ThreadRef{Key: "slack", Platform: "slack", Channel: "C0123ABC", Thread: "C0123ABC:1727000000.123456"}
			posted, err := provider.Post(t.Context(), "slack", boardsync.ResolvedTarget{
				Key: "slack", Platform: "slack", Channel: "C0123ABC", Thread: thread.Thread, Kind: boardsync.TargetThread,
			}, boardsync.OutboundMessage{Markdown: "change me"})
			if err != nil {
				t.Fatal(err)
			}
			if err := editor.Edit(t.Context(), "slack", thread, posted.Message, boardsync.OutboundMessage{Markdown: "changed"}); err != nil {
				t.Fatal(err)
			}
			deleter, ok := provider.(boardsync.Deleter)
			if !ok {
				t.Fatal("slack provider has no deleter")
			}
			if err := deleter.Delete(t.Context(), "slack", thread, posted.Message); err != nil {
				t.Fatal(err)
			}
			reactor, ok := provider.(boardsync.Reactor)
			if !ok {
				t.Fatal("slack provider has no reactor")
			}
			if err := reactor.React(t.Context(), "slack", thread, posted.Message, "thumbsup", true); err != nil {
				t.Fatal(err)
			}
			if err := reactor.React(t.Context(), "slack", thread, posted.Message, "thumbsup", false); err != nil {
				t.Fatal(err)
			}
		},
	})
}
