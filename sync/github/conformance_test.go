package github_test

import (
	"testing"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/conformance"
	"github.com/jixoai/keel-board/sync/github"
)

func TestProviderConformance(t *testing.T) {
	fake := newFakeGitHub(t)
	provider := contractProvider(t, fake.server.URL)
	body := fixture(t, "issue_comment.json")
	conformance.Run(t, conformance.Fixture{
		Provider: provider,
		Key:      "github",
		Inbound: func(t *testing.T) *boardsync.WebhookRequest {
			return request(t, "issue_comment", "delivery-1", body)
		},
		BadSignature: func(t *testing.T) *boardsync.WebhookRequest {
			request := request(t, "issue_comment", "delivery-1", body)
			request.Header.Set(github.SignatureHeader, "sha256=deadbeef")
			return request
		},
		Target: boardsync.ResolvedTarget{
			Key: "github", Platform: "github", Channel: "acme/www:pulls",
			Thread: "acme/www:123", Kind: boardsync.TargetThread,
		},
		Markdown: "**Ada**: ship it",
		Resolve:  "acme/www:123",
	})
}
