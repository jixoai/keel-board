package linear_test

import (
	"strings"
	"testing"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/conformance"
	"github.com/jixoai/keel-board/sync/linear"
)

func TestProviderConformance(t *testing.T) {
	server := graphServer(t, func(query string, _ map[string]any) any {
		if strings.Contains(query, "commentCreate") {
			return map[string]any{"commentCreate": map[string]any{
				"success": true,
				"comment": map[string]any{"id": "comment-conformance"},
			}}
		}
		return map[string]any{}
	})
	t.Cleanup(server.Close)
	provider := newProvider(t, server.URL)
	body := fixture(t, "issue_created.json")
	conformance.Run(t, conformance.Fixture{
		Provider: provider,
		Key:      "linear",
		Inbound: func(t *testing.T) *boardsync.WebhookRequest {
			return request(t, body)
		},
		BadSignature: func(t *testing.T) *boardsync.WebhookRequest {
			request := request(t, body)
			request.Header.Set(linear.SignatureHeader, "deadbeef")
			return request
		},
		Target: boardsync.ResolvedTarget{
			Key: "linear", Platform: "linear", Channel: "ENG",
			Thread: "ENG:" + testIssueID, Kind: boardsync.TargetThread,
		},
		Markdown: "**Ada**: ship it",
		Resolve:  "ENG:" + testIssueID,
	})
}
