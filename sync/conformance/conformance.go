// Package conformance is the shared provider contract suite. Native Slack,
// GitHub, and Linear adapters and the Chat SDK bridge run the same checks, so
// the generic bridge path is provably substitutable for a platform adapter.
//
// A package supplies a Fixture: a provider, one signed inbound request that
// yields a normalized record, a request with a bad signature, and an outbound
// target. The suite performs no network I/O of its own; a provider that posts
// through a fake platform server or a sidecar fixture does that inside its own
// test.
package conformance

import (
	"context"
	"errors"
	"strings"
	"testing"

	boardsync "github.com/jixoai/keel-board/sync"
)

// Fixture is one provider's test inputs. Inbound and BadSignature must return
// fresh requests; the suite may call them more than once.
type Fixture struct {
	// Provider is the adapter under test.
	Provider boardsync.Provider
	// Key is the entry key the provider is configured for.
	Key boardsync.Key
	// Inbound is a valid signed webhook that yields at least one record.
	Inbound func(t *testing.T) *boardsync.WebhookRequest
	// BadSignature is the same kind of request with an invalid signature.
	BadSignature func(t *testing.T) *boardsync.WebhookRequest
	// Target is a valid thread target for Post.
	Target boardsync.ResolvedTarget
	// Markdown is the body Post sends.
	Markdown string
	// Resolve, when set, is a native target ResolveTarget must understand.
	Resolve string
	// ExerciseChanges is optional adapter-specific edit/delete/react coverage.
	ExerciseChanges func(t *testing.T, provider boardsync.Provider)
}

// Run executes the shared provider contract.
func Run(t *testing.T, fixture Fixture) {
	t.Helper()
	if fixture.Provider == nil {
		t.Fatal("conformance: no provider")
	}
	if !fixture.Key.Valid() {
		t.Fatalf("conformance: %q isn't a sync key", fixture.Key)
	}
	if fixture.Inbound == nil || fixture.BadSignature == nil {
		t.Fatal("conformance: inbound fixtures are required")
	}
	if err := fixture.Target.Validate(); err != nil {
		t.Fatalf("conformance: target = %v", err)
	}
	provider := fixture.Provider
	ctx := context.Background()

	t.Run("identity", func(t *testing.T) {
		if strings.TrimSpace(string(provider.Platform())) == "" {
			t.Fatal("provider has no platform")
		}
		if fixture.Target.Key != fixture.Key {
			t.Fatalf("fixture target belongs to %q, not %q", fixture.Target.Key, fixture.Key)
		}
		if fixture.Target.Platform != provider.Platform() {
			t.Fatalf("fixture target is %q, provider is %q", fixture.Target.Platform, provider.Platform())
		}
	})

	t.Run("receive", func(t *testing.T) {
		intake, err := provider.Receive(ctx, fixture.Key, fixture.Inbound(t))
		if err != nil {
			t.Fatalf("Receive: %v", err)
		}
		if len(intake.Records) == 0 {
			t.Fatal("Receive returned no records")
		}
		for _, record := range intake.Records {
			if err := record.Validate(); err != nil {
				t.Fatalf("record %q: %v", record.Dedupe, err)
			}
			if record.Event.Key != fixture.Key {
				t.Fatalf("record key = %q, want %q", record.Event.Key, fixture.Key)
			}
			if record.Event.Platform != provider.Platform() {
				t.Fatalf("record platform = %q, want %q", record.Event.Platform, provider.Platform())
			}
			if record.Event.ID != record.Dedupe || record.Dedupe == "" || len(record.Dedupe) > 300 {
				t.Fatalf("record identity = id %q dedupe %q", record.Event.ID, record.Dedupe)
			}
		}
		first, err := provider.Receive(ctx, fixture.Key, fixture.Inbound(t))
		if err != nil {
			t.Fatalf("second Receive: %v", err)
		}
		second, err := provider.Receive(ctx, fixture.Key, fixture.Inbound(t))
		if err != nil {
			t.Fatalf("third Receive: %v", err)
		}
		if first.Records[0].Dedupe != second.Records[0].Dedupe {
			t.Fatalf("redelivery changed dedupe: %q / %q", first.Records[0].Dedupe, second.Records[0].Dedupe)
		}
	})

	t.Run("bad_signature", func(t *testing.T) {
		if _, err := provider.Receive(ctx, fixture.Key, fixture.BadSignature(t)); !errors.Is(err, boardsync.ErrUnauthorized) {
			t.Fatalf("bad signature = %v, want ErrUnauthorized", err)
		}
	})

	t.Run("foreign_key", func(t *testing.T) {
		if _, err := provider.Receive(ctx, "other", fixture.Inbound(t)); !errors.Is(err, boardsync.ErrBadRequest) {
			t.Fatalf("foreign key = %v, want ErrBadRequest", err)
		}
	})

	t.Run("resolve_target", func(t *testing.T) {
		if fixture.Resolve == "" {
			t.Skip("no resolve input")
		}
		resolved, err := provider.ResolveTarget(ctx, fixture.Key, fixture.Resolve)
		if err != nil {
			t.Fatalf("ResolveTarget: %v", err)
		}
		if resolved.Key != fixture.Key || resolved.Platform != provider.Platform() {
			t.Fatalf("resolved target = %+v", resolved)
		}
		if err := resolved.Validate(); err != nil {
			t.Fatalf("resolved target: %v", err)
		}
	})

	t.Run("post", func(t *testing.T) {
		posted, err := provider.Post(ctx, fixture.Key, fixture.Target, boardsync.OutboundMessage{
			Markdown: fixture.Markdown, BoardMessage: 42, BoardThread: 42,
		})
		if err != nil {
			t.Fatalf("Post: %v", err)
		}
		if err := posted.Validate(); err != nil {
			t.Fatalf("posted = %+v: %v", posted, err)
		}
	})

	if fixture.ExerciseChanges != nil {
		t.Run("changes", func(t *testing.T) {
			fixture.ExerciseChanges(t, provider)
		})
	}
}
