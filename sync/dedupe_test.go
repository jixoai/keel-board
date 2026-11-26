package sync_test

import (
	"context"
	"strings"
	"testing"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
)

func TestDedupeKeys(t *testing.T) {
	message := boardsync.MessageDedupe("slack", "C0123:1727.9", "1727.9")
	if message != boardsync.MessageDedupe("slack", "C0123:1727.9", "1727.9") {
		t.Fatal("message identity is not stable")
	}
	if message == boardsync.MessageDedupe("slack", "C0123:1727.9", "1727.10") {
		t.Fatal("different messages share an identity")
	}
	if message == boardsync.MessageDedupe("acme", "C0123:1727.9", "1727.9") {
		t.Fatal("different keys share an identity")
	}
	if !strings.HasPrefix(message, "message:slack:") {
		t.Fatalf("message identity = %q", message)
	}

	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	edited := boardsync.EditedDedupe("slack", "C0123:1727.9", "1727.9", "hello", at)
	if edited == boardsync.EditedDedupe("slack", "C0123:1727.9", "1727.9", "hello!", at) {
		t.Fatal("different edit texts share an identity")
	}
	if edited != boardsync.EditedDedupe("slack", "C0123:1727.9", "1727.9", "hello", at) {
		t.Fatal("edit identity is not stable")
	}
	if edited == boardsync.EditedDedupe("slack", "C0123:1727.9", "1727.9", "hello", at.Add(time.Second)) {
		t.Fatal("two edits with the same text and different times share an identity")
	}

	added := boardsync.ReactionDedupe("slack", "C0123:1", "1", "thumbsup", "U1", true, "evt-1")
	removed := boardsync.ReactionDedupe("slack", "C0123:1", "1", "thumbsup", "U1", false, "evt-2")
	if added == removed {
		t.Fatal("an added and a removed reaction share an identity")
	}
	if added != boardsync.ReactionDedupe("slack", "C0123:1", "1", "thumbsup", "U1", true, "evt-1") {
		t.Fatal("reaction identity is not stable")
	}

	long := boardsync.MessageDedupe("slack", strings.Repeat("t", 400), strings.Repeat("m", 400))
	if len(long) > boardsync.MaxDedupeLen {
		t.Fatalf("long identity is %d bytes", len(long))
	}
	if !strings.HasPrefix(long, "message:slack:") {
		t.Fatalf("long identity lost its prefix: %q", long)
	}
}

func TestMarkerRoundTrip(t *testing.T) {
	marker := boardsync.Marker{Key: "github", Message: 812, Thread: "github:acme/www:12"}
	body := boardsync.WithMarker("hello\nworld\n", marker)
	if !strings.HasPrefix(body, "hello\nworld\n\n<!-- keel-board:sync") {
		t.Fatalf("WithMarker = %q", body)
	}
	got, ok := boardsync.ParseMarker(body)
	if !ok || got != marker {
		t.Fatalf("ParseMarker = %+v, %v; want %+v", got, ok, marker)
	}
	if !boardsync.IsOwnMarker(body, "github") {
		t.Fatal("IsOwnMarker should find the key")
	}
	if boardsync.IsOwnMarker(body, "slack") {
		t.Fatal("IsOwnMarker found another key")
	}
	if stripped := boardsync.StripMarker(body); stripped != "hello\nworld" {
		t.Fatalf("StripMarker = %q", stripped)
	}
	if _, ok := boardsync.ParseMarker("no marker here"); ok {
		t.Fatal("parsed a marker that isn't there")
	}
	if _, ok := boardsync.ParseMarker("<!-- keel-board:sync key= -->"); ok {
		t.Fatal("parsed a marker with no key")
	}
}

func TestMemoryDedupe(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	store := boardsync.NewMemoryDedupe(time.Minute, 10).WithClock(func() time.Time { return now })
	ctx := context.Background()
	if seen, err := store.SeenOrMark(ctx, "a"); err != nil || seen {
		t.Fatalf("first SeenOrMark = %v, %v", seen, err)
	}
	if seen, err := store.SeenOrMark(ctx, "a"); err != nil || !seen {
		t.Fatalf("second SeenOrMark = %v, %v", seen, err)
	}
	now = now.Add(2 * time.Minute)
	if seen, err := store.Seen(ctx, "a"); err != nil || seen {
		t.Fatalf("expired Seen = %v, %v", seen, err)
	}
}
