package sync_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	boardsync "github.com/jixoai/keel-board/sync"
)

func TestThreadRefIDs(t *testing.T) {
	ref := boardsync.ThreadRef{Key: "slack", Platform: "slack", Channel: "C0123", Thread: "C0123:1727.9", Subthread: "C0123:1727.9:r1"}
	if ref.IsChannel() {
		t.Fatal("a thread ref is not a channel")
	}
	if got, want := ref.External(), "slack:C0123:1727.9"; got != want {
		t.Fatalf("External() = %q, want %q", got, want)
	}
	if got, want := ref.ChannelID(), "slack:C0123"; got != want {
		t.Fatalf("ChannelID() = %q, want %q", got, want)
	}
	if got, want := ref.SubthreadID(), "slack:C0123:1727.9:r1"; got != want {
		t.Fatalf("SubthreadID() = %q, want %q", got, want)
	}
	channel := boardsync.ThreadRef{Key: "slack", Platform: "slack", Channel: "C0123"}
	if !channel.IsChannel() || channel.External() != "slack:C0123" {
		t.Fatalf("channel ref = %+v", channel)
	}
}

func TestEventValidate(t *testing.T) {
	valid := boardsync.Event{
		ID: "message:slack:C0123:1727.9", Key: "slack", Platform: "slack",
		Kind: boardsync.KindMessage, Type: boardsync.TypeMessage,
		Channel: "C0123", Thread: "C0123:1727.9", Message: "1727.9",
		Text: "hello", Author: boardsync.Author{ID: "slack:user:U0456", Handle: "ada", Name: "Ada"},
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid event: %v", err)
	}
	cases := []struct {
		name string
		edit func(*boardsync.Event)
	}{
		{"no id", func(e *boardsync.Event) { e.ID = "" }},
		{"bad key", func(e *boardsync.Event) { e.Key = "Slack!" }},
		{"no thread", func(e *boardsync.Event) { e.Thread = "" }},
		{"no author", func(e *boardsync.Event) { e.Author = boardsync.Author{} }},
		{"empty message", func(e *boardsync.Event) { e.Text = "" }},
		{"bad kind", func(e *boardsync.Event) { e.Kind = "wat" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			event := valid
			tc.edit(&event)
			if err := event.Validate(); err == nil {
				t.Fatalf("expected an error for %+v", event)
			}
		})
	}
	reaction := boardsync.Event{
		ID: "reaction:slack:C0123:1727.9", Key: "slack", Platform: "slack",
		Kind: boardsync.KindReaction, Type: boardsync.TypeReactionAdded,
		Channel: "C0123", Thread: "C0123:1727.9", Message: "1727.9",
		Emoji: "thumbsup", Added: true, Author: boardsync.Author{ID: "slack:user:U1", Handle: "ada"},
	}
	if err := reaction.Validate(); err != nil {
		t.Fatalf("reaction: %v", err)
	}
	reaction.Added = false
	if err := reaction.Validate(); err == nil {
		t.Fatal("a removed reaction with an added type must be refused")
	}
}

func TestTargetsForDeliverySkipsOrigin(t *testing.T) {
	slack := boardsync.ThreadRef{Key: "slack", Platform: "slack", Channel: "C0123", Thread: "C0123:1727.9"}
	github := boardsync.ThreadRef{Key: "github", Platform: "github", Channel: "acme/www:pulls", Thread: "acme/www:12"}
	msg := boardsync.Message{ID: 1, Thread: 1, From: "slack", Origin: &boardsync.Origin{Thread: "slack:C0123:1727.9", Message: "1727.9"}}
	got := boardsync.TargetsForDelivery(msg, []boardsync.ThreadRef{slack, github})
	if len(got) != 1 || got[0] != github {
		t.Fatalf("targets = %+v, want only github", got)
	}
	msg.From = ""
	msg.Origin = &boardsync.Origin{Thread: "gitlab:acme/www:12", Message: "1"}
	got = boardsync.TargetsForDelivery(msg, []boardsync.ThreadRef{slack, github})
	if len(got) != 2 {
		t.Fatalf("targets = %+v, want both", got)
	}
	if msg.CameFrom(slack) {
		t.Fatal("a gitlab origin doesn't make a slack target its own")
	}
}

func TestResolvedTargetValidate(t *testing.T) {
	channel := boardsync.ResolvedTarget{Key: "slack", Platform: "slack", Channel: "C0123", Kind: boardsync.TargetChannel}
	if err := channel.Validate(); err != nil {
		t.Fatalf("channel target: %v", err)
	}
	thread := boardsync.ResolvedTarget{Key: "slack", Platform: "slack", Channel: "C0123", Thread: "C0123:1", Kind: boardsync.TargetThread}
	if err := thread.Validate(); err != nil {
		t.Fatalf("thread target: %v", err)
	}
	bad := boardsync.ResolvedTarget{Key: "slack", Platform: "slack", Channel: "C0123", Kind: boardsync.TargetThread}
	if err := bad.Validate(); err == nil {
		t.Fatal("a thread target without a thread must be refused")
	}
}

func TestSecretRedaction(t *testing.T) {
	secret := boardsync.Secret("super-secret-token")
	for _, format := range []string{"%v", "%+v", "%s", "%q", "%#v"} {
		if got := fmt.Sprintf(format, secret); strings.Contains(got, "super-secret-token") {
			t.Fatalf("%s leaked the secret: %s", format, got)
		}
	}
	encoded, err := json.Marshal(struct {
		Token boardsync.Secret `json:"token"`
	}{Token: secret})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "super-secret-token") {
		t.Fatalf("JSON leaked the secret: %s", encoded)
	}
	err = boardsync.RedactError(errors.New("request with super-secret-token failed"), secret)
	if strings.Contains(err.Error(), "super-secret-token") || !strings.Contains(err.Error(), boardsync.Redacted) {
		t.Fatalf("RedactError = %q", err)
	}
}

func TestKeyedID(t *testing.T) {
	if got := boardsync.KeyedID("slack-eu", "slack", "slack:user:U0456"); got != "slack-eu:user:U0456" {
		t.Fatalf("KeyedID = %q", got)
	}
	if got := boardsync.KeyedID("slack", "slack", "slack:user:U0456"); got != "slack:user:U0456" {
		t.Fatalf("KeyedID same key = %q", got)
	}
	if got := boardsync.KeyedID("acme", "github", "github:user:42"); got != "acme:user:42" {
		t.Fatalf("KeyedID = %q", got)
	}
}

func TestKeyValidation(t *testing.T) {
	for _, key := range []boardsync.Key{"slack", "acme", "slack-eu_2", "a"} {
		if !key.Valid() {
			t.Fatalf("%q should be valid", key)
		}
	}
	for _, key := range []boardsync.Key{"", "-slack", "Slack", "a b", "a.b", boardsync.Key(strings.Repeat("a", 65))} {
		if key.Valid() {
			t.Fatalf("%q should be invalid", key)
		}
	}
}

func TestStorableText(t *testing.T) {
	if got := boardsync.StorableText("a\x00b", 10); got != "ab" {
		t.Fatalf("NUL removal = %q", got)
	}
	got := boardsync.StorableText("hello world", 6)
	if len(got) > 6 || !strings.HasSuffix(got, "…") {
		t.Fatalf("clamp = %q", got)
	}
}
