package sync_test

import (
	"strings"
	"testing"

	boardsync "github.com/jixoai/keel-board/sync"
)

func TestMapProseLeavesCodeAlone(t *testing.T) {
	text := "a @[slack:user:U1] `@[slack:user:U2]` and\n```\n@[slack:user:U3]\n```\nend"
	got := boardsync.MapProse(text, func(prose string) string {
		return strings.ReplaceAll(prose, "slack", "x")
	})
	if !strings.Contains(got, "@[x:user:U1]") {
		t.Fatalf("prose not mapped: %q", got)
	}
	if !strings.Contains(got, "`@[slack:user:U2]`") || !strings.Contains(got, "@[slack:user:U3]") {
		t.Fatalf("code rewritten: %q", got)
	}
}

func TestReplaceMentionsScopesToTheEntryKey(t *testing.T) {
	text := "hi @[slack:user:U1] and @[acme:user:U2] and @[slack:user:U3]"
	got := boardsync.ReplaceMentions(text, "slack", func(id string) (string, bool) {
		if id == "U3" {
			return "", false
		}
		return "<@" + id + ">", true
	})
	if !strings.Contains(got, "<@U1>") {
		t.Fatalf("own mention not replaced: %q", got)
	}
	if !strings.Contains(got, "@[acme:user:U2]") {
		t.Fatalf("foreign mention rewritten: %q", got)
	}
	if !strings.Contains(got, "@[slack:user:U3]") {
		t.Fatalf("refused mention not kept: %q", got)
	}
}

func TestReplaceMentionsKeepsUnterminatedToken(t *testing.T) {
	text := "a @[slack:user:U1 and more"
	got := boardsync.ReplaceMentions(text, "slack", func(string) (string, bool) { return "<@x>", true })
	if got != text {
		t.Fatalf("unterminated token changed: %q", got)
	}
}
