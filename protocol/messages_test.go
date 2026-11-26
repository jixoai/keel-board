package protocol_test

import (
	"strings"
	"testing"

	"github.com/jixoai/keel-board/protocol"
)

func TestFindBlocks(t *testing.T) {
	const commit = "0123456789abcdef0123456789abcdef01234567"
	tests := []struct {
		name    string
		text    string
		blocks  int
		problem string
		schema  string
	}{
		{
			name:   "a valid block",
			text:   "Deployed:\n\n```json#git-commit@1\n{\"sha\": \"" + commit + "\", \"title\": \"fix it\"}\n```\n",
			blocks: 1,
			schema: "git-commit",
		},
		{
			name:   "code fences that name no schema are not blocks",
			text:   "```go\nfunc main() {}\n```\n",
			blocks: 0,
		},
		{
			name:   "a block inside another fence is that fence's text",
			text:   "```text\n```json#git-commit@1\n{\"sha\": \"" + commit + "\", \"title\": \"x\"}\n```\n```\n",
			blocks: 0,
		},
		{
			name:    "an unknown schema",
			text:    "```json#nope@1\n{}\n```\n",
			blocks:  1,
			problem: "doesn't have",
		},
		{
			name:    "a fence that never closes",
			text:    "```json#git-commit@1\n{\"sha\": \"" + commit + "\", \"title\": \"x\"}",
			blocks:  1,
			problem: "never closes",
		},
		{
			name:    "not an object",
			text:    "```json#git-commit@1\n[]\n```\n",
			blocks:  1,
			problem: "isn't a JSON object",
		},
		{
			name:    "not JSON",
			text:    "```json#git-commit@1\nnot json\n```\n",
			blocks:  1,
			problem: "isn't JSON",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			found := protocol.FindBlocks(test.text)
			if len(found) != test.blocks {
				t.Fatalf("found %d blocks, want %d: %+v", len(found), test.blocks, found)
			}
			if test.blocks == 0 {
				return
			}
			block := found[0]
			if test.problem != "" && !strings.Contains(block.Problem, test.problem) {
				t.Fatalf("problem %q doesn't contain %q", block.Problem, test.problem)
			}
			if test.problem == "" && block.Problem != "" {
				t.Fatalf("unexpected problem: %s", block.Problem)
			}
			if test.schema != "" && block.Name != test.schema {
				t.Fatalf("schema %q, want %q", block.Name, test.schema)
			}
		})
	}
}

func TestValidateBlocks(t *testing.T) {
	const commit = "0123456789abcdef0123456789abcdef01234567"
	tests := []struct {
		name string
		text string
		code protocol.Code
	}{
		{"valid", "```json#git-commit@1\n{\"sha\": \"" + commit + "\", \"title\": \"fix\"}\n```", ""},
		{"missing sha", "```json#git-commit@1\n{\"title\": \"fix\"}\n```", protocol.CodeInvalidBlock},
		{"extra field", "```json#git-commit@1\n{\"sha\": \"" + commit + "\", \"title\": \"fix\", \"x\": 1}\n```", protocol.CodeInvalidBlock},
		{"bad sha", "```json#git-commit@1\n{\"sha\": \"abc\", \"title\": \"fix\"}\n```", protocol.CodeInvalidBlock},
		{"unknown schema", "```json#custom@1\n{\"x\": 1}\n```", protocol.CodeInvalidBlock},
		{"valid deployment", "```json#vercel-deployment@1\n{\"url\": \"https://example.com\"}\n```", ""},
		{"bad url", "```json#vercel-deployment@1\n{\"url\": \"http://example.com\"}\n```", protocol.CodeInvalidBlock},
		{"prose only", "no blocks here", ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := protocol.ValidateBlocks(test.text)
			if test.code == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || err.Code != test.code {
				t.Fatalf("got %v, want %s", err, test.code)
			}
		})
	}
}

func TestBlocksIn(t *testing.T) {
	text := "```json#git-commit@1\n{\"sha\": \"0123456789abcdef0123456789abcdef01234567\", \"title\": \"fix\"}\n```"
	blocks := protocol.BlocksIn(text)
	if len(blocks) != 1 || blocks[0].Schema != "git-commit" || blocks[0].Version != 1 {
		t.Fatalf("unexpected blocks: %+v", blocks)
	}
	if blocks[0].Data["title"] != "fix" {
		t.Fatalf("data: %+v", blocks[0].Data)
	}
	if got := protocol.BlocksIn("plain text"); len(got) != 0 {
		t.Fatalf("expected no blocks: %+v", got)
	}
}

func TestMentions(t *testing.T) {
	tests := []struct {
		name  string
		text  string
		valid bool
		kinds []protocol.MentionKind
		terms []string
	}{
		{"exact", "hi @[local:user:bob]", true, []protocol.MentionKind{protocol.MentionActor}, []string{"local:user:bob"}},
		{"handle", "hi @Alice and @bob/claude", true, []protocol.MentionKind{protocol.MentionHandle, protocol.MentionHandle}, []string{"alice", "bob/claude"}},
		{"invalid exact", "@[not-an-actor]", false, []protocol.MentionKind{protocol.MentionActor}, []string{"not-an-actor"}},
		{"email is not an actor id", "@[ada@example.com]", false, []protocol.MentionKind{protocol.MentionActor}, []string{"ada@example.com"}},
		{"inside inline code", "`@[bad]` and @alice", true, []protocol.MentionKind{protocol.MentionHandle}, []string{"alice"}},
		{"inside a fence", "```\n@[bad]\n```\n@alice", true, []protocol.MentionKind{protocol.MentionHandle}, []string{"alice"}},
		{"an email is not a mention", "mail me at ada@example.com", true, nil, nil},
		{"deduplicated", "@alice @alice @[local:user:bob] @[local:user:bob]", true, []protocol.MentionKind{protocol.MentionHandle, protocol.MentionActor}, []string{"alice", "local:user:bob"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := protocol.ValidateMentions(test.text)
			if test.valid && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !test.valid && err == nil {
				t.Fatal("expected an error")
			}
			mentions := protocol.ExtractMentions(test.text)
			if len(mentions) != len(test.kinds) {
				t.Fatalf("mentions %+v, want %d", mentions, len(test.kinds))
			}
			for at, kind := range test.kinds {
				if mentions[at].Kind != kind {
					t.Fatalf("mention %d is %s, want %s", at, mentions[at].Kind, kind)
				}
			}
			terms := protocol.MentionTerms(test.text)
			if len(terms) != len(test.terms) {
				t.Fatalf("terms %+v, want %+v", terms, test.terms)
			}
			for at, term := range test.terms {
				if terms[at] != term {
					t.Fatalf("term %d is %q, want %q", at, terms[at], term)
				}
			}
		})
	}
	if !protocol.MentionsMatch("hi @[local:user:bob]", []string{"local:user:bob"}) {
		t.Fatal("expected a match")
	}
	if !protocol.MentionsMatch("hi @alice/claude", []string{"alice"}) {
		t.Fatal("a handle profile should match its handle prefix")
	}
	if !protocol.MentionsMatch("hi @alice/claude", []string{"alice/claude"}) {
		t.Fatal("an exact handle should match")
	}
	if !protocol.MentionsMatch("hi @[local:user:alice+claude]", []string{"local:user:alice"}) {
		t.Fatal("an actor profile should match its actor ID prefix")
	}
	if protocol.MentionsMatch("hi @[local:user:alice+claude]", []string{"alice"}) {
		t.Fatal("a handle prefix must not match an actor ID")
	}
	if protocol.MentionsMatch("hi @alice", []string{"bob"}) {
		t.Fatal("unexpected match")
	}
	if protocol.MentionsMatch("hi @alicebob", []string{"alice"}) {
		t.Fatal("a prefix must end at a segment boundary")
	}
	if protocol.MentionsMatchExact("hi @alice/claude", []string{"alice"}) {
		t.Fatal("an exact match must not take a profile")
	}
	if !protocol.MentionsMatchExact("hi @alice/claude", []string{"alice/claude"}) {
		t.Fatal("an exact handle should match exactly")
	}
}

func TestEmoji(t *testing.T) {
	tests := []struct {
		input string
		want  string
		code  protocol.Code
	}{
		{"thumbs_up", "thumbs_up", ""},
		{":eyes:", "eyes", ""},
		{"+1", "thumbs_up", ""},
		{"white_check_mark", "check", ""},
		{"Thumbs_Up", "thumbs_up", ""},
		{"not an emoji", "", protocol.CodeInvalidRequest},
		{"", "", protocol.CodeInvalidRequest},
		{"constructor", "", protocol.CodeInvalidRequest},
		{strings.Repeat("a", 65), "", protocol.CodeInvalidRequest},
	}
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			got, err := protocol.NormalizeEmoji(test.input)
			if test.code != "" {
				if err == nil || err.Code != test.code {
					t.Fatalf("got %v, want %s", err, test.code)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != test.want {
				t.Fatalf("got %q, want %q", got, test.want)
			}
		})
	}
}

func TestThreadPositions(t *testing.T) {
	value := protocol.FormatThreadPosition(42, 7)
	if value != "42.7" {
		t.Fatalf("unexpected cursor %q", value)
	}
	activity, root, ok := protocol.ParseThreadPosition(value)
	if !ok || activity != 42 || root != 7 {
		t.Fatalf("parsed %d.%d, ok %v", activity, root, ok)
	}
	for _, bad := range []string{"", "42", "0.7", "42.0", "a.b", "42.7.1"} {
		if _, _, ok := protocol.ParseThreadPosition(bad); ok {
			t.Fatalf("%q should not parse", bad)
		}
	}
}

func TestAttachmentValidation(t *testing.T) {
	valid := "abcdefghijklmnopqrstuv"
	if err := protocol.ValidateAttachmentID(valid); err != nil {
		t.Fatalf("valid ID refused: %v", err)
	}
	if err := protocol.ValidateAttachmentIDs([]string{"bad"}); err == nil {
		t.Fatal("expected an invalid ID error")
	}
	ids := make([]string, protocol.AttachmentsPerMessageMax+1)
	for at := range ids {
		ids[at] = valid
	}
	if err := protocol.ValidateAttachmentIDs(ids); err == nil {
		t.Fatal("expected a count error")
	}
	if protocol.SanitizeFileName(`..\nasty/..\evil.png`) != "evil.png" {
		t.Fatalf("unexpected name: %q", protocol.SanitizeFileName(`..\nasty/..\evil.png`))
	}
	if protocol.SanitizeFileName("   ") != "file" {
		t.Fatalf("empty name: %q", protocol.SanitizeFileName("   "))
	}
	if protocol.MediaType("image/png; charset=binary") != "image/png" {
		t.Fatalf("media type: %q", protocol.MediaType("image/png; charset=binary"))
	}
	if protocol.MediaType("not a type") != "application/octet-stream" {
		t.Fatalf("media type: %q", protocol.MediaType("not a type"))
	}
}
