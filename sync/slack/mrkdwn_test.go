package slack

import (
	"strings"
	"testing"
)

func TestMRKDWNLeavesCodeLiteral(t *testing.T) {
	text := "hi <@U1> ` <@U2> <#C3|x> <https://x.example|y> <!channel> ` done"
	got := mrkdwnToMarkdown("slack", text)
	for _, want := range []string{"@[slack:user:U1]", "` <@U2> <#C3|x> <https://x.example|y> <!channel> `", "done"} {
		if !strings.Contains(got, want) {
			t.Fatalf("mrkdwn = %q, want %q", got, want)
		}
	}
	for _, forbidden := range []string{"@[slack:user:U2]", "#x", "[y](https://x.example)", "@channel"} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("code was rewritten to %q: %q", forbidden, got)
		}
	}
}

func TestMRKDWNFencedBlockIsLiteral(t *testing.T) {
	text := "```\n<@U1> <!here> <#C2|chan> <https://x.example|link>\n```\n<@U3> <!channel>"
	got := mrkdwnToMarkdown("slack", text)
	if !strings.Contains(got, "<@U1> <!here> <#C2|chan> <https://x.example|link>") {
		t.Fatalf("fence was rewritten: %q", got)
	}
	if !strings.Contains(got, "@[slack:user:U3]") || !strings.Contains(got, "@channel") {
		t.Fatalf("prose after the fence was not converted: %q", got)
	}
	if strings.Contains(got, "@[slack:user:U1]") {
		t.Fatalf("fence mention became a token: %q", got)
	}
}

func TestMRKDWNMentionRoundTrip(t *testing.T) {
	token := mrkdwnToMarkdown("slack", "ping <@U0456ABC>")
	if token != "ping @[slack:user:U0456ABC]" {
		t.Fatalf("inbound token = %q", token)
	}
	if out := restoreMentions(token, "slack"); out != "ping <@U0456ABC>" {
		t.Fatalf("outbound mention = %q", out)
	}
	if out := restoreMentions("`@[slack:user:U0456ABC]`", "slack"); out != "`@[slack:user:U0456ABC]`" {
		t.Fatalf("code mention was rewritten: %q", out)
	}
}
