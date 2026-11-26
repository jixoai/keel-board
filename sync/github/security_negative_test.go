package github_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/github"
)

func TestConfigValidateNegatives(t *testing.T) {
	base := func() github.Config {
		return github.Config{
			Key: "github", WebhookSecret: testSecret,
			Repos: []string{"acme/www"},
		}
	}
	cases := []struct {
		name   string
		mutate func(*github.Config)
		want   error
	}{
		{"missing secret", func(c *github.Config) { c.WebhookSecret = "" }, boardsync.ErrUnauthorized},
		{"bad key", func(c *github.Config) { c.Key = "GitHub!" }, boardsync.ErrBadRequest},
		{"no allowlist", func(c *github.Config) { c.Repos = nil }, boardsync.ErrBadRequest},
		{"bad repo", func(c *github.Config) { c.Repos = []string{"acme"} }, boardsync.ErrBadRequest},
		{"traversal repo", func(c *github.Config) { c.Repos = []string{"../www"} }, boardsync.ErrBadRequest},
		{"bad write repo", func(c *github.Config) { c.WriteRepos = []string{"acme/www/extra"} }, boardsync.ErrBadRequest},
		{"negative installation", func(c *github.Config) { c.InstallationID = -1 }, boardsync.ErrBadRequest},
		{"negative bot id", func(c *github.Config) { c.BotUserID = -1 }, boardsync.ErrBadRequest},
		{"cleartext public API", func(c *github.Config) { c.APIBaseURL = "http://api.github.com" }, boardsync.ErrBadRequest},
		{"API credentials", func(c *github.Config) { c.APIBaseURL = "https://user:pass@api.github.com" }, boardsync.ErrBadRequest},
		{"API query", func(c *github.Config) { c.APIBaseURL = "https://api.github.com?x=1" }, boardsync.ErrBadRequest},
		{"API fragment", func(c *github.Config) { c.APIBaseURL = "https://api.github.com#x" }, boardsync.ErrBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base()
			tc.mutate(&cfg)
			if _, err := github.New(cfg); !errors.Is(err, tc.want) {
				t.Fatalf("New = %v, want %v", err, tc.want)
			}
		})
	}
	for _, allowed := range []string{"http://127.0.0.1:8080", "http://localhost:8080", "http://[::1]:8080", "https://api.github.com"} {
		t.Run("allowed "+allowed, func(t *testing.T) {
			cfg := base()
			cfg.APIBaseURL = allowed
			if _, err := github.New(cfg); err != nil {
				t.Fatalf("New(%s): %v", allowed, err)
			}
		})
	}
}

func TestBoundedTagKeysDoNotCollide(t *testing.T) {
	first := strings.Repeat("x", 69) + "a"
	second := strings.Repeat("x", 69) + "b"
	body := mutateFixture(t, "pr_opened.json", func(payload map[string]any) {
		payload["pull_request"].(map[string]any)["labels"] = []any{
			map[string]any{"name": first},
			map[string]any{"name": second},
		}
	})
	event := receive(t, newProvider(t, false), "pull_request", body)[0].Event
	keys := make([]string, 0, 2)
	for key, value := range event.Lifecycle.Labels {
		if !strings.HasPrefix(key, "tag.") {
			continue
		}
		if len(key) > 64 {
			t.Fatalf("tag key %q is %d bytes", key, len(key))
		}
		if value != first && value != second {
			t.Fatalf("tag value = %q", value)
		}
		keys = append(keys, key)
	}
	if len(keys) != 2 || keys[0] == keys[1] {
		t.Fatalf("long labels collided: %v", keys)
	}
}

func TestLabelValueIsTruncatedAndCleaned(t *testing.T) {
	long := strings.Repeat("y", 300) + "\n"
	body := mutateFixture(t, "pr_opened.json", func(payload map[string]any) {
		payload["pull_request"].(map[string]any)["labels"] = []any{map[string]any{"name": long}}
	})
	event := receive(t, newProvider(t, false), "pull_request", body)[0].Event
	for key, value := range event.Lifecycle.Labels {
		if !strings.HasPrefix(key, "tag.") {
			continue
		}
		if len([]rune(value)) != 256 {
			t.Fatalf("label value runes = %d, want 256", len([]rune(value)))
		}
		if strings.ContainsAny(value, "\n\r\t") {
			t.Fatalf("label value has a control character: %q", value)
		}
		return
	}
	t.Fatalf("no tag label in %+v", event.Lifecycle.Labels)
}

func TestResolveChannelAndTargetBoundaries(t *testing.T) {
	p := newProvider(t, false)
	ctx := context.Background()
	if channel, err := p.ResolveChannel(ctx, "github", "acme/www"); err != nil || channel != "acme/www:pulls" {
		t.Fatalf("bare repo = %q, %v", channel, err)
	}
	if channel, err := p.ResolveChannel(ctx, "github", "acme/www:issues"); err != nil || channel != "acme/www:issues" {
		t.Fatalf("issues channel = %q, %v", channel, err)
	}
	if _, err := p.ResolveChannel(ctx, "github", "other/repo"); !errors.Is(err, boardsync.ErrNotFound) {
		t.Fatalf("unknown repo channel = %v", err)
	}
	if _, err := p.ResolveChannel(ctx, "github", "acme/www:branches"); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("unknown channel suffix = %v", err)
	}
	if _, err := p.ResolveTarget(ctx, "github", "other/repo:1"); !errors.Is(err, boardsync.ErrNotFound) {
		t.Fatalf("unknown repo target = %v", err)
	}
	if _, err := p.ResolveTarget(ctx, "github", "acme/www:issue:45:rc:300"); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("issue review target = %v", err)
	}
}

func TestFetchRootIssue(t *testing.T) {
	f := newFakeGitHub(t)
	p := contractProvider(t, f.server.URL)
	draft, err := p.FetchRoot(context.Background(), "github", "acme/www:issue:45")
	if err != nil {
		t.Fatalf("FetchRoot issue: %v", err)
	}
	if !strings.Contains(draft.Text, "Issue #45: blank export page") {
		t.Fatalf("text = %q", draft.Text)
	}
	if draft.Labels["issue"] != "acme/www#45" || draft.Labels["state"] != "open" {
		t.Fatalf("labels = %+v", draft.Labels)
	}
	if draft.Author.ID != "github:user:1001" {
		t.Fatalf("author = %+v", draft.Author)
	}
}

func TestIssueStateFallbackAndInvalidHead(t *testing.T) {
	weird := mutateFixture(t, "issue_opened.json", func(payload map[string]any) {
		payload["issue"].(map[string]any)["state"] = "mystery"
	})
	event := receive(t, newProvider(t, false), "issues", weird)[0].Event
	if event.Lifecycle.Labels["state"] != "open" {
		t.Fatalf("issue state = %q", event.Lifecycle.Labels["state"])
	}
	badSHA := mutateFixture(t, "pr_opened.json", func(payload map[string]any) {
		payload["pull_request"].(map[string]any)["head"] = map[string]any{"sha": "not a sha"}
	})
	event = receive(t, newProvider(t, false), "pull_request", badSHA)[0].Event
	if _, exists := event.Lifecycle.Labels["head"]; exists {
		t.Fatalf("invalid SHA became a head label: %+v", event.Lifecycle.Labels)
	}
}

func TestDirectClientValidatesBaseURL(t *testing.T) {
	client := github.NewClient("http://api.github.com", nil, "token")
	if _, err := client.Viewer(context.Background()); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("NewClient cleartext viewer = %v", err)
	}
	offline := github.DoerFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("offline")
	})
	if _, err := github.NewClient("https://api.github.com", offline, "token").Viewer(context.Background()); err == nil || errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("valid base URL was rejected as configuration: %v", err)
	}
}

func TestMissingNumbersAreRejected(t *testing.T) {
	p := newProvider(t, false)
	pr := mutateFixture(t, "pr_opened.json", func(payload map[string]any) {
		payload["number"] = 0
		payload["pull_request"].(map[string]any)["number"] = 0
	})
	if _, err := receiveRequest(t, p, "pull_request", "n-1", pr); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("PR without number = %v", err)
	}
	item := mutateFixture(t, "issue_opened.json", func(payload map[string]any) {
		payload["issue"].(map[string]any)["number"] = 0
	})
	if _, err := receiveRequest(t, p, "issues", "n-2", item); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("issue without number = %v", err)
	}
	comment := mutateFixture(t, "issue_comment.json", func(payload map[string]any) {
		payload["issue"].(map[string]any)["number"] = 0
	})
	if _, err := receiveRequest(t, p, "issue_comment", "n-3", comment); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("comment on a numberless issue = %v", err)
	}
}
