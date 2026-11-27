package main

import (
	"context"
	"strings"
	"testing"

	boardsync "github.com/jixoai/keel-board/sync"
)

func TestParseSyncFlags(t *testing.T) {
	flags := &syncFlags{
		tenant:  "t1",
		keys:    []string{"slack=slack", "acme=slack"},
		mirrors: []string{"slack=C0123:support", "acme=C0999:support"},
		starts:  []string{"slack=C0123"},
	}
	configs, err := flags.parseSync()
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(configs) != 2 {
		t.Fatalf("configs = %d", len(configs))
	}
	slack := configs["slack"]
	if slack == nil || slack.mirrors["C0123"] != "support" {
		t.Fatalf("slack = %+v", slack)
	}
	if len(slack.starts) != 1 || slack.starts[0] != "C0123" {
		t.Fatalf("slack starts = %+v", slack.starts)
	}
	// An explicit start list doesn't grow the mirror defaults.
	acme := configs["acme"]
	if len(acme.starts) != 1 || acme.starts[0] != "C0999" {
		t.Fatalf("acme default starts = %+v", acme.starts)
	}

	for _, test := range []struct {
		name  string
		flags syncFlags
	}{
		{"key without platform", syncFlags{keys: []string{"slack"}, mirrors: []string{"slack=C:s"}}},
		{"mirror without key", syncFlags{keys: []string{"slack=slack"}, mirrors: []string{"other=C:s"}}},
		{"key without mirror", syncFlags{keys: []string{"slack=slack"}}},
		{"bad mirror", syncFlags{keys: []string{"slack=slack"}, mirrors: []string{"slack=C"}}},
		{"duplicate key", syncFlags{keys: []string{"slack=slack", "slack=github"}, mirrors: []string{"slack=C:s"}}},
	} {
		if _, err := test.flags.parseSync(); err == nil {
			t.Fatalf("%s: parsed without an error", test.name)
		}
	}
}

func TestBuildProviderRequiresBotIdentity(t *testing.T) {
	t.Setenv("KEEL_SYNC_SLACK_SIGNING_SECRET", "signing")
	slackConfig := &keyConfig{key: "slack", platform: "slack", mirrors: map[string]string{"C1": "support"}}
	if _, err := buildProvider(slackConfig); err == nil {
		t.Fatal("slack without a bot identity should be refused")
	}
	t.Setenv("KEEL_SYNC_SLACK_BOT_USER_ID", "U1")
	if _, err := buildProvider(slackConfig); err != nil {
		t.Fatalf("slack with an identity: %v", err)
	}

	t.Setenv("KEEL_SYNC_LINEAR_WEBHOOK_SECRET", "signing")
	t.Setenv("KEEL_SYNC_LINEAR_TEAMS", "ENG")
	linearConfig := &keyConfig{key: "linear", platform: "linear", mirrors: map[string]string{"ENG": "support"}}
	if _, err := buildProvider(linearConfig); err == nil {
		t.Fatal("linear without a bot identity should be refused")
	}
	t.Setenv("KEEL_SYNC_LINEAR_BOT_USER_ID", "app-1")
	if _, err := buildProvider(linearConfig); err != nil {
		t.Fatalf("linear with an identity: %v", err)
	}
}

func TestSlackJoinChannelsFromEnv(t *testing.T) {
	t.Setenv("KEEL_SYNC_SLACK_SIGNING_SECRET", "signing")
	t.Setenv("KEEL_SYNC_SLACK_BOT_USER_ID", "U1")
	// Default: join is off.
	cfg, err := slackConfigFromEnv("slack")
	if err != nil {
		t.Fatalf("slack config: %v", err)
	}
	if cfg.JoinChannels {
		t.Fatal("join is on without KEEL_SYNC_SLACK_JOIN_CHANNELS")
	}
	// buildProvider goes through the same config, so a valid true is enough
	// to construct the adapter.
	t.Setenv("KEEL_SYNC_SLACK_JOIN_CHANNELS", "true")
	cfg, err = slackConfigFromEnv("slack")
	if err != nil || !cfg.JoinChannels {
		t.Fatalf("join true = %+v, %v", cfg, err)
	}
	if _, err := buildProvider(&keyConfig{key: "slack", platform: "slack", mirrors: map[string]string{"C1": "support"}}); err != nil {
		t.Fatalf("slack provider with join: %v", err)
	}
	t.Setenv("KEEL_SYNC_SLACK_JOIN_CHANNELS", "FALSE")
	cfg, err = slackConfigFromEnv("slack")
	if err != nil || cfg.JoinChannels {
		t.Fatalf("join FALSE = %+v, %v", cfg, err)
	}
	// Strict: only true/false are accepted.
	t.Setenv("KEEL_SYNC_SLACK_JOIN_CHANNELS", "yes")
	if _, err := slackConfigFromEnv("slack"); err == nil || !strings.Contains(err.Error(), "KEEL_SYNC_SLACK_JOIN_CHANNELS") {
		t.Fatalf("join yes error = %v", err)
	}
}

func TestBuildProviderChatSDK(t *testing.T) {
	t.Setenv("KEEL_SYNC_TELEGRAM_PLATFORM", "telegram")
	t.Setenv("KEEL_SYNC_TELEGRAM_BRIDGE_URL", "http://127.0.0.1:9")
	t.Setenv("KEEL_SYNC_TELEGRAM_BRIDGE_SECRET", "bridge-secret")
	config := &keyConfig{key: "telegram", platform: "chatsdk", mirrors: map[string]string{"-100": "support"}}
	if _, err := buildProvider(config); err == nil {
		t.Fatal("chatsdk without self IDs should be refused")
	}
	t.Setenv("KEEL_SYNC_TELEGRAM_SELF_IDS", "telegram:user:bot")
	provider, err := buildProvider(config)
	if err != nil {
		t.Fatalf("chatsdk provider: %v", err)
	}
	if provider.Platform() != "telegram" {
		t.Fatalf("platform = %q", provider.Platform())
	}
	self, ok := provider.(boardsync.SelfIdentifier)
	if !ok || len(self.SelfIDs()) != 1 || self.SelfIDs()[0] != "telegram:user:bot" {
		t.Fatalf("self IDs = %+v", self)
	}
	t.Setenv("KEEL_SYNC_TELEGRAM_BRIDGE_URL", "")
	if _, err := buildProvider(config); err == nil {
		t.Fatal("chatsdk without a bridge URL should be refused")
	}
}

func TestBuildSyncOptionsChatSDK(t *testing.T) {
	t.Setenv("KEEL_SYNC_TELEGRAM_PLATFORM", "telegram")
	t.Setenv("KEEL_SYNC_TELEGRAM_BRIDGE_URL", "http://127.0.0.1:9")
	t.Setenv("KEEL_SYNC_TELEGRAM_BRIDGE_SECRET", "bridge-secret")
	t.Setenv("KEEL_SYNC_TELEGRAM_SELF_IDS", "telegram:user:bot")
	runtime, err := buildSyncOptions(context.Background(), &syncFlags{
		tenant:  "t1",
		keys:    []string{"telegram=chatsdk"},
		mirrors: []string{"telegram=-100:support"},
	}, "t1")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	t.Cleanup(runtime.closeStore)
	entry, ok := runtime.registry.Get("t1", "telegram")
	if !ok || entry.Platform != "telegram" {
		t.Fatalf("entry = %+v, %v", entry, ok)
	}
	stored, found, err := runtime.store.Entry(context.Background(), "t1", "telegram")
	if err != nil || !found || stored.Platform != "telegram" {
		t.Fatalf("stored entry = %+v, %v, %v", stored, found, err)
	}
	if secret := runtime.bridgeSecrets["telegram"]; secret != "bridge-secret" {
		t.Fatalf("bridge secret = %q", secret)
	}
}
