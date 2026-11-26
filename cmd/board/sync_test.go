package main

import "testing"

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
