package access

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

func TestParsePattern(t *testing.T) {
	cases := []struct {
		value   string
		kind    PatternKind
		path    string
		wantErr bool
	}{
		{value: "*", kind: PatternAll},
		{value: "feedback", kind: PatternExact, path: "feedback"},
		{value: "feedback/", kind: PatternSubtree, path: "feedback"},
		{value: "a/b/c", kind: PatternExact, path: "a/b/c"},
		{value: "a/b/", kind: PatternSubtree, path: "a/b"},
		{value: "", wantErr: true},
		{value: "/", wantErr: true},
		{value: "a//b", wantErr: true},
		{value: "Feedback", wantErr: true},
		{value: "feedback/*", wantErr: true},
		{value: "*feedback", wantErr: true},
		{value: "feedback/**", wantErr: true},
	}
	for _, test := range cases {
		t.Run(test.value, func(t *testing.T) {
			pattern, err := ParsePattern(test.value)
			if test.wantErr {
				var invalid *InvalidCan
				if !errors.As(err, &invalid) {
					t.Fatalf("ParsePattern(%q) error = %v, want InvalidCan", test.value, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParsePattern(%q): %v", test.value, err)
			}
			if pattern.Kind != test.kind || pattern.Path != test.path {
				t.Fatalf("ParsePattern(%q) = %+v, want kind %s path %q", test.value, pattern, test.kind, test.path)
			}
			if got := pattern.String(); got != test.value {
				t.Fatalf("pattern.String() = %q, want %q", got, test.value)
			}
		})
	}
}

func TestParsePatternHints(t *testing.T) {
	_, err := ParsePattern("feedback/*")
	var invalid *InvalidCan
	if !errors.As(err, &invalid) {
		t.Fatalf("expected InvalidCan, got %v", err)
	}
	want := `invalid channel pattern "feedback/*": write feedback/ for feedback and its subtree`
	if invalid.Error() != want {
		t.Fatalf("message = %q, want %q", invalid.Error(), want)
	}
}

func TestPatternCovers(t *testing.T) {
	all, _ := ParsePattern("*")
	if !all.Covers("anything") || !all.Covers("a/b") {
		t.Fatal("* should cover every channel")
	}
	exact, _ := ParsePattern("dev")
	if !exact.Covers("dev") || exact.Covers("dev/ci") || exact.Covers("devil") {
		t.Fatal("an exact pattern should cover only itself")
	}
	subtree, _ := ParsePattern("dev/")
	for _, channel := range []string{"dev", "dev/ci", "dev/ci/x"} {
		if !subtree.Covers(channel) {
			t.Fatalf("dev/ should cover %q", channel)
		}
	}
	for _, channel := range []string{"devil", "other", ""} {
		if subtree.Covers(channel) {
			t.Fatalf("dev/ should not cover %q", channel)
		}
	}
}

func TestSubPrefix(t *testing.T) {
	cases := []struct {
		value  string
		prefix string
		ok     bool
	}{
		{value: "project_path:acme/api:*", prefix: "project_path:acme/api:", ok: true},
		{value: "repo:acme/*", prefix: "repo:acme/", ok: true},
		{value: "acme*", ok: false},
		{value: "*", ok: false},
		{value: "acme:*:x", ok: false},
		{value: "acme:api", ok: false},
	}
	for _, test := range cases {
		prefix, ok := SubPrefix(test.value)
		if ok != test.ok || prefix != test.prefix {
			t.Fatalf("SubPrefix(%q) = %q, %v, want %q, %v", test.value, prefix, ok, test.prefix, test.ok)
		}
	}
}

func TestCanJSON(t *testing.T) {
	can := Can{
		"*":      All(),
		"dev/":   Only(PermissionPost, PermissionRead),
		"secret": None(),
	}
	encoded, err := json.Marshal(can)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded Can
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(can, decoded) {
		t.Fatalf("round trip = %#v, want %#v", decoded, can)
	}
	if string(encoded) != `{"*":"all","dev/":["read","post"],"secret":[]}` {
		t.Fatalf("json = %s", encoded)
	}
}

func TestParseCanText(t *testing.T) {
	can, err := ParseCan("dev/=read,post;ci/=all;old/=")
	if err != nil {
		t.Fatalf("ParseCan: %v", err)
	}
	want := Can{
		"dev/": Only(PermissionRead, PermissionPost),
		"ci/":  All(),
		"old/": None(),
	}
	if !reflect.DeepEqual(can, want) {
		t.Fatalf("ParseCan = %#v, want %#v", can, want)
	}
	if text := CanText(can); text != "ci/=all;dev/=read,post;old/=" {
		t.Fatalf("CanText = %q", text)
	}
	if _, err := ParseCan("dev/=nope"); err == nil {
		t.Fatal("unknown permission should fail")
	}
	if _, err := ParseCan(""); err == nil {
		t.Fatal("empty can should fail")
	}
}
