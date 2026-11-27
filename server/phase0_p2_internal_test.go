package server

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/jixoai/keel-board/protocol"
)

// TestAsProtocolDoesNotEchoHostError proves a host hook's raw error text never
// becomes a response body. A custom TenantFunc/Authorizer error can carry a
// DSN, URL, or credential context.
func TestAsProtocolDoesNotEchoHostError(t *testing.T) {
	got := asProtocol(errors.New("dsn=postgres://user:secret@db/x"))
	if got == nil || got.Status != http.StatusForbidden || got.Code != protocol.CodeForbidden {
		t.Fatalf("asProtocol = %+v", got)
	}
	if strings.Contains(got.Message, "postgres") || strings.Contains(got.Message, "secret") {
		t.Fatalf("asProtocol leaked the host error: %q", got.Message)
	}
}

// TestVersionIsTheReleaseVersion keeps discovery honest after the v0.1.1 tag.
func TestVersionIsTheReleaseVersion(t *testing.T) {
	if Version != "0.1.1" {
		t.Fatalf("Version = %q, want 0.1.1", Version)
	}
}
