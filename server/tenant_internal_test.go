package server

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jixoai/keel-board/log"
)

func TestResolveNormalizesTenantWhitespace(t *testing.T) {
	server := New(Options{
		Board:  log.New(log.Options{}),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Tenant: func(*http.Request) (string, error) { return "   ", nil },
	})
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, APIPrefix, nil)
	resolved, ok := server.resolve(recorder, request)
	if !ok {
		t.Fatalf("resolve refused: %s", recorder.Body.String())
	}
	if resolved.tenant != "default" {
		t.Fatalf("tenant %q, want default", resolved.tenant)
	}
}
