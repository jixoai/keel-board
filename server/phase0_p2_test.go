package server_test

import (
	"net/http"
	"strings"
	"testing"
)

// TestDiscoveryNamesWhetherSyncIsConfigured proves discovery is honest about
// the process's sync wiring without listing tenant entries.
func TestDiscoveryNamesWhetherSyncIsConfigured(t *testing.T) {
	h := newSyncHarness(t, syncHarnessOptions{})
	status, body := h.request(http.MethodGet, "/v1/board", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("discovery = %d %s", status, body)
	}
	if !strings.Contains(string(body), `"syncConfigured":true`) {
		t.Fatalf("discovery doesn't report sync: %s", body)
	}
}
