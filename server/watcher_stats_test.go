package server_test

import (
	"errors"
	"net/http"
	"testing"
)

// TestWatcherViewCarriesStats proves the management DTO exposes the real
// delivery facts: dead count, the latest stable error code, and last activity.
func TestWatcherViewCarriesStats(t *testing.T) {
	h, store := newAutomationHarness(t)
	const alice = "local:user:alice"
	status, data := h.request(http.MethodPost, "/watchers", map[string]any{
		"id": "alerts", "channel": "work",
		"action": map[string]any{"name": "post", "params": map[string]string{"text": "x"}},
	}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)

	if _, err := store.AdvanceWatcher(t.Context(), "default", "alerts", 2, []uint64{1, 2}); err != nil {
		t.Fatalf("advance: %v", err)
	}
	leased, err := store.LeaseDeliveries(t.Context(), "default", 10, "test")
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease = %+v, %v", leased, err)
	}
	for attempt := 0; attempt < 3; attempt++ {
		ack, err := store.AckDelivery(t.Context(), "default", "alerts", leased[0].EventSeq, leased[0].Lease, errors.New("rate_limited"))
		if err != nil {
			t.Fatalf("ack %d: %v", attempt, err)
		}
		if ack.Dead {
			break
		}
		leased, err = store.LeaseDeliveries(t.Context(), "default", 10, "test")
		if err != nil || len(leased) != 1 {
			t.Fatalf("retry lease = %+v, %v", leased, err)
		}
	}

	status, data = h.request(http.MethodGet, "/watchers", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	var list struct {
		Watchers []struct {
			ID             string `json:"id"`
			Dead           int    `json:"dead"`
			LastError      string `json:"lastError"`
			LastDeliveryAt string `json:"lastDeliveryAt"`
		} `json:"watchers"`
	}
	if err := jsonUnmarshal(data, &list); err != nil {
		t.Fatalf("decode: %v (%s)", err, data)
	}
	if len(list.Watchers) != 1 {
		t.Fatalf("watchers = %+v", list.Watchers)
	}
	watcher := list.Watchers[0]
	if watcher.Dead != 1 || watcher.LastError == "" || watcher.LastDeliveryAt == "" {
		t.Fatalf("watcher stats = %+v", watcher)
	}
	if status, data := h.request(http.MethodGet, "/watchers/alerts", nil, alice, "other", nil); status != http.StatusNotFound {
		t.Fatalf("cross-tenant watcher = %d: %s", status, data)
	}
}
