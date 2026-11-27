package server

import (
	"net/http"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/chatsdk"
	"github.com/jixoai/keel-board/syncstore"
)

// bridgeEvents serves POST /v1/board/sync/bridge/{key}/events. A Chat SDK
// sidecar authenticates with HMAC v1 (timestamp, nonce, signature over
// timestamp+nonce+body), the board claims the nonce durably, validates the
// normalized batch, and stores it before answering. It never reads a platform
// secret and never calls a URL named by the payload.
func (s *syncService) bridgeEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeBridgeStatus(w, http.StatusMethodNotAllowed)
		return
	}
	tenant, err := s.tenant(r)
	if err != nil {
		s.logger.Warn("sync bridge tenant refused", "error", syncstore.ErrorCode(err))
		writeBridgeStatus(w, http.StatusUnauthorized)
		return
	}
	if !boardsync.ValidTenant(tenant) {
		s.logger.Warn("sync bridge tenant is not usable")
		writeBridgeStatus(w, http.StatusInternalServerError)
		return
	}
	key, ok := bridgeKey(r)
	if !ok {
		writeBridgeStatus(w, http.StatusNotFound)
		return
	}
	entry, found, err := s.store.Entry(r.Context(), tenant, key)
	if err != nil {
		s.logger.Error("sync bridge entry lookup failed", "tenant", tenant, "key", key, "error", syncstore.ErrorCode(err))
		writeBridgeStatus(w, http.StatusInternalServerError)
		return
	}
	if !found {
		writeBridgeStatus(w, http.StatusNotFound)
		return
	}
	if !entry.Enabled {
		// A disabled entry is accepted without reading the body so the
		// sidecar stops retrying a connection the operator switched off.
		writeBridgeStatus(w, http.StatusOK)
		return
	}
	registered, ok := s.registry.Get(tenant, key)
	if !ok {
		s.logger.Warn("sync bridge entry is not registered", "tenant", tenant, "key", key)
		writeBridgeStatus(w, http.StatusServiceUnavailable)
		return
	}
	if registered.Platform != entry.Platform || registered.Provider.Platform() != entry.Platform {
		s.logger.Error("sync bridge platform mismatch", "tenant", tenant, "key", key,
			"entryPlatform", entry.Platform, "providerPlatform", registered.Provider.Platform())
		writeBridgeStatus(w, http.StatusInternalServerError)
		return
	}
	if s.bridgeSecret == nil {
		s.logger.Error("sync bridge secret resolver is not configured", "tenant", tenant, "key", key)
		writeBridgeStatus(w, http.StatusServiceUnavailable)
		return
	}
	secret, ok := s.bridgeSecret(tenant, key)
	if !ok || secret.Empty() {
		// Fail closed: an entry without a secret must not accept events.
		s.logger.Error("sync bridge secret is missing", "tenant", tenant, "key", key)
		writeBridgeStatus(w, http.StatusServiceUnavailable)
		return
	}
	request, err := boardsync.NewWebhookRequest(r, chatsdk.MaxBodyBytes)
	if err != nil {
		writeBridgeStatus(w, http.StatusRequestEntityTooLarge)
		return
	}
	if err := chatsdk.VerifyRequestV1(secret, request.Header, request.Body, time.Now(), chatsdk.DefaultWindow); err != nil {
		// The error text intentionally stays out of the log: it could quote a
		// header. The status is the diagnosis.
		s.logger.Warn("sync bridge event signature refused", "tenant", tenant, "key", key, "status", http.StatusUnauthorized)
		writeBridgeStatus(w, http.StatusUnauthorized)
		return
	}
	records, err := chatsdk.DecodeInbound(request.Body, key, entry.Platform)
	if err != nil {
		s.logger.Warn("sync bridge batch refused", "tenant", tenant, "key", key, "error", syncstore.ErrorCode(err))
		writeBridgeStatus(w, http.StatusBadRequest)
		return
	}
	nonce := request.Header.Get(chatsdk.HeaderNonce)
	claimed, err := s.store.ClaimNonce(r.Context(), tenant, key, nonce, chatsdk.DefaultWindow)
	if err != nil {
		s.logger.Error("sync bridge nonce claim failed", "tenant", tenant, "key", key, "error", syncstore.ErrorCode(err))
		writeBridgeStatus(w, http.StatusInternalServerError)
		return
	}
	if !claimed {
		s.logger.Warn("sync bridge nonce replayed", "tenant", tenant, "key", key, "status", http.StatusConflict)
		writeBridgeStatus(w, http.StatusConflict)
		return
	}
	stored, err := s.registry.StoreRecords(r.Context(), tenant, key, records, syncstore.StoreGate{Store: s.store},
		syncstore.NewTenantInbox(s.store, tenant, "bridge"))
	if err != nil {
		s.logger.Error("sync bridge store failed", "tenant", tenant, "key", key, "error", syncstore.ErrorCode(err))
		writeBridgeStatus(w, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, map[string]int{"stored": stored})
}

// bridgeKey reads the {key} path segment.
func bridgeKey(r *http.Request) (boardsync.Key, bool) {
	value := r.PathValue("key")
	if value == "" {
		return "", false
	}
	key := boardsync.Key(value)
	if !key.Valid() {
		return "", false
	}
	return key, true
}

func writeBridgeStatus(w http.ResponseWriter, status int) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
}
