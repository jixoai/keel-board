package syncstore

import (
	"errors"
	"log/slog"
	"net/http"
	"strings"

	boardsync "github.com/jixoai/keel-board/sync"
)

// TenantFunc resolves the tenant a webhook belongs to. The server supplies it
// from its own authentication; the handler never trusts a tenant named by the
// platform's payload or a header by itself.
type TenantFunc func(*http.Request) (string, error)

// GateFactory builds the gate for one resolved connection. A handler must not
// share a static gate across tenants: the factory's tenant/key are the scope
// the gate must answer for. Return an error to make the webhook answer 500.
type GateFactory func(tenant string, key boardsync.Key) (boardsync.Gate, error)

// WebhookConfig configure a webhook handler.
type WebhookConfig struct {
	// Registry holds the sync entries and their providers. Required.
	Registry *boardsync.Registry
	// Store is the durable inbox. Required.
	Store Store
	// Tenant resolves the board a request is on. Required.
	Tenant TenantFunc
	// GateFactory builds the gate per (tenant, key). A nil factory leaves the
	// gate nil: only a record whose channel the entry's own Routes names is
	// kept (fail closed for thread matching).
	GateFactory GateFactory
	// MaxBodyBytes caps the request body before a provider parses it. Zero
	// means one MiB.
	MaxBodyBytes int64
	// Logger receives failures. Zero uses slog.Default.
	Logger *slog.Logger
}

// WebhookHandler serves POST /webhooks/{key}. It is independent of the board
// server's route table: the server mounts it when it is ready.
//
// The handler resolves the tenant and the persisted entry first. A key this
// tenant doesn't own, or a disabled entry, is a silent 200 with no body read;
// an owned enabled entry this process hasn't registered is a 503; a platform
// mismatch is a 500. A deployment must UpsertEntry for a tenant before serving
// its webhooks.
// It then delegates to Registry.HandleWebhook, which verifies and normalizes
// through the provider, gates the records, and stores them before the handler
// answers. A store failure becomes a 500 and the platform retries; the handler
// never answers 2xx before the inbox commit.
type WebhookHandler struct {
	registry    *boardsync.Registry
	store       Store
	tenant      TenantFunc
	gateFactory GateFactory
	maxBody     int64
	logger      *slog.Logger
}

// NewWebhookHandler validates cfg and returns a handler.
func NewWebhookHandler(cfg WebhookConfig) (*WebhookHandler, error) {
	if cfg.Registry == nil {
		return nil, errors.New("syncstore: a webhook handler needs a registry")
	}
	if cfg.Store == nil {
		return nil, errors.New("syncstore: a webhook handler needs a store")
	}
	if cfg.Tenant == nil {
		return nil, errors.New("syncstore: a webhook handler needs a tenant resolver")
	}
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = 1 << 20
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &WebhookHandler{
		registry:    cfg.Registry,
		store:       cfg.Store,
		tenant:      cfg.Tenant,
		gateFactory: cfg.GateFactory,
		maxBody:     cfg.MaxBodyBytes,
		logger:      cfg.Logger,
	}, nil
}

// ServeHTTP implements http.Handler.
func (h *WebhookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeStatus(w, http.StatusMethodNotAllowed)
		return
	}
	tenant, err := h.tenant(r)
	if err != nil {
		h.logger.Warn("sync webhook tenant refused", "error", ErrorCode(err))
		writeStatus(w, http.StatusUnauthorized)
		return
	}
	if err := validTenant(tenant); err != nil {
		h.logger.Warn("sync webhook tenant is not usable", "error", ErrorCode(err))
		writeStatus(w, http.StatusInternalServerError)
		return
	}
	key, ok := webhookKey(r)
	if !ok {
		writeStatus(w, http.StatusNotFound)
		return
	}
	entry, found, err := h.store.Entry(r.Context(), tenant, key)
	if err != nil {
		h.logger.Error("sync webhook entry lookup failed", "tenant", tenant, "key", key, "error", ErrorCode(err))
		writeStatus(w, http.StatusInternalServerError)
		return
	}
	if !found || !entry.Enabled {
		// A key this tenant doesn't own, and one it deliberately disabled, are
		// both accepted without reading the body or storing anything. The
		// platform stops retrying, and a caller can't probe which connections
		// exist.
		writeStatus(w, http.StatusOK)
		return
	}
	registered, ok := h.registry.Get(tenant, key)
	if !ok {
		// The tenant owns this enabled connection but this process hasn't
		// registered it (still starting). Answer 5xx so the platform retries;
		// 404 would drop the event.
		h.logger.Warn("sync webhook entry is not registered", "tenant", tenant, "key", key)
		writeStatus(w, http.StatusServiceUnavailable)
		return
	}
	if registered.Platform != entry.Platform || registered.Provider.Platform() != entry.Platform {
		h.logger.Error("sync webhook platform mismatch", "tenant", tenant, "key", key,
			"entryPlatform", entry.Platform, "providerPlatform", registered.Provider.Platform())
		writeStatus(w, http.StatusInternalServerError)
		return
	}
	var gate boardsync.Gate
	if h.gateFactory != nil {
		gate, err = h.gateFactory(tenant, key)
		if err != nil {
			h.logger.Error("sync webhook gate failed", "tenant", tenant, "key", key, "error", ErrorCode(err))
			writeStatus(w, http.StatusInternalServerError)
			return
		}
	}
	request, err := boardsync.NewWebhookRequest(r, h.maxBody)
	if err != nil {
		writeStatus(w, http.StatusRequestEntityTooLarge)
		return
	}
	response, err := h.registry.HandleWebhook(r.Context(), tenant, key, request, gate,
		NewTenantInbox(h.store, tenant, "webhook"))
	if err != nil {
		status := response.Status
		if status == 0 {
			status = boardsync.StatusCode(err)
		}
		if status < 400 {
			status = http.StatusInternalServerError
		}
		// The body is a generic status: a provider or store error may carry a
		// platform payload, and none of it belongs in a response.
		h.logger.Warn("sync webhook failed",
			"tenant", tenant, "key", key, "status", status, "error", ErrorCode(err))
		writeStatus(w, status)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	response.WriteTo(w)
}

// webhookKey reads the {key} path segment. PathValue is used when the handler
// is mounted with a pattern; the trailing segment is accepted when it is
// mounted behind a plain prefix such as /webhooks/.
func webhookKey(r *http.Request) (boardsync.Key, bool) {
	value := r.PathValue("key")
	if value == "" {
		const prefix = "/webhooks/"
		if !strings.HasPrefix(r.URL.Path, prefix) {
			return "", false
		}
		value = strings.TrimPrefix(r.URL.Path, prefix)
	}
	if value == "" || strings.Contains(value, "/") {
		return "", false
	}
	key := boardsync.Key(value)
	if !key.Valid() {
		return "", false
	}
	return key, true
}

func writeStatus(w http.ResponseWriter, status int) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
}
