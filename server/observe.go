package server

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/jixoai/keel-board/audit"
	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/metrics"
	"github.com/jixoai/keel-board/protocol"
	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/syncstore"
)

const (
	// auditWriteTimeout is the default bound on one audit write. A request or
	// worker blocks on an audit write at most this long; a timeout is logged
	// and never changes the result. Server.Options.AuditTimeout overrides it.
	auditWriteTimeout = time.Second

	// metricUnknownKey is the one bounded key label a request that hasn't
	// resolved to a configured, registered connection uses. A raw path segment
	// must never become a label: an unauthenticated caller could otherwise mint
	// series until the legitimate ones are dropped.
	metricUnknownKey = "unknown"

	// auditFailureBurst bounds durable audit rows a single configured key may
	// add per auditFailureWindow for verified failures (bad signatures, replayed
	// nonces, store errors). Further attempts still increment the request
	// counter; they just don't grow the append-only table without limit.
	auditFailureBurst  = 10
	auditFailureWindow = time.Minute
)

// boardObserver turns the board's bounded observations into metrics. It keeps
// a per-tenant live-claim count so the process-wide gauge is a sum, not
// whichever tenant wrote last. Tenants are accounting keys here, never metric
// labels.
type boardObserver struct {
	metrics *metrics.BoardMetrics
	logger  *slog.Logger
	mu      sync.Mutex
	claims  map[string]int
}

func newBoardObserver(boardMetrics *metrics.BoardMetrics, logger *slog.Logger) *boardObserver {
	if logger == nil {
		logger = slog.Default()
	}
	return &boardObserver{metrics: boardMetrics, logger: logger, claims: make(map[string]int)}
}

// Appended implements log.Observer.
func (o *boardObserver) Appended(tenant string, kinds []log.EventType, liveClaims int, took time.Duration) {
	// An instrumentation bug must never undo a committed write.
	defer o.recovered("board.append")
	if o.metrics == nil {
		return
	}
	expired := 0
	for _, kind := range kinds {
		o.metrics.Events.Inc(metrics.KindLabel(string(kind)))
		if kind == log.EventTaskExpired {
			expired++
		}
	}
	if expired > 0 {
		o.metrics.LeaseExpired.Add(float64(expired))
	}
	o.metrics.AppendDuration.Observe(metrics.Milliseconds(took))
	o.mu.Lock()
	if o.claims == nil {
		o.claims = make(map[string]int)
	}
	o.claims[tenant] = liveClaims
	total := 0
	for _, count := range o.claims {
		total += count
	}
	o.mu.Unlock()
	o.metrics.TasksLiveClaims.Set(float64(total))
}

// Rebuilt implements log.Observer.
func (o *boardObserver) Rebuilt(took time.Duration) {
	defer o.recovered("board.rebuild")
	if o.metrics == nil {
		return
	}
	o.metrics.ProjectionRebuildDuration.Observe(metrics.Milliseconds(took))
}

func (o *boardObserver) recovered(hook string) {
	if recovered := recover(); recovered != nil {
		o.logger.Error("observability hook panicked", "hook", hook, "panic", recovered)
	}
}

// auditWindow is one fixed-window counter for a key's failure audits.
type auditWindow struct {
	start time.Time
	count int
}

// syncTelemetry is the server's sync instrumentation and audit seam. It feeds
// the webhook handler, the bridge route, and the deployment's workers, which
// receive it as SyncWorkerHooks.
//
// Durable audit writing is deliberately gated: pre-authentication outcomes are
// metrics only, and a verified failure is durable at most auditFailureBurst
// times per key per window. An unauthenticated caller can therefore never grow
// board_audit without limit.
type syncTelemetry struct {
	metrics *metrics.BoardMetrics
	audit   audit.Store
	logger  *slog.Logger
	timeout time.Duration

	limitMu      sync.Mutex
	limitWindows map[string]auditWindow
}

// WebhookHandled implements syncstore.WebhookObserver.
func (t *syncTelemetry) WebhookHandled(obs syncstore.WebhookObservation) {
	if t == nil {
		return
	}
	defer t.recovered("webhook")
	if t.metrics != nil {
		key, platform := metricUnknownKey, metricUnknownKey
		if obs.Verified && obs.Key != "" && obs.Platform != "" {
			key, platform = string(obs.Key), string(obs.Platform)
		}
		// Every handled request is counted. An unverified probe lands on the
		// fixed unknown/unknown series, so arbitrary path segments can't mint
		// label values.
		t.metrics.SyncWebhookDuration.Observe(metrics.Milliseconds(obs.Took),
			metrics.KeyLabel(key), metrics.PlatformLabel(platform))
	}
	if obs.Tenant == "" || !obs.Verified {
		// Counted, never durable: a probe must not grow board_audit.
		return
	}
	if obs.Outcome.Accepted() {
		t.record(audit.Event{
			Tenant: obs.Tenant, Kind: audit.KindWebhookAccepted, Key: string(obs.Key),
			Platform: string(obs.Platform), Status: audit.StatusAccepted,
			Detail: webhookDetail(obs),
		})
		return
	}
	if !t.allowFailureAudit("webhook:"+string(obs.Key), time.Now()) {
		return
	}
	t.record(audit.Event{
		Tenant: obs.Tenant, Kind: audit.KindWebhookRejected, Key: string(obs.Key),
		Platform: string(obs.Platform), Status: string(obs.Outcome),
		Detail: webhookDetail(obs),
	})
}

func webhookDetail(obs syncstore.WebhookObservation) map[string]any {
	detail := map[string]any{"http": obs.Status}
	if obs.Added != 0 {
		detail["added"] = obs.Added
	}
	if obs.Skipped != 0 {
		detail["skipped"] = obs.Skipped
	}
	return detail
}

// WebhookDeduped implements syncstore.WebhookObserver.
func (t *syncTelemetry) WebhookDeduped(_ string, key boardsync.Key, hits int) {
	if t == nil {
		return
	}
	defer t.recovered("webhook.dedupe")
	if t.metrics == nil || hits <= 0 || key == "" {
		return
	}
	t.metrics.SyncDedupeHits.Add(float64(hits), metrics.KeyLabel(string(key)))
}

// bridgeFailed counts one failed bridge request. keyLabel must be a configured
// connection key only after the entry and its registration are confirmed;
// otherwise pass metricUnknownKey.
func (t *syncTelemetry) bridgeFailed(keyLabel string) {
	if t == nil {
		return
	}
	defer t.recovered("bridge.count")
	if t.metrics == nil || keyLabel == "" {
		return
	}
	t.metrics.SyncBridgeErrors.Inc(metrics.KeyLabel(keyLabel))
}

// bridgeAudit records a verified bridge failure (a refused signature, a replay,
// an unparseable batch, a store error) for a resolved tenant. It is
// rate-limited per key so a flood is counted but not persisted without bound.
func (t *syncTelemetry) bridgeAudit(tenant string, key boardsync.Key, platform boardsync.Platform, outcome string, status int) {
	if t == nil || tenant == "" {
		return
	}
	defer t.recovered("bridge.audit")
	if !t.allowFailureAudit("bridge:"+string(key), time.Now()) {
		return
	}
	t.record(audit.Event{
		Tenant: tenant, Kind: audit.KindBridgeRejected,
		Key: string(key), Platform: string(platform), Status: outcome,
		Detail: map[string]any{"http": status},
	})
}

// allowFailureAudit reports whether a verified failure for scope may add one
// durable row now. The map is bounded by configured connection keys; stale
// windows are dropped opportunistically.
func (t *syncTelemetry) allowFailureAudit(scope string, now time.Time) bool {
	t.limitMu.Lock()
	defer t.limitMu.Unlock()
	if t.limitWindows == nil {
		t.limitWindows = make(map[string]auditWindow)
	}
	if window, found := t.limitWindows[scope]; found && now.Sub(window.start) < auditFailureWindow {
		if window.count >= auditFailureBurst {
			return false
		}
		window.count++
		t.limitWindows[scope] = window
		return true
	}
	if len(t.limitWindows) > 1024 {
		for key, window := range t.limitWindows {
			if now.Sub(window.start) >= auditFailureWindow {
				delete(t.limitWindows, key)
			}
		}
	}
	t.limitWindows[scope] = auditWindow{start: now, count: 1}
	return true
}

// record persists one audit event. A failure is logged; it never changes the
// caller's result. timeout bounds how long a caller may block on it.
func (t *syncTelemetry) record(event audit.Event) {
	if t == nil || t.audit == nil {
		return
	}
	timeout := t.timeout
	if timeout <= 0 {
		timeout = auditWriteTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if _, err := t.audit.Append(ctx, event); err != nil {
		logger := t.logger
		if logger == nil {
			logger = slog.Default()
		}
		logger.Warn("board audit write failed", "kind", event.Kind, "error", err)
	}
}

func (t *syncTelemetry) recovered(hook string) {
	recovered := recover()
	if recovered == nil {
		return
	}
	logger := slog.Default()
	if t != nil && t.logger != nil {
		logger = t.logger
	}
	logger.Error("observability hook panicked", "hook", hook, "panic", recovered)
}

// recordAudit is the server's own audit write, for request-scoped paths such as
// manual retries. The write gets its own bounded context: a canceled request
// must not drop the row for a mutation that already committed.
func (s *Server) recordAudit(_ context.Context, event audit.Event) {
	if s.telemetry == nil {
		return
	}
	s.telemetry.record(event)
}

// recoverHook keeps a panicking worker hook from stopping a worker pass.
func (s *Server) recoverHook(hook string) {
	if recovered := recover(); recovered != nil {
		s.logger.Error("observability hook panicked", "hook", hook, "panic", recovered)
	}
}

// SyncWorkerHooks returns the worker hooks this server's instrumentation can
// serve. cmd/board passes them to each inbound and outbound worker. Without a
// metrics registry or an audit store every field is nil, so a worker built by
// a test changes nothing.
func (s *Server) SyncWorkerHooks() syncstore.WorkerHooks {
	if s.telemetry == nil || (s.telemetry.metrics == nil && s.telemetry.audit == nil) {
		return syncstore.WorkerHooks{}
	}
	return syncstore.WorkerHooks{
		InboxDead: func(tenant string, key boardsync.Key, platform boardsync.Platform, id string, attempts int, code string) {
			defer s.recoverHook("worker.inbox.dead")
			if s.telemetry.metrics != nil && key != "" {
				s.telemetry.metrics.SyncDeadLetters.Inc(metrics.KeyLabel(string(key)))
			}
			detail := map[string]any{"code": code}
			// id is the provider's own event identity, whose shape varies; it
			// is stored only when it is a bounded token.
			if audit.SafeToken(id) {
				detail["event"] = id
			}
			s.telemetry.record(audit.Event{
				Tenant: tenant, Kind: audit.KindInboxDead, Key: string(key),
				Platform: string(platform), Status: audit.StatusDead, Attempts: attempts,
				Detail: detail,
			})
		},
		OutboxDead: func(tenant string, key boardsync.Key, platform boardsync.Platform, delivery, target string, attempts int, code string) {
			defer s.recoverHook("worker.outbox.dead")
			if s.telemetry.metrics != nil && key != "" {
				s.telemetry.metrics.SyncDeadLetters.Inc(metrics.KeyLabel(string(key)))
			}
			event := audit.Event{
				Tenant: tenant, Kind: audit.KindOutboxDead, Key: string(key),
				Platform: string(platform), Status: audit.StatusDead, Attempts: attempts,
				Detail: map[string]any{"code": code},
			}
			// Platform target and delivery IDs come from outside the board;
			// one unusual shape must not drop the whole dead-letter row.
			if audit.SafeTarget(target) {
				event.Target = target
			}
			if audit.SafeDeliveryID(delivery) {
				event.DeliveryID = delivery
			}
			s.telemetry.record(event)
		},
		DeliveryFailed: func(tenant string, key boardsync.Key, platform boardsync.Platform, code string) {
			defer s.recoverHook("worker.delivery.failed")
			if s.telemetry.metrics != nil && key != "" && platform != "" {
				s.telemetry.metrics.SyncDeliveryFailures.Inc(
					metrics.KeyLabel(string(key)), metrics.PlatformLabel(string(platform)))
			}
		},
	}
}

// handleMetrics serves GET /metrics in Prometheus text format. It is at the
// root on purpose: scraping must not need a board token, and the exposition
// carries no tenant, message, or secret.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if s.registry == nil {
		writeError(w, protocol.NotFound(protocol.CodeNotFound, "no such route"))
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	if err := s.registry.WritePrometheus(w); err != nil {
		s.logger.Warn("writing the metrics exposition failed", "error", err)
	}
}

// observeSSEOpen counts one open stream and returns its release.
func (s *Server) observeSSEOpen() func() {
	if s.counters == nil {
		return func() {}
	}
	s.counters.SSEConnections.Inc()
	return func() { s.counters.SSEConnections.Dec() }
}

// observeSSELag records how far a stream's cursor trails the head. The gauge is
// process-wide (the design names no per-connection label): the newest stream
// sets it, and concurrent streams overwrite one another.
func (s *Server) observeSSELag(head, cursor uint64) {
	if s.counters == nil {
		return
	}
	lag := uint64(0)
	if head > cursor {
		lag = head - cursor
	}
	s.counters.SSELagSeq.Set(float64(lag))
}

// observeInboxDepth translates a status read into the depth gauges. Absent
// states are written as zero so a status that emptied doesn't leave a stale
// value behind. The values are scrape-driven: they change when GET
// /sync/status runs.
func (s *Server) observeInboxDepth(key boardsync.Key, depth syncstore.InboxDepth) {
	if s.counters == nil {
		return
	}
	label := metrics.KeyLabel(string(key))
	s.counters.SyncInboxDepth.Set(float64(depth.Pending), label, metrics.StatusLabel(syncstore.StatePending))
	s.counters.SyncInboxDepth.Set(float64(depth.Leased), label, metrics.StatusLabel(syncstore.StateLeased))
	s.counters.SyncInboxDepth.Set(float64(depth.Done), label, metrics.StatusLabel(syncstore.StateDone))
	s.counters.SyncInboxDepth.Set(float64(depth.Dead), label, metrics.StatusLabel(syncstore.StateDead))
}

// observeOutboxDepth translates one key's outbox read into the depth gauges.
func (s *Server) observeOutboxDepth(key boardsync.Key, depth syncstore.OutboxDepth) {
	if s.counters == nil {
		return
	}
	label := metrics.KeyLabel(string(key))
	s.counters.SyncOutboxDepth.Set(float64(depth.Pending), label, metrics.StatusLabel(syncstore.StatePending))
	s.counters.SyncOutboxDepth.Set(float64(depth.Leased), label, metrics.StatusLabel(syncstore.StateLeased))
	s.counters.SyncOutboxDepth.Set(float64(depth.Done), label, metrics.StatusLabel(syncstore.StateDone))
	s.counters.SyncOutboxDepth.Set(float64(depth.Dead), label, metrics.StatusLabel(syncstore.StateDead))
}
