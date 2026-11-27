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

// auditWriteTimeout bounds one audit write. An audit failure is logged and the
// request keeps its own result; it never fails a webhook or a retry.
const auditWriteTimeout = time.Second

// boardObserver turns the board's bounded observations into metrics. It keeps
// a per-tenant live-claim count so the process-wide gauge is a sum, not
// whichever tenant wrote last. Tenants are accounting keys here, never metric
// labels.
type boardObserver struct {
	metrics *metrics.BoardMetrics
	mu      sync.Mutex
	claims  map[string]int
}

func newBoardObserver(boardMetrics *metrics.BoardMetrics) *boardObserver {
	return &boardObserver{metrics: boardMetrics, claims: make(map[string]int)}
}

// Appended implements log.Observer.
func (o *boardObserver) Appended(tenant string, kinds []log.EventType, liveClaims int, took time.Duration) {
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
	if o.metrics == nil {
		return
	}
	o.metrics.ProjectionRebuildDuration.Observe(metrics.Milliseconds(took))
}

// syncTelemetry is the server's sync instrumentation and audit seam. It feeds
// the webhook handler, the bridge route, and the deployment's workers, which
// receive it as SyncWorkerHooks.
type syncTelemetry struct {
	metrics *metrics.BoardMetrics
	audit   audit.Store
	logger  *slog.Logger
}

// WebhookHandled implements syncstore.WebhookObserver.
func (t *syncTelemetry) WebhookHandled(obs syncstore.WebhookObservation) {
	if t == nil {
		return
	}
	if t.metrics != nil && obs.Key != "" && obs.Platform != "" {
		t.metrics.SyncWebhookDuration.Observe(metrics.Milliseconds(obs.Took),
			metrics.KeyLabel(string(obs.Key)), metrics.PlatformLabel(string(obs.Platform)))
	}
	if obs.Tenant == "" {
		// A refused tenant can't be attributed to a board, so it has no audit
		// row: writing one would put a row in another tenant's namespace.
		return
	}
	kind, status := audit.KindWebhookRejected, string(obs.Outcome)
	if obs.Outcome.Accepted() {
		kind, status = audit.KindWebhookAccepted, audit.StatusAccepted
	}
	detail := map[string]any{"http": obs.Status}
	if obs.Added != 0 {
		detail["added"] = obs.Added
	}
	if obs.Skipped != 0 {
		detail["skipped"] = obs.Skipped
	}
	t.record(audit.Event{
		Tenant: obs.Tenant, Kind: kind, Key: string(obs.Key),
		Platform: string(obs.Platform), Status: status, Detail: detail,
	})
}

// WebhookDeduped implements syncstore.WebhookObserver.
func (t *syncTelemetry) WebhookDeduped(_ string, key boardsync.Key, hits int) {
	if t == nil {
		return
	}
	if t.metrics == nil || hits <= 0 || key == "" {
		return
	}
	t.metrics.SyncDedupeHits.Add(float64(hits), metrics.KeyLabel(string(key)))
}

// bridgeRejected counts one failed bridge request and audits it. outcome is a
// bounded token ("signature", "replay", "unregistered", ...), never an error
// string that might quote a header.
func (t *syncTelemetry) bridgeRejected(tenant string, key boardsync.Key, platform boardsync.Platform, outcome string, status int) {
	if t == nil {
		return
	}
	if t.metrics != nil && key != "" {
		t.metrics.SyncBridgeErrors.Inc(metrics.KeyLabel(string(key)))
	}
	if tenant == "" {
		return
	}
	t.record(audit.Event{
		Tenant: tenant, Kind: audit.KindBridgeRejected,
		Key: string(key), Platform: string(platform), Status: outcome,
		Detail: map[string]any{"http": status},
	})
}

// record persists one audit event. A failure is logged; it never changes the
// caller's result.
func (t *syncTelemetry) record(event audit.Event) {
	if t == nil || t.audit == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), auditWriteTimeout)
	defer cancel()
	if _, err := t.audit.Append(ctx, event); err != nil {
		t.logger.Warn("board audit write failed", "kind", event.Kind, "error", err)
	}
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
			if s.telemetry.metrics != nil && key != "" {
				s.telemetry.metrics.SyncDeadLetters.Inc(metrics.KeyLabel(string(key)))
			}
			// id is the provider's own event identity, whose shape varies; it
			// belongs in bounded detail, not in the delivery column.
			s.telemetry.record(audit.Event{
				Tenant: tenant, Kind: audit.KindInboxDead, Key: string(key),
				Platform: string(platform), Status: audit.StatusDead, Attempts: attempts,
				Detail: map[string]any{"code": code, "event": id},
			})
		},
		OutboxDead: func(tenant string, key boardsync.Key, platform boardsync.Platform, delivery, target string, attempts int, code string) {
			if s.telemetry.metrics != nil && key != "" {
				s.telemetry.metrics.SyncDeadLetters.Inc(metrics.KeyLabel(string(key)))
			}
			s.telemetry.record(audit.Event{
				Tenant: tenant, Kind: audit.KindOutboxDead, Key: string(key),
				Platform: string(platform), Target: target, DeliveryID: delivery,
				Status: audit.StatusDead, Attempts: attempts,
				Detail: map[string]any{"code": code},
			})
		},
		DeliveryFailed: func(tenant string, key boardsync.Key, platform boardsync.Platform, code string) {
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
// sets it.
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
// value behind.
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
