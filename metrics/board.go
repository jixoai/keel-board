package metrics

import "time"

// The design's metric names (§18.2). Wiring uses these constants so a typo
// can't invent a metric the dashboards don't know; the exposition carries the
// same names.
const (
	MetricEventsTotal                 = "board_events_total"
	MetricAppendDurationMilliseconds  = "board_append_duration_ms"
	MetricTasksLiveClaims             = "board_tasks_live_claims"
	MetricLeaseExpiredTotal           = "board_lease_expired_total"
	MetricSSEConnections              = "board_sse_connections"
	MetricSSELagSeq                   = "board_sse_lag_seq"
	MetricProjectionRebuildDurationMS = "board_projection_rebuild_duration_ms"

	MetricSyncInboxDepth            = "board_sync_inbox_depth"
	MetricSyncOutboxDepth           = "board_sync_outbox_depth"
	MetricSyncDeliveryFailuresTotal = "board_sync_delivery_failures_total"
	MetricSyncDeadLettersTotal      = "board_sync_dead_letters_total"
	MetricSyncWebhookDurationMS     = "board_sync_webhook_duration_ms"
	MetricSyncDedupeHitsTotal       = "board_sync_dedupe_hits_total"
	MetricSyncBridgeErrorsTotal     = "board_sync_bridge_errors_total"
)

// BoardMetrics is the process's board and sync instrumentation. A nil field
// means the metric wasn't registered (a conflicting registration); the
// nil-safe metric methods make that harmless.
type BoardMetrics struct {
	// Board.

	// Events counts committed events by kind.
	Events *Counter
	// AppendDuration observes the whole read-decide-append in milliseconds.
	AppendDuration *Histogram
	// TasksLiveClaims is how many tasks currently hold an unexpired claim.
	TasksLiveClaims *Gauge
	// LeaseExpired counts committed task.expired events.
	LeaseExpired *Counter
	// SSEConnections is the number of open event streams in this process.
	SSEConnections *Gauge
	// SSELagSeq is how far the newest stream's cursor trails the head.
	SSELagSeq *Gauge
	// ProjectionRebuildDuration observes one projection rebuild attempt.
	ProjectionRebuildDuration *Histogram

	// Sync.

	// SyncInboxDepth is a key's inbound rows by state.
	SyncInboxDepth *Gauge
	// SyncOutboxDepth is a key's outbound rows by state.
	SyncOutboxDepth *Gauge
	// SyncDeliveryFailures counts failed outbound delivery attempts by key and
	// platform.
	SyncDeliveryFailures *Counter
	// SyncDeadLetters counts rows that reached the dead state by key.
	SyncDeadLetters *Counter
	// SyncWebhookDuration observes an authenticated platform webhook by key and
	// platform.
	SyncWebhookDuration *Histogram
	// SyncDedupeHits counts inbound events an inbox already held, by key.
	SyncDedupeHits *Counter
	// SyncBridgeErrors counts bridge requests that failed, by key.
	SyncBridgeErrors *Counter
}

// NewBoardMetrics registers the design's metrics on registry. It is idempotent:
// calling it twice with one registry returns instrumentation over the same
// families.
func NewBoardMetrics(registry *Registry) *BoardMetrics {
	if registry == nil {
		return &BoardMetrics{}
	}
	return &BoardMetrics{
		Events: registry.Counter(MetricEventsTotal,
			"Board events appended, by kind.", LabelKind),
		AppendDuration: registry.Histogram(MetricAppendDurationMilliseconds,
			"Board append duration in milliseconds, read-decide-append through commit.", nil),
		TasksLiveClaims: registry.Gauge(MetricTasksLiveClaims,
			"Tasks holding an unexpired claim after the last observed write; refreshed on writes."),
		LeaseExpired: registry.Counter(MetricLeaseExpiredTotal,
			"Task leases that expired and were written as task.expired."),
		SSEConnections: registry.Gauge(MetricSSEConnections,
			"Open server-sent event streams in this process."),
		SSELagSeq: registry.Gauge(MetricSSELagSeq,
			"Sequence lag between the board head and the newest stream's cursor; process-wide, the newest stream overwrites."),
		ProjectionRebuildDuration: registry.Histogram(MetricProjectionRebuildDurationMS,
			"Projection rebuild duration in milliseconds.", nil),

		SyncInboxDepth: registry.Gauge(MetricSyncInboxDepth,
			"Sync inbox rows by connection key and state; refreshed by GET /sync/status scrapes.", LabelKey, LabelStatus),
		SyncOutboxDepth: registry.Gauge(MetricSyncOutboxDepth,
			"Sync outbox rows by connection key and state; refreshed by GET /sync/status scrapes.", LabelKey, LabelStatus),
		SyncDeliveryFailures: registry.Counter(MetricSyncDeliveryFailuresTotal,
			"Sync outbound delivery failures by connection key and platform.", LabelKey, LabelPlatform),
		SyncDeadLetters: registry.Counter(MetricSyncDeadLettersTotal,
			"Sync rows that reached the dead state, by connection key.", LabelKey),
		SyncWebhookDuration: registry.Histogram(MetricSyncWebhookDurationMS,
			"Sync webhook handling duration in milliseconds, by connection key and platform.",
			nil, LabelKey, LabelPlatform),
		SyncDedupeHits: registry.Counter(MetricSyncDedupeHitsTotal,
			"Sync inbound events an inbox already held, by connection key.", LabelKey),
		SyncBridgeErrors: registry.Counter(MetricSyncBridgeErrorsTotal,
			"Chat SDK bridge requests that failed, by connection key.", LabelKey),
	}
}

// Milliseconds converts a duration for a millisecond histogram.
func Milliseconds(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}
