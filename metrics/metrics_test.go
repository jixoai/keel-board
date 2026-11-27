package metrics_test

import (
	"strings"
	"sync"
	"testing"

	"github.com/jixoai/keel-board/metrics"
)

func TestCounterGaugeHistogramExposition(t *testing.T) {
	registry := metrics.NewRegistry()
	events := registry.Counter("board_events_total", "Board events appended, by kind.", metrics.LabelKind)
	events.Inc(metrics.KindLabel("message.posted"))
	events.Add(2, metrics.KindLabel("message.posted"))
	events.Inc(metrics.KindLabel("task.claimed"))
	connections := registry.Gauge("board_sse_connections", "Open streams in this process.")
	connections.Set(2)
	depth := registry.Gauge("board_sync_inbox_depth", "Inbox rows by key and state.",
		metrics.LabelKey, metrics.LabelStatus)
	depth.Set(3, metrics.KeyLabel("slack"), metrics.StatusLabel("pending"))
	depth.Set(1, metrics.KeyLabel("slack"), metrics.StatusLabel("dead"))
	duration := registry.Histogram("board_append_duration_ms", "Append duration.",
		[]float64{10, 1, 5})
	duration.Observe(0.5)
	duration.Observe(3)
	duration.Observe(25)

	text := registry.Text()
	for _, want := range []string{
		"# HELP board_events_total Board events appended, by kind.\n",
		"# TYPE board_events_total counter\n",
		`board_events_total{kind="message.posted"} 3`,
		`board_events_total{kind="task.claimed"} 1`,
		"# TYPE board_sse_connections gauge\n",
		"board_sse_connections 2\n",
		`board_sync_inbox_depth{key="slack",status="dead"} 1`,
		`board_sync_inbox_depth{key="slack",status="pending"} 3`,
		"# TYPE board_append_duration_ms histogram\n",
		`board_append_duration_ms_bucket{le="1"} 1`,
		`board_append_duration_ms_bucket{le="5"} 2`,
		`board_append_duration_ms_bucket{le="10"} 2`,
		`board_append_duration_ms_bucket{le="+Inf"} 3`,
		"board_append_duration_ms_sum 28.5",
		"board_append_duration_ms_count 3",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("exposition missing %q\n%s", want, text)
		}
	}
	// Bucket order is ascending even though the caller passed them unsorted,
	// and le is the last label on a bucket line.
	if strings.Index(text, `le="1"`) > strings.Index(text, `le="5"`) {
		t.Fatalf("buckets aren't sorted:\n%s", text)
	}
	// Repeated gathers agree: ordering is stable.
	if again := registry.Text(); again != text {
		t.Fatalf("exposition isn't deterministic:\n%s\n---\n%s", text, again)
	}
}

func TestDuplicateRegistrationIsIdempotent(t *testing.T) {
	registry := metrics.NewRegistry()
	first := registry.Counter("board_events_total", "first", metrics.LabelKind)
	second := registry.Counter("board_events_total", "second", metrics.LabelKind)
	first.Inc(metrics.KindLabel("message.posted"))
	second.Inc(metrics.KindLabel("message.posted"))
	if got := first.Value(metrics.KindLabel("message.posted")); got != 2 {
		t.Fatalf("same-family counters see each other: got %v", got)
	}
	if help := registry.Text(); !strings.Contains(help, "# HELP board_events_total first") {
		t.Fatalf("the first registration's help was replaced:\n%s", help)
	}

	// A conflicting registration yields an inert metric, never a panic, and
	// leaves the first family intact.
	conflict := registry.Gauge("board_events_total", "conflict")
	conflict.Set(99)
	text := registry.Text()
	if strings.Contains(text, "# TYPE board_events_total gauge") || strings.Contains(text, "99") {
		t.Fatalf("a conflicting family replaced the counter:\n%s", text)
	}
	if got := first.Value(metrics.KindLabel("message.posted")); got != 2 {
		t.Fatalf("counter changed through a conflict: %v", got)
	}
	// The same name with another label schema is also inert.
	other := registry.Counter("board_events_total", "labels", metrics.LabelKey)
	other.Inc(metrics.KeyLabel("slack"))
	if got := other.Value(metrics.KeyLabel("slack")); got != 0 {
		t.Fatalf("a schema conflict recorded an alert: got %v", got)
	}
}

func TestLabelsAreBoundedAndValidated(t *testing.T) {
	registry := metrics.NewRegistry()
	counter := registry.Counter("board_events_total", "events", metrics.LabelKind)
	for name, label := range map[string]metrics.Label{
		"message id":     metrics.KindLabel("18446744073709551615"),
		"space":          metrics.KindLabel("message posted"),
		"quote":          metrics.KindLabel(`message"posted`),
		"newline":        metrics.KindLabel("message\nposted"),
		"too long":       metrics.KindLabel(strings.Repeat("a", 65)),
		"empty":          metrics.KindLabel(""),
		"unicode":        metrics.KindLabel("消息"),
		"arbitrary name": {Name: "tenant", Value: "acme"},
	} {
		counter.Inc(label)
		if got := counter.Value(label); got != 0 {
			t.Fatalf("%s: invalid label recorded a series (%v)", name, got)
		}
	}
	// A label set that doesn't match the family schema is dropped too.
	counter.Inc(metrics.KeyLabel("slack"))
	if strings.Contains(registry.Text(), `key=`) {
		t.Fatalf("a mismatched label set was exported:\n%s", registry.Text())
	}
	if counter.Value(metrics.KindLabel("message.posted")) != 0 {
		t.Fatal("the valid series was touched by invalid observations")
	}
}

func TestRegistryConcurrentRecording(t *testing.T) {
	registry := metrics.NewRegistry()
	counter := registry.Counter("board_events_total", "events", metrics.LabelKind)
	gauge := registry.Gauge("board_tasks_live_claims", "claims")
	histogram := registry.Histogram("board_append_duration_ms", "duration", nil)
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				counter.Inc(metrics.KindLabel("message.posted"))
				gauge.Set(float64(i))
				histogram.Observe(float64(i))
				_ = registry.Text()
			}
		}()
	}
	wg.Wait()
	if got := counter.Value(metrics.KindLabel("message.posted")); got != 4000 {
		t.Fatalf("counter = %v, want 4000", got)
	}
	if count, _ := histogram.Stats(); count != 4000 {
		t.Fatalf("histogram count = %d, want 4000", count)
	}
}

func TestBoardMetricNamesMatchTheDesign(t *testing.T) {
	registry := metrics.NewRegistry()
	metrics.NewBoardMetrics(registry)
	text := registry.Text()
	for _, name := range []string{
		"board_events_total",
		"board_append_duration_ms",
		"board_tasks_live_claims",
		"board_lease_expired_total",
		"board_sse_connections",
		"board_sse_lag_seq",
		"board_projection_rebuild_duration_ms",
		"board_sync_inbox_depth",
		"board_sync_outbox_depth",
		"board_sync_delivery_failures_total",
		"board_sync_dead_letters_total",
		"board_sync_webhook_duration_ms",
		"board_sync_dedupe_hits_total",
		"board_sync_bridge_errors_total",
	} {
		if !strings.Contains(text, "# TYPE "+name+" ") {
			t.Fatalf("metric %s is missing from the exposition:\n%s", name, text)
		}
	}
	// A second construction over one registry must not panic or duplicate.
	metrics.NewBoardMetrics(registry)
}
