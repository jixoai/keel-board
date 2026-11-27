// Package metrics is a small, dependency-free metrics registry with a
// Prometheus text exposition.
//
// Its label model is deliberately narrower than Prometheus': a label name must
// be one of a fixed set (kind, key, platform, status) and a label value must
// look like an identifier (letters, digits, '.', '_', '-', ':', '/', at most
// 64 bytes, never all digits). A message ID, an external target, a message
// body, a token, or a tenant name cannot be a label value by construction:
// callers name them through KindLabel, KeyLabel, PlatformLabel, or StatusLabel,
// and a value that fails the check is never exported. The design's rule (§18.2)
// is that metrics stay bounded; this package enforces the shape of that bound.
//
// A metric family is created once per Registry. Constructing the same name
// again returns the same family, so wiring that runs twice is harmless. A
// construction that conflicts (another type, another label schema) yields an
// inert metric rather than a panic; the Registry keeps the first family.
package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Label names the exposition accepts. Anything else is refused, so no caller
// can invent a high-cardinality dimension by accident.
const (
	LabelKind     = "kind"
	LabelKey      = "key"
	LabelPlatform = "platform"
	LabelStatus   = "status"
)

// maxSeriesPerFamily bounds one family's series count. It is a backstop for a
// caller that passes a dynamic-looking value the shape check accepts; the
// design's labels are bounded by configuration, so a real deployment stays far
// below this.
const maxSeriesPerFamily = 1024

// maxLabelValueLen bounds one label value.
const maxLabelValueLen = 64

// Label is one validated label pair. Build it with the typed constructors;
// the zero Label is invalid and is ignored on observation.
type Label struct {
	Name  string
	Value string
	valid bool
}

func newLabel(name, value string) Label {
	return Label{Name: name, Value: value, valid: validLabelValue(value)}
}

// KindLabel names an event or work kind, such as "message.posted".
//
// The caller must pass a value from a fixed vocabulary (log.EventType
// constants or the package's own kind constants), never text a user wrote.
func KindLabel(value string) Label { return newLabel(LabelKind, value) }

// KeyLabel names a configured sync connection key. Keys come from deployment
// configuration, not from a platform payload.
func KeyLabel(value string) Label { return newLabel(LabelKey, value) }

// PlatformLabel names a platform, such as "slack" or "github".
func PlatformLabel(value string) Label { return newLabel(LabelPlatform, value) }

// StatusLabel names a row's state, such as "pending" or "dead".
func StatusLabel(value string) Label { return newLabel(LabelStatus, value) }

// validLabelValue accepts a short identifier-like token and refuses anything
// that could name a message (all digits), carry text (whitespace, quotes), or
// leak a secret (a long opaque string).
func validLabelValue(value string) bool {
	if len(value) == 0 || len(value) > maxLabelValueLen {
		return false
	}
	digitsOnly := true
	for i := 0; i < len(value); i++ {
		c := value[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
			digitsOnly = false
		case c >= '0' && c <= '9':
		case c == '.' || c == '_' || c == '-' || c == ':' || c == '/':
			digitsOnly = false
		default:
			return false
		}
	}
	return !digitsOnly
}

func allowedLabelName(name string) bool {
	switch name {
	case LabelKind, LabelKey, LabelPlatform, LabelStatus:
		return true
	default:
		return false
	}
}

// Registry owns metric families. It is safe for concurrent use.
type Registry struct {
	mu       sync.RWMutex
	families map[string]*family
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{families: make(map[string]*family)}
}

type metricKind uint8

const (
	counterKind metricKind = iota + 1
	gaugeKind
	histogramKind
)

func (k metricKind) String() string {
	switch k {
	case counterKind:
		return "counter"
	case gaugeKind:
		return "gauge"
	case histogramKind:
		return "histogram"
	default:
		return "unknown"
	}
}

type family struct {
	name       string
	help       string
	kind       metricKind
	labelNames []string
	buckets    []float64

	mu     sync.Mutex
	series map[string]*series
}

type series struct {
	labels  []Label
	value   float64
	buckets []uint64
	count   uint64
	sum     float64
}

// Counter is a non-negative monotone counter family.
type Counter struct{ family *family }

// Gauge is a value that goes up and down.
type Gauge struct{ family *family }

// Histogram is a cumulative observation histogram.
type Histogram struct{ family *family }

// Counter returns the counter family name. Repeating a construction with the
// same schema returns the same family; a conflicting one returns an inert
// counter.
func (r *Registry) Counter(name, help string, labelNames ...string) *Counter {
	return &Counter{family: r.register(name, help, counterKind, labelNames, nil)}
}

// Gauge returns the gauge family name.
func (r *Registry) Gauge(name, help string, labelNames ...string) *Gauge {
	return &Gauge{family: r.register(name, help, gaugeKind, labelNames, nil)}
}

// Histogram returns the histogram family name. Buckets are sorted and made
// cumulative on output; the +Inf bucket is implicit. An empty bucket list uses
// DefaultBuckets.
func (r *Registry) Histogram(name, help string, buckets []float64, labelNames ...string) *Histogram {
	return &Histogram{family: r.register(name, help, histogramKind, labelNames, normalizeBuckets(buckets))}
}

// register returns an existing family with the same schema or creates one. A
// conflict returns nil, which every wrapper treats as inert.
func (r *Registry) register(name, help string, kind metricKind, labelNames []string, buckets []float64) *family {
	names, ok := normalizeLabelNames(labelNames)
	if !ok || !validMetricName(name) {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, found := r.families[name]; found {
		if existing.kind == kind && sameLabelNames(existing.labelNames, names) {
			return existing
		}
		return nil
	}
	created := &family{
		name: name, help: help, kind: kind, labelNames: names,
		buckets: buckets, series: make(map[string]*series),
	}
	r.families[name] = created
	return created
}

// validMetricName enforces the Prometheus metric-name grammar: [a-zA-Z_:]
// followed by [a-zA-Z0-9_:].
func validMetricName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c == '_', c == ':':
		case c >= '0' && c <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}

// normalizeLabelNames deduplicates and sorts the declared label names. Every
// name must be one of the package's bounded dimensions.
func normalizeLabelNames(labelNames []string) ([]string, bool) {
	out := make([]string, 0, len(labelNames))
	seen := make(map[string]struct{}, len(labelNames))
	for _, name := range labelNames {
		if !allowedLabelName(name) {
			return nil, false
		}
		if _, duplicate := seen[name]; duplicate {
			return nil, false
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	sort.Strings(out)
	return out, true
}

func sameLabelNames(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// normalizeBuckets copies, sorts, and deduplicates explicit buckets. Drop
// non-finite and negative values, and the implicit +Inf bucket.
func normalizeBuckets(buckets []float64) []float64 {
	if len(buckets) == 0 {
		return append([]float64(nil), DefaultBuckets...)
	}
	out := make([]float64, 0, len(buckets))
	for _, bucket := range buckets {
		if bucket < 0 || math.IsNaN(bucket) || math.IsInf(bucket, 1) {
			continue
		}
		out = append(out, bucket)
	}
	sort.Float64s(out)
	deduped := out[:0]
	for _, bucket := range out {
		if len(deduped) == 0 || deduped[len(deduped)-1] != bucket {
			deduped = append(deduped, bucket)
		}
	}
	if len(deduped) == 0 {
		return append([]float64(nil), DefaultBuckets...)
	}
	return deduped
}

// DefaultBuckets is the millisecond bucket set the design's duration histograms
// use: fast writes under a millisecond through a rebuild measured in seconds.
var DefaultBuckets = []float64{1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000}

// seriesFor finds or creates the series for a label set. An invalid or
// mismatched label set returns nil, so the observation is dropped and the
// process never panics on a bad call site.
func (f *family) seriesFor(labels []Label) *series {
	if len(labels) != len(f.labelNames) {
		return nil
	}
	sorted := make([]Label, len(labels))
	copy(sorted, labels)
	for _, label := range sorted {
		if !label.valid || !allowedLabelName(label.Name) {
			return nil
		}
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	for i := 1; i < len(sorted); i++ {
		if sorted[i].Name == sorted[i-1].Name {
			return nil
		}
	}
	for i, name := range f.labelNames {
		if sorted[i].Name != name {
			return nil
		}
	}
	key := seriesKey(sorted)
	f.mu.Lock()
	defer f.mu.Unlock()
	if existing, found := f.series[key]; found {
		return existing
	}
	if len(f.series) >= maxSeriesPerFamily {
		return nil
	}
	created := &series{labels: sorted}
	if f.kind == histogramKind {
		created.buckets = make([]uint64, len(f.buckets))
	}
	f.series[key] = created
	return created
}

func seriesKey(labels []Label) string {
	var b strings.Builder
	for _, label := range labels {
		b.WriteString(strconv.Itoa(len(label.Value)))
		b.WriteByte(':')
		b.WriteString(label.Value)
		b.WriteByte('|')
	}
	return b.String()
}

// Inc adds one.
func (c *Counter) Inc(labels ...Label) { c.Add(1, labels...) }

// Add adds delta. A negative delta is ignored: the family is monotone.
func (c *Counter) Add(delta float64, labels ...Label) {
	if c == nil || c.family == nil || delta < 0 || delta != delta {
		return
	}
	s := c.family.seriesFor(labels)
	if s == nil {
		return
	}
	c.family.mu.Lock()
	s.value += delta
	c.family.mu.Unlock()
}

// Value returns the series' current value, or 0 for an absent or invalid
// series. It is for tests and diagnostics, not a hot path.
func (c *Counter) Value(labels ...Label) float64 {
	if c == nil || c.family == nil {
		return 0
	}
	s := c.family.seriesFor(labels)
	if s == nil {
		return 0
	}
	c.family.mu.Lock()
	defer c.family.mu.Unlock()
	return s.value
}

// Set replaces the value.
func (g *Gauge) Set(value float64, labels ...Label) {
	if g == nil || g.family == nil || value != value {
		return
	}
	s := g.family.seriesFor(labels)
	if s == nil {
		return
	}
	g.family.mu.Lock()
	s.value = value
	g.family.mu.Unlock()
}

// Add adds delta, which may be negative.
func (g *Gauge) Add(delta float64, labels ...Label) {
	if g == nil || g.family == nil || delta != delta {
		return
	}
	s := g.family.seriesFor(labels)
	if s == nil {
		return
	}
	g.family.mu.Lock()
	s.value += delta
	g.family.mu.Unlock()
}

// Inc adds one.
func (g *Gauge) Inc(labels ...Label) { g.Add(1, labels...) }

// Dec subtracts one.
func (g *Gauge) Dec(labels ...Label) { g.Add(-1, labels...) }

// Value returns the series' current value, or 0.
func (g *Gauge) Value(labels ...Label) float64 {
	if g == nil || g.family == nil {
		return 0
	}
	s := g.family.seriesFor(labels)
	if s == nil {
		return 0
	}
	g.family.mu.Lock()
	defer g.family.mu.Unlock()
	return s.value
}

// Observe records one observation.
func (h *Histogram) Observe(value float64, labels ...Label) {
	if h == nil || h.family == nil || value != value {
		return
	}
	s := h.family.seriesFor(labels)
	if s == nil {
		return
	}
	h.family.mu.Lock()
	defer h.family.mu.Unlock()
	s.count++
	s.sum += value
	for i, upper := range h.family.buckets {
		if value <= upper {
			s.buckets[i]++
		}
	}
}

// Stats returns a series' observation count and sum, for tests.
func (h *Histogram) Stats(labels ...Label) (count uint64, sum float64) {
	if h == nil || h.family == nil {
		return 0, 0
	}
	s := h.family.seriesFor(labels)
	if s == nil {
		return 0, 0
	}
	h.family.mu.Lock()
	defer h.family.mu.Unlock()
	return s.count, s.sum
}

// WritePrometheus writes the registry's families in Prometheus text exposition
// format (version 0.0.4). Output is deterministic: families are sorted by
// name, series by their label values, and labels by name (with `le` last on a
// histogram bucket line).
func (r *Registry) WritePrometheus(w io.Writer) error {
	r.mu.RLock()
	families := make([]*family, 0, len(r.families))
	for _, f := range r.families {
		families = append(families, f)
	}
	r.mu.RUnlock()
	sort.Slice(families, func(i, j int) bool { return families[i].name < families[j].name })
	buffer := &strings.Builder{}
	for _, f := range families {
		f.write(buffer)
	}
	_, err := io.WriteString(w, buffer.String())
	return err
}

// Text returns the exposition as a string.
func (r *Registry) Text() string {
	var b strings.Builder
	_ = r.WritePrometheus(&b)
	return b.String()
}

// write renders one family. It holds the family lock while copying the series,
// then formats outside it.
func (f *family) write(b *strings.Builder) {
	type snapshot struct {
		labels  []Label
		value   float64
		buckets []uint64
		count   uint64
		sum     float64
	}
	f.mu.Lock()
	series := make([]snapshot, 0, len(f.series))
	for _, s := range f.series {
		copied := snapshot{labels: append([]Label(nil), s.labels...), value: s.value, count: s.count, sum: s.sum}
		copied.buckets = append([]uint64(nil), s.buckets...)
		series = append(series, copied)
	}
	f.mu.Unlock()
	sort.Slice(series, func(i, j int) bool { return seriesKey(series[i].labels) < seriesKey(series[j].labels) })

	if f.help != "" {
		fmt.Fprintf(b, "# HELP %s %s\n", f.name, escapeHelp(f.help))
	}
	fmt.Fprintf(b, "# TYPE %s %s\n", f.name, f.kind)
	for _, s := range series {
		base := renderLabels(s.labels)
		switch f.kind {
		case counterKind, gaugeKind:
			fmt.Fprintf(b, "%s%s %s\n", f.name, base, formatFloat(s.value))
		case histogramKind:
			for i, upper := range f.buckets {
				bucketLabels := renderLabelsWith(s.labels, "le", formatFloat(upper))
				fmt.Fprintf(b, "%s_bucket%s %d\n", f.name, bucketLabels, s.buckets[i])
			}
			fmt.Fprintf(b, "%s_bucket%s %d\n", f.name,
				renderLabelsWith(s.labels, "le", "+Inf"), s.count)
			fmt.Fprintf(b, "%s_sum%s %s\n", f.name, base, formatFloat(s.sum))
			fmt.Fprintf(b, "%s_count%s %d\n", f.name, base, s.count)
		}
	}
}

// renderLabels renders {name="value",...} for a sorted label set.
func renderLabels(labels []Label) string {
	return renderLabelsWith(labels)
}

// renderLabelsWith renders labels plus an optional trailing pair, which is how
// a histogram's `le` stays last regardless of its alphabetical position.
func renderLabelsWith(labels []Label, extra ...string) string {
	if len(labels) == 0 && len(extra) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	first := true
	write := func(name, value string) {
		if !first {
			b.WriteString(",")
		}
		first = false
		b.WriteString(name)
		b.WriteString(`="`)
		b.WriteString(escapeLabelValue(value))
		b.WriteString(`"`)
	}
	for _, label := range labels {
		write(label.Name, label.Value)
	}
	for i := 0; i+1 < len(extra); i += 2 {
		write(extra[i], extra[i+1])
	}
	b.WriteByte('}')
	return b.String()
}

func escapeLabelValue(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return replacer.Replace(value)
}

func escapeHelp(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, "\n", `\n`)
	return replacer.Replace(value)
}

func formatFloat(value float64) string {
	if value != value {
		return "NaN"
	}
	return strconv.FormatFloat(value, 'g', -1, 64)
}
