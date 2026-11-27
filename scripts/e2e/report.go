package main

import (
	"encoding/json"
	"fmt"
	"html"
	"html/template"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Status is one step's or one report's outcome.
type Status string

const (
	StatusPass Status = "PASS"
	StatusFail Status = "FAIL"
	StatusSkip Status = "SKIP"
)

// Assertion is one expected/actual pair a step checked.
type Assertion struct {
	Name     string `json:"name"`
	Expected string `json:"expected"`
	Actual   string `json:"actual"`
	Pass     bool   `json:"pass"`
}

// Trace is one HTTP exchange kept as evidence (the report has no product UI
// screenshot; these API/JSON views are its substitute).
type Trace struct {
	Label    string `json:"label,omitempty"`
	Method   string `json:"method"`
	URL      string `json:"url"`
	Status   int    `json:"status,omitempty"`
	Request  string `json:"request,omitempty"`
	Response string `json:"response,omitempty"`
}

// Step is one reported E2E action.
type Step struct {
	ID         string        `json:"id"`
	Group      string        `json:"group"`
	Name       string        `json:"name"`
	Action     string        `json:"action"`
	Expect     string        `json:"expect"`
	Actual     string        `json:"actual"`
	Status     Status        `json:"status"`
	Started    time.Time     `json:"started"`
	Duration   time.Duration `json:"duration"`
	Assertions []Assertion   `json:"assertions"`
	Traces     []Trace       `json:"traces,omitempty"`
	Logs       []string      `json:"logs,omitempty"`
	SkipReason string        `json:"skipReason,omitempty"`
}

// failed reports whether any assertion failed.
func (s *Step) failed() bool {
	for _, a := range s.Assertions {
		if !a.Pass {
			return true
		}
	}
	return false
}

func (s *Step) AssertionsPassed() int {
	n := 0
	for _, a := range s.Assertions {
		if a.Pass {
			n++
		}
	}
	return n
}

// Check records one assertion and returns whether it passed.
func (s *Step) Check(name string, pass bool, expected, actual string) bool {
	s.Assertions = append(s.Assertions, Assertion{Name: name, Pass: pass, Expected: expected, Actual: actual})
	if pass {
		if s.Actual == "" {
			s.Actual = "符合预期：" + actual
		}
		return true
	}
	s.Actual = "不符合预期：" + actual
	return false
}

// CheckBool records a boolean expectation.
func (s *Step) CheckBool(name string, want, got bool) bool {
	return s.Check(name, want == got, fmt.Sprintf("%t", want), fmt.Sprintf("%t", got))
}

// CheckEQ records an equality expectation with formatted strings.
func (s *Step) CheckEQ(name string, want, got any) bool {
	return s.Check(name, fmt.Sprint(want) == fmt.Sprint(got), fmt.Sprint(want), fmt.Sprint(got))
}

// CheckContains records a substring expectation.
func (s *Step) CheckContains(name, want, got string) bool {
	return s.Check(name, strings.Contains(got, want), "contains "+want, got)
}

// Note appends a human-readable log line to the step.
func (s *Step) Note(format string, args ...any) {
	s.Logs = append(s.Logs, fmt.Sprintf(format, args...))
}

// Trace records one HTTP exchange.
func (s *Step) Trace(label, method, url string, status int, request, response string) {
	s.Traces = append(s.Traces, Trace{
		Label: label, Method: method, URL: url, Status: status,
		Request: clip(request, 6000), Response: clip(response, 6000),
	})
}

// clip bounds evidence so a report stays readable.
func clip(text string, max int) string {
	if len(text) <= max {
		return text
	}
	return text[:max] + fmt.Sprintf("\n… (%d bytes clipped)", len(text)-max)
}

// Skip is a planned, explicit non-coverage item.
type Skip struct {
	Feature string `json:"feature"`
	Reason  string `json:"reason"`
	Gate    string `json:"gate,omitempty"`
}

// Coverage is one row of the coverage matrix.
type Coverage struct {
	Feature string `json:"feature"`
	Status  Status `json:"status"`
	Detail  string `json:"detail"`
	Steps   string `json:"steps,omitempty"`
}

// Report is the generated E2E report.
type Report struct {
	Title     string            `json:"title"`
	Commit    string            `json:"commit"`
	Branch    string            `json:"branch"`
	Started   time.Time         `json:"started"`
	Finished  time.Time         `json:"finished"`
	GoVersion string            `json:"goVersion"`
	Platform  string            `json:"platform"`
	DSN       string            `json:"dsn"`
	Result    Status            `json:"result"`
	Steps     []*Step           `json:"steps"`
	Coverage  []Coverage        `json:"coverage"`
	Skips     []Skip            `json:"skips"`
	Artifacts map[string]string `json:"artifacts"`

	scenarioSeq int
}

// Scenario groups steps for one area and aborts its remaining steps after a
// failure, recording them as skipped instead of silently omitting them.
func (r *Report) Scenario(group string) *Scenario {
	return &Scenario{report: r, group: group}
}

// Scenario is a named run of steps.
type Scenario struct {
	report          *Report
	group           string
	seq             int
	aborted         bool
	abortedRecorded bool
}

// Step adds and runs one step. A returned error, a failed assertion, or a
// panic marks the step FAIL and aborts the rest of the scenario.
func (sc *Scenario) Step(name, action, expect string, fn func(*Step) error) error {
	sc.seq++
	step := &Step{
		ID: fmt.Sprintf("%s.%d", sc.group, sc.seq), Group: sc.group,
		Name: name, Action: action, Expect: expect, Started: time.Now(),
	}
	if sc.aborted {
		step.Status = StatusSkip
		step.SkipReason = "前置步骤失败，场景已中止"
		step.Duration = 0
		sc.report.Steps = append(sc.report.Steps, step)
		return fmt.Errorf("scenario %s aborted", sc.group)
	}
	sc.report.Steps = append(sc.report.Steps, step)
	var runErr error
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				runErr = fmt.Errorf("panic: %v", recovered)
			}
		}()
		runErr = fn(step)
	}()
	step.Duration = time.Since(step.Started)
	switch {
	case runErr != nil:
		step.Status = StatusFail
		if step.Actual == "" {
			step.Actual = "错误：" + runErr.Error()
		} else {
			step.Actual += "；错误：" + runErr.Error()
		}
		step.Logs = append(step.Logs, "错误: "+runErr.Error())
	case step.failed():
		step.Status = StatusFail
	default:
		step.Status = StatusPass
		if step.Actual == "" {
			step.Actual = "符合预期"
		}
	}
	if step.Status == StatusFail {
		sc.aborted = true
		if !sc.abortedRecorded {
			sc.abortedRecorded = true
			sc.report.Steps = append(sc.report.Steps, &Step{
				ID: sc.group + ".aborted", Group: sc.group,
				Name: "场景中止：后续步骤未执行", Action: "不再执行剩余步骤",
				Expect: "记录为 SKIP，不静默消失", Actual: "SKIP",
				Status: StatusSkip, SkipReason: "上一步 " + step.ID + " 失败，场景中止",
				Started: time.Now(),
			})
		}
		return fmt.Errorf("%s: %s", step.ID, step.Actual)
	}
	return nil
}

// Skip records a planned skipped step without aborting the scenario.
func (sc *Scenario) Skip(name, action, reason string) {
	sc.seq++
	sc.report.Steps = append(sc.report.Steps, &Step{
		ID: fmt.Sprintf("%s.%d", sc.group, sc.seq), Group: sc.group,
		Name: name, Action: action, Expect: reason, Actual: "SKIP（待 change 合入 / 产品缝缺失）",
		Status: StatusSkip, SkipReason: reason, Started: time.Now(),
	})
}

// AddCoverage appends a coverage row.
func (r *Report) AddCoverage(feature string, status Status, detail, steps string) {
	r.Coverage = append(r.Coverage, Coverage{Feature: feature, Status: status, Detail: detail, Steps: steps})
}

// AddSkip appends an explicit non-coverage item.
func (r *Report) AddSkip(feature, reason, gate string) {
	r.Skips = append(r.Skips, Skip{Feature: feature, Reason: reason, Gate: gate})
}

// Counts sums step statuses.
func (r *Report) Counts() map[Status]int {
	counts := map[Status]int{StatusPass: 0, StatusFail: 0, StatusSkip: 0}
	for _, step := range r.Steps {
		counts[step.Status]++
	}
	return counts
}

func (r *Report) PassCount() int { return r.Counts()[StatusPass] }
func (r *Report) FailCount() int { return r.Counts()[StatusFail] }
func (r *Report) SkipCount() int { return r.Counts()[StatusSkip] }

// GroupStatus summarizes one scenario group: FAIL if any step failed, PASS if
// at least one step passed and none failed, otherwise SKIP.
func (r *Report) GroupStatus(group string) Status {
	status := StatusSkip
	for _, step := range r.Steps {
		if step.Group != group {
			continue
		}
		switch step.Status {
		case StatusFail:
			return StatusFail
		case StatusPass:
			status = StatusPass
		}
	}
	return status
}

// StepsIn lists the step IDs of one group for the coverage matrix.
func (r *Report) StepsIn(group string) string {
	var ids []string
	for _, step := range r.Steps {
		if step.Group == group && step.Status != StatusSkip {
			ids = append(ids, step.ID)
		}
	}
	return strings.Join(ids, ", ")
}

// AssertionCounts sums assertion outcomes.
func (r *Report) AssertionCounts() (passed, failed int) {
	for _, step := range r.Steps {
		for _, assertion := range step.Assertions {
			if assertion.Pass {
				passed++
			} else {
				failed++
			}
		}
	}
	return passed, failed
}

// Finalize computes the overall result.
func (r *Report) Finalize() {
	r.Finished = time.Now()
	counts := r.Counts()
	switch {
	case counts[StatusFail] > 0:
		r.Result = StatusFail
	case counts[StatusPass] == 0:
		r.Result = StatusSkip
	default:
		r.Result = StatusPass
	}
}

// Duration is the whole run's wall time.
func (r *Report) Duration() time.Duration { return r.Finished.Sub(r.Started) }

// Write emits report.html, report.json and returns the HTML path.
func (r *Report) Write(dir string) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	encoded, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(dir, "report.json"), encoded, 0o644); err != nil {
		return "", err
	}
	for name, path := range r.Artifacts {
		if _, err := os.Stat(path); err == nil {
			r.Artifacts[name] = path
		}
	}
	htmlPath := filepath.Join(dir, "report.html")
	file, err := os.Create(htmlPath)
	if err != nil {
		return "", err
	}
	defer file.Close()
	if err := reportTemplate.Execute(file, r); err != nil {
		return "", err
	}
	return htmlPath, nil
}

// templateFuncs are the small presentation helpers the template uses.
var templateFuncs = template.FuncMap{
	"statusClass": func(status Status) string {
		switch status {
		case StatusPass:
			return "pass"
		case StatusFail:
			return "fail"
		case StatusSkip:
			return "skip"
		default:
			return ""
		}
	},
	"dur": func(d time.Duration) string {
		if d <= 0 {
			return "0ms"
		}
		if d < time.Second {
			return fmt.Sprintf("%dms", d.Milliseconds())
		}
		return fmt.Sprintf("%.2fs", d.Seconds())
	},
	"timefmt": func(t time.Time) string {
		if t.IsZero() {
			return ""
		}
		return t.Local().Format("2006-01-02 15:04:05")
	},
	"pre": func(text string) template.HTML {
		return template.HTML("<pre>" + html.EscapeString(text) + "</pre>")
	},
	"sortedArtifacts": func(artifacts map[string]string) []string {
		keys := make([]string, 0, len(artifacts))
		for key := range artifacts {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		return keys
	},
}

// reportTemplate is the generated HTML view. It is never hand-edited output:
// the harness renders it from the Report model on every run.
var reportTemplate = template.Must(template.New("report").Funcs(templateFuncs).Parse(`<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{.Title}}</title>
<style>
:root { color-scheme: light dark; --pass:#137333; --fail:#b3261e; --skip:#8a6d00; --line:#d7d7d7; --muted:#666; --bg:#fff; --panel:#f7f7f8; }
@media (prefers-color-scheme: dark) { :root { --line:#3a3a3a; --muted:#9a9a9a; --bg:#161616; --panel:#1f1f21; --pass:#6fd58a; --fail:#ff8a80; --skip:#e0c060; } }
body { margin:0; padding:32px; background:var(--bg); color:inherit; font:14px/1.55 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue","PingFang SC","Hiragino Sans GB","Microsoft YaHei",sans-serif; }
h1 { font-size:24px; margin:0 0 4px; }
h2 { font-size:18px; margin:32px 0 12px; border-bottom:1px solid var(--line); padding-bottom:8px; }
.meta { color:var(--muted); font-size:13px; }
.meta code { color:inherit; }
.cards { display:flex; flex-wrap:wrap; gap:12px; margin:20px 0; }
.card { border:1px solid var(--line); border-radius:10px; padding:12px 16px; min-width:120px; background:var(--panel); }
.card .k { color:var(--muted); font-size:12px; }
.card .v { font-size:22px; font-weight:600; }
.badge { display:inline-block; padding:2px 10px; border-radius:999px; color:#fff; font-weight:600; font-size:12px; letter-spacing:.03em; }
.badge.pass { background:var(--pass); } .badge.fail { background:var(--fail); } .badge.skip { background:var(--skip); }
table { width:100%; border-collapse:collapse; }
th, td { text-align:left; vertical-align:top; padding:8px 10px; border-bottom:1px solid var(--line); }
th { font-size:12px; text-transform:uppercase; letter-spacing:.04em; color:var(--muted); }
tr.step.pass td:first-child { border-left:4px solid var(--pass); }
tr.step.fail td:first-child { border-left:4px solid var(--fail); }
tr.step.skip td:first-child { border-left:4px solid var(--skip); }
.mono { font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace; font-size:12px; }
pre { white-space:pre-wrap; word-break:break-word; background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:10px; margin:6px 0; }
details { margin:4px 0; }
summary { cursor:pointer; color:var(--muted); }
.assert.pass { color:var(--pass); } .assert.fail { color:var(--fail); }
.small { font-size:12px; color:var(--muted); }
.note { border:1px solid var(--line); border-radius:10px; padding:12px 16px; background:var(--panel); margin:12px 0; }
ul { margin:6px 0 6px 20px; padding:0; }
footer { margin-top:40px; color:var(--muted); font-size:12px; border-top:1px solid var(--line); padding-top:12px; }
</style>
</head>
<body>
<h1>{{.Title}}</h1>
<div class="meta">
结果 <span class="badge {{statusClass .Result}}">{{.Result}}</span>
&nbsp;·&nbsp; 分支 <code>{{.Branch}}</code> &nbsp;·&nbsp; commit <code>{{.Commit}}</code>
&nbsp;·&nbsp; {{timefmt .Started}} → {{timefmt .Finished}}（{{dur .Duration}}）
&nbsp;·&nbsp; {{.GoVersion}} / {{.Platform}}
</div>

<div class="cards">
  <div class="card"><div class="k">步骤</div><div class="v">{{len .Steps}}</div></div>
  <div class="card"><div class="k">PASS</div><div class="v">{{.PassCount}}</div></div>
  <div class="card"><div class="k">FAIL</div><div class="v">{{.FailCount}}</div></div>
  <div class="card"><div class="k">SKIP</div><div class="v">{{.SkipCount}}</div></div>
  <div class="card"><div class="k">断言</div><div class="v">{{.AssertionPassedTotal}} / {{.AssertionTotal}}</div></div>
</div>

<div class="note">
<strong>证据形式说明：</strong>board 本期没有产品 UI，因此不使用产品截图，也不伪造截图。每一步的证据是真实 HTTP API trace、平台侧收到的请求、JSON 视图与进程日志（见每步「证据」折叠区）。
报告链接形态为 <code>local-*</code>（本机静态文件 / 127.0.0.1），不可从公网访问；若需公网曝光，请把 <code>report.html</code> 与同目录产物整体发布。
</div>
<div class="note">
<strong>运行形态：</strong><code>real-serve</code> 组跑真实 <code>board serve</code> 二进制；镜像/automation/bridge 组跑 harness-hosted server（<code>server.New</code> + <code>syncstore</code> + Slack/GitHub/Linear/ChatSDK adapter + sync workers + watcher/schedule workers），因为 <code>cmd/board</code> 没有平台 endpoint/transport 注入缝，而本次约束不修改产品代码。<code>real-serve</code> 不经过 fake 平台，因此只覆盖 API、sync 启动面和进程重启。
</div>
<div class="note">
<strong>装配与策略差异（审阅重点）：</strong>
<ul>
<li>tenant：harness 固定 <code>e2e</code>；<code>board serve</code> 默认 <code>default</code>（本 harness 未传 <code>--sync-tenant/--automation-tenant</code>）。</li>
<li>sync 重试：harness <code>Inbound 5 次 / Outbound 3 次，30ms→120ms，lease 2s</code>；<code>cmd/board</code> 用默认 <code>Inbound 10 次 / Outbound 5 次，1s→30s+jitter，lease 1m</code>。E2E 缩短是为了让失败/重试/dead-letter 可测。</li>
<li>automation 重试：harness <code>3 次，20ms→120ms，lease 5s，poll 10ms，action timeout 2s</code>；<code>cmd/board</code> 默认 <code>5 次，1s→30s+jitter，lease 5m，poll 1s，action timeout 2m</code>。</li>
<li>HTTP surface：harness 在进程内监听临端口；<code>real-serve</code> 是独立子进程。两者都使用产品 <code>server.New</code> 路由。</li>
</ul>
</div>

<h2>覆盖矩阵</h2>
<table>
<thead><tr><th>能力</th><th>状态</th><th>说明</th><th>步骤</th></tr></thead>
<tbody>
{{range .Coverage}}
<tr><td>{{.Feature}}</td><td><span class="badge {{statusClass .Status}}">{{.Status}}</span></td><td>{{.Detail}}</td><td class="mono">{{.Steps}}</td></tr>
{{end}}
</tbody>
</table>

<h2>步骤明细</h2>
<table>
<thead><tr><th style="width:36px">#</th><th style="width:18%">步骤</th><th style="width:16%">动作</th><th style="width:14%">预期</th><th style="width:14%">实际</th><th style="width:6%">状态</th><th style="width:5%">耗时</th></tr></thead>
<tbody>
{{range .Steps}}
<tr class="step {{statusClass .Status}}">
<td class="mono">{{.ID}}</td>
<td><strong>{{.Name}}</strong>{{if .SkipReason}}<div class="small">SKIP：{{.SkipReason}}</div>{{end}}</td>
<td class="small">{{.Action}}</td>
<td class="small">{{.Expect}}</td>
<td class="small">{{.Actual}}</td>
<td><span class="badge {{statusClass .Status}}">{{.Status}}</span></td>
<td class="small">{{dur .Duration}}</td>
</tr>
<tr><td></td><td colspan="6">
{{if .Assertions}}
<details><summary>断言 {{len .Assertions}} 条（通过 {{.AssertionsPassed}}）</summary>
<table>
<thead><tr><th>断言</th><th>期望</th><th>实际</th><th>结果</th></tr></thead>
<tbody>
{{range .Assertions}}<tr><td>{{.Name}}</td><td class="mono">{{.Expected}}</td><td class="mono">{{.Actual}}</td><td class="{{if .Pass}}assert pass{{else}}assert fail{{end}}">{{if .Pass}}PASS{{else}}FAIL{{end}}</td></tr>{{end}}
</tbody></table>
</details>
{{end}}
{{if .Logs}}
<details><summary>关键日志 / 说明</summary>
{{range .Logs}}<div class="mono">{{.}}</div>{{end}}
</details>
{{end}}
{{if .Traces}}
<details><summary>证据：HTTP trace / JSON 视图（{{len .Traces}}）</summary>
{{range .Traces}}
<div class="small"><strong>{{.Label}}</strong> <code>{{.Method}} {{.URL}}</code> → {{.Status}}</div>
{{if .Request}}<div class="small">请求</div>{{pre .Request}}{{end}}
{{if .Response}}<div class="small">响应</div>{{pre .Response}}{{end}}
{{end}}
</details>
{{end}}
</td></tr>
{{end}}
</tbody>
</table>

<h2>未覆盖 / 跳过项</h2>
{{if .Skips}}
<ul>
{{range .Skips}}<li><strong>{{.Feature}}</strong>：{{.Reason}}{{if .Gate}}<span class="small">（门禁：{{.Gate}}）</span>{{end}}</li>{{end}}
</ul>
{{else}}<p class="small">无。</p>{{end}}

<h2>产物</h2>
<table>
<thead><tr><th>名称</th><th>路径</th></tr></thead>
<tbody>
{{range $name := sortedArtifacts .Artifacts}}<tr><td>{{$name}}</td><td class="mono">{{index $.Artifacts $name}}</td></tr>{{end}}
</tbody>
</table>

<footer>
由 <code>scripts/e2e</code> 自动生成（{{timefmt .Finished}}）；报告由脚本渲染，未手写 HTML，不含产品截图。
DSN：<span class="mono">{{.DSN}}</span>
</footer>
</body>
</html>
`))

// The template calls these methods through `call`; defining them on Report
// keeps the HTML free of precomputed strings.
func (r *Report) AssertionTotal() int {
	passed, failed := r.AssertionCounts()
	return passed + failed
}

func (r *Report) AssertionPassedTotal() int {
	passed, _ := r.AssertionCounts()
	return passed
}
