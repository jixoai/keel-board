// Command e2e is the Phase 0 end-to-end acceptance harness for keel-board.
//
// It requires a real PostgreSQL (KEEL_BOARD_TEST_DSN), never Docker, never a
// real platform. It runs:
//
//   - the actual `board serve` binary from ./cmd/board, over PostgreSQL;
//   - a harness-hosted server built from the same product packages
//     (server.New + syncstore + the Slack/GitHub/Linear adapters + workers),
//     pointed at a local TLS fake "internet" so every adapter URL validation
//     still sees slack.com, files.slack.com, api.github.com, api.linear.app;
//
// and emits a script-generated HTML report with per-step action/expect/actual,
// assertions, HTTP traces, JSON views and process logs. Failures exit non-zero.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

func main() {
	os.Exit(run())
}

func run() int {
	var (
		reportDirFlag = flag.String("report-dir", "", "report directory (default scripts/e2e/out/<timestamp>)")
		keepSchemas   = flag.Bool("keep-schemas", false, "do not drop the per-run PostgreSQL schemas (debugging)")
		injectFailure = flag.Bool("inject-failure", false, "self-test: append a deliberately failing step (verifies non-zero exit and report)")
	)
	flag.Parse()

	dsn := strings.TrimSpace(os.Getenv("KEEL_BOARD_TEST_DSN"))
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "keel-board e2e: KEEL_BOARD_TEST_DSN is required (a PostgreSQL URL; no Docker is started).")
		fmt.Fprintln(os.Stderr, "example: KEEL_BOARD_TEST_DSN='postgres://user@127.0.0.1:5432/postgres?sslmode=disable' go run ./scripts/e2e")
		return 2
	}
	root, err := findRepoRoot()
	if err != nil {
		fmt.Fprintln(os.Stderr, "keel-board e2e:", err)
		return 2
	}
	outDir := *reportDirFlag
	if outDir == "" {
		outDir = os.Getenv("KEEL_E2E_REPORT_DIR")
	}
	if outDir == "" {
		outDir = filepath.Join(root, "scripts", "e2e", "out", time.Now().UTC().Format("20060102T150405Z"))
	}
	if !filepath.IsAbs(outDir) {
		outDir = filepath.Join(root, outDir)
	}
	if err := os.MkdirAll(filepath.Join(outDir, "logs"), 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "keel-board e2e: create report dir:", err)
		return 2
	}

	report := &Report{
		Title:   "keel-board Phase 0 E2E（task 9.2）",
		Commit:  gitOutput(root, "rev-parse", "--short", "HEAD"),
		Branch:  gitOutput(root, "rev-parse", "--abbrev-ref", "HEAD"),
		Started: time.Now(), GoVersion: runtime.Version(),
		Platform: runtime.GOOS + "/" + runtime.GOARCH,
		DSN:      redactDSN(dsn), Artifacts: map[string]string{},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	base := fmt.Sprintf("keel_e2e_%d", time.Now().UnixNano())
	harnessSchema, serveSchema := base+"_h", base+"_s"
	cleanupHarness, err := createSchema(ctx, dsn, harnessSchema)
	if err != nil {
		report.Steps = append(report.Steps, &Step{
			ID: "preflight.1", Group: "preflight", Name: "创建隔离 schema",
			Action: "CREATE SCHEMA " + harnessSchema, Expect: "成功", Actual: err.Error(),
			Status: StatusFail, Started: time.Now(), Assertions: []Assertion{{Name: "schema", Pass: false, Expected: "成功", Actual: err.Error()}},
		})
		report.Finalize()
		writeReport(report, outDir)
		fmt.Fprintln(os.Stderr, "keel-board e2e:", err)
		return 1
	}
	cleanupServe, err := createSchema(ctx, dsn, serveSchema)
	if err != nil {
		cleanupHarness()
		report.Finalize()
		writeReport(report, outDir)
		fmt.Fprintln(os.Stderr, "keel-board e2e:", err)
		return 1
	}
	if *keepSchemas {
		fmt.Printf("keeping schemas: %s, %s\n", harnessSchema, serveSchema)
	} else {
		defer cleanupHarness()
		defer cleanupServe()
	}

	fake, err := newFakePlatform()
	if err != nil {
		report.Steps = append(report.Steps, &Step{
			ID: "preflight.2", Group: "preflight", Name: "启动本地 TLS fake 平台",
			Action: "生成 CA/证书并监听 127.0.0.1:0", Expect: "成功", Actual: err.Error(),
			Status: StatusFail, Started: time.Now(),
		})
		report.Finalize()
		writeReport(report, outDir)
		fmt.Fprintln(os.Stderr, "keel-board e2e:", err)
		return 1
	}
	defer fake.Close()

	env := &e2eEnv{
		ctx: ctx, report: report, fake: fake, root: root, outDir: outDir,
		dsn: dsn, harnessSchema: harnessSchema, serveSchema: serveSchema,
		injectFailure: *injectFailure,
	}
	env.run()

	fake.Calls()
	if data, err := json.MarshalIndent(sortedCalls(fake.History()), "", "  "); err == nil {
		path := filepath.Join(outDir, "fake-platform.json")
		_ = os.WriteFile(path, data, 0o644)
		report.Artifacts["fake-platform.json"] = "fake-platform.json"
	}
	if env.harness != nil {
		logPath := filepath.Join(outDir, "logs", "harness-server.log")
		_ = os.WriteFile(logPath, []byte(env.harness.Logs()), 0o644)
		report.Artifacts["harness-server.log"] = "logs/harness-server.log"
	}
	if env.serveLogs != "" {
		logPath := filepath.Join(outDir, "logs", "board-serve.log")
		_ = os.WriteFile(logPath, []byte(env.serveLogs), 0o644)
		report.Artifacts["board-serve.log"] = "logs/board-serve.log"
	}

	report.Finalize()
	htmlPath, err := report.Write(outDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "keel-board e2e: write report:", err)
		return 1
	}
	passed := report.AssertionPassedTotal()
	total := report.AssertionTotal()
	fmt.Printf("E2E %s: %d steps (%d pass, %d fail, %d skip), assertions %d/%d\n",
		report.Result, len(report.Steps), report.PassCount(), report.FailCount(), report.SkipCount(), passed, total)
	fmt.Printf("report: %s\n", htmlPath)
	if report.Result == StatusFail {
		for _, step := range report.Steps {
			if step.Status == StatusFail {
				fmt.Printf("  FAIL %s %s: %s\n", step.ID, step.Name, step.Actual)
			}
		}
		return 1
	}
	return 0
}

// e2eEnv carries one run's context and artifacts.
type e2eEnv struct {
	ctx           context.Context
	report        *Report
	fake          *fakePlatform
	root          string
	outDir        string
	dsn           string
	harnessSchema string
	serveSchema   string
	harness       *harness
	serveLogs     string
	injectFailure bool
}

// run executes every scenario in dependency order.
func (e *e2eEnv) run() {
	e.scenarioRealServe()
	e.scenarioHarnessBoot()
	if e.harness != nil {
		e.scenarioAutomation()
		e.scenarioChatSDK()
		e.scenarioSlackMirror()
		e.scenarioSlackChanges()
		e.scenarioSlackAttach()
		e.scenarioSlackFailures()
		e.scenarioSlackRestart()
		e.scenarioGitHub()
		e.scenarioLinear()
		e.scenarioAdapterTyping()
		e.harness.Stop()
	} else {
		for _, group := range []string{"automation", "chatsdk", "slack-mirror", "slack-changes", "slack-attach", "slack-failures", "slack-restart", "github", "linear", "adapter-typing"} {
			e.report.Scenario(group).Skip("场景未执行", group, "harness 服务器未启动（前置步骤失败），按 SKIP 记录而非假通过")
		}
	}
	if e.injectFailure {
		sc := e.report.Scenario("self-test")
		_ = sc.Step("失败注入（--inject-failure）", "故意失败的断言", "FAIL 并以非零退出；报告保留 Actual 与中止 SKIP", func(step *Step) error {
			step.Check("故意失败", false, "PASS", "FAIL（注入）")
			return nil
		})
	}
	e.scenarioSkips()
	e.addCoverage()
}

// addCoverage fills the matrix from the scenario groups and the explicit
// non-coverage decisions.
func (e *e2eEnv) addCoverage() {
	r := e.report
	r.AddCoverage("真实 board serve + PostgreSQL + 进程重启", r.GroupStatus("real-serve"),
		"真实二进制；store=postgres；API 写入/读回/编辑/反应；SIGTERM 后重启读回；sync 条目持久化", r.StepsIn("real-serve"))
	r.AddCoverage("Slack webhook → inbox → board（root/reply/dedupe/echo/签名）", r.GroupStatus("slack-mirror"),
		"真实 Slack adapter 验签与归一化；inbox dedupe；bot 回声丢弃；未签名 401", r.StepsIn("slack-mirror"))
	r.AddCoverage("Slack board → outbound → fake 平台", r.GroupStatus("slack-mirror"),
		"chat.postMessage 线程内投递；CameFrom 阻止回声", r.StepsIn("slack-mirror"))
	r.AddCoverage("双向 edit / delete / reaction", r.GroupStatus("slack-changes"),
		"Slack message_changed/deleted/reaction 入站；board edit/reaction/delete 出站 chat.update/reactions/chat.delete", r.StepsIn("slack-changes"))
	r.AddCoverage("文件事件（入站元数据 + 安全过滤）", r.GroupStatus("slack-mirror"),
		"files.slack.com 元数据保留；非白名单 URL 丢弃；不抓取字节（产品 open item）", r.StepsIn("slack-mirror"))
	r.AddCoverage("attach channel target + backfill", r.GroupStatus("slack-attach"),
		"root 先发，再从 outbox 依序回填 live replies；已删除回复跳过", r.StepsIn("slack-attach"))
	r.AddCoverage("thread target future-only", r.GroupStatus("slack-attach"),
		"attach 不回发旧消息；attach 之后的消息只发一次；入站落回同一 board 线程", r.StepsIn("slack-attach"))
	r.AddCoverage("失败 / 重试 / dead letter / 手动 retry", r.GroupStatus("slack-failures"),
		"出站 500 瞬态重试；永久失败 attempts≥3 → failures + undelivered → retry；入站 copy 未就绪重试；入站 dead + retry", r.StepsIn("slack-failures"))
	r.AddCoverage("重启恢复（outbox + cursor）", r.GroupStatus("slack-restart"),
		"阻塞投递 + 停服期间独立 log 写入；重启后两件事各成功一次；outbox 排空", r.StepsIn("slack-restart"))
	r.AddCoverage("GitHub REST 双向", r.GroupStatus("github"),
		"issue_comment 入站/create/edit/delete；board comment/edit/reaction/delete 出站；marker；重复 delivery；签名 401", r.StepsIn("github"))
	r.AddCoverage("Linear GraphQL 双向", r.GroupStatus("linear"),
		"Comment create/update/remove 入站；commentCreate/commentUpdate/reactionCreate/commentDelete 出站；签名/时间窗 401", r.StepsIn("linear"))
	r.AddCoverage("typing（adapter 级）", r.GroupStatus("adapter-typing"),
		"真实 Slack adapter → fake assistant.threads.setStatus；board 无 wiring", r.StepsIn("adapter-typing"))
	r.AddCoverage("watchers / schedules（automation）", r.GroupStatus("automation"),
		"POST /watchers 后触发 board 事件 → bot post；一次性 schedule 到点写入 board；视图不回显 schedule text", r.StepsIn("automation"))
	r.AddCoverage("Chat SDK bridge（sidecar fixture）", r.GroupStatus("chatsdk"),
		"sidecar /deliver message/edit/reaction/delete；PushInbound 签名事件入站/编辑；HMAC/重放 nonce/时间窗；self 身份丢弃", r.StepsIn("chatsdk"))
	r.AddCoverage("source read / join（本期范围外）", StatusSkip, "本 harness 未覆盖；非本期 Phase 0 E2E 范围，适配器单测覆盖", "skips")
	r.AddCoverage("文件字节（入站抓取 / 出站上传，本期范围外）", StatusSkip, "本 harness 未覆盖文件字节；Host 不抓取字节、OutboundMessage.Files 无来源，属产品 open item", "skips")
}

// scenarioRealServe exercises the actual board serve binary over PostgreSQL.
func (e *e2eEnv) scenarioRealServe() {
	sc := e.report.Scenario("real-serve")
	binDir, tempErr := os.MkdirTemp("", "keel-e2e-bin-")
	if tempErr != nil {
		_ = sc.Step("准备二进制临时目录", "os.MkdirTemp", "成功", func(step *Step) error { return tempErr })
		return
	}
	defer os.RemoveAll(binDir)
	boardBin, err := buildBoardBinary(e.ctx, binDir)
	if err := sc.Step("构建真实 board 二进制", "go build ./cmd/board", "编译成功", func(step *Step) error {
		if err != nil {
			return err
		}
		info, statErr := os.Stat(boardBin)
		step.Check("二进制存在", statErr == nil, "文件存在", fmt.Sprint(statErr))
		if statErr == nil {
			step.Check("二进制大小 > 0", info.Size() > 0, ">0", fmt.Sprint(info.Size()))
			step.Check("二进制可执行", info.Mode()&0o111 != 0, "可执行位", info.Mode().String())
		}
		artifact, evidenceErr := writeBoardBuildEvidence(e.ctx, boardBin, filepath.Join(e.outDir, "logs"), e.report.Commit)
		if evidenceErr != nil {
			return evidenceErr
		}
		e.report.Artifacts["board-build.txt"] = artifact
		step.Check("构建证据已生成", artifact != "", "非空", artifact)
		step.Note("binary: bin/board；证据: %s", artifact)
		return nil
	}); err != nil {
		return
	}

	board := newRealBoard(boardBin, e.dsn, e.serveSchema)
	if err := sc.Step("启动真实 board serve（PostgreSQL + sync 键）", "board serve --dsn <schema> --sync-key slack=slack --sync-mirror slack=C_SUPPORT:support", "listening，store=postgres，/v1/board 200", func(step *Step) error {
		if err := board.Start(e.ctx); err != nil {
			return err
		}
		status, body, requestErr := board.Request("", "GET", "", nil)
		if requestErr != nil {
			return requestErr
		}
		step.Trace("discovery", "GET", "/v1/board", status, "", string(body))
		step.CheckEQ("discovery 状态", 200, status)
		step.CheckContains("postgres store", "store=postgres", board.Logs())
		step.CheckBool("board 已监听", strings.Contains(board.Logs(), "board listening on http"), true)
		return nil
	}); err != nil {
		e.serveLogs = board.Logs()
		return
	}

	var realRoot uint64
	if err := sc.Step("真实 serve 的 board API 写入与读回", "POST /channels, POST /messages, POST /messages/{id}/edit, /reactions, GET /messages/{id}", "201/200，写入可读回", func(step *Step) error {
		status, body, err := board.Request("", "POST", "/channels", map[string]any{"path": "general"})
		step.Trace("create channel", "POST", "/v1/board/channels", status, `{"path":"general"}`, string(body))
		if err != nil {
			return err
		}
		step.CheckEQ("创建频道状态", 201, status)
		status, body, err = board.Request("", "POST", "/messages", map[string]any{"channel": "general", "text": "real-serve root"})
		step.Trace("post root", "POST", "/v1/board/messages", status, `{"channel":"general"}`, string(body))
		if err != nil {
			return err
		}
		step.CheckEQ("发根消息状态", 201, status)
		var posted struct {
			Message struct {
				ID string `json:"id"`
			} `json:"message"`
		}
		if err := json.Unmarshal(body, &posted); err != nil {
			return err
		}
		var ref uint64
		_, _ = fmt.Sscan(strings.TrimPrefix(posted.Message.ID, "#"), &ref)
		realRoot = ref
		step.Check("根消息 ID 非零", ref > 0, ">0", fmt.Sprint(ref))
		status, body, err = board.Request("", "POST", "/messages", map[string]any{"re": posted.Message.ID, "text": "real-serve reply"})
		step.Trace("post reply", "POST", "/v1/board/messages", status, posted.Message.ID, string(body))
		if err != nil {
			return err
		}
		step.CheckEQ("发回复状态", 201, status)
		status, body, err = board.Request("", "POST", fmt.Sprintf("/messages/%d/edit", ref), map[string]any{"text": "real-serve root edited"})
		step.Trace("edit root", "POST", fmt.Sprintf("/v1/board/messages/%d/edit", ref), status, "", string(body))
		if err != nil {
			return err
		}
		step.CheckEQ("编辑状态", 200, status)
		status, body, err = board.Request("", "POST", fmt.Sprintf("/messages/%d/reactions", ref), map[string]any{"emoji": "thumbs_up"})
		step.Trace("react root", "POST", fmt.Sprintf("/v1/board/messages/%d/reactions", ref), status, "", string(body))
		if err != nil {
			return err
		}
		step.CheckEQ("反应状态", 200, status)
		status, body, err = board.Request("", "GET", fmt.Sprintf("/messages/%d", ref), nil)
		step.Trace("read thread", "GET", fmt.Sprintf("/v1/board/messages/%d", ref), status, "", string(body))
		if err != nil {
			return err
		}
		step.CheckEQ("线程读取状态", 200, status)
		var thread threadResponse
		if err := json.Unmarshal(body, &thread); err != nil {
			return err
		}
		step.CheckContains("根文本已编辑", "real-serve root edited", thread.Message.Text)
		step.CheckEQ("回复数量", 1, len(thread.Replies))
		step.Check("反应已记录", reactionCount(thread.Message.Reactions, "thumbs_up") == 1, "1 个 thumbs_up", fmt.Sprint(thread.Message.Reactions))
		step.Actual = fmt.Sprintf("root=%d，回复=%d，reactions=%v", ref, len(thread.Replies), thread.Message.Reactions)
		return nil
	}); err != nil {
		e.serveLogs = board.Logs()
		_ = board.Stop()
		return
	}

	if err := sc.Step("真实 serve 的 sync 启动面", "GET /sync/entries、GET /sync/status、POST /webhooks/slack（未签名）", "entries 含 slack 且 owned；未签名 401；未知 key 静默 200", func(step *Step) error {
		status, body, err := board.Request("", "GET", "/sync/entries", nil)
		step.Trace("sync entries", "GET", "/v1/board/sync/entries", status, "", string(body))
		if err != nil {
			return err
		}
		step.CheckEQ("entries 状态", 200, status)
		var entries struct {
			Entries []struct {
				Key     string `json:"key"`
				Enabled bool   `json:"enabled"`
				Owned   bool   `json:"owned"`
			} `json:"entries"`
		}
		if err := json.Unmarshal(body, &entries); err != nil {
			return err
		}
		found := false
		for _, entry := range entries.Entries {
			if entry.Key == "slack" {
				found = entry.Enabled && entry.Owned
			}
		}
		step.CheckBool("slack entry enabled+owned", true, found)
		status, body, err = board.Request("", "GET", "/sync/status", nil)
		step.Trace("sync status", "GET", "/v1/board/sync/status", status, "", string(body))
		if err != nil {
			return err
		}
		step.CheckEQ("status 状态", 200, status)
		var statusBody syncStatusResponse
		if err := json.Unmarshal(body, &statusBody); err != nil {
			return err
		}
		step.Check("status 列出连接", len(statusBody.Keys) >= 1, ">=1", fmt.Sprint(len(statusBody.Keys)))
		// A real adapter verifies the signature: no header means 401, without
		// any network call to Slack.
		request, reqErr := newHTTPRequest(http.MethodPost, board.BaseURL()+"/webhooks/slack", []byte(`{}`))
		if reqErr != nil {
			return reqErr
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := httpClientFor().Do(request)
		if err != nil {
			return err
		}
		unsignedBody, _ := readAllLimited(response.Body, 4096)
		_ = response.Body.Close()
		step.Trace("unsigned webhook", "POST", "/webhooks/slack", response.StatusCode, "{}", unsignedBody)
		step.CheckEQ("未签名 webhook 拒绝", 401, response.StatusCode)
		unknown, unknownBody, unknownErr := postRaw(board.BaseURL()+"/webhooks/unknown", []byte(`{}`), map[string]string{"Content-Type": "application/json"})
		if unknownErr != nil {
			return unknownErr
		}
		step.Trace("unknown webhook key", "POST", "/webhooks/unknown", unknown, "{}", string(unknownBody))
		step.CheckEQ("未知 key 静默 200", 200, unknown)
		return nil
	}); err != nil {
		e.serveLogs = board.Logs()
		_ = board.Stop()
		return
	}

	if err := sc.Step("真实 serve 进程重启恢复", "SIGTERM → 重启同一 DSN/schema → 读回线程与 sync entry", "干净退出并读回 edited 文本与 1 条回复", func(step *Step) error {
		if err := board.Stop(); err != nil {
			return err
		}
		step.Note("第一次进程退出码 0")
		if err := board.Start(e.ctx); err != nil {
			return err
		}
		status, body, err := board.Request("", "GET", fmt.Sprintf("/messages/%d", realRoot), nil)
		step.Trace("read after restart", "GET", fmt.Sprintf("/v1/board/messages/%d", realRoot), status, "", string(body))
		if err != nil {
			return err
		}
		step.CheckEQ("重启后读取状态", 200, status)
		var thread threadResponse
		if err := json.Unmarshal(body, &thread); err != nil {
			return err
		}
		step.CheckContains("编辑文本持久化", "real-serve root edited", thread.Message.Text)
		step.CheckEQ("回复持久化", 1, len(thread.Replies))
		status, body, err = board.Request("", "GET", "/sync/entries", nil)
		step.Trace("entries after restart", "GET", "/v1/board/sync/entries", status, "", string(body))
		if err != nil {
			return err
		}
		step.CheckEQ("restart entries 状态", 200, status)
		step.CheckContains("sync entry 持久化", `"key":"slack"`, string(body))
		return nil
	}); err != nil {
		e.serveLogs = board.Logs()
		_ = board.Stop()
		return
	}
	e.serveLogs = board.Logs()
	if err := board.Stop(); err != nil {
		_ = sc.Step("关闭真实 serve", "SIGTERM", "退出码 0", func(step *Step) error { return err })
	}
}

// scenarioHarnessBoot starts the harness-hosted server and checks its wiring.
func (e *e2eEnv) scenarioHarnessBoot() {
	sc := e.report.Scenario("harness")
	e.harness = newHarness(e.fake, e.dsn, e.harnessSchema)
	if err := sc.Step("启动 harness 服务器（真实 server.New + syncstore + PG）", "构建 Slack/GitHub/Linear entry，PrepareSync，启动 inbound/outbound/subscription", "GET /v1/board 200，三个 key owned", func(step *Step) error {
		if err := e.harness.Start(e.ctx); err != nil {
			return err
		}
		status, body, err := e.harness.API("GET", "", nil)
		step.Trace("discovery", "GET", "/v1/board", status, "", string(body))
		if err != nil {
			return err
		}
		step.CheckEQ("discovery 状态", 200, status)
		status, body, err = e.harness.API("GET", "/sync/status", nil)
		step.Trace("sync status", "GET", "/v1/board/sync/status", status, "", string(body))
		if err != nil {
			return err
		}
		var statusBody syncStatusResponse
		if err := json.Unmarshal(body, &statusBody); err != nil {
			return err
		}
		owned := map[string]bool{}
		for _, key := range statusBody.Keys {
			owned[key.Key] = key.Owned
		}
		step.CheckBool("slack owned", true, owned["slack"])
		step.CheckBool("github owned", true, owned["github"])
		step.CheckBool("linear owned", true, owned["linear"])
		step.CheckBool("chatty bridge owned", true, owned[string(chattyKey)])
		step.CheckEQ("outbox 深度为空", 0, statusBody.Outbox.Pending+statusBody.Outbox.Leased)
		step.Actual = fmt.Sprintf("keys=%d，outbox pending=%d", len(statusBody.Keys), statusBody.Outbox.Pending)
		return nil
	}); err != nil {
		e.harness.Stop()
		e.harness = nil
	}
}

// scenarioSkips records the explicitly missing product seams or changes.
func (e *e2eEnv) scenarioSkips() {
	sc := e.report.Scenario("skips")
	sc.Skip("board 级 typing", "board 活动 → 平台 typing", "本 harness 未覆盖 board 级 typing（本期范围外）：Typing 只有 adapter 方法，server/worker 无调用缝。adapter 级 setStatus 已单独覆盖")
	sc.Skip("source read / join", "平台 source read/join", "本 harness 未覆盖（本期 Phase 0 E2E 范围外）：server 未挂载 sources/join 路由；适配器 source 单测覆盖")
	sc.Skip("文件字节（入站抓取）", "平台文件下载落盘", "本 harness 未覆盖（产品 open item）：SyncHost.Import 只保留附件元数据，不抓取字节")
	sc.Skip("文件字节（出站上传）", "board attachment → 平台文件上传", "本 harness 未覆盖（产品 open item）：OutboundMessage.Files 在 sync/call 中没有填充路径")
	sc.Skip("真实 cmd/board 承载镜像链路", "cmd/board 进程 + fake 平台双向镜像", "本 harness 用 harness-hosted server 跑镜像：cmd/board 没有平台 endpoint/transport 注入缝且约束不改产品代码；real-serve 组只验证真实二进制的 API/sync boot/重启")

	e.report.AddSkip("typing（board 级）", "本 harness 未覆盖/本期范围外；adapter 级已覆盖", "task 4.7")
	e.report.AddSkip("source read / join", "本 harness 未覆盖/本期范围外", "task 4.6/4.7")
	e.report.AddSkip("文件字节（双向）", "本 harness 未覆盖；Host 不抓取入站字节，OutboundMessage.Files 无来源", "SYNC.md open items")
	e.report.AddSkip("真实 cmd/board 承载镜像链路", "cmd/board 无平台 endpoint 注入；mirror 场景使用 harness-hosted server，装配差异见 README", "设计约束：不修改产品代码")
}

// Helpers shared by scenarios.

func writeReport(report *Report, outDir string) {
	report.Finalize()
	if _, err := report.Write(outDir); err != nil {
		fmt.Fprintln(os.Stderr, "write report:", err)
	}
}

func gitOutput(root string, args ...string) string {
	command := exec.Command("git", args...)
	command.Dir = root
	output, err := command.Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(output))
}
