# Keel-board E2E harness（task 9.2）

这是 Phase 0 的端到端验收 harness：用真实 PostgreSQL、真实 `board serve` 二进制、
真实 sync/automation 栈（`server.New` + `syncstore` + Slack/GitHub/Linear/ChatSDK
adapter + sync workers + watcher/schedule workers），对一个本地 TLS fake 平台和
Chat SDK sidecar fixture 运行双向镜像/自动化场景，并由脚本生成 HTML 报告。

## 运行

```sh
# 需要一个可建 schema 的 PostgreSQL；不会启动 Docker，也不会接触真实平台
export KEEL_BOARD_TEST_DSN='postgres://user@127.0.0.1:5432/postgres?sslmode=disable'

go run ./scripts/e2e
# 或指定报告目录
go run ./scripts/e2e -report-dir scripts/e2e/out/$(date +%Y%m%dT%H%M%SZ)

# 调试时保留本次 schema
go run ./scripts/e2e -keep-schemas
```

- 退出码（二进制）：0 = 全 PASS（允许显式 SKIP）；1 = 有 FAIL；2 = 前置条件/用法错误。
  注意 `go run` 会把程序的非零退出统一包装成自身的 `exit status`（`go run` 自身返回 1，
  即使程序返回 2）；要精确判断 2 请先 `go build -o /tmp/keel-e2e ./scripts/e2e` 再运行。
- 每次运行创建两个隔离 schema（`keel_e2e_<ns>_h` / `_s`），结束 `DROP SCHEMA ... CASCADE`
  （`-keep-schemas` 时保留）。
- 临时端口全部由 `127.0.0.1:0` 分配；报告写入 `scripts/e2e/out/`（已 gitignore）。
- CI 已把 `go run ./scripts/e2e` 作为门禁步骤，并 `always()` 上传报告 artifact；
  CI 已有 `KEEL_BOARD_TEST_DSN`，不需要额外服务/Docker。

## 架构

```text
cmd/board (真实二进制) ── board API / restart / sync boot ── PostgreSQL schema A
        ▲
        │ 不能注入平台 endpoint（不改产品代码）
        │
harness 进程
  ├─ harness-hosted server: 真实 server.New + syncstore + adapters + sync/automation workers + PG schema B
  ├─ Chat SDK sidecar fixture（reference sidecar，HMAC v1）
  ├─ fake TLS "internet": slack.com / files.slack.com / api.github.com / api.linear.app
  │     通过注入的 http.Client DialContext 指向 127.0.0.1，URL/hostname 校验保持真实
  └─ 场景 + 断言 + HTML/JSON 报告
```

两套 server 的原因：任务要求真实 `board serve`，同时禁止修改产品代码；`cmd/board`
没有平台 endpoint/transport 注入缝。因此：

- `real-serve` 场景直接跑 `go build ./cmd/board` + `board serve`，验证 PostgreSQL、
  board API、sync 启用/条目持久化、未签名 webhook 401、SIGTERM 重启恢复；它不经过
  fake 平台，因此不承载镜像链路；
- 镜像/automation/bridge 场景跑 harness-hosted server，复制 `cmd/board/serve` 的
  sync + automation worker 装配，只把 adapter 的 HTTP client 指向 fake/ sidecar。
  这不是“逐行相同的 cmd/board 装配”：tenant、retry/poll/lease、HTTP 进程形态有明确
  差异，见下表；`real-serve` 与 harness 使用不同 schema，互不影响。

fake 平台使用自签 CA/证书，只被 harness 注入的 client 信任；adapter 看到的 URL 仍是
`https://slack.com/api/...` 等，因此 Slack 文件域、GitHub base URL、Linear GraphQL
等生产校验没有被绕过。

## 覆盖

| 能力 | 覆盖 |
| --- | --- |
| 真实 `board serve` + PostgreSQL + 进程重启 | 构建、启动、API 写读/编辑/反应、SIGTERM→重启读回、sync entries 持久化 |
| Slack webhook → inbox → board | 签名验证、root/reply、dedupe、bot 回声抑制、未签名 401 |
| board → outbound → fake Slack | `chat.postMessage` 线程内投递、CameFrom 跳过来源 |
| 双向 edit / delete / reaction | Slack 入站三类事件；board 出站 `chat.update`/`reactions.*`/`chat.delete` |
| 文件 | 入站元数据 + 安全过滤（非白名单 URL 在 adapter 层被剥离）；不抓取字节 |
| attach channel target + backfill | root 先发、live replies 依序回填、已删除回复跳过 |
| thread target future-only | attach 不回发旧消息；之后消息只发一次；入站回到同一线程 |
| 失败/重试/dead letter | 出站 500 瞬态重试、永久失败 dead + `undelivered` + 手动 retry；入站 copy 未就绪重试；入站 dead + retry 端点 |
| 重启恢复 | 阻塞投递中停服、独立 log handle 在停服期间写入、重启后两件事各成功一次 |
| GitHub | `issue_comment` create/edit/delete 入站；comment/edit/reaction/delete 出站；marker；重复 delivery；签名 401 |
| Linear | Comment create/update/remove 入站；commentCreate/commentUpdate/reactionCreate/commentDelete 出站；签名/时间窗 401 |
| automation watcher/schedule | 创建 watcher、匹配标签事件→bot post、delivery done；一次性 schedule 到点写入、status done/sent=1、视图不回显 text |
| Chat SDK bridge | sidecar `/deliver` message/edit/reaction/delete；`PushInbound` root/edit；HMAC 坏签名/重放 nonce/过期 timestamp；self author 丢弃 |
| typing | adapter 级 `assistant.threads.setStatus`；board 级未接线（本期范围外） |

显式 SKIP 都是“本 harness 未覆盖/本期范围外”，不是“产品不存在”：
board 级 typing（只有 adapter 方法）、source read/join（本期范围外）、文件字节入站
抓取/出站上传（产品 open item），以及“真实 cmd/board 承载镜像链路”（无平台
endpoint 注入缝，改用 harness-hosted server；`real-serve` 只验证二进制 API/同步启动/
重启）。报告「未覆盖 / 跳过项」逐条列出原因。

## 装配与策略差异

| 项 | harness-hosted | `cmd/board serve` |
| --- | --- | --- |
| tenant | `e2e` | `default`（本 harness 未传 tenant flags） |
| sync 重试 | inbound 5 / outbound 3，30ms→120ms，lease 2s | inbound 10 / outbound 5，1s→30s+jitter，lease 1m |
| automation 重试 | 3 次，20ms→120ms，lease 5s | 5 次，1s→30s+jitter，lease 5m |
| poll / action timeout | 10ms / 2s | 1s / 2m |
| HTTP | 进程内临端口 | 独立子进程 |

缩短重试/轮询是为了让失败、重试、dead-letter、重启场景在 CI 内可测；生产默认值由
`cmd/board` 的默认配置保持。

## 报告

`out/<时间戳>/report.html` 由 `report.go` 的模板渲染（不手写 HTML），包含：

- 总览：结果、分支/commit、耗时、步骤/断言计数；
- 覆盖矩阵（PASS/FAIL/SKIP + 步骤 ID）；
- 步骤表：name/action/expect/actual/状态/耗时；
- 每步可展开：断言表（期望/实际）、关键日志、HTTP trace / JSON 视图；
- 未覆盖/跳过项；产物清单（`report.json`、`logs/board-serve.log`、
  `logs/harness-server.log`、`logs/board-build.txt`、`fake-platform.json`）。
- `logs/board-build.txt` 记录真实二进制的 size/mode/sha256/commit/`board version`，
  作为构建证据，避免在报告目录里带 90MB 二进制。

board 本期没有产品 UI，因此不使用产品截图、也不伪造截图；证据是真实 API trace、
fake 平台/sidecar 收到的请求、JSON 视图与进程日志。报告中的链接是 `local-*`
（本机静态文件 / 127.0.0.1），**不可从公网访问**；需要公网曝光时请整体发布
`report.html` 与同目录产物（不含二进制）。

## 增加场景

1. 在 `scenarios_*.go` 新建 `func (e *e2eEnv) scenarioX()`；
2. `sc := e.report.Scenario("group")`，每个 `sc.Step(name, action, expect, fn)` 内用
   `step.Check*` 写断言，用 `step.Trace` 记录 HTTP 证据；
3. 在 `main.go` 的 `e.run()` 里调用，并在 `addCoverage()` 加覆盖行；
4. 失败要真实返回非零：`Step` 返回 error 或任一断言失败即 FAIL，后续步骤自动 SKIP。
