# Keel-board E2E harness（task 9.2）

这是 Phase 0 的端到端验收 harness：用真实 PostgreSQL、真实 `board serve` 二进制、
真实 sync 栈（`server.New` + `syncstore` + Slack/GitHub/Linear adapter + workers），
对一个本地 TLS fake 平台运行双向镜像场景，并由脚本生成 HTML 报告。

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

- 退出码：0 = 全 PASS（允许显式 SKIP）；1 = 有 FAIL；2 = 前置条件/用法错误。
- 每次运行创建两个隔离 schema（`keel_e2e_<ns>_h` / `_s`），结束 `DROP SCHEMA ... CASCADE`
  （`-keep-schemas` 时保留）。
- 临时端口全部由 `127.0.0.1:0` 分配；报告写入 `scripts/e2e/out/`（已 gitignore）。
- CI 命令示例：`go run ./scripts/e2e`（CI 已有 `KEEL_BOARD_TEST_DSN`；不需要额外服务）。

## 架构

```text
cmd/board (真实二进制) ── board API / restart / sync boot ── PostgreSQL schema A
        ▲
        │ 不能注入平台 endpoint（不改产品代码）
        │
harness 进程
  ├─ harness-hosted server: 真实 server.New + syncstore + adapters + workers + PG schema B
  ├─ fake TLS "internet": slack.com / files.slack.com / api.github.com / api.linear.app
  │     通过注入的 http.Client DialContext 指向 127.0.0.1，URL/hostname 校验保持真实
  └─ 场景 + 断言 + HTML/JSON 报告
```

两套 server 的原因：任务要求真实 `board serve`，同时禁止修改产品代码；`cmd/board`
没有平台 endpoint/transport 注入缝。因此：

- `real-serve` 场景直接跑 `go build ./cmd/board` + `board serve`，验证 PostgreSQL、
  board API、sync 启用/条目持久化、未签名 webhook 401、SIGTERM 重启恢复；
- 镜像全链路场景跑 harness-hosted server，使用与 `cmd/board/serve` 相同的产品包
  和 worker 装配，只把 adapter 的 HTTP client 指向 fake。报告里明确标注该差异。

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
| typing | adapter 级 `assistant.threads.setStatus`；board 级未接线（SKIP） |

显式 SKIP（报告「未覆盖 / 跳过项」有原因与门禁）：ChatSDK sidecar（`cmd/board`
无 chatsdk 平台入口，包级 bridge 由单测覆盖）、watchers（未合入 server/log）、
source read/join/bridge events（管理 API 未挂载）、文件字节入站抓取/出站上传
（`Host` 不抓取字节，`OutboundMessage.Files` 无来源）。

## 报告

`out/<时间戳>/report.html` 由 `report.go` 的模板渲染（不手写 HTML），包含：

- 总览：结果、分支/commit、耗时、步骤/断言计数；
- 覆盖矩阵（PASS/FAIL/SKIP + 步骤 ID）；
- 步骤表：name/action/expect/actual/状态/耗时；
- 每步可展开：断言表（期望/实际）、关键日志、HTTP trace / JSON 视图；
- 未覆盖/跳过项；产物清单（`report.json`、`logs/board-serve.log`、
  `logs/harness-server.log`、`fake-platform.json`）。

board 本期没有产品 UI，因此不使用产品截图、也不伪造截图；证据是真实 API trace、
fake 平台收到的请求、JSON 视图与进程日志。报告中的链接是 `local-*`（本机静态文件/
127.0.0.1），不能从公网访问；需要公网曝光时整体发布 `report.html` 与同目录产物。

## 增加场景

1. 在 `scenarios_*.go` 新建 `func (e *e2eEnv) scenarioX()`；
2. `sc := e.report.Scenario("group")`，每个 `sc.Step(name, action, expect, fn)` 内用
   `step.Check*` 写断言，用 `step.Trace` 记录 HTTP 证据；
3. 在 `main.go` 的 `e.run()` 里调用，并在 `addCoverage()` 加覆盖行；
4. 失败要真实返回非零：`Step` 返回 error 或任一断言失败即 FAIL，后续步骤自动 SKIP。
